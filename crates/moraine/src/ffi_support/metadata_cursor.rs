//! Bounded, operation-owned pages of committed metadata.

use std::{any::Any, sync::Arc};

use super::{
    ColumnRecord, DataFileRecord, DeleteFileRecord, FileColumnStatsRecord, GcFileRecord,
    MacroRecord, MappingRecord, PartitionRecord, SchemaRecord, SortRecord, TableColumnStatsRecord,
    TableRecord, TableStatsRecord, ViewRecord, staged::TableKind,
};
use crate::{
    Error, MemoryReservation, ReadOnlyCatalog, Result,
    store::read::{self, EntityRecord, EntityRecordKind, OwnedEntityCursor, OwnedSnapshotCursor},
};

const PAGE_RECORDS: usize = 64;

/// One metadata page and its working-memory allowance.
pub struct MetadataPage<T> {
    /// Decoded records in store order.
    pub records: Vec<T>,
    _memory: Vec<Arc<MemoryReservation>>,
}

/// A metadata scan retaining one immutable read point, without a decoded cache.
pub struct MetadataCursor {
    table: TableKind,
    source: MetadataSource,
    pending: Option<Box<dyn Any + Send>>,
}

enum MetadataSource {
    Entities(OwnedEntityCursor),
    Snapshots(OwnedSnapshotCursor),
}

struct PendingRows<T> {
    records: std::vec::IntoIter<T>,
    memory: Arc<MemoryReservation>,
}

impl MetadataCursor {
    /// Opens the current and, when needed, ended records for one metadata kind.
    /// `live_bound` skips ended versions only when it reaches the pinned head.
    ///
    /// # Errors
    /// Returns an error for unsupported kinds or an unavailable read scope.
    pub async fn open(
        catalog: &ReadOnlyCatalog,
        table: TableKind,
        table_id: Option<u64>,
        live_bound: Option<u64>,
    ) -> Result<Self> {
        let scope = catalog.index_read_scope().await?.ok_or_else(|| {
            Error::Constraint("metadata cursor requires a fixed read scope".into())
        })?;
        let handle = scope.reads().owned_read_handle();
        if matches!(table, TableKind::Snapshot | TableKind::SnapshotChanges) {
            if table_id.is_some() {
                return Err(Error::Constraint(
                    "snapshots do not support a table scope".into(),
                ));
            }
            return Ok(Self {
                table,
                source: MetadataSource::Snapshots(OwnedSnapshotCursor::open(handle).await?),
                pending: None,
            });
        }
        let kind = match table {
            TableKind::Schema => EntityRecordKind::Schema,
            TableKind::Table => EntityRecordKind::Table,
            TableKind::View | TableKind::ViewColumnTag => EntityRecordKind::View,
            TableKind::Column | TableKind::ColumnTag => EntityRecordKind::Column,
            TableKind::DataFile | TableKind::FilePartitionValue => EntityRecordKind::File,
            TableKind::DeleteFile => EntityRecordKind::DeleteFile,
            TableKind::FileColumnStats | TableKind::FileVariantStats => {
                EntityRecordKind::FileColumnStats
            }
            TableKind::TableStats => EntityRecordKind::TableStats,
            TableKind::TableColumnStats => EntityRecordKind::TableColumnStats,
            TableKind::PartitionInfo | TableKind::PartitionColumn => EntityRecordKind::Partition,
            TableKind::SortInfo | TableKind::SortExpression => EntityRecordKind::Sort,
            TableKind::Macro | TableKind::MacroImpl | TableKind::MacroParameters => {
                EntityRecordKind::Macro
            }
            TableKind::ColumnMapping | TableKind::NameMapping => EntityRecordKind::Mapping,
            TableKind::FilesScheduledForDeletion => EntityRecordKind::GcFile,
            TableKind::Tag => EntityRecordKind::Tag,
            TableKind::Metadata => EntityRecordKind::Option,
            _ => {
                return Err(Error::Constraint(format!(
                    "{table:?} does not have an entity cursor"
                )));
            }
        };
        if matches!(table, TableKind::ColumnTag | TableKind::ViewColumnTag) {
            if table_id.is_some() {
                return Err(Error::Constraint(
                    "parent tags do not support a table scope".into(),
                ));
            }
            return Ok(Self {
                table,
                pending: None,
                source: MetadataSource::Entities(
                    OwnedEntityCursor::open_latest(handle, kind).await?,
                ),
            });
        }
        let head = crate::transaction::commit::read_head_value(handle.borrow()).await?;
        let versions = read::versions_for(live_bound, Some(head.snapshot_id));
        Ok(Self {
            table,
            pending: None,
            source: MetadataSource::Entities(
                OwnedEntityCursor::open(handle, kind, versions, table_id).await?,
            ),
        })
    }

    /// Reads a bounded page for snapshot headers or their change descriptions.
    ///
    /// # Errors
    /// Returns an error if this is not a snapshot cursor or a record is
    /// invalid.
    pub async fn snapshots(&mut self) -> Result<MetadataPage<super::SnapshotRecord>> {
        let MetadataSource::Snapshots(cursor) = &mut self.source else {
            return Err(Error::Constraint(
                "snapshot page requested from entity cursor".into(),
            ));
        };
        let mut records = Vec::new();
        let mut memory = Vec::new();
        for _ in 0..PAGE_RECORDS {
            let Some((record, allowance)) = cursor.next().await? else {
                break;
            };
            records.push(record);
            memory.push(Arc::new(allowance));
        }
        Ok(MetadataPage {
            records,
            _memory: memory,
        })
    }

    async fn page<T: Send + 'static>(
        &mut self,
        table: TableKind,
        project: impl Fn(EntityRecord) -> Result<Vec<T>>,
    ) -> Result<MetadataPage<T>> {
        if self.table != table {
            return Err(Error::Constraint(
                "metadata cursor called with the wrong row kind".into(),
            ));
        }
        let MetadataSource::Entities(cursor) = &mut self.source else {
            return Err(Error::Constraint(
                "entity page requested from snapshot cursor".into(),
            ));
        };
        let mut records = Vec::new();
        let mut memory = Vec::new();
        // Embedded children may produce no rows. Only EOF returns an empty page.
        while records.is_empty() {
            for _ in 0..PAGE_RECORDS {
                let mut pending = if let Some(pending) = self.pending.take() {
                    *pending.downcast::<PendingRows<T>>().map_err(|_| {
                        Error::Corruption("metadata cursor changed its output type".into())
                    })?
                } else {
                    let Some((record, allowance)) = cursor.next().await? else {
                        return Ok(MetadataPage {
                            records,
                            _memory: memory,
                        });
                    };
                    PendingRows {
                        records: project(record)?.into_iter(),
                        memory: Arc::new(allowance),
                    }
                };
                if pending.records.len() == 0 {
                    continue;
                }
                records.extend(pending.records.by_ref().take(PAGE_RECORDS - records.len()));
                memory.push(pending.memory.clone());
                if pending.records.len() != 0 {
                    self.pending = Some(Box::new(pending));
                }
                if records.len() == PAGE_RECORDS {
                    break;
                }
            }
        }
        Ok(MetadataPage {
            records,
            _memory: memory,
        })
    }
}

macro_rules! pages {
    ($($method:ident, $table:ident, $variant:ident, $record:ty;)+) => {
        impl MetadataCursor {
            $(
                #[doc = concat!("Reads the next bounded page of `", stringify!($table), "` records.")]
                ///
                /// # Errors
                /// Returns an error if the store read or record decoding fails.
                pub async fn $method(&mut self) -> Result<MetadataPage<$record>> {
                    self.page(TableKind::$table, |record| match record {
                        EntityRecord::$variant(value) => Ok(vec![(*value).clone()]),
                        _ => Err(Error::Corruption("metadata cursor returned an unexpected record".into())),
                    }).await
                }
            )+
        }
    };
}

pages! {
    schemas, Schema, Schema, SchemaRecord;
    tables, Table, Table, TableRecord;
    views, View, View, ViewRecord;
    columns, Column, Column, ColumnRecord;
    data_files, DataFile, File, DataFileRecord;
    delete_files, DeleteFile, DeleteFile, DeleteFileRecord;
    file_column_stats, FileColumnStats, FileColumnStats, FileColumnStatsRecord;
    table_stats, TableStats, TableStats, TableStatsRecord;
    table_column_stats, TableColumnStats, TableColumnStats, TableColumnStatsRecord;
    partitions, PartitionInfo, Partition, PartitionRecord;
    sorts, SortInfo, Sort, SortRecord;
    macros, Macro, Macro, MacroRecord;
    mappings, ColumnMapping, Mapping, MappingRecord;
    scheduled_deletions, FilesScheduledForDeletion, GcFile, GcFileRecord;
}

impl MetadataCursor {
    /// Reads tags from each column's authoritative parent only.
    ///
    /// # Errors
    /// Returns an error for an invalid record or unavailable read point.
    pub async fn column_tags(&mut self) -> Result<MetadataPage<super::ColumnTagRow>> {
        self.page(TableKind::ColumnTag, |record| match record {
            EntityRecord::Column(value) => {
                Ok(super::column_tag_rows_from([super::ColumnTags::from(
                    value.as_ref(),
                )]))
            }
            _ => Err(Error::Corruption(
                "column tag cursor returned an unexpected parent".into(),
            )),
        })
        .await
    }

    /// Reads column tags from each view's authoritative parent only.
    ///
    /// # Errors
    /// Returns an error for an invalid record or unavailable read point.
    pub async fn view_column_tags(&mut self) -> Result<MetadataPage<super::ViewColumnTagRow>> {
        self.page(TableKind::ViewColumnTag, |record| match record {
            EntityRecord::View(value) => Ok(super::view_column_tag_rows_from([
                super::ViewColumnTags::from(value.as_ref()),
            ])),
            _ => Err(Error::Corruption(
                "view column tag cursor returned an unexpected parent".into(),
            )),
        })
        .await
    }

    /// Reads a page of `Tag` rows from their parent records.
    ///
    /// # Errors
    /// Returns an error if the cursor kind or a stored record is invalid.
    pub async fn tags(&mut self) -> Result<MetadataPage<super::TagRow>> {
        self.page(TableKind::Tag, |record| match record {
            EntityRecord::Tag(value) => Ok(super::tag_rows_from(vec![(*value).clone()])),
            _ => Err(Error::Corruption(
                "metadata cursor returned an unexpected parent".into(),
            )),
        })
        .await
    }
    /// Reads a page of `PartitionColumn` rows from their parent records.
    ///
    /// # Errors
    /// Returns an error if the cursor kind or a stored record is invalid.
    pub async fn partition_columns(&mut self) -> Result<MetadataPage<super::PartitionColumnRow>> {
        self.page(TableKind::PartitionColumn, |record| match record {
            EntityRecord::Partition(value) => {
                Ok(super::partition_column_rows_from(vec![(*value).clone()]))
            }
            _ => Err(Error::Corruption(
                "metadata cursor returned an unexpected parent".into(),
            )),
        })
        .await
    }
    /// Reads a page of `SortExpression` rows from their parent records.
    ///
    /// # Errors
    /// Returns an error if the cursor kind or a stored record is invalid.
    pub async fn sort_expressions(&mut self) -> Result<MetadataPage<super::SortExpressionRow>> {
        self.page(TableKind::SortExpression, |record| match record {
            EntityRecord::Sort(value) => {
                Ok(super::sort_expression_rows_from(vec![(*value).clone()]))
            }
            _ => Err(Error::Corruption(
                "metadata cursor returned an unexpected parent".into(),
            )),
        })
        .await
    }
    /// Reads a page of `FilePartitionValue` rows from their parent records.
    ///
    /// # Errors
    /// Returns an error if the cursor kind or a stored record is invalid.
    pub async fn file_partition_values(
        &mut self,
    ) -> Result<MetadataPage<super::FilePartitionValueRow>> {
        self.page(TableKind::FilePartitionValue, |record| match record {
            EntityRecord::File(value) => Ok(super::file_partition_value_rows_from(vec![
                (*value).clone(),
            ])),
            _ => Err(Error::Corruption(
                "metadata cursor returned an unexpected parent".into(),
            )),
        })
        .await
    }
    /// Reads a page of `FileVariantStats` rows from their parent records.
    ///
    /// # Errors
    /// Returns an error if the cursor kind or a stored record is invalid.
    pub async fn file_variant_stats(&mut self) -> Result<MetadataPage<super::FileVariantStatsRow>> {
        self.page(TableKind::FileVariantStats, |record| match record {
            EntityRecord::FileColumnStats(value) => {
                Ok(super::file_variant_stats_rows_from(vec![(*value).clone()]))
            }
            _ => Err(Error::Corruption(
                "metadata cursor returned an unexpected parent".into(),
            )),
        })
        .await
    }
    /// Reads a page of `MacroImpl` rows from their parent records.
    ///
    /// # Errors
    /// Returns an error if the cursor kind or a stored record is invalid.
    pub async fn macro_impls(&mut self) -> Result<MetadataPage<super::MacroImplRow>> {
        self.page(TableKind::MacroImpl, |record| match record {
            EntityRecord::Macro(value) => Ok(super::macro_impl_rows_from(vec![(*value).clone()])),
            _ => Err(Error::Corruption(
                "metadata cursor returned an unexpected parent".into(),
            )),
        })
        .await
    }
    /// Reads a page of `MacroParameters` rows from their parent records.
    ///
    /// # Errors
    /// Returns an error if the cursor kind or a stored record is invalid.
    pub async fn macro_parameters(&mut self) -> Result<MetadataPage<super::MacroParameterRow>> {
        self.page(TableKind::MacroParameters, |record| match record {
            EntityRecord::Macro(value) => {
                Ok(super::macro_parameter_rows_from(vec![(*value).clone()]))
            }
            _ => Err(Error::Corruption(
                "metadata cursor returned an unexpected parent".into(),
            )),
        })
        .await
    }
    /// Reads a page of `NameMapping` rows from their parent records.
    ///
    /// # Errors
    /// Returns an error if the cursor kind or a stored record is invalid.
    pub async fn name_mappings(&mut self) -> Result<MetadataPage<super::NameMappingRow>> {
        self.page(TableKind::NameMapping, |record| match record {
            EntityRecord::Mapping(value) => {
                Ok(super::name_mapping_rows_from(vec![(*value).clone()]))
            }
            _ => Err(Error::Corruption(
                "metadata cursor returned an unexpected parent".into(),
            )),
        })
        .await
    }
    /// Reads a page of `Metadata` rows from their parent records.
    ///
    /// # Errors
    /// Returns an error if the cursor kind or a stored record is invalid.
    pub async fn options(&mut self) -> Result<MetadataPage<super::OptionRow>> {
        self.page(TableKind::Metadata, |record| match record {
            EntityRecord::Option {
                scope_kind,
                scope_id,
                value,
            } => Ok(super::option_rows_from(vec![(
                scope_kind,
                scope_id,
                (*value).clone(),
            )])),
            _ => Err(Error::Corruption(
                "metadata cursor returned an unexpected parent".into(),
            )),
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        Catalog, CatalogOptions,
        store::key::{EntityKey, Key},
    };

    #[tokio::test]
    async fn snapshot_pages_keep_the_original_read_point() {
        let catalog = Catalog::open(
            Arc::new(object_store::memory::InMemory::new()),
            CatalogOptions::default(),
        )
        .await
        .unwrap();
        let write = catalog.begin_write_tx().unwrap();
        for snapshot_id in 1..130 {
            write
                .put(
                    Key::Snapshot { snapshot_id }.encode(),
                    crate::store::value::encode_value(&super::super::SnapshotRecord {
                        snapshot_id,
                        ..Default::default()
                    }),
                )
                .unwrap();
        }
        catalog.commit_write_tx(write).await.unwrap();
        let mut cursor = MetadataCursor::open(&catalog, TableKind::Snapshot, None, None)
            .await
            .unwrap();
        assert_eq!(cursor.snapshots().await.unwrap().records.len(), 64);
        let write = catalog.begin_write_tx().unwrap();
        write
            .delete(Key::Snapshot { snapshot_id: 129 }.encode())
            .unwrap();
        write
            .put(Key::Snapshot { snapshot_id: 130 }.encode(), [0xff])
            .unwrap();
        catalog.commit_write_tx(write).await.unwrap();
        assert_eq!(cursor.snapshots().await.unwrap().records.len(), 64);
        let last = cursor.snapshots().await.unwrap();
        assert_eq!(
            last.records
                .iter()
                .map(|row| row.snapshot_id)
                .collect::<Vec<_>>(),
            [128, 129]
        );
        assert!(cursor.snapshots().await.unwrap().records.is_empty());
        catalog.close().await.unwrap();
    }

    async fn tagged_parent_catalog() -> Catalog {
        use crate::store::{
            proto::{ColumnTag, ColumnValue, ViewColumnTag, ViewValue},
            value::encode_value,
        };
        let catalog = Catalog::open(
            Arc::new(object_store::memory::InMemory::new()),
            CatalogOptions::default(),
        )
        .await
        .unwrap();
        let write = catalog.begin_write_tx().unwrap();
        for id in 1..4 {
            for end_snapshot in [Some(2), Some(3), None] {
                if id == 2 && end_snapshot.is_none() {
                    continue;
                }
                let column = ColumnValue {
                    table_id: 1,
                    column_id: id,
                    end_snapshot,
                    tags: (0..70)
                        .map(|tag| ColumnTag {
                            key: tag.to_string(),
                            value: format!("{end_snapshot:?}"),
                            begin_snapshot: 1,
                            end_snapshot: None,
                        })
                        .collect(),
                    ..Default::default()
                };
                let view = ViewValue {
                    view_id: id,
                    end_snapshot,
                    column_tags: (0..70)
                        .map(|tag| ViewColumnTag {
                            column_name: "c".into(),
                            key: tag.to_string(),
                            value: format!("{end_snapshot:?}"),
                            begin_snapshot: 1,
                            end_snapshot: None,
                        })
                        .collect(),
                    ..Default::default()
                };
                for (entity, value) in [
                    (
                        EntityKey::Column {
                            table_id: 1,
                            column_id: id,
                        },
                        encode_value(&column),
                    ),
                    (EntityKey::View { view_id: id }, encode_value(&view)),
                ] {
                    let key = end_snapshot
                        .map_or_else(|| Key::current(entity), |end| Key::history(entity, end));
                    write.put(key.encode(), value).unwrap();
                }
            }
        }
        catalog.commit_write_tx(write).await.unwrap();
        catalog
    }

    #[tokio::test]
    async fn tag_pages_use_only_the_authoritative_parent_version() {
        let catalog = tagged_parent_catalog().await;
        let mut columns = MetadataCursor::open(&catalog, TableKind::ColumnTag, None, None)
            .await
            .unwrap();
        let mut actual = Vec::new();
        loop {
            let page = columns.column_tags().await.unwrap();
            assert!(page.records.len() <= 64);
            if page.records.is_empty() {
                break;
            }
            actual.extend(page.records);
        }
        actual.sort_by_key(|row| (row.table_id, row.column_id, row.key.clone()));
        let mut expected = super::super::dump_column_tags(&catalog).await.unwrap();
        expected.sort_by_key(|row| (row.table_id, row.column_id, row.key.clone()));
        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 210);
        let mut views = MetadataCursor::open(&catalog, TableKind::ViewColumnTag, None, None)
            .await
            .unwrap();
        let mut actual = Vec::new();
        loop {
            let page = views.view_column_tags().await.unwrap();
            assert!(page.records.len() <= 64);
            if page.records.is_empty() {
                break;
            }
            actual.extend(page.records);
        }
        actual.sort_by_key(|row| (row.view_id, row.key.clone()));
        let mut expected = super::super::dump_view_column_tags(&catalog).await.unwrap();
        expected.sort_by_key(|row| (row.view_id, row.key.clone()));
        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 210);
        let write = catalog.begin_write_tx().unwrap();
        for id in 1..4 {
            for entity in [
                EntityKey::Column {
                    table_id: 1,
                    column_id: id,
                },
                EntityKey::View { view_id: id },
            ] {
                write.put(Key::history(entity, 2).encode(), [0xff]).unwrap();
            }
        }
        catalog.commit_write_tx(write).await.unwrap();
        let mut columns = MetadataCursor::open(&catalog, TableKind::ColumnTag, None, None)
            .await
            .unwrap();
        let mut count = 0;
        loop {
            let page = columns.column_tags().await.unwrap();
            if page.records.is_empty() {
                break;
            }
            count += page.records.len();
        }
        assert_eq!(count, 210);
        let mut views = MetadataCursor::open(&catalog, TableKind::ViewColumnTag, None, None)
            .await
            .unwrap();
        let mut count = 0;
        loop {
            let page = views.view_column_tags().await.unwrap();
            if page.records.is_empty() {
                break;
            }
            count += page.records.len();
        }
        assert_eq!(count, 210);
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn snapshot_pages_decode_only_requested_rows() {
        let catalog = Catalog::open(
            Arc::new(object_store::memory::InMemory::new()),
            CatalogOptions::default(),
        )
        .await
        .unwrap();
        let write = catalog.begin_write_tx().unwrap();
        for snapshot_id in 1..130 {
            write
                .put(
                    Key::Snapshot { snapshot_id }.encode(),
                    crate::store::value::encode_value(&super::super::SnapshotRecord {
                        snapshot_id,
                        ..Default::default()
                    }),
                )
                .unwrap();
        }
        write
            .put(Key::Snapshot { snapshot_id: 130 }.encode(), [0xff])
            .unwrap();
        catalog.commit_write_tx(write).await.unwrap();
        for kind in [TableKind::Snapshot, TableKind::SnapshotChanges] {
            let mut cursor = MetadataCursor::open(&catalog, kind, None, None)
                .await
                .unwrap();
            assert_eq!(cursor.snapshots().await.unwrap().records.len(), 64);
            assert_eq!(cursor.snapshots().await.unwrap().records.len(), 64);
            assert!(matches!(
                cursor.snapshots().await,
                Err(Error::Corruption(_))
            ));
        }
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn one_parent_can_span_multiple_output_pages() {
        let catalog = crate::Catalog::open(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            crate::CatalogOptions::default(),
        )
        .await
        .unwrap();
        let write = catalog.begin_write_tx().unwrap();
        write
            .put(
                Key::current(EntityKey::Tag { object_id: 1 }).encode(),
                crate::store::value::encode_value(&crate::store::proto::TagValue {
                    object_id: 1,
                    entries: (0..130)
                        .map(|id| crate::store::proto::TagEntry {
                            key: id.to_string(),
                            value: "tag".into(),
                            ..Default::default()
                        })
                        .collect(),
                }),
            )
            .unwrap();
        catalog.commit_write_tx(write).await.unwrap();
        let mut cursor = MetadataCursor::open(&catalog, TableKind::Tag, None, None)
            .await
            .unwrap();
        assert_eq!(cursor.tags().await.unwrap().records.len(), 64);
        let second = cursor.tags().await.unwrap();
        assert_eq!(second.records.len(), 64);
        assert_eq!(second.records[0].key, "64");
        assert_eq!(cursor.tags().await.unwrap().records.len(), 2);
        assert!(cursor.tags().await.unwrap().records.is_empty());
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn embedded_pages_skip_empty_parents_and_preserve_rows() {
        let catalog = crate::Catalog::open(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            crate::CatalogOptions::default(),
        )
        .await
        .unwrap();
        let transaction = catalog.begin_write_tx().unwrap();
        for data_file_id in 0..200 {
            let values = if data_file_id >= 190 {
                vec![crate::store::proto::FilePartitionValue {
                    partition_key_index: 0,
                    partition_value: data_file_id.to_string(),
                }]
            } else {
                Vec::new()
            };
            transaction
                .put(
                    Key::current(EntityKey::File {
                        table_id: 1,
                        data_file_id,
                    })
                    .encode(),
                    crate::store::value::encode_value(&DataFileRecord {
                        table_id: 1,
                        data_file_id,
                        partition_values: values,
                        ..Default::default()
                    }),
                )
                .unwrap();
        }
        catalog.commit_write_tx(transaction).await.unwrap();
        let mut cursor =
            MetadataCursor::open(&catalog, TableKind::FilePartitionValue, Some(1), None)
                .await
                .unwrap();
        let first = cursor.file_partition_values().await.unwrap();
        assert_eq!(first.records.len(), 2);
        #[allow(clippy::used_underscore_binding)]
        let retained_parents = first._memory.len();
        assert_eq!(retained_parents, 2);
        assert_eq!(first.records[0].partition_value, "190");
        let second = cursor.file_partition_values().await.unwrap();
        assert_eq!(second.records.len(), 8);
        assert!(
            cursor
                .file_partition_values()
                .await
                .unwrap()
                .records
                .is_empty()
        );
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn embedded_metadata_kinds_open_without_materializing_parents() {
        let catalog = crate::Catalog::open(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            crate::CatalogOptions::default(),
        )
        .await
        .unwrap();
        for kind in [
            TableKind::Tag,
            TableKind::Metadata,
            TableKind::PartitionColumn,
            TableKind::SortExpression,
            TableKind::FilePartitionValue,
            TableKind::FileVariantStats,
            TableKind::MacroImpl,
            TableKind::MacroParameters,
            TableKind::NameMapping,
        ] {
            MetadataCursor::open(&catalog, kind, None, None)
                .await
                .unwrap();
        }
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn pages_are_bounded_and_keep_their_original_read_point() {
        let catalog = Catalog::open(
            Arc::new(object_store::memory::InMemory::new()),
            CatalogOptions::default(),
        )
        .await
        .unwrap();
        let transaction = catalog.begin_write_tx().unwrap();
        for data_file_id in 0..130 {
            transaction
                .put(
                    Key::current(EntityKey::File {
                        table_id: 1,
                        data_file_id,
                    })
                    .encode(),
                    crate::store::value::encode_value(&DataFileRecord {
                        table_id: 1,
                        data_file_id,
                        ..Default::default()
                    }),
                )
                .unwrap();
        }
        catalog.commit_write_tx(transaction).await.unwrap();
        let mut cursor = MetadataCursor::open(&catalog, TableKind::DataFile, Some(1), None)
            .await
            .unwrap();
        assert_eq!(cursor.data_files().await.unwrap().records.len(), 64);
        let transaction = catalog.begin_write_tx().unwrap();
        transaction
            .delete(
                Key::current(EntityKey::File {
                    table_id: 1,
                    data_file_id: 100,
                })
                .encode(),
            )
            .unwrap();
        catalog.commit_write_tx(transaction).await.unwrap();
        assert_eq!(cursor.data_files().await.unwrap().records.len(), 64);
        assert_eq!(cursor.data_files().await.unwrap().records.len(), 2);
        assert!(cursor.data_files().await.unwrap().records.is_empty());
        catalog.close().await.unwrap();
    }
}
