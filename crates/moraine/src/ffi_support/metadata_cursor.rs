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

mod schema_versions;

const PAGE_RECORDS: usize = 64;
// Stop after the parent crossing this allowance; one parent is indivisible.
const PAGE_BYTES: u64 = 1024 * 1024;

/// One metadata page and its working-memory allowance.
pub struct MetadataPage<T> {
    /// Decoded records in deterministic cursor order.
    pub records: Vec<T>,
    memory: Vec<Arc<MemoryReservation>>,
}

impl<T> MetadataPage<T> {
    /// Working-memory reservations the page holds until it is dropped, one per
    /// decoded parent record.
    #[must_use]
    pub fn retained_memory(&self) -> &[Arc<MemoryReservation>] {
        &self.memory
    }
}

/// A metadata scan retaining one immutable read point, without a decoded cache.
pub struct MetadataCursor {
    table: TableKind,
    scope: ReadOnlyCatalog,
    table_id: Option<u64>,
    live_bound: Option<u64>,
    overlay: Option<Arc<crate::transaction::staged::paging::Overlay>>,
    inserted: usize,
    base_finished: bool,
    source: MetadataSource,
    pending: Option<Box<dyn Any + Send>>,
}

enum MetadataSource {
    Entities(OwnedEntityCursor),
    Snapshots(OwnedSnapshotCursor),
    SchemaVersions(Box<schema_versions::Cursor>),
}

impl MetadataSource {
    async fn open(
        handle: crate::store::handle::OwnedReadHandle,
        table: TableKind,
        table_id: Option<u64>,
        live_bound: Option<u64>,
    ) -> Result<Self> {
        if matches!(
            table,
            TableKind::Snapshot | TableKind::SnapshotChanges | TableKind::SchemaVersions
        ) {
            if table_id.is_some() {
                return Err(Error::Constraint(
                    "snapshot and schema-version cursors do not support a table scope".into(),
                ));
            }
            return if table == TableKind::SchemaVersions {
                Ok(Self::SchemaVersions(Box::new(
                    schema_versions::Cursor::open(handle).await?,
                )))
            } else {
                Ok(Self::Snapshots(OwnedSnapshotCursor::open(handle).await?))
            };
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
            return Ok(Self::Entities(
                OwnedEntityCursor::open_latest(handle, kind).await?,
            ));
        }
        let head = crate::transaction::commit::read_head_value(handle.borrow()).await?;
        let versions = read::versions_for(live_bound, Some(head.snapshot_id));
        Ok(Self::Entities(
            OwnedEntityCursor::open(handle, kind, versions, table_id).await?,
        ))
    }
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
        let scope = catalog.index_read_scope().await?.reads().clone();
        let source =
            MetadataSource::open(scope.owned_read_handle(), table, table_id, live_bound).await?;
        Ok(Self {
            table,
            scope,
            table_id,
            live_bound,
            overlay: None,
            inserted: 0,
            base_finished: false,
            source,
            pending: None,
        })
    }

    pub(crate) fn with_overlay(
        mut self,
        overlay: Arc<crate::transaction::staged::paging::Overlay>,
    ) -> Self {
        if let MetadataSource::SchemaVersions(cursor) = &mut self.source {
            cursor.overlay = Some(overlay.clone());
        }
        self.overlay = Some(overlay);
        self
    }

    /// Restarts this scan at its original read point and authored overlay.
    ///
    /// # Errors
    /// Returns an error if the retained read point can no longer be read.
    pub async fn fork(&self) -> Result<Self> {
        let mut cursor =
            Self::open(&self.scope, self.table, self.table_id, self.live_bound).await?;
        if let Some(overlay) = &self.overlay {
            cursor = cursor.with_overlay(overlay.clone());
        }
        Ok(cursor)
    }

    async fn next_entity(&mut self) -> Result<Option<(EntityRecord, MemoryReservation)>> {
        let MetadataSource::Entities(cursor) = &mut self.source else {
            return Err(Error::Constraint(
                "entity page requested from a different cursor".into(),
            ));
        };
        while !self.base_finished {
            let Some((record, memory)) = cursor.next().await? else {
                self.base_finished = true;
                break;
            };
            let record = match &self.overlay {
                Some(overlay) => overlay.apply(record)?,
                None => Some(record),
            };
            if let Some(record) = record {
                return Ok(Some((record, memory)));
            }
        }
        let Some(overlay) = &self.overlay else {
            return Ok(None);
        };
        let row = overlay
            .next_inserted(self.inserted, self.scope.owned_read_handle())
            .await?;
        self.inserted += usize::from(row.is_some());
        Ok(row)
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
        let mut bytes = 0_u64;
        while records.len() < PAGE_RECORDS {
            let Some((record, allowance)) = cursor.next().await? else {
                break;
            };
            if self
                .overlay
                .as_ref()
                .is_some_and(|overlay| !overlay.includes_snapshot(record.snapshot_id))
            {
                continue;
            }
            records.push(record);
            bytes = bytes.saturating_add(allowance.bytes());
            memory.push(Arc::new(allowance));
            if bytes >= PAGE_BYTES {
                break;
            }
        }
        Ok(MetadataPage { records, memory })
    }

    /// Reads stored and legacy schema versions followed by file floor rows.
    ///
    /// # Errors
    /// Returns an error for the wrong cursor kind or an invalid stored record.
    pub async fn schema_versions(&mut self) -> Result<MetadataPage<super::SchemaVersionRow>> {
        let MetadataSource::SchemaVersions(cursor) = &mut self.source else {
            return Err(Error::Constraint(
                "schema-version page requested from another cursor".into(),
            ));
        };
        let mut records = Vec::new();
        let mut memory = Vec::new();
        for _ in 0..PAGE_RECORDS {
            let Some((row, allowance)) = cursor.next().await? else {
                break;
            };
            records.push(row);
            memory.push(Arc::new(allowance));
        }
        Ok(MetadataPage { records, memory })
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
        let mut records = Vec::new();
        let mut memory = Vec::new();
        // Embedded children may produce no rows. Only EOF returns an empty page.
        while records.is_empty() {
            let mut bytes = 0_u64;
            for _ in 0..PAGE_RECORDS {
                let mut pending = if let Some(pending) = self.pending.take() {
                    *pending.downcast::<PendingRows<T>>().map_err(|_| {
                        Error::Corruption("metadata cursor changed its output type".into())
                    })?
                } else {
                    let Some((record, allowance)) = self.next_entity().await? else {
                        return Ok(MetadataPage { records, memory });
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
                bytes = bytes.saturating_add(pending.memory.bytes());
                memory.push(pending.memory.clone());
                if pending.records.len() != 0 {
                    self.pending = Some(Box::new(pending));
                }
                if records.len() == PAGE_RECORDS || bytes >= PAGE_BYTES {
                    break;
                }
            }
        }
        Ok(MetadataPage { records, memory })
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
                        EntityRecord::$variant(value) => Ok(vec![Arc::unwrap_or_clone(value)]),
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
    async fn large_records_bound_page_bytes_without_losing_rows() {
        let catalog = Catalog::open(
            Arc::new(object_store::memory::InMemory::new()),
            CatalogOptions::default(),
        )
        .await
        .unwrap();
        let write = catalog.begin_write_tx().unwrap();
        for id in 1..=20 {
            let payload = "x".repeat(128 * 1024);
            write
                .put(
                    Key::current(EntityKey::File {
                        table_id: 1,
                        data_file_id: id,
                    })
                    .encode(),
                    crate::store::value::encode_value(&DataFileRecord {
                        table_id: 1,
                        data_file_id: id,
                        path: payload.clone(),
                        ..Default::default()
                    }),
                )
                .unwrap();
            write
                .put(
                    Key::Snapshot { snapshot_id: id }.encode(),
                    crate::store::value::encode_value(&super::super::SnapshotRecord {
                        snapshot_id: id,
                        changes_made: payload,
                        ..Default::default()
                    }),
                )
                .unwrap();
        }
        catalog.commit_write_tx(write).await.unwrap();
        for table in [TableKind::DataFile, TableKind::SnapshotChanges] {
            let mut cursor = MetadataCursor::open(&catalog, table, None, None)
                .await
                .unwrap();
            for fork in [false, true] {
                if fork {
                    cursor = cursor.fork().await.unwrap();
                }
                let mut ids = Vec::new();
                loop {
                    let (page_ids, memory) = if table == TableKind::DataFile {
                        let page = cursor.data_files().await.unwrap();
                        (
                            page.records
                                .iter()
                                .map(|row| row.data_file_id)
                                .collect::<Vec<_>>(),
                            page.retained_memory().to_vec(),
                        )
                    } else {
                        let page = cursor.snapshots().await.unwrap();
                        (
                            page.records
                                .iter()
                                .map(|row| row.snapshot_id)
                                .collect::<Vec<_>>(),
                            page.retained_memory().to_vec(),
                        )
                    };
                    if page_ids.is_empty() {
                        break;
                    }
                    let bytes: u64 = memory.iter().map(|reservation| reservation.bytes()).sum();
                    let largest = memory
                        .iter()
                        .map(|reservation| reservation.bytes())
                        .max()
                        .unwrap();
                    assert!(
                        bytes <= 1024 * 1024 + largest,
                        "page retained {bytes} bytes"
                    );
                    ids.extend(page_ids);
                }
                let first = u64::from(table == TableKind::DataFile);
                assert_eq!(ids, (first..=20).collect::<Vec<_>>());
            }
        }
        catalog.close().await.unwrap();
    }

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
        let retained_parents = first.retained_memory().len();
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
