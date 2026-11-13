//! Dependencies loaded by public catalog reads and mutations.

use std::collections::{BTreeSet, HashSet};

use crate::{
    Error, Result,
    catalog::{
        CatalogSnapshot, ColumnAlteration, ColumnDef, ColumnId, ColumnInfo, ColumnOrder,
        ColumnStats, DataFile, DataFileId, DataFileInfo, DeleteFile, DeleteFileId, DeleteFileInfo,
        FileIndexEntry, FileIndexRemoval, FlushedDataFile, IndexDef, IndexEntry, IndexId,
        IndexInfo, IndexMaintenance, IndexState, InlineChunk, MacroId, MacroImplementationDef,
        MacroInfo, MappingInfo, OptionScope, PartitionColumnDef, PartitionId, PartitionSpec,
        SchemaId, SchemaInfo, SnapshotId, SnapshotInfo, SortId, SortKeyDef, SortSpec, TableId,
        TableInfo, TableStats, TagEntry, TagTarget, ViewId, ViewInfo,
    },
    store::{
        handle::ReadHandle,
        key::{EntityKey, Key, NameKind},
        read::{self, EntityCursor, EntityRecord, EntityRecordKind as Kind, Versions},
    },
    transaction::{names, table_read::retain},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Selection {
    Point(EntityKey),
    Range(Kind, Option<u64>),
    Namespace(Kind, u64, Option<String>),
}

impl Selection {
    pub(crate) async fn load(
        &self,
        handle: ReadHandle<'_>,
        view: &mut CatalogSnapshot,
    ) -> Result<()> {
        match self {
            Self::Point(entity) => {
                let key = Key::current(*entity);
                if let Some(bytes) = handle.get(key.encode()).await? {
                    retain(view, read::decode_current_record(key, &bytes)?)?;
                }
            }
            Self::Namespace(kind, parent, Some(name))
                if matches!(kind, Kind::Schema | Kind::Table) =>
            {
                let name_kind = if *kind == Kind::Schema {
                    NameKind::Schema
                } else {
                    NameKind::Table
                };
                if let Some(id) = names::lookup(handle, name_kind, *parent, name).await? {
                    let entity = if *kind == Kind::Schema {
                        EntityKey::Schema { schema_id: id }
                    } else {
                        EntityKey::Table { table_id: id }
                    };
                    let key = Key::current(entity);
                    if let Some(bytes) = handle.get(key.encode()).await? {
                        retain(view, read::decode_current_record(key, &bytes)?)?;
                    }
                }
            }
            selection => {
                let (kind, table) = match selection {
                    Self::Range(kind, table) => (*kind, *table),
                    Self::Namespace(kind, ..) => (*kind, None),
                    Self::Point(_) => {
                        return Err(Error::Corruption("invalid range selection".into()));
                    }
                };
                let mut cursor = EntityCursor::open(
                    handle,
                    kind,
                    Versions::Live,
                    table,
                    crate::read_policy::CacheAdmission::Retain,
                )
                .await?;
                while let Some(record_owner) = cursor.next().await? {
                    let record = record_owner.value().clone();
                    if selection.matches(&record) {
                        retain(view, record)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn matches(&self, record: &EntityRecord) -> bool {
        let Self::Namespace(_, parent, name) = self else {
            return true;
        };
        let (scope, actual) = match record {
            EntityRecord::Schema(value) => (0, &value.schema_name),
            EntityRecord::Table(value) => (value.schema_id, &value.table_name),
            EntityRecord::View(value) => (value.schema_id, &value.view_name),
            EntityRecord::Macro(value) => (value.schema_id, &value.macro_name),
            _ => return false,
        };
        scope == *parent && name.as_ref().is_none_or(|name| name == actual)
    }
}

/// A mutation handle that loads dependencies as each operation needs them.
/// Reads include this transaction's staged changes. Commit callbacks may be
/// retried and must not perform external side effects.
pub struct Transaction {
    pub(crate) session: std::sync::Arc<super::session::CommitSession>,
    pub(crate) base: CatalogSnapshot,
    pub(crate) inner: super::verbs::Transaction,
    loaded: HashSet<Selection>,
    keys: BTreeSet<EntityKey>,
    memory: crate::MemoryReservation,
}

impl Transaction {
    pub(crate) fn new(
        session: std::sync::Arc<super::session::CommitSession>,
        base: CatalogSnapshot,
        pending: &[super::commit::StagedWrite],
    ) -> Result<Self> {
        let mut keys = BTreeSet::new();
        for (key, _) in pending {
            if let Key::Current(crate::store::key::CurrentKey::Entity(entity)) = Key::decode(key)? {
                keys.insert(entity);
            }
        }
        let inner = super::verbs::Transaction::new(base.clone(), base.snapshot.snapshot_id + 1);
        Ok(Self {
            session,
            base,
            inner,
            loaded: HashSet::new(),
            memory: crate::MemoryReservation::reserve((keys.len() as u64).saturating_mul(128))?,
            keys,
        })
    }

    async fn ensure(&mut self, selection: Selection) -> Result<()> {
        if self.loaded.contains(&selection) {
            return Ok(());
        }
        let name_bytes = match &selection {
            Selection::Namespace(_, _, Some(name)) => name.len() as u64,
            _ => 0,
        };
        self.memory.resize(
            self.memory
                .bytes()
                .saturating_add(256)
                .saturating_add(name_bytes),
        )?;
        let mut incoming = CatalogSnapshot::default();
        selection.load(self.session.handle(), &mut incoming).await?;
        for kind in ALL_KINDS {
            for record in incoming.live_records(kind) {
                if let Some(key) = record_key(&record)
                    && !self.keys.contains(&key)
                {
                    retain(&mut self.base, record.clone())?;
                    self.memory
                        .resize(self.memory.bytes().saturating_add(128))?;
                    retain(&mut self.inner.state, record)?;
                    self.keys.insert(key);
                }
            }
        }
        if let Selection::Point(key) = selection {
            self.keys.insert(key);
        }
        self.loaded.insert(selection);
        Ok(())
    }

    async fn point(&mut self, key: EntityKey) -> Result<()> {
        self.ensure(Selection::Point(key)).await
    }
    async fn range(&mut self, kind: Kind, table: Option<u64>) -> Result<()> {
        self.ensure(Selection::Range(kind, table)).await
    }
    async fn namespace(&mut self, kind: Kind, schema: u64, name: Option<&str>) -> Result<()> {
        self.ensure(Selection::Namespace(kind, schema, name.map(str::to_owned)))
            .await
    }
    async fn relation(&mut self, schema: SchemaId, name: &str) -> Result<()> {
        self.point(EntityKey::Schema {
            schema_id: schema.get(),
        })
        .await?;
        self.namespace(Kind::Table, schema.get(), Some(name))
            .await?;
        self.namespace(Kind::View, schema.get(), Some(name)).await
    }
    async fn definition(&mut self, table: TableId) -> Result<()> {
        self.point(EntityKey::Table {
            table_id: table.get(),
        })
        .await?;
        self.point(EntityKey::TableStats {
            table_id: table.get(),
        })
        .await?;
        if let Some(table) = self.inner.table_by_id(table) {
            self.point(EntityKey::Schema {
                schema_id: table.schema_id.get(),
            })
            .await?;
        }
        for kind in [Kind::Column, Kind::Index, Kind::Partition, Kind::Sort] {
            self.range(kind, Some(table.get())).await?;
        }
        Ok(())
    }
    async fn options(&mut self, scope: OptionScope) -> Result<()> {
        let (scope_kind, scope_id) = scope.key_components();
        self.point(EntityKey::Option {
            scope_kind,
            scope_id,
        })
        .await?;
        match scope {
            OptionScope::Global => Ok(()),
            OptionScope::Schema(id) => {
                self.point(EntityKey::Schema {
                    schema_id: id.get(),
                })
                .await
            }
            OptionScope::Table(id) => self.point(EntityKey::Table { table_id: id.get() }).await,
        }
    }
    async fn index_namespace(&mut self, schema: SchemaId) -> Result<()> {
        self.namespace(Kind::Table, schema.get(), None).await?;
        for table in self.inner.tables_in(schema) {
            self.range(Kind::Index, Some(table.id.get())).await?;
        }
        Ok(())
    }
    async fn index(&mut self, index: IndexId) -> Result<()> {
        if !self.inner.index_tables.contains_key(&index.get()) {
            let mut cursor = EntityCursor::open(
                self.session.handle(),
                Kind::Index,
                Versions::Live,
                None,
                crate::read_policy::CacheAdmission::ReuseOnly,
            )
            .await?;
            let mut found = None;
            while let Some(record_owner) = cursor.next().await? {
                let record = record_owner.value().clone();
                if let EntityRecord::Index(value) = record
                    && value.index_id == index.get()
                {
                    found = Some(value.table_id);
                    break;
                }
            }
            if let Some(table_id) = found {
                self.point(EntityKey::Index {
                    table_id,
                    index_id: index.get(),
                })
                .await?;
            }
        }
        Ok(())
    }
}

pub(crate) const ALL_KINDS: [Kind; 17] = [
    Kind::Schema,
    Kind::Table,
    Kind::View,
    Kind::Column,
    Kind::File,
    Kind::DeleteFile,
    Kind::Partition,
    Kind::Sort,
    Kind::Macro,
    Kind::Mapping,
    Kind::Index,
    Kind::FileColumnStats,
    Kind::TableStats,
    Kind::TableColumnStats,
    Kind::Option,
    Kind::Tag,
    Kind::GcFile,
];

pub(crate) fn record_key(record: &EntityRecord) -> Option<EntityKey> {
    Some(match record {
        EntityRecord::Schema(v) => EntityKey::Schema {
            schema_id: v.schema_id,
        },
        EntityRecord::Table(v) => EntityKey::Table {
            table_id: v.table_id,
        },
        EntityRecord::View(v) => EntityKey::View { view_id: v.view_id },
        EntityRecord::Column(v) => EntityKey::Column {
            table_id: v.table_id,
            column_id: v.column_id,
        },
        EntityRecord::File(v) => EntityKey::File {
            table_id: v.table_id,
            data_file_id: v.data_file_id,
        },
        EntityRecord::DeleteFile(v) => EntityKey::DeleteFile {
            table_id: v.table_id,
            delete_file_id: v.delete_file_id,
        },
        EntityRecord::Partition(v) => EntityKey::Partition {
            table_id: v.table_id,
            partition_id: v.partition_id,
        },
        EntityRecord::Sort(v) => EntityKey::Sort {
            table_id: v.table_id,
            sort_id: v.sort_id,
        },
        EntityRecord::Macro(v) => EntityKey::Macro {
            macro_id: v.macro_id,
        },
        EntityRecord::Mapping(v) => EntityKey::Mapping {
            table_id: v.table_id,
            mapping_id: v.mapping_id,
        },
        EntityRecord::Index(v) => EntityKey::Index {
            table_id: v.table_id,
            index_id: v.index_id,
        },
        EntityRecord::FileColumnStats(v) => EntityKey::FileColumnStats {
            table_id: v.table_id,
            data_file_id: v.data_file_id,
            column_id: v.column_id,
        },
        EntityRecord::TableStats(v) => EntityKey::TableStats {
            table_id: v.table_id,
        },
        EntityRecord::TableColumnStats(v) => EntityKey::TableColumnStats {
            table_id: v.table_id,
            column_id: v.column_id,
        },
        EntityRecord::Tag(v) => EntityKey::Tag {
            object_id: v.object_id,
        },
        EntityRecord::Option {
            scope_kind,
            scope_id,
            ..
        } => EntityKey::Option {
            scope_kind: *scope_kind,
            scope_id: *scope_id,
        },
        EntityRecord::GcFile(_) => return None,
    })
}

mod mutations;
mod reads;

#[cfg(test)]
mod tests;
