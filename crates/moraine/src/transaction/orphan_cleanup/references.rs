//! Retained file references resolved through current and historical parents.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use super::paths;
use crate::{
    Error, MemoryReservation, Result,
    read_policy::CacheAdmission,
    store::{
        handle::ReadHandle,
        key::{CurrentKey, EntityKey, Key},
        proto::{OptionScopeValue, SchemaValue, TableValue},
        read::{EntityCursor, EntityRecord, EntityRecordKind, Versions},
        value,
    },
    transaction::commit::StagedWrite,
};

pub(super) struct Paths {
    pub(super) values: BTreeSet<String>,
    memory: MemoryReservation,
}

impl Paths {
    pub(super) fn new() -> Result<Self> {
        Ok(Self {
            values: BTreeSet::new(),
            memory: MemoryReservation::reserve(0)?,
        })
    }

    pub(super) fn insert(&mut self, path: String) -> Result<()> {
        if !self.values.contains(&path) {
            self.memory.resize(
                self.memory
                    .bytes()
                    .saturating_add(path.len() as u64)
                    .saturating_add(128),
            )?;
            self.values.insert(path);
        }
        Ok(())
    }
}

pub(super) async fn root(handle: ReadHandle<'_>, writes: &[StagedWrite]) -> Result<String> {
    let entity = EntityKey::Option {
        scope_kind: 0,
        scope_id: 0,
    };
    let options: Option<OptionScopeValue> = current(handle, writes, entity).await?;
    Ok(options
        .and_then(|options| options.options.get("data_path").cloned())
        .unwrap_or_default())
}

pub(super) async fn current<T: prost::Message + Default>(
    handle: ReadHandle<'_>,
    writes: &[StagedWrite],
    entity: EntityKey,
) -> Result<Option<T>> {
    let key = Key::current(entity).encode();
    if let Some((_, bytes)) = writes.iter().rev().find(|(written, _)| *written == key) {
        return bytes.as_deref().map(value::decode_value).transpose();
    }
    handle
        .get(key)
        .await?
        .as_deref()
        .map(value::decode_value)
        .transpose()
}

pub(super) async fn publication(
    handle: ReadHandle<'_>,
    writes: &[StagedWrite],
    root: &str,
    table: u64,
    path: &str,
    relative: bool,
) -> Result<Vec<String>> {
    if !relative {
        return paths::identities(path);
    }
    let table: TableValue = current(handle, writes, EntityKey::Table { table_id: table })
        .await?
        .ok_or_else(|| Error::Corruption("published file has no table path".into()))?;
    let table_path = if table.path_is_relative {
        let schema: SchemaValue = current(
            handle,
            writes,
            EntityKey::Schema {
                schema_id: table.schema_id,
            },
        )
        .await?
        .ok_or_else(|| Error::Corruption("published file has no schema path".into()))?;
        paths::join(
            &paths::join(root, &schema.path, schema.path_is_relative),
            &table.path,
            true,
        )
    } else {
        table.path
    };
    paths::identities(&paths::join(&table_path, path, true))
}

#[derive(Default)]
struct Parents {
    schemas: BTreeMap<u64, Vec<Arc<SchemaValue>>>,
    tables: BTreeMap<u64, Vec<Arc<TableValue>>>,
    memory: Vec<MemoryReservation>,
}

impl Parents {
    fn add(&mut self, record: EntityRecord) {
        match record {
            EntityRecord::Schema(schema) => self
                .schemas
                .entry(schema.schema_id)
                .or_default()
                .push(schema),
            EntityRecord::Table(table) => {
                self.tables.entry(table.table_id).or_default().push(table);
            }
            _ => {}
        }
    }

    fn visit_paths(
        &self,
        root: &str,
        table: u64,
        path: &str,
        relative: bool,
        mut visit: impl FnMut(String) -> Result<()>,
    ) -> Result<()> {
        if !relative {
            for identity in paths::identities(path)? {
                visit(identity)?;
            }
            return Ok(());
        }
        let tables = self
            .tables
            .get(&table)
            .ok_or_else(|| Error::Corruption("retained file has no table path".into()))?;
        for table in tables {
            if !table.path_is_relative {
                for identity in paths::identities(&paths::join(&table.path, path, true))? {
                    visit(identity)?;
                }
                continue;
            }
            let schemas = self
                .schemas
                .get(&table.schema_id)
                .ok_or_else(|| Error::Corruption("retained file has no schema path".into()))?;
            for schema in schemas {
                let prefix = paths::join(
                    &paths::join(root, &schema.path, schema.path_is_relative),
                    &table.path,
                    true,
                );
                for identity in paths::identities(&paths::join(&prefix, path, true))? {
                    visit(identity)?;
                }
            }
        }
        Ok(())
    }
}

pub(super) async fn retained(
    handle: ReadHandle<'_>,
    writes: &[StagedWrite],
    selected: Option<&BTreeSet<String>>,
) -> Result<Paths> {
    let root = root(handle, writes).await?;
    let mut parents = Parents::default();
    for kind in [EntityRecordKind::Schema, EntityRecordKind::Table] {
        let mut records = EntityCursor::open(
            handle,
            kind,
            Versions::LiveAndEnded,
            None,
            CacheAdmission::ReuseOnly,
        )
        .await?;
        while let Some(record) = records.next().await? {
            let (record, memory) = record.into_parts();
            parents.add(record);
            parents.memory.push(memory);
        }
    }
    for (key, bytes) in writes {
        let Some(bytes) = bytes else { continue };
        match Key::decode(key)? {
            Key::Current(CurrentKey::Entity(EntityKey::Schema { .. })) => {
                parents.add(EntityRecord::Schema(Arc::new(value::decode_value(bytes)?)));
            }
            Key::Current(CurrentKey::Entity(EntityKey::Table { .. })) => {
                parents.add(EntityRecord::Table(Arc::new(value::decode_value(bytes)?)));
            }
            _ => {}
        }
    }
    let mut retained = Paths::new()?;
    for kind in [
        EntityRecordKind::File,
        EntityRecordKind::DeleteFile,
        EntityRecordKind::GcFile,
    ] {
        let mut records = EntityCursor::open(
            handle,
            kind,
            Versions::LiveAndEnded,
            None,
            CacheAdmission::ReuseOnly,
        )
        .await?;
        while let Some(record) = records.next().await? {
            let mut retain = |path: String| {
                if selected.is_none_or(|selected| selected.contains(&path)) {
                    retained.insert(path)?;
                }
                Ok(())
            };
            match record.value() {
                EntityRecord::File(file) => parents.visit_paths(
                    &root,
                    file.table_id,
                    &file.path,
                    file.path_is_relative,
                    &mut retain,
                )?,
                EntityRecord::DeleteFile(file) => parents.visit_paths(
                    &root,
                    file.table_id,
                    &file.path,
                    file.path_is_relative,
                    &mut retain,
                )?,
                EntityRecord::GcFile(file) => {
                    for identity in
                        paths::identities(&paths::join(&root, &file.path, file.path_is_relative))?
                    {
                        retain(identity)?;
                    }
                }
                _ => {
                    return Err(Error::Corruption(
                        "unexpected cleanup reference record".into(),
                    ));
                }
            }
        }
    }
    Ok(retained)
}
