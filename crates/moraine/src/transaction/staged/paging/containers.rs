//! Per-container edits and authored rows for previously absent containers.

use super::{
    Arc, BTreeMap, EntityKey, EntityRecord, Error, MemoryReservation, Result, RowOperation,
    TableKind, decode, export, overlay,
};
use crate::store::{handle::OwnedReadHandle, key::Key};

pub(super) struct Containers {
    edits: BTreeMap<EntityKey, Vec<RowOperation>>,
    inserted: Vec<EntityRecord>,
}

impl Containers {
    pub(super) async fn new(
        table: TableKind,
        operations: &[RowOperation],
        handle: OwnedReadHandle,
    ) -> Result<Self> {
        let mut edits: BTreeMap<EntityKey, Vec<RowOperation>> = BTreeMap::new();
        for operation in operations {
            let key = match operation {
                RowOperation::Insert {
                    table: TableKind::Tag,
                    cells,
                }
                | RowOperation::Delete {
                    table: TableKind::Tag,
                    cells,
                }
                | RowOperation::UpdateSetEnd {
                    table: TableKind::Tag,
                    cells,
                } if table == TableKind::Tag => {
                    let mut cursor = super::super::Cursor::new(table, cells);
                    EntityKey::Tag {
                        object_id: cursor.u64()?,
                    }
                }
                RowOperation::Insert {
                    table: TableKind::Metadata,
                    cells,
                } if table == TableKind::Metadata => {
                    let ((scope_kind, scope_id), _, _) = decode::decode_metadata(cells)?;
                    EntityKey::Option {
                        scope_kind,
                        scope_id,
                    }
                }
                RowOperation::Delete {
                    table: TableKind::Metadata,
                    cells,
                } if table == TableKind::Metadata => {
                    let ((scope_kind, scope_id), _) = decode::decode_metadata_key(cells)?;
                    EntityKey::Option {
                        scope_kind,
                        scope_id,
                    }
                }
                _ => continue,
            };
            edits.entry(key).or_default().push(operation.clone());
        }
        let mut inserted = Vec::new();
        for (key, operations) in &edits {
            if handle
                .borrow()
                .get(Key::current(*key).encode())
                .await?
                .is_some()
            {
                continue;
            }
            match key {
                EntityKey::Tag { .. } => inserted.extend(
                    overlay::overlay_tag_containers(operations, Vec::new())?
                        .into_iter()
                        .map(|row| EntityRecord::Tag(Arc::new(row))),
                ),
                EntityKey::Option { .. } => inserted.extend(
                    overlay::overlay_option_scopes(operations, Vec::new())?
                        .into_iter()
                        .map(|(scope_kind, scope_id, value)| EntityRecord::Option {
                            scope_kind,
                            scope_id,
                            value: Arc::new(value),
                        }),
                ),
                _ => return Err(Error::Corruption("invalid staged container key".into())),
            }
        }
        Ok(Self { edits, inserted })
    }

    pub(super) fn apply(&self, record: EntityRecord) -> Result<Option<EntityRecord>> {
        match record {
            EntityRecord::Tag(row) => {
                let Some(operations) = self.edits.get(&EntityKey::Tag {
                    object_id: row.object_id,
                }) else {
                    return Ok(Some(EntityRecord::Tag(row)));
                };
                Ok(
                    overlay::overlay_tag_containers(operations, vec![(*row).clone()])?
                        .into_iter()
                        .next()
                        .map(|row| EntityRecord::Tag(Arc::new(row))),
                )
            }
            EntityRecord::Option {
                scope_kind,
                scope_id,
                value,
            } => {
                let Some(operations) = self.edits.get(&EntityKey::Option {
                    scope_kind,
                    scope_id,
                }) else {
                    return Ok(Some(EntityRecord::Option {
                        scope_kind,
                        scope_id,
                        value,
                    }));
                };
                Ok(overlay::overlay_option_scopes(
                    operations,
                    vec![(scope_kind, scope_id, (*value).clone())],
                )?
                .into_iter()
                .next()
                .map(|(scope_kind, scope_id, value)| EntityRecord::Option {
                    scope_kind,
                    scope_id,
                    value: Arc::new(value),
                }))
            }
            _ => Err(Error::Corruption(
                "staged container received another entity kind".into(),
            )),
        }
    }

    pub(super) fn inserted(
        &self,
        index: usize,
    ) -> Result<Option<(EntityRecord, MemoryReservation)>> {
        match self.inserted.get(index) {
            Some(EntityRecord::Tag(row)) => export(Some(row.as_ref()), EntityRecord::Tag),
            Some(EntityRecord::Option {
                scope_kind,
                scope_id,
                value,
            }) => export(Some(value.as_ref()), |value| EntityRecord::Option {
                scope_kind: *scope_kind,
                scope_id: *scope_id,
                value,
            }),
            None => Ok(None),
            _ => Err(Error::Corruption("invalid staged container tail".into())),
        }
    }
}
