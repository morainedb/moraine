//! Commit premises for operations confined to named catalog records.

use super::{
    commit, names,
    staged::dependencies::header,
    table_read::{self, TableRead, point, retain},
};
use crate::{
    Catalog, CatalogSnapshot, Error, OptionScope, Result, SnapshotId, TableId,
    store::{
        handle::ReadHandle,
        key::{EntityKey, NameKind},
        read::{self, EntityCursor, EntityRecord, EntityRecordKind, Versions},
    },
    transaction::verbs::Transaction,
};

#[derive(Clone, Copy)]
pub(crate) enum CommitRead<'a> {
    Table(TableId, TableRead),
    Options(OptionScope),
    IndexName { schema: &'a str, name: &'a str },
}

impl CommitRead<'_> {
    pub(crate) async fn load(self, handle: ReadHandle<'_>) -> Result<CatalogSnapshot> {
        read::consistent(handle, || async {
            match self {
                Self::Table(table, selection) => {
                    table_read::load_table(handle, table, selection).await
                }
                Self::Options(scope) => {
                    let mut view = header(handle).await?;
                    let (scope_kind, scope_id) = scope.key_components();
                    point(
                        handle,
                        &mut view,
                        EntityKey::Option {
                            scope_kind,
                            scope_id,
                        },
                    )
                    .await?;
                    match scope {
                        OptionScope::Global => {}
                        OptionScope::Schema(schema) => {
                            point(
                                handle,
                                &mut view,
                                EntityKey::Schema {
                                    schema_id: schema.get(),
                                },
                            )
                            .await?;
                        }
                        OptionScope::Table(table) => {
                            point(
                                handle,
                                &mut view,
                                EntityKey::Table {
                                    table_id: table.get(),
                                },
                            )
                            .await?;
                        }
                    }
                    Ok(view)
                }
                Self::IndexName { schema, name } => index_name(handle, schema, name).await,
            }
        })
        .await
    }
}

async fn index_name(handle: ReadHandle<'_>, schema: &str, name: &str) -> Result<CatalogSnapshot> {
    let mut view = header(handle).await?;
    let Some(schema_id) = names::lookup(handle, NameKind::Schema, 0, schema).await? else {
        return Ok(view);
    };
    point(handle, &mut view, EntityKey::Schema { schema_id }).await?;
    let mut indexes =
        EntityCursor::open(handle, EntityRecordKind::Index, Versions::Live, None).await?;
    while let Some((record, _memory)) = indexes.next().await? {
        let EntityRecord::Index(index) = record else {
            continue;
        };
        if !index.index_name.eq_ignore_ascii_case(name) {
            continue;
        }
        let key = crate::store::key::Key::current(EntityKey::Table {
            table_id: index.table_id,
        });
        if let Some(bytes) = handle.get(key.encode()).await? {
            let record = read::decode_current_record(key, &bytes)?;
            if matches!(&record, EntityRecord::Table(table) if table.schema_id == schema_id && table.end_snapshot.is_none())
            {
                retain(&mut view, record)?;
                retain(&mut view, EntityRecord::Index(index))?;
            }
        }
    }
    Ok(view)
}

impl Catalog {
    pub(crate) async fn commit_scoped<F>(
        &self,
        selection: CommitRead<'_>,
        mutate: F,
    ) -> Result<SnapshotId>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        let member = commit::ScopedMember { selection, mutate };
        let ids = commit::commit_cycle(&self.store(), &member, self.commits()).await?;
        ids.first()
            .copied()
            .ok_or_else(|| Error::Corruption("scoped commit returned no snapshot".into()))
    }
}
