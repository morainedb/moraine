//! Scoped option and index mutations.

use std::cell::Cell;

use crate::{
    Catalog, ColumnOrder, DataStore, Error, IndexDef, IndexId, IndexMaintenance, OptionScope,
    Result, SnapshotId, TableId,
    transaction::{commit, scoped::CommitRead, table_read::TableRead},
};

impl Catalog {
    /// Sets one option without reading unrelated catalog entities.
    ///
    /// # Errors
    /// Returns an error for a missing scope, reserved option, or failed commit.
    pub async fn set_option(
        &self,
        scope: OptionScope,
        key: &str,
        value: &str,
    ) -> Result<SnapshotId> {
        self.commit_scoped(CommitRead::Options(scope), |tx| {
            tx.set_option(scope, key, value)
        })
        .await
    }

    /// Records the data path once, accepting an already recorded matching path.
    ///
    /// # Errors
    /// Returns a constraint error for a different recorded path, or a store
    /// error.
    pub async fn adopt_data_path(&self, path: &str) -> Result<()> {
        self.commit_scoped(CommitRead::Options(OptionScope::Global), |tx| {
            if let Some(recorded) = tx.option(OptionScope::Global, "data_path") {
                if recorded.trim_end_matches('/') != path.trim_end_matches('/') {
                    return Err(Error::Constraint(
                        "the catalog already records a different data path".into(),
                    ));
                }
                return Ok(());
            }
            tx.set_option(OptionScope::Global, "data_path", path)
        })
        .await
        .map(|_| ())
    }

    /// Drops an index by its schema-level name, using case-insensitive index
    /// matching.
    ///
    /// # Errors
    /// Returns an error for an absent or ambiguous name, unless `if_exists`
    /// tolerates absence.
    pub async fn drop_index(&self, schema: &str, name: &str, if_exists: bool) -> Result<()> {
        self.commit_scoped(CommitRead::IndexName { schema, name }, |tx| {
            let Some(schema) = tx.schema_by_name(schema) else {
                return if if_exists {
                    Ok(())
                } else {
                    Err(Error::NotFound(format!("schema {schema}")))
                };
            };
            match tx.index_by_schema_name(schema.id, name)? {
                Some(index) => tx.drop_index(index.id),
                None if if_exists => Ok(()),
                None => Err(Error::NotFound(format!("index {name}"))),
            }
        })
        .await
        .map(|_| ())
    }

    /// Backfills and creates an index atomically from one fixed read revision.
    /// Use a staged build when the derived entries exceed the working-memory
    /// budget.
    ///
    /// # Errors
    /// Returns an error for an invalid definition, unreadable data, or an
    /// unsuccessful commit.
    pub async fn create_index(
        &self,
        table: TableId,
        definition: &IndexDef,
        orders: &[ColumnOrder],
        maintenance: IndexMaintenance,
        data_store: Option<DataStore>,
        data_prefix: &str,
    ) -> Result<IndexId> {
        for _ in 0..commit::MAX_COMMIT_ATTEMPTS {
            let scope = self.index_read_scope().await?;
            let reads = scope.reads();
            let session = reads.begin_read().await?;
            super::index_build::require_data_store_at(
                session.handle(),
                table,
                data_store.is_some(),
            )
            .await?;
            let head = commit::read_head_value(session.handle()).await?;
            session.finish();
            let files = async {
                match &data_store {
                    Some(store) => {
                        reads
                            .scoped_backfill_entries(
                                store.clone(),
                                data_prefix,
                                table,
                                &definition.columns,
                            )
                            .await
                    }
                    None => Ok(Vec::new()),
                }
            };
            let (mut backfill, inline) = tokio::try_join!(
                files,
                reads.inline_backfill_entries(table, &definition.columns)
            )?;
            backfill.extend(inline);
            let index = Cell::new(None);
            let result = self
                .commit_table(table, TableRead::IndexCreation, |tx| {
                    if tx.current_snapshot().id.get() != head.snapshot_id
                        || tx.batch_seq != head.batch_seq
                    {
                        return Err(Error::CommitConflict(
                            "catalog changed during index derivation".into(),
                        ));
                    }
                    index.set(Some(tx.create_index_ordered_with_maintenance(
                        table,
                        definition,
                        orders,
                        maintenance,
                        &backfill,
                    )?));
                    Ok(())
                })
                .await;
            match result {
                Err(Error::CommitConflict(_)) => {}
                Err(error) => return Err(error),
                Ok(_) => {
                    return index.get().ok_or_else(|| {
                        Error::Corruption("index creation returned no index ID".into())
                    });
                }
            }
        }
        Err(Error::RetryBudgetExhausted(
            "catalog changed repeatedly during index derivation".into(),
        ))
    }
}

#[cfg(test)]
mod tests;
