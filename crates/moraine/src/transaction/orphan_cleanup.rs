//! Durable orphan-deletion reservations and publication validation.

use std::sync::Arc;

use crate::{
    Catalog, Error, MemoryReservation, Result,
    store::{
        handle::ReadHandle,
        key::{EntityKind, current_entity_kind_prefix},
        proto::{DataFileValue, DeleteFileValue},
        read, value,
    },
    transaction::commit::{self, StagedWrite},
};

mod paths;
mod references;
#[cfg(test)]
mod tests;

impl Catalog {
    /// Durably reserves unreferenced candidate paths for orphan deletion.
    /// Only the returned subset may be deleted. Reserved paths can never be
    /// published again; retries return still-unreferenced reserved paths.
    ///
    /// # Errors
    /// Returns an error for an older format, an invalid path, or a failed
    /// commit. An error authorizes no physical deletion.
    ///
    /// # Examples
    /// ```
    /// # async fn example() -> moraine::Result<()> {
    /// use std::sync::Arc;
    ///
    /// use moraine::{Catalog, CatalogOptions};
    /// use object_store::memory::InMemory;
    /// let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// let candidates = vec!["s3://lake/data/abandoned.parquet".to_owned()];
    /// let approved = catalog.plan_orphan_deletion(&candidates).await?;
    /// assert_eq!(approved, candidates);
    /// // The caller may now remove these objects; their paths stay reserved.
    /// catalog.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn plan_orphan_deletion(&self, candidates: &[String]) -> Result<Vec<String>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let _memory = MemoryReservation::reserve(candidates.iter().fold(0_u64, |bytes, path| {
            bytes.saturating_add(path.len() as u64 * 3 + 256)
        }))?;
        let mut canonical = references::Paths::new()?;
        let mut originals = Vec::new();
        for path in candidates {
            let identity = paths::identities(path)?;
            if identity.iter().all(|path| canonical.values.contains(path)) {
                continue;
            }
            for path in &identity {
                canonical.insert(path.clone())?;
            }
            originals.push((path.clone(), identity));
        }
        let store = self.store();
        let durability = store.commit_durability();
        for attempt in 0..commit::MAX_COMMIT_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(commit::retry_backoff(attempt)).await;
            }
            let session = store.begin_commit_session().await?;
            let base = commit::read_head_value(session.handle()).await?;
            commit::refuse_mid_migration_at(session.handle(), self.projections(), &base).await?;
            if read::read_format(session.handle())
                .await?
                .is_none_or(|format| format.format_version < commit::FORMAT_WITH_ORPHAN_DELETION)
            {
                return Err(Error::Migration("safe orphan deletion requires format 13; quiesce writers and migrate the catalog first".into()));
            }
            let retained =
                references::retained(session.handle(), &[], Some(&canonical.values)).await?;
            let approved: Vec<_> = originals
                .iter()
                .filter(|(_, paths)| !paths.iter().any(|path| retained.values.contains(path)))
                .collect();
            if approved.is_empty() {
                return Ok(Vec::new());
            }
            let mut writes: Vec<StagedWrite> = approved
                .iter()
                .flat_map(|(_, paths)| {
                    paths
                        .iter()
                        .map(|path| (paths::key(path), Some(Vec::new())))
                })
                .collect();
            writes.push((paths::key(""), Some(Vec::new())));
            writes.push(commit::head_stamp(base.snapshot_id, base.batch_seq));
            let staged = commit::stage_writes_on(&session, &writes).await?;
            let landed = commit::commit_batch(
                Arc::new(session),
                commit::HeadTransition {
                    before: base.snapshot_id,
                    after: base.snapshot_id,
                },
                &writes,
                staged,
                commit::Landing {
                    projections: self.projections(),
                    log: store.commit_log(),
                },
                &durability,
            )
            .await?;
            if matches!(landed, commit::Landed::Committed(_)) {
                return Ok(approved.into_iter().map(|(path, _)| path.clone()).collect());
            }
        }
        Err(Error::RetryBudgetExhausted(
            "orphan deletion plan lost every commit attempt".into(),
        ))
    }
}

pub(super) async fn validate_publication(
    handle: ReadHandle<'_>,
    writes: &[StagedWrite],
) -> Result<()> {
    let file_prefix = current_entity_kind_prefix(EntityKind::File);
    let delete_prefix = current_entity_kind_prefix(EntityKind::DeleteFile);
    let parent_prefixes =
        [EntityKind::Schema, EntityKind::Table, EntityKind::Option].map(current_entity_kind_prefix);
    let mut files_changed = false;
    let mut parents_changed = false;
    for (key, bytes) in writes {
        if bytes.is_none() {
            continue;
        }
        files_changed |= key.starts_with(&file_prefix) || key.starts_with(&delete_prefix);
        parents_changed |= parent_prefixes.iter().any(|prefix| key.starts_with(prefix));
    }
    if (!files_changed && !parents_changed) || handle.get(paths::key("")).await?.is_none() {
        return Ok(());
    }
    let root = references::root(handle, writes).await?;
    for (key, bytes) in writes {
        let Some(bytes) = bytes else {
            continue;
        };
        if !key.starts_with(&file_prefix) && !key.starts_with(&delete_prefix) {
            continue;
        }
        let _memory =
            MemoryReservation::reserve((bytes.len() as u64).saturating_mul(3).saturating_add(256))?;
        let (table, path, relative, ended) = if key.starts_with(&file_prefix) {
            let file: DataFileValue = value::decode_value(bytes)?;
            (
                file.table_id,
                file.path,
                file.path_is_relative,
                file.end_snapshot.is_some(),
            )
        } else {
            let file: DeleteFileValue = value::decode_value(bytes)?;
            (
                file.table_id,
                file.path,
                file.path_is_relative,
                file.end_snapshot.is_some(),
            )
        };
        if !ended {
            let path =
                references::publication(handle, writes, &root, table, &path, relative).await?;
            for identity in path {
                refuse_reserved(handle, &identity).await?;
            }
        }
    }
    if parents_changed {
        let retained = references::retained(handle, writes, None).await?;
        for path in &retained.values {
            refuse_reserved(handle, path).await?;
        }
    }
    Ok(())
}

async fn refuse_reserved(handle: ReadHandle<'_>, path: &str) -> Result<()> {
    if handle.get(paths::key(path)).await?.is_some() {
        return Err(Error::Constraint(
            "orphan deletion reserved this file path; write a new object before retrying".into(),
        ));
    }
    Ok(())
}
