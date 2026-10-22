//! Bounded publication steps with a resumable file frontier.

use std::{collections::BTreeSet, ops::Bound, sync::Arc};

use futures::{StreamExt, stream};

use super::{
    CatalogSnapshot, DataStore, Error, ReadOnlyCatalog, Result, SnapshotId, TableId, data_file,
    data_file_info,
};

type Position = (u64, u64);
const MAX_RETRY_FILES: usize = 1024;

/// Progress through one selected snapshot's row-summary publication.
/// Successful steps survive cancellation of a later step. Drop this value to
/// release its snapshot; progress is local to this process.
pub struct RowSummaryProgress {
    snapshot: Option<Arc<CatalogSnapshot>>,
    tables: Option<BTreeSet<TableId>>,
    since: Option<SnapshotId>,
    after: Option<Position>,
    failed: BTreeSet<Position>,
    retrying: bool,
    retry_cursor: Option<Position>,
    overflow_after: Option<Bound<Position>>,
    complete: bool,
}

impl RowSummaryProgress {
    /// Selects tables (`None` means all) and an exclusive snapshot cutoff.
    /// Keep the same catalog and data location for every step.
    #[must_use]
    pub fn new(tables: Option<Vec<TableId>>, since: Option<SnapshotId>) -> Self {
        Self {
            snapshot: None,
            tables: tables.map(|tables| tables.into_iter().collect()),
            since,
            after: None,
            failed: BTreeSet::new(),
            retrying: false,
            retry_cursor: None,
            overflow_after: None,
            complete: false,
        }
    }

    async fn select(&mut self, snapshot: &CatalogSnapshot) -> Vec<Position> {
        if self.retrying {
            return self
                .failed
                .range((
                    self.retry_cursor.map_or(Bound::Unbounded, Bound::Excluded),
                    Bound::Unbounded,
                ))
                .take(data_file::PUBLISH_CONCURRENCY)
                .copied()
                .collect();
        }
        let lower = self
            .after
            .map_or(Bound::Unbounded, |(table, _)| Bound::Included(table));
        let mut selected = Vec::new();
        let mut inspected = 0_usize;
        for (&table, files) in snapshot.data_files.range((lower, Bound::Unbounded)) {
            tokio::task::yield_now().await;
            if self
                .tables
                .as_ref()
                .is_some_and(|tables| !tables.contains(&TableId::new(table)))
            {
                continue;
            }
            let lower = match self.after {
                Some((previous, file)) if previous == table => Bound::Excluded(file),
                _ => Bound::Unbounded,
            };
            for (&file, value) in files.range((lower, Bound::Unbounded)) {
                inspected += 1;
                if inspected.is_multiple_of(1024) {
                    tokio::task::yield_now().await;
                }
                if self
                    .since
                    .is_some_and(|since| value.begin_snapshot <= since.get())
                {
                    if selected.is_empty() {
                        self.after = Some((table, file));
                    }
                    continue;
                }
                selected.push((table, file));
                if selected.len() == data_file::PUBLISH_CONCURRENCY {
                    return selected;
                }
            }
        }
        selected
    }

    fn finish_round(&mut self) -> Result<bool> {
        self.snapshot = None;
        if !self.failed.is_empty() && (!self.retrying || self.overflow_after.is_none()) {
            self.retrying = true;
            self.retry_cursor = None;
        } else if let Some(after) = self.overflow_after.take() {
            self.retrying = false;
            self.after = match after {
                Bound::Excluded(position) | Bound::Included(position) => Some(position),
                Bound::Unbounded => None,
            };
        } else {
            self.complete = true;
            return Ok(true);
        }
        Err(Error::Constraint(
            "row summary publication has files to retry".into(),
        ))
    }

    /// Publishes at most four files, returning `true` when the pass is
    /// complete. Retries remember up to 1,024 failed file IDs; further
    /// failures retain a scan frontier instead, bounding progress memory
    /// without losing work.
    ///
    /// # Errors
    ///
    /// Returns the first file error in this step, a catalog read error, or a
    /// retry notice after reaching the end with failed files outstanding.
    pub async fn advance(
        &mut self,
        catalog: &ReadOnlyCatalog,
        data_store: &DataStore,
        data_prefix: &str,
    ) -> Result<bool> {
        if self.complete {
            return Ok(true);
        }
        let snapshot = if let Some(snapshot) = &self.snapshot {
            Arc::clone(snapshot)
        } else {
            let snapshot = catalog.snapshot().await?;
            self.snapshot = Some(Arc::clone(&snapshot));
            snapshot
        };
        let selected = self.select(&snapshot).await;
        if selected.is_empty() {
            return self.finish_round();
        }
        let mut results = stream::iter(selected)
            .map(|position| {
                let snapshot = &snapshot;
                async move {
                    let result = async {
                        let Some(file) = snapshot
                            .data_files
                            .get(&position.0)
                            .and_then(|files| files.get(&position.1))
                        else {
                            return Ok(false);
                        };
                        let table = TableId::new(position.0);
                        let table_prefix = snapshot.table_data_prefix(table)?;
                        catalog
                            .publish_file(
                                data_store,
                                data_prefix,
                                &table_prefix,
                                table,
                                data_file_info(file),
                            )
                            .await
                    }
                    .await;
                    (position, result)
                }
            })
            .buffered(data_file::PUBLISH_CONCURRENCY);
        let mut failure = None;
        while let Some((position, result)) = results.next().await {
            if let Err(error) = result {
                if self.retrying || self.failed.len() < MAX_RETRY_FILES {
                    self.failed.insert(position);
                } else {
                    self.overflow_after
                        .get_or_insert(self.after.map_or(Bound::Unbounded, Bound::Excluded));
                }
                failure.get_or_insert(error);
            } else {
                self.failed.remove(&position);
            }
            if self.retrying {
                self.retry_cursor = Some(position);
            } else {
                self.after = Some(position);
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overflow_retries_progress_despite_a_persistent_known_failure() {
        let mut progress = RowSummaryProgress::new(None, None);
        progress.failed.insert((1, 1));
        progress.retrying = true;
        progress.overflow_after = Some(Bound::Excluded((1, 1024)));
        assert!(progress.finish_round().is_err());
        assert!(!progress.retrying);
        assert_eq!(progress.after, Some((1, 1024)));
        assert!(progress.failed.contains(&(1, 1)));
    }
}
