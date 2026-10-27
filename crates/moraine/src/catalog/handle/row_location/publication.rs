//! Bounded publication steps with a resumable file frontier.

use std::{collections::BTreeSet, ops::Bound, sync::Arc};

use futures::{StreamExt, stream};

use super::{
    CatalogSnapshot, DataStore, Error, ReadOnlyCatalog, Result, SnapshotId, TableId, data_file,
    data_file_info, retry_queue::RetryQueue,
};

pub(super) type Position = (u64, u64);

/// Progress through one selected snapshot's row-summary publication.
/// Successful steps survive cancellation of a later step. Drop this value to
/// release its snapshot; progress is local to this process.
pub struct RowSummaryProgress {
    snapshot: Option<Arc<CatalogSnapshot>>,
    tables: Option<BTreeSet<TableId>>,
    since: Option<SnapshotId>,
    after: Option<Position>,
    failed: RetryQueue,
    retries: RetryQueue,
    retrying: bool,
    complete: bool,
    files: Option<std::collections::VecDeque<Position>>,
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
            failed: RetryQueue::default(),
            retries: RetryQueue::default(),
            retrying: false,
            complete: false,
            files: None,
        }
    }

    /// Publishes only the supplied files, without traversing the catalog.
    #[must_use]
    pub fn for_files(files: Vec<(TableId, crate::DataFileId)>) -> Self {
        let mut progress = Self::new(None, None);
        progress.files = Some(
            files
                .into_iter()
                .map(|(table, file)| (table.get(), file.get()))
                .collect(),
        );
        progress
    }

    async fn select(&mut self, snapshot: &CatalogSnapshot) -> Result<Vec<Position>> {
        if self.retrying {
            return self.retries.peek(data_file::PUBLISH_CONCURRENCY).await;
        }
        if let Some(files) = &self.files {
            return Ok(files
                .iter()
                .take(data_file::PUBLISH_CONCURRENCY)
                .copied()
                .collect());
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
                    return Ok(selected);
                }
            }
        }
        Ok(selected)
    }

    fn finish_round(&mut self) -> Result<bool> {
        self.snapshot = None;
        if self.failed.is_empty() {
            self.complete = true;
            return Ok(true);
        }
        self.retrying = true;
        self.retries = std::mem::take(&mut self.failed);
        Err(Error::Constraint(
            "row summary publication has files to retry".into(),
        ))
    }

    /// Publishes at most four files, returning `true` when the pass is
    /// complete. Failed file IDs spill to temporary storage after 1,024
    /// resident entries, so retries never rescan successful files.
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
        let selected = self.select(&snapshot).await?;
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
                self.failed.push(position).await?;
                failure.get_or_insert(error);
            }
            if self.retrying {
                self.retries.acknowledge(1);
            } else if let Some(files) = &mut self.files {
                files.pop_front();
            } else {
                self.after = Some(position);
            }
        }
        // A summary build sorts and encodes row IDs for every file in the
        // window; that scratch is gone once the window is done.
        crate::memory::release_after_phase();

        match failure {
            Some(error) => Err(error),
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn retry_queue_spills_without_losing_or_revisiting_files() {
        let mut queue = RetryQueue::default();
        for file in 0..2053 {
            queue.push((1, file)).await.unwrap();
        }
        assert!(queue.resident_len() <= 1024);
        for start in (0..2053).step_by(4) {
            let expected: Vec<_> = (start..(start + 4).min(2053))
                .map(|file| (1, file))
                .collect();
            assert_eq!(queue.peek(4).await.unwrap(), expected);
            assert_eq!(
                queue.peek(4).await.unwrap(),
                expected,
                "an interrupted step does not advance"
            );
            queue.acknowledge(expected.len());
        }
        assert!(queue.is_empty());
    }
}
