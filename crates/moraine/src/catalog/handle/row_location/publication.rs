//! Bounded publication steps with a resumable file frontier.

use std::{collections::BTreeSet, ops::Bound};

use futures::{StreamExt, stream};

use super::{
    DataStore, Error, ReadOnlyCatalog, Result, SnapshotId, TableId, data_file,
    retry_queue::RetryQueue,
};
use crate::store::{
    handle::ScanShape,
    key::{EntityKey, EntityKind, Key, current_entity_kind_prefix},
    proto::DataFileValue,
    value,
};

pub(super) type Position = (u64, u64);

/// Progress through one selected snapshot's row-summary publication.
/// Successful steps survive cancellation of a later step. Drop this value to
/// release its snapshot; progress is local to this process.
pub struct RowSummaryProgress {
    handle: Option<crate::store::handle::OwnedReadHandle>,
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
            handle: None,
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

    async fn select(
        &mut self,
        handle: crate::store::handle::ReadHandle<'_>,
    ) -> Result<Vec<Position>> {
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

        let prefix = current_entity_kind_prefix(EntityKind::File);
        let lower = self
            .after
            .map_or(Bound::Unbounded, |(table_id, data_file_id)| {
                Bound::Excluded(
                    Key::current(EntityKey::File {
                        table_id,
                        data_file_id,
                    })
                    .encode()[prefix.len()..]
                        .to_vec(),
                )
            });
        let mut cursor = handle
            .scan_prefix(prefix, (lower, Bound::Unbounded), ScanShape::Streaming)
            .await?;
        let mut selected = Vec::new();
        while let Some(entry) = cursor.next().await? {
            let file: DataFileValue = value::decode_value(&entry.value)?;
            if self
                .tables
                .as_ref()
                .is_some_and(|tables| !tables.contains(&TableId::new(file.table_id)))
                || self
                    .since
                    .is_some_and(|since| file.begin_snapshot <= since.get())
            {
                if selected.is_empty() {
                    self.after = Some((file.table_id, file.data_file_id));
                }
                continue;
            }
            selected.push((file.table_id, file.data_file_id));
            if selected.len() == data_file::PUBLISH_CONCURRENCY {
                break;
            }
        }
        Ok(selected)
    }

    fn finish_round(&mut self) -> Result<bool> {
        self.handle = None;
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
        if self.handle.is_none() {
            let scope = catalog.index_read_scope().await?.ok_or_else(|| {
                Error::Constraint("publication requires a fixed read scope".into())
            })?;
            self.handle = Some(scope.reads().owned_read_handle());
        }
        let handle = self
            .handle
            .clone()
            .ok_or_else(|| Error::Corruption("publication has no read scope".into()))?;
        let selected = self.select(handle.borrow()).await?;
        if selected.is_empty() {
            return self.finish_round();
        }
        let mut results = stream::iter(selected)
            .map(|position| {
                let handle = handle.borrow();
                async move {
                    let result = async {
                        use crate::store::{
                            key::{EntityKey, Key},
                            proto::DataFileValue,
                            read::read_singleton,
                        };
                        let Some(file): Option<DataFileValue> = read_singleton(
                            handle,
                            Key::current(EntityKey::File {
                                table_id: position.0,
                                data_file_id: position.1,
                            }),
                        )
                        .await?
                        else {
                            return Ok(false);
                        };
                        catalog
                            .publish_registered_file(handle, data_store, data_prefix, &file)
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
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn empty_publication_does_not_decode_unrelated_entities() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let catalog = crate::Catalog::open(store.clone(), crate::CatalogOptions::default())
            .await
            .unwrap();
        let tx = catalog.begin_write_tx().unwrap();
        tx.put(
            crate::store::key::Key::current(crate::store::key::EntityKey::View { view_id: 99 })
                .encode(),
            [0xff],
        )
        .unwrap();
        catalog.commit_write_tx(tx).await.unwrap();
        let mut progress = RowSummaryProgress::for_files(Vec::new());
        let data = DataStore::new(store);
        assert!(progress.advance(&catalog, &data, "").await.unwrap());
        catalog.close().await.unwrap();
    }

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
