//! Bounded publication steps with a resumable file frontier.

use std::collections::{BTreeSet, VecDeque};

use futures::{StreamExt, stream};

use super::{
    DataStore, Error, ReadOnlyCatalog, Result, SnapshotId, TableId, data_file,
    retry_queue::RetryQueue,
};
use crate::store::{handle::ScanShape, proto::DataFileValue, read::files::CurrentFiles};

mod context;
pub(super) use context::TableContext;

pub(super) type Position = (u64, u64);

const DISCOVERY_FILES: usize = 128;
const DISCOVERY_BYTES: usize = 256 * 1024;

type DiscoveredFile = crate::memory::WorkingValue<DataFileValue>;

struct SelectedFile {
    position: Position,
    file: Option<DiscoveredFile>,
}

#[derive(Default, Debug)]
struct PublicationCounts {
    discovered: u64,
    published: u64,
    skipped: u64,
    failed: u64,
}

/// Progress through one selected snapshot's row-summary publication.
/// Successful steps survive cancellation of a later step. Drop this value to
/// release its snapshot; progress is local to this process.
pub struct RowSummaryProgress {
    read: Option<TableContext>,
    tables: Option<BTreeSet<TableId>>,
    since: Option<SnapshotId>,
    after: Option<Position>,
    failed: RetryQueue,
    retries: RetryQueue,
    retrying: bool,
    complete: bool,
    files: Option<std::collections::VecDeque<Position>>,
    page: VecDeque<DiscoveredFile>,
    counts: PublicationCounts,
    reported: std::time::Instant,
}

impl RowSummaryProgress {
    /// Selects tables (`None` means all) and an exclusive snapshot cutoff.
    /// Keep the same catalog and data location for every step.
    #[must_use]
    pub fn new(tables: Option<Vec<TableId>>, since: Option<SnapshotId>) -> Self {
        Self {
            read: None,
            tables: tables.map(|tables| tables.into_iter().collect()),
            since,
            after: None,
            failed: RetryQueue::default(),
            retries: RetryQueue::default(),
            retrying: false,
            complete: false,
            files: None,
            page: VecDeque::new(),
            counts: PublicationCounts::default(),
            reported: std::time::Instant::now(),
        }
    }

    /// Publishes only the supplied files, without traversing the catalog.
    #[must_use]
    pub fn for_files(files: Vec<(TableId, crate::DataFileId)>) -> Self {
        let mut progress = Self::new(None, None);
        progress.counts.discovered = files.len() as u64;
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
    ) -> Result<Vec<SelectedFile>> {
        if self.retrying {
            return Ok(self
                .retries
                .peek(data_file::PUBLISH_CONCURRENCY)
                .await?
                .into_iter()
                .map(|position| SelectedFile {
                    position,
                    file: None,
                })
                .collect());
        }
        if let Some(files) = &self.files {
            return Ok(files
                .iter()
                .take(data_file::PUBLISH_CONCURRENCY)
                .copied()
                .map(|position| SelectedFile {
                    position,
                    file: None,
                })
                .collect());
        }

        if !self.page.is_empty() {
            crate::telemetry::read::decoded_reused(
                self.page.len().min(data_file::PUBLISH_CONCURRENCY),
            );
            return Ok(self.window());
        }
        let mut cursor =
            CurrentFiles::open(handle, self.after, ScanShape::bounded().retain()).await?;
        let mut selected = VecDeque::new();
        let mut bytes = 0;
        while let Some(file) = cursor.next().await? {
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
            let charge = usize::try_from(file.bytes()).unwrap_or(usize::MAX);
            if !selected.is_empty() && bytes + charge > DISCOVERY_BYTES {
                break;
            }
            bytes += charge;
            selected.push_back(file);
            if selected.len() == DISCOVERY_FILES || bytes >= DISCOVERY_BYTES {
                break;
            }
        }
        crate::telemetry::read::decoded_page(selected.len(), bytes);
        self.counts.discovered += selected.len() as u64;
        self.page = selected;
        Ok(self.window())
    }

    fn window(&self) -> Vec<SelectedFile> {
        self.page
            .iter()
            .take(data_file::PUBLISH_CONCURRENCY)
            .map(|file| SelectedFile {
                position: (file.table_id, file.data_file_id),
                file: Some(file.clone()),
            })
            .collect()
    }

    fn finish_round(&mut self) -> Result<bool> {
        if let Some(context) = self.read.take() {
            tracing::info!(
                table_context_loads = context.loads(),
                table_context_reuses = context.reuses(),
                "publication read scope released"
            );
        }
        if self.failed.is_empty() {
            self.complete = true;
            tracing::info!(?self.counts, "row summary publication completed");
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
        if self.read.is_none() {
            self.read = Some(TableContext::new(catalog.read_scope().await?));
        }
        let handle = self
            .read
            .as_ref()
            .ok_or_else(|| Error::Corruption("publication has no read scope".into()))?
            .reads()
            .owned_read_handle();
        let selected = self.select(handle.borrow()).await?;
        if selected.is_empty() {
            return self.finish_round();
        }
        let context = self
            .read
            .as_ref()
            .ok_or_else(|| Error::Corruption("publication has no read scope".into()))?;
        let mut results = stream::iter(selected)
            .map(|selected| {
                let position = selected.position;
                let handle = handle.borrow();
                async move {
                    let result = async {
                        let loaded;
                        let file = if let Some(discovered) = &selected.file {
                            &**discovered
                        } else {
                            loaded =
                                crate::store::read::files::file(handle, position.0, position.1)
                                    .await?;
                            let Some(file) = loaded.as_ref() else {
                                return Ok(false);
                            };
                            file
                        };
                        let prefix = context.resolve(TableId::new(file.table_id)).await?;
                        catalog
                            .publish_file(
                                data_store,
                                data_prefix,
                                &prefix,
                                TableId::new(file.table_id),
                                super::data_file_info(file),
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
            match &result {
                Ok(true) => self.counts.published += 1,
                Ok(false) => self.counts.skipped += 1,
                Err(_) => self.counts.failed += 1,
            }
            if let Err(error) = result {
                tracing::warn!(table_id = position.0, file_id = position.1, %error, ?self.counts, "row summary file will retry");
                self.failed.push(position).await?;
                failure.get_or_insert(error);
            }
            if self.retrying {
                self.retries.acknowledge(1);
            } else if let Some(files) = &mut self.files {
                files.pop_front();
            } else {
                self.after = Some(position);
                self.page.pop_front();
            }
        }
        if self.reported.elapsed() >= std::time::Duration::from_secs(10) {
            tracing::info!(?self.counts, table_context_loads = context.loads(), table_context_reuses = context.reuses(), remaining_in_page = self.page.len(), retained_decoded_bytes = self.page.iter().map(DiscoveredFile::bytes).sum::<u64>(), "row summary publication advanced");
            self.reported = std::time::Instant::now();
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
    use crate::store::{
        key::{EntityKey, Key},
        value,
    };

    #[tokio::test]
    async fn discovery_retains_a_page_across_unacknowledged_windows() {
        let objects = Arc::new(object_store::memory::InMemory::new());
        let catalog = crate::Catalog::open(objects, crate::CatalogOptions::default())
            .await
            .unwrap();
        let tx = catalog.begin_write_tx().unwrap();
        for id in 1..=200 {
            let file = DataFileValue {
                table_id: 1,
                data_file_id: id,
                ..Default::default()
            };
            tx.put(
                Key::current(EntityKey::File {
                    table_id: 1,
                    data_file_id: id,
                })
                .encode(),
                value::encode_value(&file),
            )
            .unwrap();
        }
        catalog.commit_write_tx(tx).await.unwrap();
        let scope = catalog.index_read_scope().await.unwrap();
        let handle = scope.reads().owned_read_handle();
        let mut progress = RowSummaryProgress::new(None, None);
        let first = progress.select(handle.borrow()).await.unwrap();
        assert_eq!(first.len(), 4);
        assert_eq!(progress.page.len(), DISCOVERY_FILES);
        let again = progress.select(handle.borrow()).await.unwrap();
        assert!(std::ptr::eq(
            &raw const **first[0].file.as_ref().unwrap(),
            &raw const **again[0].file.as_ref().unwrap()
        ));
        for item in &first {
            progress.after = Some(item.position);
            progress.page.pop_front();
        }
        let next = progress.select(handle.borrow()).await.unwrap();
        assert_eq!(next[0].position, (1, 5));
        assert_eq!(progress.page.len(), DISCOVERY_FILES - 4);
        drop(handle);
        drop(scope);
        catalog.close().await.unwrap();
    }

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
