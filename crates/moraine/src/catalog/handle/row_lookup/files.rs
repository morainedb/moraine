//! File membership through verified dense intervals and conservative sparse
//! probes.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use imbl::{OrdMap, ordmap::DiffItem};
use prost::Message as _;
use tracing::{debug, warn};

use super::{FileDirectory, Intervals};
use crate::{
    catalog::{
        CatalogSnapshot, DataFileId, DataFileInfo, ReadOnlyCatalog, TableId,
        snapshot::data_file_info,
    },
    data_file::{DataStore, FileSummary},
    error::Result,
    store::proto::DataFileValue,
};

type Placements = HashMap<u64, Vec<DataFileId>>;

/// Lookups a failed summary waits before it is read again, doubling with
/// each retry that fails.
const FIRST_RETRY_SKIP: u32 = 16;

/// The longest that wait grows to.
const MAX_RETRY_SKIP: u32 = 4096;

/// The lookups to wait after `retries` consecutive failures.
fn skip_for(retries: u32) -> u32 {
    FIRST_RETRY_SKIP
        .checked_shl(retries)
        .unwrap_or(MAX_RETRY_SKIP)
        .min(MAX_RETRY_SKIP)
}

/// Non-overlapping row ranges keyed by first row, `start -> (last, file)`,
/// with each file's start for removal. A range that would overlap a live
/// one is refused.
#[derive(Clone, Default)]
pub(super) struct DenseRanges {
    by_start: OrdMap<u64, (u64, u64)>,
    starts: OrdMap<u64, u64>,
}

impl DenseRanges {
    /// Admits `file` holding `start..=end`; `false` when a live range
    /// overlaps it.
    fn insert(&mut self, start: u64, end: u64, file: u64) -> bool {
        let overlaps = self
            .by_start
            .get_prev(&end)
            .is_some_and(|(_, (last, _))| *last >= start);
        if overlaps {
            return false;
        }

        self.by_start.insert(start, (end, file));
        self.starts.insert(file, start);
        true
    }

    fn remove(&mut self, file: u64) {
        if let Some(start) = self.starts.remove(&file) {
            self.by_start.remove(&start);
        }
    }

    /// The file whose range holds `row`, if any.
    fn file_holding(&self, row: u64) -> Option<u64> {
        self.by_start
            .get_prev(&row)
            .and_then(|(_, (end, file))| (row <= *end).then_some(*file))
    }

    pub(super) fn estimated_bytes(&self) -> u64 {
        // Two entries per file: `(start, end, file)` and `(file, start)`.
        u64::try_from(self.by_start.len().saturating_mul(40)).unwrap_or(u64::MAX)
    }
}

/// What a directory resolves files against; a directory built under a
/// different scope is not reused.
struct DirectoryScope<'a> {
    store: &'a DataStore,
    data_prefix: &'a str,
    table_prefix: &'a str,
    table: TableId,
}

impl FileDirectory {
    fn estimated_bytes(&self) -> u64 {
        let summary_bytes = self.summaries.values().fold(0_u64, |bytes, summary| {
            bytes
                .saturating_add(summary.estimated_bytes())
                .saturating_add(16)
        });
        self.ranges
            .estimated_bytes()
            .saturating_add(self.spans.estimated_bytes())
            .saturating_add(summary_bytes)
            .saturating_add(self.spanned.len() as u64 * 16)
            .saturating_add(
                u64::try_from(self.failed.capacity().saturating_mul(8)).unwrap_or(u64::MAX),
            )
            .saturating_add(self.file_bytes)
    }

    fn retained_summary(
        &self,
        scope: &DirectoryScope<'_>,
        file: &DataFileInfo,
    ) -> Option<FileSummary> {
        if !self.built_under(scope) || data_file_info(self.files.get(&file.id.get())?) != *file {
            return None;
        }
        let mut summary = self.summaries.get(&file.id.get())?.clone();
        summary.built = false;
        Some(summary)
    }

    fn empty(scope: &DirectoryScope<'_>) -> Self {
        Self {
            identity: scope.store.cache_identity(),
            data_prefix: scope.data_prefix.into(),
            table_prefix: scope.table_prefix.into(),
            files: OrdMap::new(),
            summaries: OrdMap::new(),
            ranges: DenseRanges::default(),
            spanned: OrdMap::new(),
            spans: Arc::new(Intervals::new([])),
            failed: Vec::new(),
            failed_retries: 0,
            retry_skip: AtomicU32::new(0),
            file_bytes: 0,
            bytes: 0,
        }
    }

    fn built_under(&self, scope: &DirectoryScope<'_>) -> bool {
        self.identity == scope.store.cache_identity()
            && self.data_prefix == scope.data_prefix
            && self.table_prefix == scope.table_prefix
    }

    /// Whether this directory already describes `files` with no summary
    /// read due.
    fn describes(&self, files: &OrdMap<u64, DataFileValue>) -> bool {
        self.files.ptr_eq(files) && !self.retry_due()
    }

    /// Whether the files that failed to summarize have waited out their
    /// skip and are to be read again.
    fn retry_due(&self) -> bool {
        !self.failed.is_empty() && self.retry_skip.load(Ordering::Relaxed) == 0
    }

    /// Counts one lookup against the wait before the next retry.
    fn note_lookup(&self) {
        let _ = self
            .retry_skip
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |skip| {
                skip.checked_sub(1)
            });
    }

    /// Keeps nonempty summaries for the span index built on refresh.
    fn admit_summary(&mut self, file: u64, summary: FileSummary) {
        if summary.bounds().is_some() {
            self.spanned.insert(file, summary);
        }
    }

    fn forget(&mut self, file: u64) {
        self.summaries.remove(&file);
        self.ranges.remove(file);
        self.spanned.remove(&file);
    }

    /// The files that hold, or may hold, each of `row_ids`, and how many
    /// summaries were probed to say so.
    fn place(&self, row_ids: &[u64]) -> (Placements, usize) {
        let mut placements = Placements::new();
        let mut probes = 0;
        for &row in row_ids {
            if let Some(file) = self.ranges.file_holding(row) {
                placements
                    .entry(row)
                    .or_default()
                    .push(DataFileId::new(file));
            }
            self.spans.visit(row, |&file| {
                probes += 1;
                if self
                    .spanned
                    .get(&file)
                    .is_some_and(|summary| summary.contains(row))
                {
                    placements
                        .entry(row)
                        .or_default()
                        .push(DataFileId::new(file));
                }
            });
        }

        for file in &self.failed {
            for &row in row_ids {
                placements
                    .entry(row)
                    .or_default()
                    .push(DataFileId::new(*file));
            }
        }

        (placements, probes)
    }
}

/// What advancing a directory to a new file map selects: files to summarize,
/// files to drop, and the encoded size of the new map.
struct FileChanges {
    selected: BTreeMap<u64, DataFileInfo>,
    removed: HashSet<u64>,
    file_bytes: u64,
}

impl FileDirectory {
    /// The changes from this directory's file map to `files`. When
    /// `retry_failed`, every failed file still present is selected again.
    fn changes_to(&self, files: &OrdMap<u64, DataFileValue>, retry_failed: bool) -> FileChanges {
        let mut changes = FileChanges {
            selected: BTreeMap::new(),
            removed: HashSet::new(),
            file_bytes: self.file_bytes,
        };
        for change in self.files.diff(files) {
            match change {
                DiffItem::Add(id, value) => {
                    changes.selected.insert(*id, data_file_info(value));
                    changes.file_bytes = changes.file_bytes.saturating_add(encoded_bytes(value));
                }
                DiffItem::Update {
                    old: (id, before),
                    new: (_, after),
                } => {
                    changes.removed.insert(*id);
                    changes.selected.insert(*id, data_file_info(after));
                    changes.file_bytes = changes
                        .file_bytes
                        .saturating_sub(encoded_bytes(before))
                        .saturating_add(encoded_bytes(after));
                }
                DiffItem::Remove(id, value) => {
                    changes.removed.insert(*id);
                    changes.file_bytes = changes.file_bytes.saturating_sub(encoded_bytes(value));
                }
            }
        }

        if retry_failed {
            for id in &self.failed {
                if !changes.removed.contains(id)
                    && let Some(value) = files.get(id)
                {
                    changes
                        .selected
                        .entry(*id)
                        .or_insert_with(|| data_file_info(value));
                }
            }
        }

        changes
    }
}

fn encoded_bytes(value: &DataFileValue) -> u64 {
    u64::try_from(value.encoded_len()).unwrap_or(u64::MAX)
}

impl ReadOnlyCatalog {
    pub(in crate::catalog::handle) fn retained_file_summary(
        &self,
        store: &DataStore,
        data_prefix: &str,
        table_prefix: &str,
        table: TableId,
        file: &DataFileInfo,
    ) -> Option<FileSummary> {
        let scope = DirectoryScope {
            store,
            data_prefix,
            table_prefix,
            table,
        };
        super::lookup(&self.row_lookups.files, table)?.retained_summary(&scope, file)
    }

    pub(in crate::catalog::handle) async fn locate_files(
        &self,
        store: &DataStore,
        data_prefix: &str,
        snapshot: &CatalogSnapshot,
        table: TableId,
        row_ids: &[u64],
    ) -> Result<Placements> {
        let table_prefix = snapshot.table_data_prefix(table)?;
        let Some(files) = snapshot.data_files.get(&table.get()) else {
            return Ok(HashMap::new());
        };
        let scope = DirectoryScope {
            store,
            data_prefix,
            table_prefix: &table_prefix,
            table,
        };

        let held = super::lookup(&self.row_lookups.files, table)
            .filter(|directory| directory.built_under(&scope));
        if let Some(directory) = &held {
            directory.note_lookup();
        }
        let directory = match held {
            Some(directory) if directory.describes(files) => directory,
            held => {
                let previous = held.unwrap_or_else(|| Arc::new(FileDirectory::empty(&scope)));
                let directory = Arc::new(self.refresh_directory(&scope, &previous, files).await);
                super::install(&self.row_lookups.files, table, Arc::clone(&directory));
                directory
            }
        };

        let (placements, probes) = directory.place(row_ids);
        self.row_lookups.note_summary_probes(probes);
        Ok(placements)
    }

    /// `previous` advanced to `files`: only files it has not summarized,
    /// and those it failed to, are read.
    async fn refresh_directory(
        &self,
        scope: &DirectoryScope<'_>,
        previous: &FileDirectory,
        files: &OrdMap<u64, DataFileValue>,
    ) -> FileDirectory {
        let retry_failed = previous.retry_due();
        let FileChanges {
            selected,
            removed,
            file_bytes,
        } = previous.changes_to(files, retry_failed);

        let mut directory = FileDirectory {
            identity: scope.store.cache_identity(),
            data_prefix: scope.data_prefix.into(),
            table_prefix: scope.table_prefix.into(),
            files: files.clone(),
            summaries: previous.summaries.clone(),
            ranges: previous.ranges.clone(),
            spanned: previous.spanned.clone(),
            spans: previous.spans.clone(),
            failed: if retry_failed {
                Vec::new()
            } else {
                previous
                    .failed
                    .iter()
                    .copied()
                    .filter(|file| !removed.contains(file) && files.contains_key(file))
                    .collect()
            },
            failed_retries: previous.failed_retries,
            retry_skip: AtomicU32::new(previous.retry_skip.load(Ordering::Relaxed)),
            file_bytes,
            bytes: 0,
        };
        for file in &removed {
            directory.forget(*file);
        }

        self.row_lookups.note_summarized(selected.len());
        if !selected.is_empty() {
            debug!(
                table_id = scope.table.get(),
                files = selected.len(),
                retrying_failed = retry_failed,
                "reading data file row summaries"
            );
        }
        let summaries = self
            .file_summaries(
                scope.store,
                scope.data_prefix,
                scope.table_prefix,
                scope.table,
                selected.into_values().collect(),
            )
            .await;
        for (file, summary) in summaries {
            if let Ok(summary) = &summary {
                directory.summaries.insert(file.get(), summary.clone());
            }
            match summary {
                Ok(summary) => match summary.dense_range() {
                    Some(range) => {
                        // An empty range is a file holding no rows.
                        if let Some(end) =
                            range.end.checked_sub(1).filter(|end| *end >= range.start)
                            && !directory.ranges.insert(range.start, end, file.get())
                        {
                            directory.admit_summary(file.get(), summary);
                        }
                    }
                    None => directory.admit_summary(file.get(), summary),
                },
                Err(error) => {
                    warn!(table_id = scope.table.get(), data_file_id = file.get(), %error,
                        "row location fell back to every requested row for this file");
                    directory.failed.push(file.get());
                }
            }
        }
        if !directory.spanned.ptr_eq(&previous.spanned) {
            directory.spans = Arc::new(Intervals::new(directory.spanned.iter().filter_map(
                |(&file, summary)| summary.bounds().map(|(first, last)| (first, last, file)),
            )));
        }
        directory.failed.sort_unstable();
        directory.failed.dedup();
        if directory.failed.is_empty() {
            directory.failed_retries = 0;
            directory.retry_skip.store(0, Ordering::Relaxed);
        } else if retry_failed || previous.failed.is_empty() {
            if retry_failed {
                directory.failed_retries = previous.failed_retries.saturating_add(1);
            }
            directory
                .retry_skip
                .store(skip_for(directory.failed_retries), Ordering::Relaxed);
        }

        directory.bytes = directory.estimated_bytes();

        directory
    }
}

#[cfg(test)]
mod tests;
