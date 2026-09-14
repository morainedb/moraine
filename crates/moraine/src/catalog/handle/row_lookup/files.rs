//! File membership through verified dense intervals and conservative sparse
//! probes.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use imbl::{OrdMap, ordmap::DiffItem};
use prost::Message as _;
use tracing::warn;

use super::FileDirectory;
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
    fn empty(scope: &DirectoryScope<'_>) -> Self {
        Self {
            identity: scope.store.cache_identity(),
            data_prefix: scope.data_prefix.into(),
            table_prefix: scope.table_prefix.into(),
            files: OrdMap::new(),
            ranges: DenseRanges::default(),
            spanned: OrdMap::new(),
            spans: DenseRanges::default(),
            probed: OrdMap::new(),
            failed: Vec::new(),
            file_bytes: 0,
            bytes: 0,
        }
    }

    fn built_under(&self, scope: &DirectoryScope<'_>) -> bool {
        self.identity == scope.store.cache_identity()
            && self.data_prefix == scope.data_prefix
            && self.table_prefix == scope.table_prefix
    }

    /// Whether this directory already describes `files` with nothing left
    /// to retry.
    fn describes(&self, files: &OrdMap<u64, DataFileValue>) -> bool {
        self.files.ptr_eq(files) && self.failed.is_empty()
    }

    /// Keeps `summary` for `file`: behind its span when no kept span
    /// overlaps it, otherwise probed on every lookup.
    fn admit_summary(&mut self, file: u64, summary: FileSummary) {
        let spanned = summary
            .bounds()
            .is_some_and(|(first, last)| self.spans.insert(first, last, file));
        if spanned {
            self.spanned.insert(file, summary);
        } else {
            self.probed.insert(file, summary);
        }
    }

    fn forget(&mut self, file: u64) {
        self.ranges.remove(file);
        self.spans.remove(file);
        self.spanned.remove(&file);
        self.probed.remove(&file);
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
            if let Some(file) = self.spans.file_holding(row) {
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
            }
        }

        for (file, summary) in &self.probed {
            probes += row_ids.len();
            for row in summary.matching(row_ids) {
                placements
                    .entry(row)
                    .or_default()
                    .push(DataFileId::new(*file));
            }
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
    /// The changes from this directory's file map to `files`, with every
    /// failed file still present selected again.
    fn changes_to(&self, files: &OrdMap<u64, DataFileValue>) -> FileChanges {
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

        changes
    }
}

fn encoded_bytes(value: &DataFileValue) -> u64 {
    u64::try_from(value.encoded_len()).unwrap_or(u64::MAX)
}

impl ReadOnlyCatalog {
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
        let FileChanges {
            selected,
            removed,
            file_bytes,
        } = previous.changes_to(files);

        let mut directory = FileDirectory {
            identity: scope.store.cache_identity(),
            data_prefix: scope.data_prefix.into(),
            table_prefix: scope.table_prefix.into(),
            files: files.clone(),
            ranges: previous.ranges.clone(),
            spanned: previous.spanned.clone(),
            spans: previous.spans.clone(),
            probed: previous.probed.clone(),
            failed: Vec::new(),
            file_bytes,
            bytes: 0,
        };
        for file in &removed {
            directory.forget(*file);
        }

        self.row_lookups.note_summarized(selected.len());
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
        directory.failed.sort_unstable();

        let summary_bytes = directory
            .spanned
            .values()
            .chain(directory.probed.values())
            .map(|summary| summary.estimated_bytes().saturating_add(16))
            .sum::<u64>();
        directory.bytes = directory
            .ranges
            .estimated_bytes()
            .saturating_add(directory.spans.estimated_bytes())
            .saturating_add(summary_bytes)
            .saturating_add(
                u64::try_from(directory.failed.capacity().saturating_mul(8)).unwrap_or(u64::MAX),
            )
            .saturating_add(file_bytes);

        directory
    }
}

#[cfg(test)]
mod tests;
