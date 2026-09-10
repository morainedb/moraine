//! File membership through verified dense intervals and conservative sparse
//! probes.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use imbl::{OrdMap, ordmap::DiffItem};
use prost::Message as _;
use tracing::warn;

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
            ranges: Intervals::new([]),
            arbitrary: HashMap::new(),
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

    /// The files that hold, or may hold, each of `row_ids`.
    fn place(&self, row_ids: &[u64]) -> Placements {
        let mut placements = Placements::new();
        for &row in row_ids {
            self.ranges.visit(row, |file| {
                placements
                    .entry(row)
                    .or_default()
                    .push(DataFileId::new(*file));
            });
        }

        for (file, summary) in &self.arbitrary {
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

        placements
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

        Ok(directory.place(row_ids))
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

        let mut ranges: Vec<_> = previous
            .ranges
            .iter()
            .filter(|(_, _, file)| !removed.contains(*file))
            .map(|(start, end, file)| (start, end, *file))
            .collect();
        let mut arbitrary: HashMap<u64, FileSummary> = previous
            .arbitrary
            .iter()
            .filter(|(file, _)| !removed.contains(*file))
            .map(|(file, summary)| (*file, summary.clone()))
            .collect();
        let mut failed = Vec::new();

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
                        {
                            ranges.push((range.start, end, file.get()));
                        }
                    }
                    None => {
                        arbitrary.insert(file.get(), summary);
                    }
                },
                Err(error) => {
                    warn!(table_id = scope.table.get(), data_file_id = file.get(), %error,
                        "row location fell back to every requested row for this file");
                    failed.push(file.get());
                }
            }
        }
        failed.sort_unstable();

        let ranges = Intervals::new(ranges);
        let bytes = ranges
            .estimated_bytes()
            .saturating_add(
                arbitrary
                    .values()
                    .map(|summary| summary.estimated_bytes().saturating_add(16))
                    .sum(),
            )
            .saturating_add(u64::try_from(failed.capacity().saturating_mul(8)).unwrap_or(u64::MAX))
            .saturating_add(file_bytes);

        FileDirectory {
            identity: scope.store.cache_identity(),
            data_prefix: scope.data_prefix.into(),
            table_prefix: scope.table_prefix.into(),
            files: files.clone(),
            ranges,
            arbitrary,
            failed,
            file_bytes,
            bytes,
        }
    }
}

#[cfg(test)]
mod tests;
