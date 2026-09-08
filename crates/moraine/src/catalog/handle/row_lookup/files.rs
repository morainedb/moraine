//! File membership through verified dense intervals and conservative sparse
//! probes.

use std::{collections::HashMap, sync::Arc};

use prost::Message as _;
use tracing::warn;

use super::{FileDirectory, Intervals};
use crate::{
    catalog::{CatalogSnapshot, DataFileId, ReadOnlyCatalog, TableId},
    data_file::{DataStore, FileSummary},
    error::Result,
};

type Placements = HashMap<u64, Vec<DataFileId>>;

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
        let cached = self
            .row_lookups
            .files
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&table)
            .filter(|directory| {
                directory.identity == store.cache_identity()
                    && directory.data_prefix == data_prefix
                    && directory.table_prefix == table_prefix
                    && directory.files.ptr_eq(files)
            })
            .cloned();
        let mut placements = HashMap::new();
        let directory = if let Some(directory) = cached {
            let selected = directory
                .arbitrary
                .iter()
                .filter_map(|id| files.get(id))
                .map(crate::catalog::snapshot::data_file_info)
                .collect();
            for (file, summary) in self
                .file_summaries(store, data_prefix, &table_prefix, table, selected)
                .await
            {
                add_summary(&mut placements, table, file, summary, row_ids);
            }
            directory
        } else {
            let mut ranges = Vec::new();
            let mut arbitrary = Vec::new();
            for (file, summary) in self
                .file_summaries(
                    store,
                    data_prefix,
                    &table_prefix,
                    table,
                    snapshot.data_files_of(table),
                )
                .await
            {
                if let Some(range) = summary.as_ref().ok().and_then(FileSummary::dense_range) {
                    if let Some(end) = range.end.checked_sub(1).filter(|end| *end >= range.start) {
                        ranges.push((range.start, end, file.get()));
                    }
                } else {
                    arbitrary.push(file.get());
                    add_summary(&mut placements, table, file, summary, row_ids);
                }
            }
            let ranges = Intervals::new(ranges);
            let bytes = ranges
                .estimated_bytes()
                .saturating_add(
                    u64::try_from(arbitrary.capacity().saturating_mul(8)).unwrap_or(u64::MAX),
                )
                .saturating_add(
                    files
                        .values()
                        .map(|file| u64::try_from(file.encoded_len()).unwrap_or(u64::MAX))
                        .sum(),
                );
            let directory = Arc::new(FileDirectory {
                identity: store.cache_identity(),
                data_prefix: data_prefix.into(),
                table_prefix,
                files: files.clone(),
                ranges,
                bytes,
                arbitrary,
            });
            super::install(&self.row_lookups.files, table, directory.clone());
            directory
        };
        for &row in row_ids {
            directory.ranges.visit(row, |file| {
                placements
                    .entry(row)
                    .or_default()
                    .push(DataFileId::new(*file));
            });
        }
        Ok(placements)
    }
}

fn add_summary(
    placements: &mut Placements,
    table: TableId,
    file: DataFileId,
    summary: Result<FileSummary>,
    requested: &[u64],
) {
    let matched = match summary {
        Ok(summary) => summary.matching(requested),
        Err(error) => {
            warn!(table_id = table.get(), data_file_id = file.get(), %error,
                "row location fell back to every requested row for this file");
            requested.to_vec()
        }
    };
    for row in matched {
        placements.entry(row).or_default().push(file);
    }
}
