//! Cost estimates from exact positions and projected Parquet page sizes.

use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::ParquetRecordBatchStreamBuilder},
    file::{metadata::PageIndexPolicy, page_index::offset_index::OffsetIndexMetaData},
};

use super::{
    ParquetFile, RowIdSource, RowPositions, columns::resolve_row_id_source, corrupt,
    read_projection, reader::ObjectStoreReader,
};
use crate::error::Result;

#[derive(Default, Clone, Copy)]
pub(crate) struct ReadCoverage {
    pub(crate) selected_bytes: u64,
    pub(crate) total_bytes: u64,
    pub(crate) selected_ranges: u64,
    pub(crate) total_ranges: u64,
    pub(crate) groups: usize,
}

impl ReadCoverage {
    pub(crate) fn add(&mut self, other: Self) {
        self.selected_bytes = self.selected_bytes.saturating_add(other.selected_bytes);
        self.total_bytes = self.total_bytes.saturating_add(other.total_bytes);
        self.selected_ranges = self.selected_ranges.saturating_add(other.selected_ranges);
        self.total_ranges = self.total_ranges.saturating_add(other.total_ranges);
        self.groups = self.groups.saturating_add(other.groups);
    }

    pub(crate) fn selective(&self) -> bool {
        // Small files do not justify another reader solely on a coverage estimate.
        let selected = self
            .selected_bytes
            .saturating_add(self.selected_ranges.saturating_mul(8192));
        let total = self
            .total_bytes
            .saturating_add(self.total_ranges.saturating_mul(8192));
        self.total_bytes == 0
            || self.groups < 16
            || selected.saturating_mul(5) < total.saturating_mul(4)
    }
}

pub(crate) async fn read_coverage(
    file: &ParquetFile,
    requested: &[usize],
    positions: &RowPositions,
    row_id_start: Option<u64>,
) -> Result<ReadCoverage> {
    let reader = ObjectStoreReader::new(file, PageIndexPolicy::Optional);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(
        reader,
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional),
    )
    .await
    .map_err(corrupt("scan coverage"))?;
    let (row_id, _) = resolve_row_id_source(
        builder.parquet_schema(),
        RowIdSource::Resolve { row_id_start },
        &file.path,
    )?;
    let (mask, _, _, _) = read_projection(&builder, file.columns.as_deref(), requested, row_id)?;
    let metadata = builder.metadata();
    let mut coverage = ReadCoverage::default();
    let mut start = 0_u64;
    for (group_index, group) in metadata.row_groups().iter().enumerate() {
        let rows = u64::try_from(group.num_rows()).map_err(corrupt("scan coverage rows"))?;
        let end = start.saturating_add(rows);
        let selected =
            &positions.as_slice()[positions.as_slice().partition_point(|&row| row < start)
                ..positions.as_slice().partition_point(|&row| row < end)];
        coverage.groups += 1;
        for (column_index, column) in group.columns().iter().enumerate() {
            if !mask.leaf_included(column_index) {
                continue;
            }
            let bytes =
                u64::try_from(column.compressed_size()).map_err(corrupt("scan coverage bytes"))?;
            coverage.total_bytes = coverage.total_bytes.saturating_add(bytes);
            coverage.total_ranges += 1;
            if selected.is_empty() {
                continue;
            }
            let group_page_index = metadata.page_index_for_row_group(group_index);
            let pages = group_page_index
                .offset_index(column_index)
                .map(OffsetIndexMetaData::page_locations);
            if let Some(pages) = pages.filter(|pages| !pages.is_empty()) {
                let mut selected_bytes = 0_u64;
                let mut runs = 0;
                let mut previous_selected = false;
                for (index, page) in pages.iter().enumerate() {
                    let from = u64::try_from(page.first_row_index)
                        .map_err(corrupt("scan coverage page"))?;
                    let to = pages
                        .get(index + 1)
                        .map_or(Ok(rows), |next| u64::try_from(next.first_row_index))
                        .map_err(corrupt("scan coverage page"))?;
                    let offset = selected.partition_point(|&row| row < start.saturating_add(from));
                    let hit = selected
                        .get(offset)
                        .is_some_and(|&row| row < start.saturating_add(to));
                    if hit {
                        selected_bytes = selected_bytes.saturating_add(
                            u64::try_from(page.compressed_page_size)
                                .map_err(corrupt("scan coverage page bytes"))?,
                        );
                        if !previous_selected {
                            runs += 1;
                        }
                    }
                    previous_selected = hit;
                }
                // Dictionary/header bytes accompany any selected data pages.
                let page_bytes: u64 = pages
                    .iter()
                    .filter_map(|page| u64::try_from(page.compressed_page_size).ok())
                    .sum();
                coverage.selected_bytes = coverage
                    .selected_bytes
                    .saturating_add(selected_bytes)
                    .saturating_add(bytes.saturating_sub(page_bytes));
                coverage.selected_ranges += runs;
            } else {
                coverage.selected_bytes = coverage.selected_bytes.saturating_add(bytes);
                coverage.selected_ranges += 1;
            }
        }
        start = end;
    }
    Ok(coverage)
}
