//! Pure materialization of live inlined rows from already-scanned
//! `inline/insert` chunks and `inline/inline_delete` tombstones (see
//! [`crate::store::inline`] for the scans). No store I/O here; the read
//! model DuckLake's four inline scan variants select over.

use std::collections::HashMap;

use crate::store::{
    key::InlineOperation,
    proto::{InlineChunkValue, InlineInlineDeleteValue},
};

/// One inlined row, addressed by dense `row_id` and located in its chunk
/// by index + offset. `end_snapshot` is `None` until a matching
/// `inline/inline_delete` tombstones the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InlineRow {
    /// The row's dense id (`chunk.row_id_start + offset_in_chunk`).
    pub row_id: u64,
    /// The commit snapshot that inserted this row.
    pub begin_snapshot: u64,
    /// The commit snapshot that tombstoned this row, if any.
    pub end_snapshot: Option<u64>,
    /// Index into the `chunks` slice `materialize_inline_rows` was called
    /// with — the row's owning chunk.
    pub chunk: usize,
    /// The row's offset within its chunk's `row_count`.
    pub offset_in_chunk: u64,
}

/// Every row across every chunk — one per `(chunk, offset)`, `row_id =
/// chunk.row_id_start + offset` for `offset` in `0..chunk.row_count` —
/// with `end_snapshot` resolved from `inline_deletes`. Includes tombstoned
/// rows; callers apply the scan-kind predicate via [`InlineScanKind::select`].
/// `chunks` entries that are not `InlineOperation::Insert` are skipped.
pub fn materialize_inline_rows(
    chunks: &[(InlineOperation, InlineChunkValue)],
    inline_deletes: &[(u64, InlineInlineDeleteValue)],
) -> Vec<InlineRow> {
    let tombstones: HashMap<u64, u64> = inline_deletes
        .iter()
        .map(|(row_id, inline_delete)| (*row_id, inline_delete.end_snapshot))
        .collect();

    let mut rows = Vec::new();
    for (chunk_index, (op, value)) in chunks.iter().enumerate() {
        let InlineOperation::Insert { begin_snapshot, .. } = *op else {
            continue;
        };

        for offset in 0..value.row_count {
            let row_id = value.row_id_start + offset;
            // A tombstone ends only a version begun before it: an `UPDATE`
            // tombstones a row and re-inserts its `row_id` in one commit,
            // and the re-inserted version must stay live (DuckLake's own
            // writer guards `begin_snapshot != {SNAPSHOT_ID}`).
            let end_snapshot = tombstones
                .get(&row_id)
                .copied()
                .filter(|&end| begin_snapshot < end);
            rows.push(InlineRow {
                row_id,
                begin_snapshot,
                end_snapshot,
                chunk: chunk_index,
                offset_in_chunk: offset,
            });
        }
    }
    rows
}

/// The four ways DuckLake's inline reader queries a table, each a
/// predicate over `(begin_snapshot, end_snapshot)` at snapshot `S`
/// (optionally windowed from `start` for the incremental variants).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InlineScanKind {
    /// `SCAN_TABLE`: live rows at `S`.
    Table,
    /// `SCAN_INSERTIONS`: rows begun in `[start, S]`.
    Insertions,
    /// `SCAN_DELETIONS`: rows ended in `[start, S]`.
    Deletions,
    /// `SCAN_FOR_FLUSH`: every row begun at or before `S`, including rows
    /// already tombstoned — the flush input needs the full history to
    /// write both the data file and its deletion file.
    ForFlush,
}

impl InlineScanKind {
    /// Filters `rows` per this scan's predicate at snapshot `S`, ordered
    /// by `(row_id, begin_snapshot)` — only `ForFlush` can return more
    /// than one row per `row_id`, so only it observes the tiebreak.
    /// `start` is only read by `Insertions`/`Deletions`.
    #[must_use]
    pub fn select(self, rows: &[InlineRow], snapshot: u64, start: u64) -> Vec<InlineRow> {
        let included = |row: &InlineRow| match self {
            Self::Table => {
                row.begin_snapshot <= snapshot && row.end_snapshot.is_none_or(|end| snapshot < end)
            }
            Self::Insertions => row.begin_snapshot >= start && row.begin_snapshot <= snapshot,
            Self::Deletions => row
                .end_snapshot
                .is_some_and(|end| end >= start && end <= snapshot),
            Self::ForFlush => row.begin_snapshot <= snapshot,
        };

        let mut selected: Vec<InlineRow> = rows.iter().copied().filter(included).collect();
        selected.sort_by_key(|row| (row.row_id, row.begin_snapshot));
        selected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert(begin_snapshot: u64) -> InlineOperation {
        InlineOperation::Insert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot,
            chunk_seq: 0,
        }
    }

    fn chunk(row_id_start: u64, row_count: u64) -> InlineChunkValue {
        InlineChunkValue {
            body: Vec::new().into(),
            row_id_start,
            row_count,
            data_file_id: None,
        }
    }

    /// Three chunks (rows 0-1 begun at 1, rows 2-3 begun at 3, row 4
    /// begun at 5) and two tombstones (row 1 ends at 4, row 3 ends at
    /// 6), fed to `materialize_inline_rows` out of row-id order to prove
    /// `select` — not input order — determines the result order.
    fn fixture() -> Vec<InlineRow> {
        let chunks = vec![
            (insert(5), chunk(4, 1)),
            (insert(1), chunk(0, 2)),
            (insert(3), chunk(2, 2)),
        ];
        let inline_deletes = vec![
            (1, InlineInlineDeleteValue { end_snapshot: 4 }),
            (3, InlineInlineDeleteValue { end_snapshot: 6 }),
        ];
        materialize_inline_rows(&chunks, &inline_deletes)
    }

    fn row_ids(rows: &[InlineRow]) -> Vec<u64> {
        rows.iter().map(|row| row.row_id).collect()
    }

    #[test]
    fn materializes_every_row_with_dense_ids_and_resolved_tombstones() {
        let rows = fixture();
        assert_eq!(rows.len(), 5);

        let by_id: HashMap<u64, InlineRow> = rows.iter().map(|row| (row.row_id, *row)).collect();
        assert_eq!(by_id[&0].begin_snapshot, 1);
        assert_eq!(by_id[&0].end_snapshot, None);
        assert_eq!(by_id[&0].offset_in_chunk, 0);
        assert_eq!(by_id[&1].begin_snapshot, 1);
        assert_eq!(by_id[&1].end_snapshot, Some(4));
        assert_eq!(by_id[&1].offset_in_chunk, 1);
        assert_eq!(by_id[&2].begin_snapshot, 3);
        assert_eq!(by_id[&2].end_snapshot, None);
        assert_eq!(by_id[&3].begin_snapshot, 3);
        assert_eq!(by_id[&3].end_snapshot, Some(6));
        assert_eq!(by_id[&4].begin_snapshot, 5);
        assert_eq!(by_id[&4].end_snapshot, None);
    }

    #[test]
    fn table_scan_excludes_rows_not_yet_begun() {
        let rows = fixture();
        assert_eq!(row_ids(&InlineScanKind::Table.select(&rows, 2, 0)), [0, 1]);
    }

    #[test]
    fn table_scan_shows_tombstoned_row_only_before_its_end_snapshot() {
        let rows = fixture();
        // Row 1 ends at 4: present at 3, absent from 4 onward.
        assert_eq!(
            row_ids(&InlineScanKind::Table.select(&rows, 3, 0)),
            [0, 1, 2, 3]
        );
        assert_eq!(
            row_ids(&InlineScanKind::Table.select(&rows, 4, 0)),
            [0, 2, 3]
        );
    }

    #[test]
    fn table_scan_at_a_later_snapshot_drops_both_tombstoned_rows_and_orders_by_row_id() {
        let rows = fixture();
        assert_eq!(
            row_ids(&InlineScanKind::Table.select(&rows, 6, 0)),
            [0, 2, 4]
        );
    }

    #[test]
    fn insertions_scan_returns_rows_begun_in_the_window() {
        let rows = fixture();
        assert_eq!(
            row_ids(&InlineScanKind::Insertions.select(&rows, 4, 2)),
            [2, 3]
        );
    }

    #[test]
    fn deletions_scan_returns_exactly_the_tombstoned_rows_in_the_window() {
        let rows = fixture();
        assert_eq!(row_ids(&InlineScanKind::Deletions.select(&rows, 5, 1)), [1]);
        assert_eq!(row_ids(&InlineScanKind::Deletions.select(&rows, 6, 5)), [3]);
        assert_eq!(
            row_ids(&InlineScanKind::Deletions.select(&rows, 6, 1)),
            [1, 3]
        );
    }

    /// An `UPDATE` tombstones a row and re-inserts the same `row_id` in
    /// one commit. The tombstone ends only the pre-existing version
    /// (DuckLake's own writer guards `begin_snapshot != {SNAPSHOT_ID}`);
    /// the re-inserted version stays live.
    #[test]
    fn tombstone_does_not_end_the_same_commit_reinsertion() {
        let chunks = vec![(insert(2), chunk(0, 1)), (insert(5), chunk(0, 1))];
        let inline_deletes = vec![(0, InlineInlineDeleteValue { end_snapshot: 5 })];
        let rows = materialize_inline_rows(&chunks, &inline_deletes);

        assert_eq!(rows.len(), 2);
        assert_eq!((rows[0].begin_snapshot, rows[0].end_snapshot), (2, Some(5)));
        assert_eq!((rows[1].begin_snapshot, rows[1].end_snapshot), (5, None));

        // The updated row is live at its own snapshot; the deletions scan
        // reports only the pre-existing version.
        assert_eq!(InlineScanKind::Table.select(&rows, 5, 0).len(), 1);
        let deleted = InlineScanKind::Deletions.select(&rows, 5, 5);
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0].begin_snapshot, 2);
    }

    #[test]
    fn for_flush_scan_includes_superseded_tombstoned_rows() {
        let rows = fixture();
        // Row 1 is tombstoned by snapshot 4, but SCAN_FOR_FLUSH still
        // needs it to write the deletion file.
        assert_eq!(
            row_ids(&InlineScanKind::ForFlush.select(&rows, 4, 0)),
            [0, 1, 2, 3]
        );
        assert_eq!(
            row_ids(&InlineScanKind::ForFlush.select(&rows, 10, 0)),
            [0, 1, 2, 3, 4]
        );
    }
}
