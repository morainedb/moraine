//! Moving a staged commit's head-derived values onto the head it lands on.
//!
//! DuckLake stamps a staged commit's snapshot id, file ids, row ids and
//! statistics against the head it read. Landing it on a later head —
//! behind other members of a batch, or after commits that won a race —
//! means moving every one of those by the distance the head moved, which
//! is the arithmetic DuckLake's own retry performs when it re-drives.

use std::collections::{BTreeMap, BTreeSet};

use imbl::OrdMap;

use super::{CatalogSnapshot, EntityKey, RowOperation, TableKind, decode, proto};

/// How far one table's row-id counter moved between a member's base and
/// the premise it lands on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RowShift {
    pub(super) base_next_row_id: u64,
    pub(super) delta: u64,
}

/// The distances between the head a member read and the head it lands on.
#[derive(Debug, Clone)]
pub(super) struct Rebase {
    /// The head the member read: references above it are the member's own.
    pub(super) base_head: u64,
    pub(super) snapshot_delta: u64,
    /// The member allocated file ids from here.
    pub(super) base_next_file_id: u64,
    pub(super) file_delta: u64,
    /// The counters the member read and the counters it lands on: what it
    /// changed between the two is carried across as a delta, which for a
    /// data-only member is nothing.
    pub(super) base_next_catalog_id: u64,
    pub(super) next_catalog_id: u64,
    pub(super) base_schema_version: u64,
    pub(super) schema_version: u64,
    /// Per table the premise appended to since the member's base.
    pub(super) rows: BTreeMap<u64, RowShift>,
    pub(super) base_table_stats: OrdMap<u64, proto::TableStatsValue>,
    pub(super) premise_table_stats: OrdMap<u64, proto::TableStatsValue>,
    pub(super) base_column_stats: OrdMap<u64, OrdMap<u64, proto::TableColumnStatsValue>>,
    pub(super) premise_column_stats: OrdMap<u64, OrdMap<u64, proto::TableColumnStatsValue>>,
    pub(super) column_types: OrdMap<u64, OrdMap<u64, proto::ColumnValue>>,
}

impl Rebase {
    /// No movement: the member lands on the head it read.
    pub(super) fn identity(base: &CatalogSnapshot) -> Self {
        Self::between(base, base)
    }

    /// The distances from `base`, the head the member read, to `premise`.
    pub(super) fn between(base: &CatalogSnapshot, premise: &CatalogSnapshot) -> Self {
        let rows = premise
            .table_stats
            .iter()
            .filter_map(|(table_id, premise_stats)| {
                let base_next_row_id = base
                    .table_stats
                    .get(table_id)
                    .map_or(0, |stats| stats.next_row_id);
                let delta = premise_stats.next_row_id.checked_sub(base_next_row_id)?;
                (delta > 0).then_some((
                    *table_id,
                    RowShift {
                        base_next_row_id,
                        delta,
                    },
                ))
            })
            .collect();

        Self {
            base_head: base.snapshot.snapshot_id,
            snapshot_delta: premise
                .snapshot
                .snapshot_id
                .saturating_sub(base.snapshot.snapshot_id),
            base_next_file_id: base.snapshot.next_file_id,
            file_delta: premise
                .snapshot
                .next_file_id
                .saturating_sub(base.snapshot.next_file_id),
            base_next_catalog_id: base.snapshot.next_catalog_id,
            next_catalog_id: premise.snapshot.next_catalog_id,
            base_schema_version: base.snapshot.schema_version,
            schema_version: premise.snapshot.schema_version,
            rows,
            base_table_stats: base.table_stats.clone(),
            premise_table_stats: premise.table_stats.clone(),
            base_column_stats: base.table_column_stats.clone(),
            premise_column_stats: premise.table_column_stats.clone(),
            column_types: premise.columns.clone(),
        }
    }

    /// Nothing moves, and no snapshot is needed to say so: for readers of
    /// staged rows that run outside a commit.
    pub(super) fn none() -> Self {
        Self {
            base_head: u64::MAX,
            snapshot_delta: 0,
            base_next_file_id: u64::MAX,
            file_delta: 0,
            base_next_catalog_id: 0,
            next_catalog_id: 0,
            base_schema_version: 0,
            schema_version: 0,
            rows: BTreeMap::new(),
            base_table_stats: OrdMap::new(),
            premise_table_stats: OrdMap::new(),
            base_column_stats: OrdMap::new(),
            premise_column_stats: OrdMap::new(),
            column_types: OrdMap::new(),
        }
    }

    /// Whether anything moves at all.
    pub(super) fn is_identity(&self) -> bool {
        self.snapshot_delta == 0 && self.file_delta == 0 && self.rows.is_empty()
    }

    /// A snapshot reference: the member's own if above the head it read.
    pub(super) fn snapshot(&self, reference: u64) -> u64 {
        if reference > self.base_head {
            reference.saturating_add(self.snapshot_delta)
        } else {
            reference
        }
    }

    /// A file id: the member's own if at or above the counter it read.
    pub(super) fn file_id(&self, id: u64) -> u64 {
        if id >= self.base_next_file_id {
            id.saturating_add(self.file_delta)
        } else {
            id
        }
    }

    /// A row id in `table_id`: the member's own if at or above the
    /// counter it read for that table.
    pub(super) fn row_id(&self, table_id: u64, row_id: u64) -> u64 {
        match self.rows.get(&table_id) {
            Some(shift) if row_id >= shift.base_next_row_id => row_id.saturating_add(shift.delta),
            _ => row_id,
        }
    }

    /// The snapshot record: its own id moved, each counter the premise's
    /// plus whatever the member advanced it by.
    pub(super) fn snapshot_value(&self, value: &mut proto::SnapshotValue) {
        value.snapshot_id = self.snapshot(value.snapshot_id);
        value.schema_version = self.schema_version.saturating_add(
            value
                .schema_version
                .saturating_sub(self.base_schema_version),
        );
        value.next_catalog_id = self.next_catalog_id.saturating_add(
            value
                .next_catalog_id
                .saturating_sub(self.base_next_catalog_id),
        );
        value.next_file_id = self.file_id(value.next_file_id);
        for id in &mut value.deleted_data_file_ids {
            *id = self.file_id(*id);
        }
    }

    pub(super) fn data_file(&self, value: &mut proto::DataFileValue) {
        value.data_file_id = self.file_id(value.data_file_id);
        value.begin_snapshot = self.snapshot(value.begin_snapshot);
        value.end_snapshot = value.end_snapshot.map(|end| self.snapshot(end));
        value.partial_max = value.partial_max.map(|at| self.snapshot(at));
        value.row_id_start = value
            .row_id_start
            .map(|start| self.row_id(value.table_id, start));
    }

    pub(super) fn delete_file(&self, value: &mut proto::DeleteFileValue) {
        value.delete_file_id = self.file_id(value.delete_file_id);
        value.data_file_id = self.file_id(value.data_file_id);
        value.begin_snapshot = self.snapshot(value.begin_snapshot);
        value.end_snapshot = value.end_snapshot.map(|end| self.snapshot(end));
        value.partial_max = value.partial_max.map(|at| self.snapshot(at));
    }

    /// Table statistics are absolute in the row DuckLake wrote; what the
    /// member meant is the change against its base, reapplied to the
    /// premise.
    pub(super) fn table_stats(&self, value: &mut proto::TableStatsValue) {
        let base = self.base_table_stats.get(&value.table_id);
        let Some(premise) = self.premise_table_stats.get(&value.table_id) else {
            return;
        };
        let carry = |written: u64, read: fn(&proto::TableStatsValue) -> u64| {
            let delta = written.saturating_sub(base.map_or(0, read));
            read(premise).saturating_add(delta)
        };
        value.record_count = carry(value.record_count, |stats| stats.record_count);
        value.next_row_id = carry(value.next_row_id, |stats| stats.next_row_id);
        value.file_size_bytes = carry(value.file_size_bytes, |stats| stats.file_size_bytes);
    }

    /// Column statistics: the member's own where nothing moved them in
    /// between, else widened over the premise's by the column's type.
    pub(super) fn table_column_stats(&self, value: &mut proto::TableColumnStatsValue) {
        let at = |stats: &OrdMap<u64, OrdMap<u64, proto::TableColumnStatsValue>>| {
            stats
                .get(&value.table_id)
                .and_then(|columns| columns.get(&value.column_id))
                .cloned()
        };
        let (Some(premise), base) = (at(&self.premise_column_stats), at(&self.base_column_stats))
        else {
            return;
        };
        if base.as_ref() == Some(&premise) {
            return;
        }
        let column_type = self
            .column_types
            .get(&value.table_id)
            .and_then(|columns| columns.get(&value.column_id))
            .map(|column| column.column_type.as_str())
            .unwrap_or_default();

        value.contains_null = merge_flag(premise.contains_null, value.contains_null);
        value.contains_nan = merge_flag(premise.contains_nan, value.contains_nan);
        value.min_value = merge_bound(
            column_type,
            premise.min_value.as_deref(),
            value.min_value.as_deref(),
            Bound::Lower,
        );
        value.max_value = merge_bound(
            column_type,
            premise.max_value.as_deref(),
            value.max_value.as_deref(),
            Bound::Upper,
        );
        value.min_is_exact = merge_exact(premise.min_is_exact, value.min_is_exact);
        value.max_is_exact = merge_exact(premise.max_is_exact, value.max_is_exact);
        if premise.extra_stats != value.extra_stats {
            value.extra_stats = None;
        }
    }

    pub(super) fn file_column_stats(&self, value: &mut proto::FileColumnStatsValue) {
        value.data_file_id = self.file_id(value.data_file_id);
    }

    /// An entity key naming a file the member may have allocated.
    pub(super) fn entity_key(&self, key: EntityKey) -> EntityKey {
        match key {
            EntityKey::File {
                table_id,
                data_file_id,
            } => EntityKey::File {
                table_id,
                data_file_id: self.file_id(data_file_id),
            },
            EntityKey::DeleteFile {
                table_id,
                delete_file_id,
            } => EntityKey::DeleteFile {
                table_id,
                delete_file_id: self.file_id(delete_file_id),
            },
            other => other,
        }
    }

    /// The inline operations with their references moved; every other
    /// operation is carried as it was, its cells decoded under this rebase
    /// where they are applied.
    pub(super) fn inline_ops(&self, ops: &[RowOperation]) -> Vec<RowOperation> {
        ops.iter()
            .map(|op| match op {
                RowOperation::InlineInsert {
                    table_id,
                    schema_version,
                    begin_snapshot,
                    row_id_start,
                    row_count,
                    arrow_body,
                } => RowOperation::InlineInsert {
                    table_id: *table_id,
                    schema_version: *schema_version,
                    begin_snapshot: self.snapshot(*begin_snapshot),
                    row_id_start: self.row_id(*table_id, *row_id_start),
                    row_count: *row_count,
                    arrow_body: arrow_body.clone(),
                },
                RowOperation::InlineInlineDelete {
                    table_id,
                    row_id,
                    end_snapshot,
                } => RowOperation::InlineInlineDelete {
                    table_id: *table_id,
                    row_id: self.row_id(*table_id, *row_id),
                    end_snapshot: self.snapshot(*end_snapshot),
                },
                RowOperation::InlineFileDelete {
                    table_id,
                    data_file_id,
                    row_id,
                    begin_snapshot,
                } => RowOperation::InlineFileDelete {
                    table_id: *table_id,
                    data_file_id: self.file_id(*data_file_id),
                    row_id: self.row_id(*table_id, *row_id),
                    begin_snapshot: self.snapshot(*begin_snapshot),
                },
                RowOperation::InlineFileDeleteRemove {
                    table_id,
                    data_file_id,
                    row_id,
                } => RowOperation::InlineFileDeleteRemove {
                    table_id: *table_id,
                    data_file_id: self.file_id(*data_file_id),
                    row_id: self.row_id(*table_id, *row_id),
                },
                other => other.clone(),
            })
            .collect()
    }
}

/// Whether a staged commit's shape is one the rebase re-derives exactly: a
/// snapshot-minting, data-only commit. Anything that names a catalog id or
/// a schema version — DDL, an inline table's creation, maintenance — is not.
pub(super) fn joinable(ops: &[RowOperation]) -> bool {
    let mut mints = false;
    for op in ops {
        let allowed = match op {
            RowOperation::Insert { table, .. } => {
                mints |= *table == TableKind::Snapshot;
                matches!(
                    table,
                    TableKind::Snapshot
                        | TableKind::SnapshotChanges
                        | TableKind::DataFile
                        | TableKind::DeleteFile
                        | TableKind::TableStats
                        | TableKind::TableColumnStats
                        | TableKind::FileColumnStats
                        | TableKind::FilePartitionValue
                )
            }
            RowOperation::UpdateSetBegin { table, .. } => *table == TableKind::DataFile,
            RowOperation::UpdateSetEnd { table, .. } => {
                matches!(table, TableKind::DataFile | TableKind::DeleteFile)
            }
            RowOperation::InlineInsert { .. }
            | RowOperation::InlineInlineDelete { .. }
            | RowOperation::InlineFileDelete { .. }
            | RowOperation::InlineFileDeleteRemove { .. } => true,
            _ => false,
        };
        if !allowed {
            return false;
        }
    }
    mints
}

/// The tables a commit's rows write.
pub(super) fn tables_touched(ops: &[RowOperation]) -> BTreeSet<u64> {
    ops.iter()
        .filter_map(|op| match op {
            RowOperation::Insert { table, cells } => match table {
                TableKind::DataFile => decode::decode_data_file(cells).ok().map(|f| f.table_id),
                TableKind::DeleteFile => decode::decode_delete_file(cells).ok().map(|f| f.table_id),
                TableKind::TableStats => decode::decode_table_stats(cells).ok().map(|s| s.table_id),
                _ => None,
            },
            RowOperation::InlineInsert { table_id, .. }
            | RowOperation::InlineInlineDelete { table_id, .. }
            | RowOperation::InlineFileDelete { table_id, .. }
            | RowOperation::InlineFileDeleteRemove { table_id, .. } => Some(*table_id),
            _ => None,
        })
        .collect()
}

/// Whether any of `tables` carries an equality index in `snapshot`.
pub(super) fn indexed(snapshot: &CatalogSnapshot, tables: &BTreeSet<u64>) -> bool {
    tables.iter().any(|table_id| {
        snapshot
            .indexes
            .get(table_id)
            .is_some_and(|indexes| !indexes.is_empty())
    })
}

/// Which end of a column's range a bound is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Bound {
    Lower,
    Upper,
}

/// Widens one bound over another by the column's type. `None` — unknown —
/// when either side is unknown, when a value does not parse as its type,
/// or when the type has no order this can read.
pub(super) fn merge_bound(
    column_type: &str,
    premise: Option<&str>,
    member: Option<&str>,
    bound: Bound,
) -> Option<String> {
    let (premise, member) = (premise?, member?);
    let ordering = compare(column_type, premise, member)?;
    let keep_premise = match bound {
        Bound::Lower => ordering.is_le(),
        Bound::Upper => ordering.is_ge(),
    };
    Some(if keep_premise { premise } else { member }.to_owned())
}

/// Orders two values of `column_type` as DuckLake writes them.
fn compare(column_type: &str, left: &str, right: &str) -> Option<std::cmp::Ordering> {
    let normalized = column_type
        .split('(')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase();
    match normalized.as_str() {
        "TINYINT" | "SMALLINT" | "INTEGER" | "INT" | "BIGINT" | "HUGEINT" | "UTINYINT"
        | "USMALLINT" | "UINTEGER" | "UBIGINT" | "UHUGEINT" => Some(
            left.trim()
                .parse::<i128>()
                .ok()?
                .cmp(&right.trim().parse::<i128>().ok()?),
        ),
        "FLOAT" | "REAL" | "DOUBLE" => left
            .trim()
            .parse::<f64>()
            .ok()?
            .partial_cmp(&right.trim().parse::<f64>().ok()?),
        "DECIMAL" | "NUMERIC" => compare_decimal(left.trim(), right.trim()),
        "VARCHAR"
        | "TEXT"
        | "STRING"
        | "CHAR"
        | "BOOLEAN"
        | "BOOL"
        | "DATE"
        | "TIME"
        | "TIMETZ"
        | "TIMESTAMP"
        | "TIMESTAMPTZ"
        | "TIMESTAMP_S"
        | "TIMESTAMP_MS"
        | "TIMESTAMP_NS"
        | "TIMESTAMP WITH TIME ZONE"
        | "TIME WITH TIME ZONE"
        | "UUID" => Some(left.cmp(right)),
        _ => None,
    }
}

/// Orders two decimal literals without going through a float: sign, then
/// integer digits by length and value, then fraction digits padded out.
fn compare_decimal(left: &str, right: &str) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;

    fn parts(text: &str) -> Option<(bool, String, String)> {
        let (negative, digits) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text.strip_prefix('+').unwrap_or(text)),
        };
        let (integer, fraction) = digits.split_once('.').unwrap_or((digits, ""));
        if integer.is_empty() && fraction.is_empty() {
            return None;
        }
        if !integer.bytes().all(|b| b.is_ascii_digit())
            || !fraction.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let integer = integer.trim_start_matches('0');
        let fraction = fraction.trim_end_matches('0');
        let zero = integer.is_empty() && fraction.is_empty();
        Some((negative && !zero, integer.to_owned(), fraction.to_owned()))
    }

    let (left_negative, left_integer, left_fraction) = parts(left)?;
    let (right_negative, right_integer, right_fraction) = parts(right)?;
    let magnitude = left_integer
        .len()
        .cmp(&right_integer.len())
        .then_with(|| left_integer.cmp(&right_integer))
        .then_with(|| {
            let width = left_fraction.len().max(right_fraction.len());
            format!("{left_fraction:0<width$}").cmp(&format!("{right_fraction:0<width$}"))
        });
    Some(match (left_negative, right_negative) {
        (false, false) => magnitude,
        (true, true) => magnitude.reverse(),
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
    })
}

/// A presence flag over two sides: known true if either is, known false
/// only if both are, else unknown.
fn merge_flag(premise: Option<bool>, member: Option<bool>) -> Option<bool> {
    match (premise, member) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

/// Exactness survives a merge only when both sides had it.
fn merge_exact(premise: Option<bool>, member: Option<bool>) -> Option<bool> {
    match (premise, member) {
        (Some(left), Some(right)) => Some(left && right),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn rebase() -> Rebase {
        Rebase {
            base_head: 10,
            snapshot_delta: 3,
            base_next_file_id: 100,
            file_delta: 7,
            base_next_catalog_id: 40,
            next_catalog_id: 50,
            base_schema_version: 2,
            schema_version: 4,
            rows: [(
                1,
                RowShift {
                    base_next_row_id: 1_000,
                    delta: 250,
                },
            )]
            .into(),
            base_table_stats: [(1_u64, stats(1, 1_000, 1_000, 4_096))]
                .into_iter()
                .collect(),
            premise_table_stats: [(1_u64, stats(1, 1_250, 1_250, 5_120))]
                .into_iter()
                .collect(),
            base_column_stats: OrdMap::new(),
            premise_column_stats: OrdMap::new(),
            column_types: OrdMap::new(),
        }
    }

    fn stats(
        table_id: u64,
        record_count: u64,
        next_row_id: u64,
        bytes: u64,
    ) -> proto::TableStatsValue {
        proto::TableStatsValue {
            table_id,
            record_count,
            next_row_id,
            file_size_bytes: bytes,
        }
    }

    #[test]
    fn a_snapshot_reference_above_the_base_head_is_the_members_own_and_shifts() {
        let rebase = rebase();
        assert_eq!(rebase.snapshot(11), 14);
        assert_eq!(
            rebase.snapshot(10),
            10,
            "the base head itself is committed state"
        );
        assert_eq!(rebase.snapshot(3), 3);
    }

    #[test]
    fn a_file_id_at_or_above_the_base_counter_was_allocated_by_the_member() {
        let rebase = rebase();
        assert_eq!(rebase.file_id(100), 107);
        assert_eq!(rebase.file_id(102), 109);
        assert_eq!(rebase.file_id(99), 99);
    }

    #[test]
    fn a_row_id_shifts_by_its_own_tables_advance_only() {
        let rebase = rebase();
        assert_eq!(rebase.row_id(1, 1_000), 1_250);
        assert_eq!(rebase.row_id(1, 999), 999);
        assert_eq!(
            rebase.row_id(2, 1_000),
            1_000,
            "a table nothing else touched"
        );
    }

    #[test]
    fn table_stats_carry_the_members_delta_onto_the_premise() {
        let rebase = rebase();
        // The member read 1,000 rows and wrote 1,010: ten rows appended.
        let mut written = stats(1, 1_010, 1_010, 4_196);
        rebase.table_stats(&mut written);
        assert_eq!(written, stats(1, 1_260, 1_260, 5_220));

        // A table the member is the first to write starts from nothing.
        let mut fresh = stats(2, 5, 5, 64);
        rebase.table_stats(&mut fresh);
        assert_eq!(fresh, stats(2, 5, 5, 64));
    }

    #[test]
    fn the_snapshot_record_takes_the_premises_counters_and_its_own_shifted_id() {
        let rebase = rebase();
        let mut snapshot = proto::SnapshotValue {
            snapshot_id: 11,
            schema_version: 2,
            next_catalog_id: 40,
            next_file_id: 103,
            ..Default::default()
        };
        rebase.snapshot_value(&mut snapshot);
        assert_eq!(snapshot.snapshot_id, 14);
        assert_eq!(snapshot.schema_version, 4);
        assert_eq!(snapshot.next_catalog_id, 50);
        assert_eq!(snapshot.next_file_id, 110);
    }

    /// What a commit advanced a counter by survives the move: a lone DDL
    /// commit under the identity rebase keeps the bump it authored.
    #[test]
    fn a_counter_the_member_advanced_stays_advanced() {
        let mut rebase = rebase();
        rebase.snapshot_delta = 0;
        rebase.next_catalog_id = rebase.base_next_catalog_id;
        rebase.schema_version = rebase.base_schema_version;
        let mut ddl = proto::SnapshotValue {
            snapshot_id: 11,
            schema_version: 3,
            next_catalog_id: 42,
            ..Default::default()
        };
        rebase.snapshot_value(&mut ddl);
        assert_eq!(ddl.snapshot_id, 11);
        assert_eq!(ddl.schema_version, 3);
        assert_eq!(ddl.next_catalog_id, 42);
    }

    #[test]
    fn a_data_file_moves_its_id_its_window_and_its_row_ids_together() {
        let rebase = rebase();
        let mut file = proto::DataFileValue {
            data_file_id: 101,
            table_id: 1,
            begin_snapshot: 11,
            end_snapshot: None,
            row_id_start: Some(1_000),
            ..Default::default()
        };
        rebase.data_file(&mut file);
        assert_eq!(file.data_file_id, 108);
        assert_eq!(file.begin_snapshot, 14);
        assert_eq!(file.row_id_start, Some(1_250));
    }

    #[test]
    fn inline_operations_are_rewritten_on_a_copy() {
        let rebase = rebase();
        let ops = vec![
            RowOperation::InlineInsert {
                table_id: 1,
                schema_version: 2,
                begin_snapshot: 11,
                row_id_start: 1_000,
                row_count: 3,
                arrow_body: Bytes::new(),
            },
            RowOperation::InlineInlineDelete {
                table_id: 1,
                row_id: 5,
                end_snapshot: 11,
            },
        ];
        let rebased = rebase.inline_ops(&ops);
        match &rebased[0] {
            RowOperation::InlineInsert {
                begin_snapshot,
                row_id_start,
                ..
            } => {
                assert_eq!(*begin_snapshot, 14);
                assert_eq!(*row_id_start, 1_250);
            }
            other => panic!("{other:?}"),
        }
        match &rebased[1] {
            RowOperation::InlineInlineDelete {
                row_id,
                end_snapshot,
                ..
            } => {
                assert_eq!(
                    *row_id, 5,
                    "an existing row is the target, not the member's"
                );
                assert_eq!(*end_snapshot, 14);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(ops.len(), rebased.len());
    }

    #[test]
    fn bounds_merge_by_the_columns_type_not_its_text() {
        assert_eq!(
            merge_bound("BIGINT", Some("9"), Some("10"), Bound::Lower).as_deref(),
            Some("9")
        );
        assert_eq!(
            merge_bound("BIGINT", Some("9"), Some("10"), Bound::Upper).as_deref(),
            Some("10")
        );
        assert_eq!(
            merge_bound("VARCHAR", Some("9"), Some("10"), Bound::Lower).as_deref(),
            Some("10")
        );
        assert_eq!(
            merge_bound("DOUBLE", Some("-1.5"), Some("2"), Bound::Lower).as_deref(),
            Some("-1.5")
        );
        assert_eq!(
            merge_bound("DATE", Some("2024-01-02"), Some("2023-12-31"), Bound::Upper).as_deref(),
            Some("2024-01-02")
        );
        assert_eq!(
            merge_bound("BOOLEAN", Some("true"), Some("false"), Bound::Lower).as_deref(),
            Some("false")
        );
    }

    #[test]
    fn an_unknown_bound_on_either_side_leaves_the_merge_unknown() {
        assert_eq!(merge_bound("BIGINT", None, Some("1"), Bound::Lower), None);
        assert_eq!(merge_bound("BIGINT", Some("1"), None, Bound::Upper), None);
        assert_eq!(
            merge_bound("BLOB", Some("a"), Some("b"), Bound::Lower),
            None,
            "unorderable"
        );
        assert_eq!(
            merge_bound("BIGINT", Some("x"), Some("1"), Bound::Lower),
            None,
            "unparseable"
        );
    }

    #[test]
    fn only_data_shapes_may_join_a_batch() {
        let data = |table| RowOperation::Insert {
            table,
            cells: Vec::new(),
        };
        assert!(joinable(&[
            data(TableKind::Snapshot),
            data(TableKind::SnapshotChanges),
            data(TableKind::DataFile),
            data(TableKind::DeleteFile),
            data(TableKind::TableStats),
            data(TableKind::TableColumnStats),
            data(TableKind::FileColumnStats),
            data(TableKind::FilePartitionValue),
        ]));
        assert!(!joinable(&[
            data(TableKind::Snapshot),
            data(TableKind::Table)
        ]));
        assert!(!joinable(&[
            data(TableKind::Snapshot),
            data(TableKind::Schema)
        ]));
        assert!(!joinable(&[
            data(TableKind::Snapshot),
            RowOperation::InlineSchema {
                table_id: 1,
                schema_version: 1,
                arrow_schema: Vec::new(),
            },
        ]));
        assert!(!joinable(&[
            data(TableKind::Snapshot),
            RowOperation::Delete {
                table: TableKind::Snapshot,
                cells: Vec::new()
            },
        ]));
        assert!(!joinable(&[]), "nothing to mint a snapshot with");
    }
}
