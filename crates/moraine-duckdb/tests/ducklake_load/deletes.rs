//! Which path a DELETE against flushed inline data takes, and what
//! decides it. DuckLake checks three things in order: whether the
//! deleted row count fits `data_inlining_row_limit` (inline the
//! deletion), whether it covers the file entirely (drop the file), and
//! otherwise writes a delete file. Only the third writes data, so the
//! shape of the flushed file decides what a delete costs.
//!
//! These pin DuckLake's behaviour rather than moraine's, because a
//! deployment tunes against them: `data_inlining_row_limit` and the
//! partition spec are the two settings that move a delete between the
//! three paths.

use crate::helpers::*;

/// Rows for `parents` parents, 8 at a time so every insert stays under
/// the default `data_inlining_row_limit` of 10 and inlines, then
/// flushed to Parquet. 16 rows per parent, in two chunks.
fn seed_flushed_rows(store: &std::path::Path, data_path: &std::path::Path, parents: u64) {
    let inserts: String = (1..=parents)
        .flat_map(|parent| {
            [
                format!("INSERT INTO lake.main.child SELECT {parent}, range FROM range(0,8);"),
                format!("INSERT INTO lake.main.child SELECT {parent}, range FROM range(8,16);"),
            ]
        })
        .collect::<Vec<_>>()
        .join("\n");
    run_ducklake_sql(store, data_path, &inserts);
    run_ducklake_sql(
        store,
        data_path,
        "CALL ducklake_flush_inlined_data('lake');",
    );
}

fn count(store: &std::path::Path, sql: &str) -> String {
    csv_rows(&run_standalone_sql(store, sql))
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
        .expect("a single count row")
}

fn live_data_files(store: &std::path::Path) -> String {
    count(
        store,
        "SELECT count(*) FROM m.ducklake_data_file WHERE end_snapshot IS NULL;",
    )
}

fn ended_data_files(store: &std::path::Path) -> String {
    count(
        store,
        "SELECT count(*) FROM m.ducklake_data_file WHERE end_snapshot IS NOT NULL;",
    )
}

fn live_delete_files(store: &std::path::Path) -> String {
    count(
        store,
        "SELECT count(*) FROM m.ducklake_delete_file WHERE end_snapshot IS NULL;",
    )
}

fn rows_per_live_file(store: &std::path::Path) -> Vec<Vec<String>> {
    csv_rows(&run_standalone_sql(
        store,
        "SELECT record_count FROM m.ducklake_data_file WHERE end_snapshot IS NULL \
         ORDER BY record_count;",
    ))
}

/// A flush concentrates every inlined chunk of an unpartitioned table
/// into one file, so a delete that covers part of it — and exceeds the
/// inlining limit, so no tombstone absorbs it — writes a delete file.
/// This is the expensive path: an object-store write per delete, and a
/// full rewrite of the delete file on each subsequent one.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_partial_delete_of_flushed_inline_data_writes_a_delete_file() {
    let dir = TempDir::new("delete-partial-store");
    let data_dir = TempDir::new("delete-partial-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.child (parent_id BIGINT, line BIGINT);",
    );
    seed_flushed_rows(store, data_path, 3);

    assert_eq!(live_data_files(store), "1", "one file per flush per table");
    assert_eq!(rows_per_live_file(store), vec![vec!["48"]]);

    // 16 rows, over the limit of 10, against a 48-row file: neither the
    // inline path nor the whole-file drop applies.
    run_ducklake_sql(
        store,
        data_path,
        "DELETE FROM lake.main.child WHERE parent_id = 1;",
    );

    assert_eq!(live_delete_files(store), "1");
    assert_eq!(live_data_files(store), "1", "the data file survives");
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT count(*) FROM lake.main.child;",
        )),
        vec![vec!["32"]]
    );
}

/// The same concentrated file, deleted from narrowly: a delete within
/// the inlining limit records tombstones and writes nothing, whatever
/// the file's size. Narrowing what a delete covers is therefore
/// independent of how the flush shaped the file — the limit is compared
/// against the delete, not the file.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_narrow_delete_of_flushed_inline_data_inlines_its_deletions() {
    let dir = TempDir::new("delete-narrow-store");
    let data_dir = TempDir::new("delete-narrow-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.child (parent_id BIGINT, line BIGINT);",
    );
    seed_flushed_rows(store, data_path, 3);
    assert_eq!(rows_per_live_file(store), vec![vec!["48"]]);

    // 8 rows, under the limit of 10, against the same 48-row file the
    // 16-row delete above wrote a delete file for.
    run_ducklake_sql(
        store,
        data_path,
        "DELETE FROM lake.main.child WHERE parent_id = 1 AND line < 8;",
    );

    assert_eq!(live_delete_files(store), "0");
    assert_eq!(
        count(store, "SELECT count(*) FROM m.ducklake_inlined_delete_1;"),
        "8"
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT count(*) FROM lake.main.child;",
        )),
        vec![vec!["40"]]
    );
}

/// Partitioning the table on the column the delete keys on makes a
/// flush write one file per partition, so the same delete covers a file
/// exactly and DuckLake drops it instead: no delete file, no Parquet
/// write. The file is *ended* rather than removed, so time travel below
/// the delete still reads it.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_partition_aligned_delete_drops_the_data_file() {
    let dir = TempDir::new("delete-aligned-store");
    let data_dir = TempDir::new("delete-aligned-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.child (parent_id BIGINT, line BIGINT);\n\
         ALTER TABLE lake.main.child SET PARTITIONED BY (parent_id);",
    );
    seed_flushed_rows(store, data_path, 3);

    assert_eq!(
        live_data_files(store),
        "3",
        "a partitioned flush writes one file per partition"
    );
    assert_eq!(
        rows_per_live_file(store),
        vec![vec!["16"], vec!["16"], vec!["16"]]
    );

    run_ducklake_sql(
        store,
        data_path,
        "DELETE FROM lake.main.child WHERE parent_id = 1;",
    );

    assert_eq!(
        live_delete_files(store),
        "0",
        "a delete covering the whole file writes nothing"
    );
    assert_eq!(live_data_files(store), "2");
    assert_eq!(ended_data_files(store), "1", "dropped by ending its row");
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT count(*) FROM lake.main.child;",
        )),
        vec![vec!["32"]]
    );
}

/// The two cheap paths are mutually exclusive on one file. A delete
/// small enough to inline leaves a tombstone, which takes that row out
/// of the file's live count — so a later delete of the rest no longer
/// covers the file, the drop does not apply, and a delete file is
/// written after all. Raising `data_inlining_row_limit` to catch small
/// deletes therefore costs the drop on every file it touches.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_inlined_deletion_blocks_the_data_file_drop() {
    let dir = TempDir::new("delete-blocked-store");
    let data_dir = TempDir::new("delete-blocked-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.child (parent_id BIGINT, line BIGINT);\n\
         ALTER TABLE lake.main.child SET PARTITIONED BY (parent_id);",
    );
    seed_flushed_rows(store, data_path, 2);
    assert_eq!(rows_per_live_file(store), vec![vec!["16"], vec!["16"]]);

    // One row, under the limit: inlined as an `inline/file_delete`.
    run_ducklake_sql(
        store,
        data_path,
        "DELETE FROM lake.main.child WHERE parent_id = 1 AND line = 0;",
    );
    assert_eq!(live_delete_files(store), "0");
    assert_eq!(
        count(store, "SELECT count(*) FROM m.ducklake_inlined_delete_1;"),
        "1"
    );

    // The remaining 15 rows are over the limit and no longer cover the
    // 16-row file, so neither cheap path applies.
    run_ducklake_sql(
        store,
        data_path,
        "DELETE FROM lake.main.child WHERE parent_id = 1;",
    );
    assert_eq!(live_delete_files(store), "1");
    assert_eq!(ended_data_files(store), "0", "the file was not dropped");
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT count(*) FROM lake.main.child;",
        )),
        vec![vec!["16"]]
    );
}

/// Nothing in the engine detects that an UPDATE changes no values.
/// DuckLake implements UPDATE as delete-plus-insert and neither half
/// compares against the stored row, so rewriting 16 rows to their own
/// values costs a snapshot, a delete file for the old copies, and a new
/// data file for the identical ones — strictly more than the delete
/// alone. Skipping unchanged rows has to happen before the statement.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_no_change_update_still_writes_both_files() {
    let dir = TempDir::new("delete-noop-store");
    let data_dir = TempDir::new("delete-noop-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.child (parent_id BIGINT, line BIGINT);",
    );
    seed_flushed_rows(store, data_path, 3);
    let before = count(store, "SELECT count(*) FROM m.ducklake_snapshot;");

    run_ducklake_sql(
        store,
        data_path,
        "UPDATE lake.main.child SET line = line WHERE parent_id = 1;",
    );

    assert_ne!(
        count(store, "SELECT count(*) FROM m.ducklake_snapshot;"),
        before,
        "a no-change UPDATE still commits"
    );
    assert_eq!(live_delete_files(store), "1", "the old copies are deleted");
    assert_eq!(
        live_data_files(store),
        "2",
        "and the identical rows are rewritten into a new file"
    );
}

/// A statement that matches no rows is free: no snapshot, no write.
/// That is what makes a diffing writer worth building — once unchanged
/// rows are filtered out upstream, the resulting empty DELETE and
/// INSERT cost nothing rather than costing an empty commit.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_zero_row_statements_mint_no_snapshot() {
    let dir = TempDir::new("delete-empty-store");
    let data_dir = TempDir::new("delete-empty-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.child (parent_id BIGINT, line BIGINT);",
    );
    seed_flushed_rows(store, data_path, 3);
    let before = count(store, "SELECT count(*) FROM m.ducklake_snapshot;");

    run_ducklake_sql(
        store,
        data_path,
        "DELETE FROM lake.main.child WHERE parent_id = 99;\n\
         INSERT INTO lake.main.child SELECT 1, 1 WHERE false;",
    );

    assert_eq!(
        count(store, "SELECT count(*) FROM m.ducklake_snapshot;"),
        before,
        "neither statement commits"
    );
    assert_eq!(live_delete_files(store), "0");
    assert_eq!(live_data_files(store), "1");
}

/// A flush cadence measured in minutes writes one file per partition
/// per flush, so file count grows with it. Merging is what bounds that,
/// and it merges within a partition — the merged file still holds one
/// partition's rows, so a partition-aligned delete still drops it.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_merge_keeps_partition_files_droppable() {
    let dir = TempDir::new("delete-merged-store");
    let data_dir = TempDir::new("delete-merged-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.child (parent_id BIGINT, line BIGINT);\n\
         ALTER TABLE lake.main.child SET PARTITIONED BY (parent_id);",
    );
    // Two flushes over two partitions: one file each, four in all.
    for range in ["range(0,8)", "range(8,16)"] {
        run_ducklake_sql(
            store,
            data_path,
            &format!(
                "INSERT INTO lake.main.child SELECT 1, range FROM {range};\n\
                 INSERT INTO lake.main.child SELECT 2, range FROM {range};\n\
                 CALL ducklake_flush_inlined_data('lake');"
            ),
        );
    }
    assert_eq!(live_data_files(store), "4");

    run_ducklake_sql(
        store,
        data_path,
        "CALL ducklake_merge_adjacent_files('lake');",
    );
    assert_eq!(live_data_files(store), "2", "merged within each partition");
    assert_eq!(rows_per_live_file(store), vec![vec!["16"], vec!["16"]]);

    run_ducklake_sql(
        store,
        data_path,
        "DELETE FROM lake.main.child WHERE parent_id = 1;",
    );
    assert_eq!(live_delete_files(store), "0");
    assert_eq!(live_data_files(store), "1");
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT count(*) FROM lake.main.child;",
        )),
        vec![vec!["16"]]
    );
}

/// Rows the metadata scan *emitted* in an analyzed plan — what it
/// materialized, which is upstream of the filter DuckDB applies above it
/// and therefore the only place narrowing is visible.
fn metadata_scan_rows(plan: &str) -> u64 {
    let scan = plan
        .split_once("MORAINE_METADATA_SCAN")
        .unwrap_or_else(|| panic!("no metadata scan in the analyzed plan:\n{plan}"))
        .1;
    scan.lines()
        // Singular at one row, and thousands carry separators.
        .find_map(|line| {
            let count: String = line
                .split_once(" row")?
                .0
                .chars()
                .filter(char::is_ascii_digit)
                .collect();
            count.parse().ok()
        })
        .unwrap_or_else(|| panic!("no row count under the metadata scan:\n{plan}"))
}

/// Every `ducklake_file_column_stats` row a table's files carry, as
/// `(data_file_id, column_id, min, max)` tuples ordered for comparison.
fn file_column_stats(store: &std::path::Path) -> Vec<Vec<String>> {
    csv_rows(&run_standalone_sql(
        store,
        "SELECT data_file_id, column_id, value_count, null_count, min_value, max_value \
         FROM m.ducklake_file_column_stats ORDER BY data_file_id, column_id;",
    ))
}

/// A delete leaves file column statistics untouched, on both the
/// delete-file path and the whole-file-drop path.
///
/// The statistics describe a data file's contents, and a delete changes
/// which rows are live rather than what a file holds — so a transaction
/// that only deletes stages nothing against that table. Compaction is
/// the opposite case and is asserted alongside, because it is what makes
/// the distinction load-bearing: the two paths must not be conflated.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_delete_leaves_file_column_stats_untouched() {
    let dir = TempDir::new("delete-stats-store");
    let data_dir = TempDir::new("delete-stats-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.child (parent_id BIGINT, line BIGINT);",
    );
    seed_flushed_rows(store, data_path, 3);
    assert_eq!(live_data_files(store), "1");

    let before = file_column_stats(store);
    assert!(!before.is_empty(), "the flushed file carries statistics");

    // 16 rows against a 48-row file: the delete-file path, which leaves
    // the data file — and therefore its statistics — in place.
    run_ducklake_sql(
        store,
        data_path,
        "DELETE FROM lake.main.child WHERE parent_id = 1;",
    );
    assert_eq!(live_delete_files(store), "1");
    assert_eq!(
        file_column_stats(store),
        before,
        "a delete-file delete writes no file column statistics"
    );

    // The whole-file drop path: every surviving row goes, so the data
    // file is dropped outright rather than masked.
    run_ducklake_sql(
        store,
        data_path,
        "DELETE FROM lake.main.child WHERE parent_id IN (2, 3);",
    );
    assert_eq!(live_data_files(store), "0", "the file is dropped whole");
    assert_eq!(
        file_column_stats(store),
        before,
        "dropping a data file leaves its statistics behind — the orphans the \
         maintenance sweep reclaims"
    );
}

/// Compaction does write file column statistics: it produces new data
/// files, and a new file needs its own. The counterpart to
/// [`ducklake_delete_leaves_file_column_stats_untouched`] — together they
/// bound which statements stage against the statistics table.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_compaction_writes_file_column_stats() {
    let dir = TempDir::new("compact-stats-store");
    let data_dir = TempDir::new("compact-stats-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.child (parent_id BIGINT, line BIGINT);",
    );
    for parent in 1..=2u64 {
        run_ducklake_sql(
            store,
            data_path,
            &format!(
                "INSERT INTO lake.main.child SELECT {parent}, range FROM range(0,16);\
                 CALL ducklake_flush_inlined_data('lake');"
            ),
        );
    }
    assert_eq!(live_data_files(store), "2");

    let before = file_column_stats(store);
    run_ducklake_sql(
        store,
        data_path,
        "CALL ducklake_merge_adjacent_files('lake');",
    );
    assert_eq!(live_data_files(store), "1", "merged into one file");
    assert_ne!(
        file_column_stats(store),
        before,
        "the merged file carries statistics of its own"
    );
}

/// A statement that narrows the file-statistics scan still sees, and
/// still writes, the right rows.
///
/// The metadata scan materializes one table's run of
/// `ducklake_file_column_stats` when an equality on `table_id` reaches it,
/// and a row's rowid counts from the start of the *whole* dump rather than
/// the run. Two tables are needed for that to bite: with one, every offset
/// is zero and a positional mistake looks correct. The delete is what a
/// wrong base would corrupt silently — it resolves rowids back to key cells
/// against the unscoped list.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_narrowed_file_column_stats_scan_reads_and_writes_the_right_rows() {
    let dir = TempDir::new("scope-stats-store");
    let data_dir = TempDir::new("scope-stats-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.first (a BIGINT, b BIGINT);\
         CREATE TABLE lake.main.second (a BIGINT, b BIGINT);",
    );
    for table in ["first", "second"] {
        run_ducklake_sql(
            store,
            data_path,
            &format!(
                "INSERT INTO lake.main.{table} SELECT range, range FROM range(0,32);\
                 CALL ducklake_flush_inlined_data('lake');"
            ),
        );
    }

    let per_table = |table: &str| -> Vec<Vec<String>> {
        csv_rows(&run_standalone_sql(
            store,
            &format!(
                "SELECT s.column_id, s.value_count FROM m.ducklake_file_column_stats s \
                 JOIN m.ducklake_table t USING (table_id) \
                 WHERE t.table_name = '{table}' AND t.end_snapshot IS NULL \
                 ORDER BY s.column_id;"
            ),
        ))
    };

    // The narrowed read: each table sees only its own statistics, and the
    // second — whose run does not start at zero — is not short-changed.
    let first = per_table("first");
    let second = per_table("second");
    assert!(!first.is_empty(), "first must carry statistics");
    assert_eq!(first, second, "both tables were loaded identically");

    let all = count(store, "SELECT count(*) FROM m.ducklake_file_column_stats;");
    assert_eq!(
        all,
        (first.len() * 2).to_string(),
        "the two runs must tile the whole dump: {first:?}"
    );

    let second_table_id = count(
        store,
        "SELECT table_id FROM m.ducklake_table \
         WHERE table_name = 'second' AND end_snapshot IS NULL;",
    );

    // The narrowing is real, not merely harmless. DuckDB applies the
    // equality itself either way, so every assertion above would pass even
    // if the scan had materialized the whole dump — what separates them is
    // how many rows the scan *emits*, before that filter runs.
    let scanned = |sql: &str| -> u64 {
        metadata_scan_rows(&run_standalone_sql(
            store,
            &format!("EXPLAIN ANALYZE {sql};"),
        ))
    };
    let one_table = scanned(&format!(
        "SELECT column_id FROM m.ducklake_file_column_stats WHERE table_id = {second_table_id}"
    ));
    let whole = scanned("SELECT column_id FROM m.ducklake_file_column_stats");
    assert_eq!(
        (one_table, whole),
        (first.len() as u64, (first.len() * 2) as u64),
        "the narrowed scan must emit one table's run, the unfiltered one the whole dump"
    );

    // The write: deleting one table's statistics must remove that table's
    // rows and leave the other's, which is what a wrong row-id base breaks.
    run_standalone_sql(
        store,
        &format!("DELETE FROM m.ducklake_file_column_stats WHERE table_id = {second_table_id};"),
    );
    assert!(per_table("second").is_empty(), "second's statistics went");
    assert_eq!(per_table("first"), first, "first's statistics stayed");
}

/// Narrowing the file-statistics scan never changes what a query returns,
/// whatever shape the filter takes.
///
/// The scan reads an equality on `table_id` to decide how much to
/// materialize and **consumes no filter**, so DuckDB still applies every
/// one of them. That is what keeps an unrecognized shape merely slower
/// instead of wrong — and it is the property that breaks the moment anyone
/// sets `filter_pushdown`, which would hand this scan predicates it does
/// not implement. Differential, because a moraine-only assertion would
/// encode whatever it currently does as correct.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_narrowing_the_statistics_scan_changes_no_answer() {
    let dir = TempDir::new("scope-safe-store");
    let data_dir = TempDir::new("scope-safe-data");
    let reference_meta = TempDir::new("scope-safe-ref-meta");
    let reference_data = TempDir::new("scope-safe-ref-data");

    let apply = |sql: &str| {
        run_ducklake_sql(dir.path(), data_dir.path(), sql);
        run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
    };
    let probe = |sql: &str| {
        let moraine_rows = csv_rows(&run_ducklake_sql(dir.path(), data_dir.path(), sql));
        let reference_rows = csv_rows(&run_reference_ducklake_sql(
            reference_meta.path(),
            reference_data.path(),
            sql,
        ));
        assert_eq!(
            moraine_rows, reference_rows,
            "moraine diverges from stock DuckLake for `{sql}`"
        );
        assert!(!moraine_rows.is_empty(), "`{sql}` must match something");
    };

    apply(
        "CREATE TABLE lake.main.first (a BIGINT, b BIGINT);\
         CREATE TABLE lake.main.second (a BIGINT, b BIGINT);\
         INSERT INTO lake.main.first SELECT range, range FROM range(0,32);\
         INSERT INTO lake.main.second SELECT range, range FROM range(0,32);\
         CALL ducklake_flush_inlined_data('lake');",
    );

    let stats = "SELECT table_id, column_id, value_count \
                 FROM __ducklake_metadata_lake.ducklake_file_column_stats";
    // The shape the scan narrows on, alone and carrying a second predicate
    // it does not implement — the second must still be applied.
    probe(&format!("{stats} WHERE table_id = 1 ORDER BY column_id;"));
    probe(&format!(
        "{stats} WHERE table_id = 1 AND value_count > 0 ORDER BY column_id;"
    ));
    // Shapes the match rejects outright: a range, a disjunction, and an
    // equality on a column that is not the scope column.
    probe(&format!(
        "{stats} WHERE table_id >= 1 ORDER BY table_id, column_id;"
    ));
    probe(&format!(
        "{stats} WHERE table_id = 1 OR table_id = 2 ORDER BY table_id, column_id;"
    ));
    probe(&format!(
        "{stats} WHERE value_count > 0 ORDER BY table_id, column_id;"
    ));
}
