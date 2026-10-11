use crate::helpers::*;

/// The verifier recomputes entries from the rows, so a freshly built index
/// over files and inlined rows reports no discrepancy — and it counts only
/// the rows a DELETE left alive.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_maintained_index_verifies_clean_through_dml() {
    let store = TempDir::new("verify-clean-store");
    let data = TempDir::new("verify-clean-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let result = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CREATE TABLE lake.main.t AS SELECT i a, i * 2 b FROM range(2000) r(i);
         CALL moraine_index_create('lake','main','t','by_a',['a'],true);
         SELECT 'built', * FROM moraine_index_verify('lake','main','t','by_a');
         DELETE FROM lake.main.t WHERE a < 100;
         SELECT 'deleted', * FROM moraine_index_verify('lake','main','t','by_a');
         INSERT INTO lake.main.t SELECT i, i FROM range(5000, 5050) r(i);
         SELECT 'inserted', * FROM moraine_index_verify('lake','main','t','by_a');
         UPDATE lake.main.t SET a = a + 100000 WHERE a >= 5000;
         SELECT 'updated', * FROM moraine_index_verify('lake','main','t','by_a');",
    );
    // stored, expected, missing, dead, mismatched
    assert!(
        result.lines().any(|line| line == "built,2000,2000,0,0,0"),
        "{result}"
    );
    assert!(
        result.lines().any(|line| line == "deleted,1900,1900,0,0,0"),
        "{result}"
    );
    assert!(
        result
            .lines()
            .any(|line| line == "inserted,1950,1950,0,0,0"),
        "{result}"
    );
    assert!(
        result.lines().any(|line| line == "updated,1950,1950,0,0,0"),
        "{result}"
    );
}

/// Verification reads: it commits nothing, so the lake's newest snapshot is
/// the same before and after.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn verification_commits_no_snapshot() {
    let store = TempDir::new("verify-readonly-store");
    let data = TempDir::new("verify-readonly-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let result = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CREATE TABLE lake.main.t AS SELECT i a FROM range(500) r(i);
         CALL moraine_index_create('lake','main','t','by_a',['a'],true);
         SELECT 'before', max(snapshot_id) FROM ducklake_snapshots('lake');
         SELECT 'verify', * FROM moraine_index_verify('lake','main','t','by_a');
         SELECT 'after', max(snapshot_id) FROM ducklake_snapshots('lake');",
    );
    let snapshot = |label: &str| {
        result
            .lines()
            .find_map(|line| line.strip_prefix(label).map(str::to_owned))
            .unwrap_or_else(|| panic!("no {label} line in {result}"))
    };
    assert!(
        result.lines().any(|line| line == "verify,500,500,0,0,0"),
        "{result}"
    );
    assert_eq!(
        snapshot("before,"),
        snapshot("after,"),
        "verification committed a snapshot: {result}"
    );
}

/// An index whose entries no longer match the rows is reported, not hidden:
/// a rebuilt index over rewritten files agrees again.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn compaction_leaves_the_index_agreeing_with_the_rows() {
    let store = TempDir::new("verify-compact-store");
    let data = TempDir::new("verify-compact-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let result = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CREATE TABLE lake.main.t(a BIGINT, b BIGINT);
         INSERT INTO lake.main.t SELECT i, i FROM range(400) r(i);
         INSERT INTO lake.main.t SELECT i, i FROM range(400, 800) r(i);
         CALL moraine_index_create('lake','main','t','by_a',['a'],true);
         DELETE FROM lake.main.t WHERE a % 4 = 0;
         CALL ducklake_merge_adjacent_files('lake');
         SELECT 'compacted', * FROM moraine_index_verify('lake','main','t','by_a');",
    );
    assert!(
        result.lines().any(|line| line == "compacted,600,600,0,0,0"),
        "{result}"
    );
}

/// An absent index is a refusal, not a clean report.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_absent_index_is_refused() {
    let store = TempDir::new("verify-absent-store");
    let data = TempDir::new("verify-absent-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let refused = run_ducklake_sql_expect_err_with_options(
        store.path(),
        data.path(),
        &options,
        "CREATE TABLE lake.main.t AS SELECT i a FROM range(100) r(i);
         CALL moraine_index_create('lake','main','t','by_a',['a'],true);
         CALL moraine_index_verify('lake','main','t','by_b');",
    );
    assert!(refused.contains("index by_b"), "got: {refused}");
}

/// A read-write attach records what the pass found: a clean index carries a
/// certificate naming the snapshot it agreed at, `moraine_indexes` reports
/// it, and a rebuild withdraws it.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_clean_pass_certifies_and_a_rebuild_withdraws() {
    let store = TempDir::new("verify-certify-store");
    let data = TempDir::new("verify-certify-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let result = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CREATE TABLE lake.main.t AS SELECT i a FROM range(600) r(i);
         CALL moraine_index_create('lake','main','t','by_a',['a'],true);
         SELECT 'fresh', certified_at IS NULL FROM moraine_indexes('lake','main','t');
         SELECT 'pass', missing, dead, mismatched, certified_at IS NOT NULL
           FROM moraine_index_verify('lake','main','t','by_a');
         SELECT 'listed', certified_at IS NOT NULL FROM moraine_indexes('lake','main','t');
         DELETE FROM lake.main.t WHERE a < 10;
         SELECT 'after_dml', certified_at IS NOT NULL FROM moraine_indexes('lake','main','t');
         CALL moraine_index_drop('lake','main','t','by_a');
         CALL moraine_index_create('lake','main','t','by_a',['a'],true);
         SELECT 'rebuilt', certified_at IS NULL FROM moraine_indexes('lake','main','t');",
    );
    assert!(result.lines().any(|line| line == "fresh,true"), "{result}");
    assert!(
        result.lines().any(|line| line == "pass,0,0,0,true"),
        "{result}"
    );
    assert!(result.lines().any(|line| line == "listed,true"), "{result}");
    assert!(
        result.lines().any(|line| line == "after_dml,true"),
        "a maintained DELETE keeps the certificate: {result}"
    );
    assert!(
        result.lines().any(|line| line == "rebuilt,true"),
        "{result}"
    );
}
