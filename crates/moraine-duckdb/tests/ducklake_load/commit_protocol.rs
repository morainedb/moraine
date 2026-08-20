//! Regression pins for the commit protocol's DuckLake-facing contracts:
//! row-id allocation, the schema-version classification boundary, the
//! retry budget DuckLake wraps every metadata commit in, and the
//! `changes_made` grammar its conflict check re-parses.
//!
//! These are pins against the *tracked DuckLake version*, not restatements
//! of moraine's own behavior, so most run differentially: an identical
//! statement stream against a stock DuckLake catalog mints identical
//! snapshot ids and row ids, and a divergence in either direction — moraine
//! drifting, or DuckLake changing under a pin bump — fails the same
//! assertion.
//!
//! What is *not* here, and why: a genuine concurrent commit race. DuckLake
//! drives moraine through one serialized metadata connection, and a second
//! read-write attach fences rather than races (the single-writer topology),
//! so DuckLake's `RunCommitLoop` cannot be made to lose a race from a
//! single CLI session. Its retry *behavior* is therefore pinned by what is
//! observable — the budget it would spend, the reads it would issue between
//! attempts, and the grammar it would re-parse — while the classification
//! those inputs feed is pinned at the core, over the same matrix
//! (`transaction::operations`'s `the_conflict_matrix`).

use crate::helpers::*;

/// A moraine store and a stock DuckLake catalog fed the identical
/// statement stream, so any probe can be compared row-for-row.
struct Twin {
    store: TempDir,
    data: TempDir,
    reference_meta: TempDir,
    reference_data: TempDir,
}

impl Twin {
    fn new(tag: &str) -> Self {
        Self {
            store: TempDir::new(&format!("{tag}-store")),
            data: TempDir::new(&format!("{tag}-data")),
            reference_meta: TempDir::new(&format!("{tag}-ref-meta")),
            reference_data: TempDir::new(&format!("{tag}-ref-data")),
        }
    }

    /// Runs `sql` against both catalogs.
    fn apply(&self, sql: &str) {
        run_ducklake_sql(self.store.path(), self.data.path(), sql);
        run_reference_ducklake_sql(self.reference_meta.path(), self.reference_data.path(), sql);
    }

    /// Runs `sql` against both catalogs and returns the rows, asserting
    /// they agree.
    fn probe(&self, sql: &str) -> Vec<Vec<String>> {
        let moraine_rows = csv_rows(&run_ducklake_sql(self.store.path(), self.data.path(), sql));
        let reference_rows = csv_rows(&run_reference_ducklake_sql(
            self.reference_meta.path(),
            self.reference_data.path(),
            sql,
        ));
        assert_eq!(
            moraine_rows, reference_rows,
            "moraine diverges from stock DuckLake for `{sql}`"
        );
        moraine_rows
    }
}

/// One scalar cell, for the single-value probes below.
fn one(rows: &[Vec<String>]) -> String {
    assert_eq!(rows.len(), 1, "expected one row, got {rows:?}");
    assert_eq!(rows[0].len(), 1, "expected one column, got {rows:?}");
    rows[0][0].clone()
}

/// Row-id allocation: `next_row_id += record_count`, per table, with the
/// ids dense and stable, matching stock DuckLake at every step.
///
/// Four properties, all differential:
///
/// - each insert advances the table's `next_row_id` by exactly the rows it
///   wrote, so the counter equals the sum of the registered files' record
///   counts and the live rowids are `0..n` with no gap or repeat;
/// - an UPDATE preserves the lineage of the rows it did not touch, and the rows
///   it did touch get ids allocated above the counter — never reused;
/// - the counter is *per table*: a second table allocates from its own, leaving
///   the first untouched (this is what makes concurrent inserts into different
///   tables share no counter to contend on);
/// - compaction allocates none, which
///   `ducklake_merge_adjacent_files_preserves_rows_and_time_travel` pins over a
///   merge.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_row_id_allocation_matches_stock_ducklake() {
    let twin = Twin::new("rowid");

    // Well above the inlining row limit, so every insert registers a data
    // file and the counter arithmetic is checkable against record counts.
    twin.apply("CREATE TABLE lake.main.t(a BIGINT);");
    twin.apply("INSERT INTO lake.main.t SELECT i FROM range(100) t(i);");
    assert_eq!(
        one(&twin.probe("SELECT next_row_id FROM __ducklake_metadata_lake.ducklake_table_stats;")),
        "100"
    );

    twin.apply("INSERT INTO lake.main.t SELECT i + 100 FROM range(50) t(i);");
    let next_row_id =
        one(&twin.probe("SELECT next_row_id FROM __ducklake_metadata_lake.ducklake_table_stats;"));
    assert_eq!(next_row_id, "150");
    assert_eq!(
        one(&twin.probe(
            "SELECT sum(record_count)::BIGINT FROM __ducklake_metadata_lake.ducklake_data_file;"
        )),
        next_row_id,
        "`next_row_id` is the running sum of the registered record counts"
    );

    // Dense and unique: 150 rows carrying rowids 0..149.
    assert_eq!(
        twin.probe(
            "SELECT count(*), count(DISTINCT rowid), min(rowid), max(rowid) FROM lake.main.t;"
        ),
        vec![vec![
            "150".to_string(),
            "150".to_string(),
            "0".to_string(),
            "149".to_string()
        ]]
    );

    // An UPDATE rewrites the rows it touches. Whatever DuckLake does with
    // their ids, it must do the same on both catalogs — and the rows it
    // left alone keep the ids they had.
    let untouched_before =
        twin.probe("SELECT rowid, a FROM lake.main.t WHERE a >= 100 ORDER BY a LIMIT 5;");
    twin.apply("UPDATE lake.main.t SET a = a + 1000 WHERE a < 10;");
    assert_eq!(
        twin.probe(
            "SELECT rowid, a FROM lake.main.t WHERE a >= 100 AND a < 1000 ORDER BY a LIMIT 5;"
        ),
        untouched_before,
        "an UPDATE must not disturb the row ids of rows it did not touch"
    );
    assert_eq!(
        one(&twin.probe("SELECT count(DISTINCT rowid)::BIGINT FROM lake.main.t;")),
        "150",
        "an UPDATE reuses no row id"
    );
    let after_update =
        one(&twin.probe("SELECT next_row_id FROM __ducklake_metadata_lake.ducklake_table_stats;"));

    // The counter is per table, not global: a second table's inserts
    // allocate from its own and leave the first where it was.
    twin.apply("CREATE TABLE lake.main.u(a BIGINT);");
    twin.apply("INSERT INTO lake.main.u SELECT i FROM range(20) t(i);");
    assert_eq!(
        twin.probe(
            "SELECT table_id, next_row_id FROM __ducklake_metadata_lake.ducklake_table_stats \
             ORDER BY table_id;"
        ),
        vec![
            vec!["1".to_string(), after_update],
            vec!["2".to_string(), "20".to_string()],
        ]
    );
}

/// The schema-version boundary, pinned where it is easiest to get wrong.
///
/// DuckDB keys its schema-metadata cache on `schema_version`, so the
/// classification is client-visible in both directions: bumping on data
/// commits defeats the cache, not bumping on schema commits serves a stale
/// one. Every case below is checked against stock DuckLake for the same
/// statement, over the full `(snapshot_id, schema_version)` history rather
/// than the latest value, so a bump landing on the wrong snapshot fails
/// too.
///
/// The cases that carry the item: comments and tags **bump** (DuckLake
/// models them as table alters), a name-mapping registration does **not**
/// (its own test below), and `set_option` neither bumps nor mints a
/// snapshot at all — global or table-scoped — while still taking effect.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_schema_version_classification_matches_stock_ducklake() {
    let twin = Twin::new("schemaver");
    let history = || {
        twin.probe(
            "SELECT snapshot_id, schema_version \
             FROM __ducklake_metadata_lake.ducklake_snapshot ORDER BY snapshot_id;",
        )
    };
    // The `(snapshot_id, schema_version)` pair the last statement left.
    let latest = |history: &[Vec<String>]| {
        let last = history.last().expect("a snapshot history is never empty");
        (last[0].clone(), last[1].clone())
    };

    twin.apply("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    let (snapshot, version) = latest(&history());

    // Data-only: a snapshot, no schema version.
    twin.apply("INSERT INTO lake.main.t SELECT i, 'v' FROM range(100) t(i);");
    let (data_snapshot, data_version) = latest(&history());
    assert_ne!(data_snapshot, snapshot, "an insert mints a snapshot");
    assert_eq!(
        data_version, version,
        "an insert must carry `schema_version` forward"
    );

    // Inlined data-only: same again, below the inlining row limit, so the
    // rows never become a file.
    twin.apply("INSERT INTO lake.main.t VALUES (999, 'inline');");
    let (inline_snapshot, inline_version) = latest(&history());
    assert_ne!(inline_snapshot, data_snapshot);
    assert_eq!(
        inline_version, data_version,
        "an inlined insert must carry `schema_version` forward"
    );

    // Structural: a column changes the shape, so the cache key must move.
    twin.apply("ALTER TABLE lake.main.t ADD COLUMN c BIGINT;");
    let (_, altered_version) = latest(&history());
    assert_ne!(
        altered_version, inline_version,
        "adding a column must bump `schema_version`"
    );

    // Comments and tags bump despite changing no column: DuckLake models
    // them as table alters and the rewritten entry enters `new_tables`.
    twin.apply("COMMENT ON TABLE lake.main.t IS 'documented';");
    let (_, commented_version) = latest(&history());
    assert_ne!(
        commented_version, altered_version,
        "a table comment must bump `schema_version`"
    );

    twin.apply("COMMENT ON COLUMN lake.main.t.a IS 'the key';");
    let (comment_snapshot, column_commented_version) = latest(&history());
    assert_ne!(
        column_commented_version, commented_version,
        "a column comment must bump `schema_version`"
    );

    // `set_option` is outside the snapshot protocol entirely: DuckLake
    // writes `ducklake_metadata` within its metadata connection, so it
    // mints no snapshot and bumps no version — while still taking effect.
    let unchanged = (comment_snapshot, column_commented_version);
    twin.apply("CALL lake.set_option('parquet_compression', 'zstd');");
    assert_eq!(
        latest(&history()),
        unchanged,
        "`set_option` must neither mint a snapshot nor bump `schema_version`"
    );
    assert_eq!(
        twin.probe(
            "SELECT key, value FROM __ducklake_metadata_lake.ducklake_metadata \
             WHERE key = 'parquet_compression';"
        ),
        vec![vec!["parquet_compression".to_string(), "zstd".to_string()]],
        "the option must be recorded, just outside the snapshot protocol"
    );

    // Scoped options land on their scope, not the global one, and the
    // last write wins — options are unversioned.
    twin.apply("CALL lake.set_option('parquet_compression', 'snappy', table_name => 't');");
    assert_eq!(
        twin.probe(
            "SELECT value, scope, scope_id FROM __ducklake_metadata_lake.ducklake_metadata \
             WHERE key = 'parquet_compression' ORDER BY scope NULLS FIRST;"
        ),
        vec![
            vec!["zstd".to_string(), "NULL".to_string(), "NULL".to_string()],
            vec!["snappy".to_string(), "table".to_string(), "1".to_string()],
        ]
    );
    assert_eq!(
        latest(&history()),
        unchanged,
        "a scoped `set_option` mints no snapshot either"
    );
}

/// Re-setting an option overwrites its row rather than failing.
///
/// DuckLake's `SetConfigOption` counts the rows already holding the key
/// at that scope and issues an `INSERT` only when there are none, so
/// every set after the first arrives as `UPDATE ducklake_metadata SET
/// value` — a spelling the staged path has to translate, or an option
/// can be set exactly once and never corrected.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_set_option_overwrites_an_existing_option_row() {
    let twin = Twin::new("setopt-twice");
    let value = || {
        twin.probe(
            "SELECT value FROM __ducklake_metadata_lake.ducklake_metadata \
             WHERE key = 'parquet_compression' AND scope IS NULL;",
        )
    };

    twin.apply("CALL lake.set_option('parquet_compression', 'zstd');");
    assert_eq!(value(), vec![vec!["zstd".to_string()]]);

    twin.apply("CALL lake.set_option('parquet_compression', 'snappy');");
    assert_eq!(
        value(),
        vec![vec!["snappy".to_string()]],
        "the second set must overwrite the row, not duplicate it or fail"
    );
}

/// An option row can be removed, which is what resets an option to its
/// default. The staged delete carries the row's key columns — key and
/// scope — as every other raw-delete kind does, so the core has to decode
/// that shape rather than a whole row.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_an_option_row_can_be_deleted() {
    let dir = TempDir::new("setopt-delete-store");
    let data_dir = TempDir::new("setopt-delete-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CALL lake.set_option('parquet_compression', 'zstd');",
    );
    run_ducklake_sql(
        store,
        data_path,
        "DELETE FROM __ducklake_metadata_lake.ducklake_metadata \
         WHERE key = 'parquet_compression';",
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_metadata \
             WHERE key = 'parquet_compression';",
        )),
        vec![vec!["0"]],
        "the option is gone, so it resolves to its default again"
    );
}

/// A name-mapping registration is data-only: registering foreign Parquet
/// writes `ducklake_column_mapping` / `ducklake_name_mapping` rows and a
/// data file, and must carry `schema_version` forward — the mapping
/// describes a *file*, not the table's shape, so DuckDB's cached column
/// list stays valid.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_name_mapping_registration_carries_schema_version_forward() {
    let twin = Twin::new("mapping");
    let foreign = TempDir::new("mapping-foreign");

    twin.apply("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    twin.apply("INSERT INTO lake.main.t SELECT i, 'v' FROM range(100) t(i);");

    // A plain DuckDB `COPY` writes no DuckLake field ids, so registering
    // it forces the name-mapping path.
    let file = foreign.path().join("foreign.parquet");
    run_ducklake_sql(
        twin.store.path(),
        twin.data.path(),
        &format!(
            "COPY (SELECT i::BIGINT AS a, 'f' AS b FROM range(20) t(i)) TO '{}' (FORMAT PARQUET);",
            file.display()
        ),
    );

    let version_before = one(&twin.probe(
        "SELECT max(schema_version)::BIGINT FROM __ducklake_metadata_lake.ducklake_snapshot;",
    ));
    twin.apply(&format!(
        "CALL ducklake_add_data_files('lake', 't', '{}');",
        file.display()
    ));

    assert_eq!(
        one(&twin.probe(
            "SELECT max(schema_version)::BIGINT FROM __ducklake_metadata_lake.ducklake_snapshot;"
        )),
        version_before,
        "registering a mapped file must carry `schema_version` forward"
    );
    assert_ne!(
        one(&twin
            .probe("SELECT count(*)::BIGINT FROM __ducklake_metadata_lake.ducklake_name_mapping;")),
        "0",
        "the registration is expected to write the file's name mapping"
    );
    assert_eq!(
        one(&twin.probe("SELECT count(*)::BIGINT FROM lake.main.t;")),
        "120"
    );
}

/// Expiry is the only thing that deletes mapping rows, and this drives it
/// end to end: register foreign Parquet so a real
/// `ducklake_column_mapping` record and its `ducklake_name_mapping` rows
/// exist, drop the table that owns them, then expire. The dead-table
/// cleanup reclaims the record, and the name-mapping rows — embedded in it
/// here rather than a table of their own — go with it.
///
/// Differential throughout: every probe asserts moraine agrees with stock
/// DuckLake row for row, so an over- or under-reclaim on either side fails.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn expiry_reclaims_the_mappings_of_a_dropped_table() {
    let twin = Twin::new("mapping-expiry");
    let foreign = TempDir::new("mapping-expiry-foreign");

    twin.apply("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    twin.apply("INSERT INTO lake.main.t SELECT i, 'v' FROM range(100) t(i);");

    // A plain DuckDB `COPY` writes no DuckLake field ids, so registering it
    // forces the name-mapping path.
    let file = foreign.path().join("foreign.parquet");
    run_ducklake_sql(
        twin.store.path(),
        twin.data.path(),
        &format!(
            "COPY (SELECT i::BIGINT AS a, 'f' AS b FROM range(20) t(i)) TO '{}' (FORMAT PARQUET);",
            file.display()
        ),
    );
    twin.apply(&format!(
        "CALL ducklake_add_data_files('lake', 't', '{}');",
        file.display()
    ));
    assert_ne!(
        one(&twin.probe(
            "SELECT count(*)::BIGINT FROM __ducklake_metadata_lake.ducklake_column_mapping;"
        )),
        "0",
        "the registration is expected to write the file's mapping"
    );

    twin.apply("DROP TABLE lake.main.t;");
    twin.apply("CALL ducklake_expire_snapshots('lake', older_than => now());");
    twin.apply("CALL ducklake_cleanup_old_files('lake', older_than => now());");

    assert_eq!(
        one(&twin.probe(
            "SELECT count(*)::BIGINT FROM __ducklake_metadata_lake.ducklake_column_mapping;"
        )),
        "0",
        "the dropped table's mapping record must be reclaimed"
    );
    assert_eq!(
        one(&twin
            .probe("SELECT count(*)::BIGINT FROM __ducklake_metadata_lake.ducklake_name_mapping;")),
        "0",
        "its name-mapping rows must go with it"
    );
}

/// The retry contract moraine composes with, pinned against the tracked
/// DuckLake version rather than assumed from its prose.
///
/// DuckLake wraps every metadata-catalog commit in a bounded retry loop
/// and, between attempts, re-reads what committed after its own snapshot
/// before re-checking its conflict matrix. Two halves of that are
/// observable from a single session and pinned here: the **budget** —
/// changing it changes how a moraine conflict composes into
/// `ducklake_max_retry_count` × moraine's own attempts — and the **read
/// surface** it re-reads, which moraine must serve while a commit of its
/// own is in flight.
///
/// The third half is the `changes_made` grammar. moraine writes that field
/// in DuckLake's own dialect, not a moraine one, because DuckLake re-parses
/// it mid-retry and its parser *throws* on an entry kind it does not know.
/// Verb-path commits (here, a maintenance merge) are the ones moraine
/// authors the field for, so those are compared to stock byte-for-byte.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_commit_retry_contract_and_change_grammar_hold() {
    let twin = Twin::new("retry");

    // The budget. These are DuckDB settings, so they read back without a
    // lake attached — but read them through the moraine chain anyway, since
    // that is the composition being pinned.
    assert_eq!(
        twin.probe(
            "SELECT name, value FROM duckdb_settings() \
             WHERE name IN ('ducklake_max_retry_count', 'ducklake_retry_wait_ms', \
                            'ducklake_retry_backoff') ORDER BY name;"
        ),
        vec![
            vec!["ducklake_max_retry_count".to_string(), "10".to_string()],
            vec!["ducklake_retry_backoff".to_string(), "1.5".to_string()],
            vec!["ducklake_retry_wait_ms".to_string(), "100".to_string()],
        ],
        "DuckLake's retry budget changed; moraine's own budget composes with it"
    );

    twin.apply("CREATE TABLE lake.main.t(a BIGINT);");
    for batch in 0..3 {
        twin.apply(&format!(
            "INSERT INTO lake.main.t SELECT i + {} FROM range(100) t(i);",
            batch * 100
        ));
    }

    // The read surface a retry re-reads: everything committed after the
    // retrying transaction's snapshot, by id, served identically to stock.
    let changes = twin.probe(
        "SELECT snapshot_id, changes_made FROM __ducklake_metadata_lake.ducklake_snapshot_changes \
         WHERE snapshot_id > 1 ORDER BY snapshot_id;",
    );
    assert!(
        changes
            .iter()
            .any(|row| row[1].contains("inserted_into_table")),
        "the inserts must be visible to a mid-retry conflict check: {changes:?}"
    );

    // The grammar, on the entries moraine itself authors: a maintenance
    // merge runs the verb path, so its `changes_made` is moraine's to write
    // and DuckLake's to be able to re-parse.
    twin.apply("CALL ducklake_merge_adjacent_files('lake');");
    let merged = twin.probe(
        "SELECT changes_made FROM __ducklake_metadata_lake.ducklake_snapshot_changes \
         ORDER BY snapshot_id DESC LIMIT 1;",
    );
    assert_eq!(
        merged,
        vec![vec!["merge_adjacent:1".to_string()]],
        "a merge's change entry must be one DuckLake's conflict matrix checks"
    );

    // And DuckLake reads the merged catalog back without complaint, which
    // is the parse the entry has to survive.
    assert_eq!(
        one(&twin.probe("SELECT count(*)::BIGINT FROM lake.main.t;")),
        "300"
    );
}
