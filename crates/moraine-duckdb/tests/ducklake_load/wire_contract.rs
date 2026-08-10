//! Regression pins on what DuckLake asks of moraine at the tracked
//! version: the nested attach it generates, the `ducklake_*` tables it
//! actually reads and writes, the conflict-resolution read it owes moraine
//! mid-retry, and the DuckDB/DuckLake build pair the loadable is linked
//! against.
//!
//! Every one of these is a fact about *DuckLake's* code, not moraine's, so
//! each is re-verified by running rather than asserted from the source
//! once. Moving a pin here is a deliberate, reviewed bump.
//!
//! The observation mechanism throughout is DuckDB's own `QueryLog`, which
//! records every statement executed on the instance — DuckLake's internal
//! metadata connection included. Nothing in moraine is instrumented for
//! these tests.

use std::{path::Path, process::Command};

use crate::helpers::*;

/// The exact DuckLake commit whose behaviour these pins describe and that
/// the repository patch is applied to.
const DUCKLAKE_SOURCE_COMMIT: &str = "d8a1881e";

/// The source revision embedded by the local `CMake` build.
const DUCKLAKE_EXTENSION_VERSION: &str = "d8a1881";

/// Runs `statements` in one CLI session with `QueryLog` capture on, and
/// returns every statement DuckDB executed — DuckLake's own metadata SQL
/// among them — newline-flattened, one per element.
fn query_log(store: &Path, data_path: &Path, alias: &str, statements: &[&str]) -> Vec<String> {
    let capture = TempDir::new("querylog");
    let out = capture.path().join("log.csv");

    let mut command = Command::new(cli_path());
    command
        .arg("-unsigned")
        .arg("-csv")
        .arg("-c")
        .arg("SET threads=1;")
        .arg("-c")
        .arg(ducklake_load_statement(&ducklake_ext_path()))
        .arg("-c")
        .arg(format!("LOAD '{}';", ext_path().display()))
        .arg("-c")
        .arg("CALL enable_logging('QueryLog', storage => 'memory');")
        .arg("-c")
        .arg(format!(
            "ATTACH 'ducklake:moraine:{}' AS {alias} (DATA_PATH '{}');",
            store.display(),
            data_path.display()
        ));
    for statement in statements {
        command.arg("-c").arg(*statement);
    }
    // Newlines are flattened in SQL so each statement stays one CSV row.
    command.arg("-c").arg(format!(
        "COPY (SELECT replace(message, chr(10), ' ') FROM duckdb_logs WHERE type = 'QueryLog') \
         TO '{}' (FORMAT CSV, HEADER false, QUOTE '');",
        out.display()
    ));

    let output = command.output().expect("failed to spawn duckdb CLI");
    assert_session_ok(output, "query-log session", &statements.join(" "));
    std::fs::read_to_string(&out)
        .expect("read the captured query log")
        .lines()
        .map(str::to_owned)
        .collect()
}

/// How DuckLake's `ducklake:` prefix names and nests the moraine attach —
/// the one fact the whole chain rests on, and previously only
/// source-verified.
///
/// Three parts, all observed rather than inferred: the statement text
/// DuckLake generates, the catalog name it derives from the outer alias,
/// and the `HIDDEN true` that keeps the nested database out of
/// `duckdb_databases()` while leaving it addressable by name.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_nests_a_hidden_moraine_attach_named_for_its_alias() {
    let dir = TempDir::new("nest-store");
    let data_dir = TempDir::new("nest-data");
    let store = dir.path();

    // A non-default alias, so the derivation is visible rather than
    // coincidental.
    let log = query_log(store, data_dir.path(), "warehouse", &["SELECT 1;"]);
    let expected = format!(
        "ATTACH OR REPLACE 'moraine:{}' AS \"__ducklake_metadata_warehouse\" (HIDDEN true)",
        store.display()
    );
    assert!(
        log.iter().any(|statement| statement.trim() == expected),
        "DuckLake's nested attach changed shape.\nexpected: {expected}\nlog: {log:#?}"
    );

    // The exists-probe that decides "this is already a DuckLake catalog",
    // which moraine's synthesized `ducklake_metadata` must answer.
    let probe = "SELECT NULL FROM \"__ducklake_metadata_warehouse\".\"main\".ducklake_metadata \
                 LIMIT 1";
    assert!(
        log.iter().any(|statement| statement.trim() == probe),
        "DuckLake's exists-probe changed shape.\nexpected: {probe}\nlog: {log:#?}"
    );

    // Addressable by that name, and hidden from the database listing.
    let visible = assert_session_ok(
        Command::new(cli_path())
            .arg("-unsigned")
            .arg("-csv")
            .arg("-c")
            .arg("SET threads=1;")
            .arg("-c")
            .arg(ducklake_load_statement(&ducklake_ext_path()))
            .arg("-c")
            .arg(format!("LOAD '{}';", ext_path().display()))
            .arg("-c")
            .arg(format!(
                "ATTACH 'ducklake:moraine:{}' AS warehouse (DATA_PATH '{}');",
                store.display(),
                data_dir.path().display()
            ))
            .arg("-c")
            .arg("SELECT count(*) FROM __ducklake_metadata_warehouse.main.ducklake_snapshot;")
            .arg("-c")
            .arg("SELECT database_name, coalesce(path, '') FROM duckdb_databases() ORDER BY 1;")
            .output()
            .expect("failed to spawn duckdb CLI"),
        "nested-catalog probe",
        "duckdb_databases",
    );
    let rows = csv_rows(&visible);
    assert_eq!(
        rows[0],
        vec!["1".to_string()],
        "the nested catalog is addressable by name: {visible}"
    );
    assert!(
        !rows
            .iter()
            .any(|row| row[0].starts_with("__ducklake_metadata")),
        "the nested attach is HIDDEN, so it must not appear in duckdb_databases(): {rows:?}"
    );
    // The outer catalog reports the nested path — the literal string the
    // `ducklake:` prefix strip hands on.
    assert!(
        rows.iter()
            .any(|row| row[0] == "warehouse" && row[1] == format!("moraine:{}", store.display())),
        "the outer catalog's path is the nested attach string: {rows:?}"
    );
}

/// Which `ducklake_*` tables DuckLake reads and writes, over a workload
/// that reaches every catalog feature moraine models.
///
/// This is what says which scans are worth optimizing, and it is a set
/// equality on purpose: a table appearing here for the first time is
/// either one moraine already serves (fine, add it) or one it does not
/// (a bind error waiting for a user), and either way the pin should say
/// so rather than let it pass.
///
/// `R` is any statement that only reads; `W` covers `INSERT`, `UPDATE`,
/// `DELETE`, and `CREATE`, so a table marked `RW` is both read and
/// mutated somewhere in the workload.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
#[allow(clippy::too_many_lines)]
fn ducklakes_catalog_access_set_is_pinned() {
    let dir = TempDir::new("access-store");
    let data_dir = TempDir::new("access-data");

    let log = query_log(
        dir.path(),
        data_dir.path(),
        "lake",
        &[
            "CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);",
            // Past the inlining limit, so a real data file and its stats land.
            "INSERT INTO lake.main.t SELECT range, 'x' FROM range(100);",
            "UPDATE lake.main.t SET b = 'y' WHERE a < 10;",
            "DELETE FROM lake.main.t WHERE a > 90;",
            "ALTER TABLE lake.main.t ADD COLUMN c DOUBLE;",
            "CREATE VIEW lake.main.v AS SELECT a FROM lake.main.t;",
            "COMMENT ON TABLE lake.main.t IS 'pinned';",
            "CALL ducklake_flush_inlined_data('lake');",
            "CALL ducklake_merge_adjacent_files('lake');",
            "CALL ducklake_expire_snapshots('lake', older_than => now());",
            "CALL ducklake_cleanup_old_files('lake', older_than => now());",
            "SELECT count(*) FROM lake.main.t;",
        ],
    );

    let mut access: std::collections::BTreeMap<String, (bool, bool)> =
        std::collections::BTreeMap::new();
    for statement in &log {
        // Only DuckLake's own metadata traffic: the outer statements name
        // `lake`, and the maintenance functions share the `ducklake_`
        // prefix with the tables.
        if !statement.contains("__ducklake_metadata_lake") {
            continue;
        }
        let writes = ["INSERT", "UPDATE", "DELETE", "CREATE", "DROP"]
            .iter()
            .any(|verb| statement.trim_start().to_uppercase().starts_with(verb));
        for table in ducklake_tables_named_in(statement) {
            let entry = access.entry(table).or_insert((false, false));
            if writes {
                entry.1 = true;
            } else {
                entry.0 = true;
            }
        }
    }
    let observed: Vec<String> = access
        .into_iter()
        .map(|(table, (read, write))| {
            let mut ops = String::new();
            if read {
                ops.push('R');
            }
            if write {
                ops.push('W');
            }
            format!("{table} {ops}")
        })
        .collect();

    // Pinned against DuckLake d8a1881e. Every entry is a table moraine
    // serves; the two dynamic inline families carry the ids this workload
    // happens to allocate.
    let expected = [
        "ducklake_column RW",
        "ducklake_column_mapping W",
        "ducklake_column_tag R",
        "ducklake_data_file RW",
        "ducklake_delete_file RW",
        "ducklake_file_column_stats RW",
        "ducklake_file_partition_value R",
        "ducklake_files_scheduled_for_deletion R",
        "ducklake_inlined_data_1_1 RW",
        "ducklake_inlined_data_1_2 RW",
        "ducklake_inlined_data_tables RW",
        "ducklake_inlined_delete_1 RW",
        "ducklake_macro RW",
        "ducklake_macro_impl RW",
        "ducklake_macro_parameters RW",
        "ducklake_metadata R",
        "ducklake_name_mapping W",
        "ducklake_partition_column R",
        "ducklake_partition_info R",
        "ducklake_schema RW",
        "ducklake_schema_versions RW",
        "ducklake_snapshot RW",
        "ducklake_snapshot_changes RW",
        "ducklake_sort_expression R",
        "ducklake_sort_info R",
        "ducklake_table RW",
        "ducklake_table_column_stats RW",
        "ducklake_table_stats RW",
        "ducklake_tag RW",
        "ducklake_view RW",
    ];
    assert_eq!(
        observed, expected,
        "DuckLake's catalog access set changed. Reconcile RFC 0006 before moving this pin: a \
         new table must be one moraine serves, and a table that gained writes must have a \
         staged-row translation."
    );

    // `ducklake_file_variant_stats` is conspicuously absent, and stays so:
    // DuckLake writes it only for a column whose extra stats are VARIANT,
    // and moraine refuses a VARIANT column at creation. The always-empty
    // stand-in is therefore exactly right until VARIANT itself is
    // supported, not an oversight.
    assert!(
        !observed
            .iter()
            .any(|entry| entry.starts_with("ducklake_file_variant_stats")),
        "DuckLake now touches ducklake_file_variant_stats: {observed:?}"
    );
}

/// Every `ducklake_*` identifier a statement names, deduplicated, in
/// first-seen order.
fn ducklake_tables_named_in(statement: &str) -> Vec<String> {
    let mut found = Vec::new();
    let bytes = statement.as_bytes();
    let mut index = 0;
    while let Some(offset) = statement[index..].find("ducklake_") {
        let start = index + offset;
        let mut end = start;
        while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
            end += 1;
        }
        let name = &statement[start..end];
        // `__ducklake_metadata_lake` is the nested database, not a table.
        if name != "ducklake_metadata_lake" && !found.iter().any(|seen| seen == name) {
            found.push(name.to_string());
        }
        index = end;
    }
    found
}

/// The second conflict-propagation obligation: moraine must serve
/// DuckLake's conflict-resolution read *inside* an open write transaction,
/// because that is where `RunCommitLoop` issues it — between a failed
/// commit attempt and the next.
///
/// The query below is `GetSnapshotAndStatsAndChangesQuery` transcribed
/// verbatim from the pinned DuckLake source, with its two placeholders
/// substituted. Driving a real lost race here is not possible: moraine
/// admits one read-write process, so a second committer fences the first
/// instead of racing it. What moraine owes is that this read works and
/// reports the truth while a transaction is open, which is what this pins.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_serves_the_conflict_resolution_read_inside_a_transaction() {
    let dir = TempDir::new("conflict-store");
    let data_dir = TempDir::new("conflict-data");

    run_ducklake_sql(
        dir.path(),
        data_dir.path(),
        "CREATE TABLE lake.main.t(a BIGINT); INSERT INTO lake.main.t VALUES (1);",
    );

    let conflict_query = |snapshot_id: u64| {
        format!(
            "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id, \
             COALESCE((SELECT STRING_AGG(changes_made, ',') \
             FROM \"__ducklake_metadata_lake\".\"main\".ducklake_snapshot_changes c \
             WHERE c.snapshot_id > {snapshot_id}), '') AS changes, \
             NULL AS table_id, NULL AS column_id, NULL AS record_count, NULL AS next_row_id, \
             NULL AS file_size_bytes, NULL AS contains_null, NULL AS contains_nan, \
             NULL AS min_value, NULL AS max_value, NULL AS extra_stats \
             FROM \"__ducklake_metadata_lake\".\"main\".ducklake_snapshot \
             WHERE snapshot_id = (SELECT MAX(snapshot_id) \
             FROM \"__ducklake_metadata_lake\".\"main\".ducklake_snapshot) \
             UNION ALL \
             SELECT NULL, NULL, NULL, NULL, NULL, table_id, column_id, record_count, \
             next_row_id, file_size_bytes, contains_null, contains_nan, min_value, max_value, \
             extra_stats \
             FROM \"__ducklake_metadata_lake\".\"main\".ducklake_table_stats \
             LEFT JOIN \"__ducklake_metadata_lake\".\"main\".ducklake_table_column_stats \
             USING (table_id) \
             WHERE record_count IS NOT NULL AND file_size_bytes IS NOT NULL \
             ORDER BY table_id NULLS FIRST;"
        )
    };

    // Inside an open write transaction, exactly where RunCommitLoop runs it.
    let inside = run_ducklake_sql(
        dir.path(),
        data_dir.path(),
        &format!(
            "BEGIN; INSERT INTO lake.main.t VALUES (2); {} ROLLBACK;",
            conflict_query(0)
        ),
    );
    let rows = csv_rows(&inside);
    assert!(
        !rows.is_empty(),
        "the conflict-resolution read returned nothing: {inside}"
    );
    // The head snapshot row comes first (table_id NULLS FIRST). Asserted
    // on the raw text rather than a parsed column: `changes` is a
    // comma-joined aggregate, which no naive CSV split survives.
    assert_eq!(rows[0][0], "2", "head snapshot id: {rows:?}");
    assert!(
        inside.contains("created_table"),
        "the changes column must aggregate every later snapshot's changes_made: {inside}"
    );
    // The statistics half of the union arrives too — DuckLake reads its
    // global stats from the same query.
    assert!(
        rows.len() > 1,
        "the table-stats half of the union is missing: {rows:?}"
    );
}

/// The DuckDB/DuckLake build pair the loadable is linked against, checked
/// by running rather than assumed.
///
/// moraine statically links DuckDB v1.5.5; the DuckLake extension that
/// CLI installs is built by DuckDB's own CI against v1.5.3. Patch-level
/// ABI friction between the two would show up as a load failure, a crash,
/// or a wrong answer at the boundary where DuckLake hands moraine C++
/// objects by pointer — so the pin is: both extensions load into one
/// process, and the full chain answers correctly.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn the_pinned_duckdb_and_ducklake_builds_interoperate() {
    let dir = TempDir::new("pin-store");
    let data_dir = TempDir::new("pin-data");

    assert!(DUCKLAKE_SOURCE_COMMIT.starts_with(DUCKLAKE_EXTENSION_VERSION));

    assert_eq!(
        csv_rows(&run_ducklake_sql(
            dir.path(),
            data_dir.path(),
            "SELECT version();"
        )),
        vec![vec![duckdb_pin()]],
        "the CLI is not the pinned DuckDB release"
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            dir.path(),
            data_dir.path(),
            "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake';",
        )),
        vec![vec![DUCKLAKE_EXTENSION_VERSION]],
        "the patched DuckLake artifact has a different source revision; re-verify every pin in \
         this file and RFC 0006's version table"
    );

    // Both extensions loaded and the chain answers: the interoperation
    // itself, not just the version strings.
    run_ducklake_sql(
        dir.path(),
        data_dir.path(),
        "CREATE TABLE lake.main.t(a BIGINT); INSERT INTO lake.main.t VALUES (1), (2);",
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            dir.path(),
            data_dir.path(),
            "SELECT sum(a) FROM lake.main.t;",
        )),
        vec![vec!["3"]],
    );
}

/// The primary DuckDB pin, read from the same manifest the build and the
/// release matrix use, so a bump moves one place. `xtask` is not a
/// dependency of this crate, so the file is included rather than called.
fn duckdb_pin() -> &'static str {
    include_str!("../../../../.github/duckdb-versions")
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .and_then(|entry| entry.split_whitespace().next())
        .expect(".github/duckdb-versions lists no DuckDB version")
}

/// The upstream DuckLake catalog-cache race the whole suite pins
/// `SET threads=1` for — still live at the tracked version, and still
/// nothing to do with moraine.
///
/// A fresh attach's catalog listing comes back **empty** right after a
/// write, under DuckDB's default multi-threaded execution. This drives the
/// reference chain — a plain duckdb-file-backed DuckLake catalog, zero
/// moraine code in it — so a failure here means the race is gone upstream,
/// not that moraine broke. When that happens, delete this test and the
/// `SET threads=1` every session runner sets.
///
/// Deliberately asserts the bug's *presence*: the workaround costs every
/// e2e session its parallelism, and without a canary nobody learns when it
/// stopped being needed. Observed at roughly three runs in five, so the
/// attempt count makes a false alarm vanishingly unlikely while staying
/// cheap.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and patched DuckLake extension"]
fn the_upstream_ducklake_listing_race_still_needs_threads_1() {
    const ATTEMPTS: usize = 15;

    // Spawned directly: every shared session runner sets `threads=1`,
    // which is the very thing under examination.
    let reference_session = |meta: &Path, data: &Path, sql: &str| -> String {
        let output = Command::new(cli_path())
            .arg("-unsigned")
            .arg("-csv")
            .arg("-c")
            .arg(ducklake_load_statement(&ducklake_ext_path()))
            .arg("-c")
            .arg(format!(
                "ATTACH 'ducklake:{}' AS lake (DATA_PATH '{}');",
                meta.join("meta.ducklake").display(),
                data.display()
            ))
            .arg("-c")
            .arg(sql)
            .output()
            .expect("failed to spawn duckdb CLI");
        assert_session_ok(output, "reference DuckLake session", sql)
    };

    let mut empty_listings = 0;
    for attempt in 0..ATTEMPTS {
        let meta_dir = TempDir::new(&format!("race-meta-{attempt}"));
        let data_dir = TempDir::new(&format!("race-data-{attempt}"));

        reference_session(
            meta_dir.path(),
            data_dir.path(),
            "CREATE TABLE lake.main.x (i BIGINT);",
        );
        let listed = reference_session(
            meta_dir.path(),
            data_dir.path(),
            "SELECT name FROM (SHOW ALL TABLES) WHERE database = 'lake';",
        );
        if csv_rows(&listed).is_empty() {
            empty_listings += 1;
        }
    }

    assert!(
        empty_listings > 0,
        "the upstream DuckLake listing race did not fire in {ATTEMPTS} attempts against a \
         plain duckdb-backed catalog. If it is fixed upstream, drop `SET threads=1` from \
         `helpers.rs` and delete this test; if it merely got rarer, raise ATTEMPTS."
    );
}
