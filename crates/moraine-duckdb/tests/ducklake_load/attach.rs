use std::{process::Command, sync::Arc};

use moraine::{Catalog, CatalogOptions, OptionScope};
use object_store::local::LocalFileSystem;

use crate::helpers::*;

/// `moraine:` prefix dispatch (no `TYPE moraine` needed): DuckDB
/// resolves the prefix before this shim ever sees the path. Proven
/// standalone here, independent of DuckLake.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extension; run via `cargo xtask e2e`"]
fn moraine_prefix_attach_without_type_clause() {
    let dir = TempDir::new("prefix");
    // Never scanned by this test — placeholder stats are fine.
    seed(dir.path(), 0, 0);

    let output = Command::new(cli_path())
        .arg("-unsigned")
        .arg("-csv")
        .arg("-c")
        .arg(format!("LOAD '{}';", ext_path().display()))
        .arg("-c")
        .arg(format!("ATTACH 'moraine:{}' AS m;", dir.path().display()))
        .arg("-c")
        .arg("SELECT database_name FROM duckdb_databases() WHERE database_name = 'm';")
        .output()
        .expect("failed to spawn duckdb CLI");
    assert!(
        output.status.success(),
        "moraine: prefix attach failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        csv_rows(&String::from_utf8_lossy(&output.stdout)),
        vec![vec!["m".to_string()]]
    );
}

/// The full `ducklake:moraine:` chain: attach, read through DuckLake's
/// own reader, count (pushdown), time travel, `ducklake_snapshots()`.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_attach_reads_through_moraine_metadata() {
    let dir = TempDir::new("store");
    let data_dir = TempDir::new("data");
    // Written first: `seed` needs the real file's size/footer stats to
    // register a data file DuckLake's own reader can open.
    let (file_size_bytes, footer_size) = write_parquet(data_dir.path());
    seed(dir.path(), file_size_bytes, footer_size);
    let store = dir.path();
    let data_path = data_dir.path();

    let select = csv_rows(&run_ducklake_sql(
        store,
        data_path,
        "SELECT * FROM lake.main.t ORDER BY id;",
    ));
    assert_eq!(
        u64::try_from(select.len()).expect("row count fits u64"),
        ROW_COUNT
    );
    assert_eq!(select[0], vec!["0".to_string(), "0.0".to_string()]);
    assert_eq!(select[4], vec!["4".to_string(), "6.0".to_string()]);

    let count = csv_rows(&run_ducklake_sql(
        store,
        data_path,
        "SELECT count(*) FROM lake.main.t;",
    ));
    assert_eq!(count, vec![vec![ROW_COUNT.to_string()]]);

    let snapshots = csv_rows(&run_ducklake_sql(
        store,
        data_path,
        "SELECT count(*) FROM ducklake_snapshots('lake');",
    ));
    assert_eq!(snapshots.len(), 1);

    // Time travel: `t` is created, gets its data file, and is renamed
    // all within `seed`'s one commit (snapshot 1); snapshot 0 is
    // bootstrap's own `main`-minting snapshot, before `t` exists at
    // all. `AT (VERSION => 1)` must see it; `AT (VERSION => 0)` must
    // not — proving version-scoped resolution runs through this shim's
    // synthesized `ducklake_table`/`ducklake_snapshot` rows.
    let at_v1 = csv_rows(&run_ducklake_sql(
        store,
        data_path,
        "SELECT count(*) FROM lake.main.t AT (VERSION => 1);",
    ));
    assert_eq!(at_v1, vec![vec![ROW_COUNT.to_string()]]);

    let output = Command::new(cli_path())
        .arg("-unsigned")
        .arg("-csv")
        .arg("-c")
        .arg(ducklake_load_statement(&ducklake_ext_path()))
        .arg("-c")
        .arg(format!("LOAD '{}';", ext_path().display()))
        .arg("-c")
        .arg(format!(
            "ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}');",
            store.display(),
            data_path.display()
        ))
        .arg("-c")
        .arg("SELECT count(*) FROM lake.main.t AT (VERSION => 0);")
        .output()
        .expect("failed to spawn duckdb CLI");
    assert!(
        !output.status.success(),
        "querying `t` AT (VERSION => 0), before it existed, unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Catalog Error") || stderr.contains("does not exist"),
        "expected a catalog error naming the missing table; got: {stderr}"
    );
}

/// Read-only attach: `READ_ONLY` opens moraine's `DbReader`
/// (never the writer `Db`, so it never fences a live writer), and reads
/// flow through it end to end. The standalone `moraine: (READ_ONLY)`
/// surface is the reference case — DuckDB sets the access mode directly
/// from the flag and the shim reads it; a read-only DuckLake chain reads
/// the committed data the same way a read-write one does. Write rejection
/// and no-fencing are pinned by the core `tests/catalog.rs` suite, and
/// DuckDB enforces the outer `READ_ONLY` at the SQL layer.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_read_only_attach_reads_through_a_reader() {
    let dir = TempDir::new("ro-store");
    let data_dir = TempDir::new("ro-data");
    let store = dir.path();
    let data_path = data_dir.path();

    // Seed through a read-write attach.
    run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.t (a BIGINT);");
    run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (1), (2);");

    // Standalone read-only (moraine's DbReader) serves the metadata
    // projection — the reference case for the shim's access-mode wiring.
    assert_eq!(
        csv_rows(&run_standalone_read_only_sql(
            store,
            "SELECT count(*) FROM m.ducklake_table WHERE end_snapshot IS NULL;",
        )),
        vec![vec!["1"]]
    );

    // A read-only DuckLake chain reads the committed rows.
    assert_eq!(
        csv_rows(&run_ducklake_read_only_sql(
            store,
            data_path,
            "SELECT a FROM lake.main.t ORDER BY a;",
        )),
        vec![vec!["1"], vec!["2"]]
    );
}

/// Whether DuckLake forwards an outer `READ_ONLY` into the nested moraine
/// metadata attach — the question the test above cannot answer, because
/// rows read back either way.
///
/// The decisive observation is a fencing one, and it needs two live
/// processes: A holds a read-write chain open, B attaches the *same* store
/// through the same chain with outer `READ_ONLY` and reads, then A writes
/// again.
///
/// - **Forwarded:** B's metadata attach opened moraine's `DbReader`, never took
///   the writer epoch, and A's second write lands.
/// - **Not forwarded:** B's metadata attach opened the writer `Db`, took the
///   epoch, and A's second write fails fenced.
///
/// The assertion pins the answer this DuckLake version gives, so a change
/// on either side surfaces here rather than as a mystery fence in the
/// field.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_forwards_read_only_into_the_metadata_attach() {
    let dir = TempDir::new("ro-forward-store");
    let data_dir = TempDir::new("ro-forward-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.t (a BIGINT);");

    let read = std::cell::RefCell::new(Vec::new());
    let output = run_ducklake_sql_around(
        store,
        data_path,
        "INSERT INTO lake.main.t VALUES (1);\n",
        std::time::Duration::from_secs(2),
        || {
            // Process B, while A still holds the store's writer epoch.
            *read.borrow_mut() = csv_rows(&run_ducklake_read_only_sql(
                store,
                data_path,
                "SELECT a FROM lake.main.t ORDER BY a;",
            ));
        },
        "INSERT INTO lake.main.t VALUES (2);\n",
    );

    // B read what A had committed, whichever handle it opened.
    assert_eq!(read.into_inner(), vec![vec!["1"]]);

    let combined = combined_output(&output);
    assert!(
        output.status.success(),
        "DuckLake did not forward the outer READ_ONLY: the read-only chain \
         opened moraine's writer and fenced the live one. Promote RFC 0017's \
         deferred entry — document READ_ONLY on the `moraine:` attach itself \
         as the escape hatch.\nstdout+stderr: {combined}"
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT a FROM lake.main.t ORDER BY a;",
        )),
        vec![vec!["1"], vec!["2"]],
        "the writer's second commit is durable, so nothing fenced it"
    );
}

/// `FLUSH_INTERVAL_MS` end to end: `ATTACH (META_FLUSH_INTERVAL_MS
/// 5)` → DuckLake's `META_` passthrough → this shim's inner attach →
/// the store's WAL flush cadence. The setting is visible only as
/// commit latency, so the assertion is that the option is accepted,
/// commits land, and a plain re-attach (default cadence) reads them
/// back.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_attach_flush_interval_option_is_applied() {
    let dir = TempDir::new("flush-store");
    let data_dir = TempDir::new("flush-data");

    run_ducklake_sql_with_options(
        dir.path(),
        data_dir.path(),
        ", META_FLUSH_INTERVAL_MS 5",
        "CREATE TABLE lake.main.t(id BIGINT); \
         INSERT INTO lake.main.t VALUES (1), (2);",
    );

    assert_eq!(
        csv_rows(&run_ducklake_sql(
            dir.path(),
            data_dir.path(),
            "SELECT count(*) FROM lake.main.t;",
        )),
        vec![vec!["2".to_string()]]
    );
}

/// The cache options end to end: `ATTACH (META_CACHE_DIR '…',
/// META_CACHE_SIZE 67108864, META_CACHE_PUTS true, META_CACHE_PRELOAD
/// 'all')` → DuckLake's `META_` passthrough → this shim's inner attach →
/// the store's on-disk object cache, its cap, its write-path filling, and
/// its open-time load. A cap shows only as disk that stays bounded, so the
/// assertion is that the four are accepted, commits land through the capped
/// cache, and the cache directory fills.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_attach_cache_options_are_applied() {
    let dir = TempDir::new("cache-size-store");
    let data_dir = TempDir::new("cache-size-data");
    let cache_dir = TempDir::new("cache-size-cache");

    let attach_options = format!(
        ", META_CACHE_DIR '{}', META_CACHE_SIZE 67108864, META_CACHE_PUTS true, \
         META_CACHE_PRELOAD 'all'",
        cache_dir.path().display()
    );
    run_ducklake_sql_with_options(
        dir.path(),
        data_dir.path(),
        &attach_options,
        "CREATE TABLE lake.main.t(id BIGINT); \
         INSERT INTO lake.main.t VALUES (1), (2);",
    );

    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            dir.path(),
            data_dir.path(),
            &attach_options,
            "SELECT count(*) FROM lake.main.t;",
        )),
        vec![vec!["2".to_string()]]
    );

    let populated =
        std::fs::read_dir(cache_dir.path()).is_ok_and(|mut entries| entries.next().is_some());
    assert!(
        populated,
        "expected a bounded object cache under {:?}",
        cache_dir.path()
    );
}

/// `ENCRYPTED` end to end. The flag travels `ATTACH (ENCRYPTED,
/// META_ENCRYPTED true)` → DuckLake's `META_` passthrough → this
/// shim's inner attach → the store's creation-time flag, which the
/// synthesized `ducklake_metadata` serves back. DuckLake then writes
/// Parquet-encrypted data files and records their keys in catalog
/// rows; a later plain attach adopts the stored flag and decrypts.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_encrypted_writes_encrypted_files_and_reads_back() {
    let dir = TempDir::new("enc-store");
    let data_dir = TempDir::new("enc-data");

    // Create and load in one encrypted session; 100 rows overflows
    // the inlining limit, forcing a real data file.
    run_ducklake_sql_with_options(
        dir.path(),
        data_dir.path(),
        ", ENCRYPTED, META_ENCRYPTED true",
        "CREATE TABLE lake.main.t(id BIGINT); \
         INSERT INTO lake.main.t SELECT range FROM range(100);",
    );

    // A plain re-attach adopts the stored flag and reads through
    // decryption.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            dir.path(),
            data_dir.path(),
            "SELECT count(*) FROM lake.main.t;",
        )),
        vec![vec!["100".to_string()]]
    );

    // The bytes at rest are not plaintext Parquet: an
    // encrypted-footer file does not end with the plaintext `PAR1`
    // trailer.
    let files = parquet_files_under(data_dir.path());
    assert!(
        !files.is_empty(),
        "no data files written under the data path"
    );
    for file in &files {
        let bytes = std::fs::read(file).expect("read data file");
        assert!(
            !bytes.ends_with(b"PAR1"),
            "{} is plaintext Parquet despite ENCRYPTED",
            file.display()
        );
    }

    // The catalog rows carry the stored flag and per-file key
    // material.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test setup: build tokio runtime");
    rt.block_on(async {
        let store =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("open local store"));
        let catalog = Catalog::open(store, CatalogOptions::default())
            .await
            .expect("open catalog");
        let head = catalog.snapshot().await.expect("snapshot");
        assert_eq!(
            head.option(OptionScope::Global, "encrypted").as_deref(),
            Some("true")
        );

        let schema = head.schema_by_name("main").expect("main schema");
        let table = head.table_by_name(schema.id, "t").expect("table t");
        let data_files = head.data_files_of(table.id);
        assert!(!data_files.is_empty());
        for file in &data_files {
            assert!(
                file.encryption_key
                    .as_deref()
                    .is_some_and(|k| !k.is_empty()),
                "data file {} carries no encryption key",
                file.path
            );
        }
        catalog.close().await.expect("close catalog");
    });
}
