//! What survives killing a process that was halfway through a write —
//! the half of that story moraine does not own.
//!
//! moraine never writes or deletes a data file. DuckLake's writer encodes
//! the Parquet and registers it through an ordinary commit, so the ordering
//! around that commit — bytes before the record that references them — is
//! the engine's contract, not the catalog's. It is also the one crash a
//! core test cannot stage, because reaching the instant between the two
//! needs a process running both halves.

use std::{sync::Arc, time::Duration};

use moraine::{Catalog, CatalogOptions};
use object_store::local::LocalFileSystem;

use crate::helpers::{
    TempDir, csv_rows, kill_ducklake_session_after, parquet_files_under, run_ducklake_sql,
};

/// How long the killed session gets to write its Parquet before it dies.
const SETTLE: Duration = Duration::from_secs(5);

/// Every Parquet file under `data_path`, by file name.
fn parquet_names(data_path: &std::path::Path) -> Vec<String> {
    parquet_files_under(data_path)
        .iter()
        .filter_map(|path| {
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .collect()
}

/// Every live data file the catalog references, by file name.
fn referenced_file_names(store_dir: &std::path::Path) -> Vec<String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test setup: build tokio runtime");
    runtime.block_on(async {
        let store =
            Arc::new(LocalFileSystem::new_with_prefix(store_dir).expect("open the local store"));
        let catalog = Catalog::open(store, CatalogOptions::default())
            .await
            .expect("open the catalog");
        let head = catalog.snapshot().await.expect("snapshot");
        let schema = head.schema_by_name("main").expect("main schema");
        let table = head.table_by_name(schema.id, "t").expect("table t");
        let names = head
            .data_files_of(table.id)
            .iter()
            .map(|file| {
                file.path
                    .rsplit('/')
                    .next()
                    .unwrap_or(&file.path)
                    .to_string()
            })
            .collect();
        catalog.close().await.expect("close the catalog");
        names
    })
}

/// Killing a writer between the Parquet write and the commit registering
/// it must leave the bytes **orphaned** — on disk, referenced by nothing —
/// and never the reverse, a live catalog row pointing at a file that was
/// never written. An orphan wastes space until cleanup reclaims it; a
/// dangling reference is unreadable data.
///
/// moraine's half is that the registering commit is one batch, so it is
/// all-or-none and the uncommitted insert leaves no trace in the catalog.
/// What this adds is the engine's half: the bytes were already there when
/// the process died, and the catalog is coherent without them.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn a_kill_before_the_commit_orphans_the_parquet_it_had_already_written() {
    let dir = TempDir::new("crash-store");
    let data_dir = TempDir::new("crash-data");
    let store = dir.path();
    let data_path = data_dir.path();

    // Both inserts are well past `data_inlining_row_limit`, so each writes
    // a real Parquet file rather than inlining into the catalog — which is
    // the whole point: an inlined row has no bytes to orphan.
    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.t(id BIGINT); \
         INSERT INTO lake.main.t SELECT range FROM range(100);",
    );
    let committed = parquet_names(data_path);
    assert_eq!(committed.len(), 1, "the seed insert writes one file");

    // Open a transaction, let its insert write the Parquet, and kill the
    // process before the commit that would register it.
    kill_ducklake_session_after(
        store,
        data_path,
        "BEGIN TRANSACTION;\nINSERT INTO lake.main.t SELECT range FROM range(100, 200);\n",
        SETTLE,
    );

    let after = parquet_names(data_path);
    let orphans: Vec<&String> = after
        .iter()
        .filter(|name| !committed.contains(name))
        .collect();
    assert_eq!(
        orphans.len(),
        1,
        "the killed writer must have written its Parquet before dying, or this proves nothing \
         (on disk: {after:?}, committed before: {committed:?})"
    );

    // The catalog never saw the commit: the row is invisible and the head
    // is the one the seed left.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT count(*) FROM lake.main.t;",
        )),
        vec![vec!["100".to_string()]],
        "an uncommitted insert must not survive the crash"
    );

    // The invariant itself, in both directions.
    let referenced = referenced_file_names(store);
    for name in &referenced {
        assert!(
            after.contains(name),
            "the catalog references {name}, which is not on disk — a dangling reference"
        );
    }
    for orphan in orphans {
        assert!(
            !referenced.contains(orphan),
            "{orphan} was written by a transaction that never committed, yet the catalog \
             references it"
        );
    }
}
