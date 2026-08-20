//! The truly-zero-write read-only attach: `moraine_create_checkpoint`
//! mints a SlateDB checkpoint, `CHECKPOINT` pins an attach to it, and
//! the pinned attach serves a fixed cut without touching the store.

use std::process::Command;

use crate::helpers::*;

/// The whole lifecycle through SQL: mint a checkpoint, attach against it,
/// read the lake, then release it.
///
/// The load-bearing assertion is the **fixed cut**. A commit landing after
/// the checkpoint is invisible to the pinned attach and visible to a plain
/// one, from the same store in the same test — which is the observable
/// consequence of the reader having stopped polling the manifest, the same
/// thing that makes the attach write-free.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn checkpoint_pinned_attach_serves_a_fixed_cut() {
    let dir = TempDir::new("ckpt-store");
    let data_dir = TempDir::new("ckpt-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.t (a BIGINT); INSERT INTO lake.main.t VALUES (1), (2);",
    );

    let minted = csv_rows(&run_standalone_sql(
        store,
        "SELECT checkpoint_id FROM moraine_create_checkpoint('m');",
    ));
    assert_eq!(minted.len(), 1, "one checkpoint minted");
    let checkpoint_id = minted[0][0].clone();
    assert_eq!(
        checkpoint_id.len(),
        36,
        "expected a hyphenated UUID, got {checkpoint_id:?}"
    );

    // The lake moves on *after* the checkpoint.
    run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (3);");

    let pinned = format!(", READ_ONLY, META_CHECKPOINT '{checkpoint_id}'");
    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            store,
            data_path,
            &pinned,
            "SELECT count(*) FROM lake.main.t;",
        )),
        vec![vec!["2"]],
        "the pinned attach serves the cut it was minted at, not head"
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT count(*) FROM lake.main.t;",
        )),
        vec![vec!["3"]],
        "an unpinned attach on the same store still follows head"
    );

    // The checkpoint is listed while it lives and gone once released.
    let listed = csv_rows(&run_standalone_sql(
        store,
        &format!(
            "SELECT checkpoint_id FROM moraine_checkpoints('{}');",
            store.display()
        ),
    ));
    assert!(
        listed.iter().any(|row| row[0] == checkpoint_id),
        "minted checkpoint missing from {listed:?}"
    );

    run_standalone_sql(
        store,
        &format!(
            "SELECT released FROM moraine_delete_checkpoint('{}', '{checkpoint_id}');",
            store.display()
        ),
    );
    let listed = csv_rows(&run_standalone_sql(
        store,
        &format!(
            "SELECT checkpoint_id FROM moraine_checkpoints('{}');",
            store.display()
        ),
    ));
    assert!(
        !listed.iter().any(|row| row[0] == checkpoint_id),
        "released checkpoint still listed in {listed:?}"
    );
}

/// Two refusals the option's contract rests on: a checkpoint id on a
/// read-write attach (a writer commits at head; a checkpoint is a past
/// cut), and an id the manifest does not carry (a reader told to serve a
/// fixed cut must never quietly serve a different one).
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn checkpoint_id_is_refused_read_write_and_when_unknown() {
    let dir = TempDir::new("ckpt-refuse-store");
    let data_dir = TempDir::new("ckpt-refuse-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.t (a BIGINT);");

    let unknown = "00000000-0000-0000-0000-00000000dead";
    let read_write = run_ducklake_sql_expect_err_with_options(
        store,
        data_path,
        &format!(", META_CHECKPOINT '{unknown}'"),
        "SELECT 1;",
    );
    assert!(
        read_write.contains("READ_ONLY"),
        "expected the refusal to name the fix; got: {read_write}"
    );

    let missing = run_ducklake_sql_expect_err_with_options(
        store,
        data_path,
        &format!(", READ_ONLY, META_CHECKPOINT '{unknown}'"),
        "SELECT 1;",
    );
    assert!(
        missing.to_lowercase().contains("checkpoint"),
        "expected the refusal to name the checkpoint; got: {missing}"
    );

    let malformed = run_ducklake_sql_expect_err_with_options(
        store,
        data_path,
        ", READ_ONLY, META_CHECKPOINT 'not-a-uuid'",
        "SELECT 1;",
    );
    assert!(
        malformed.contains("is not a valid id"),
        "expected the refusal to name the parse failure; got: {malformed}"
    );
}

/// A pinned attach leaves the store byte-for-byte as it found it, so
/// credentials with no write permission suffice. Asserted by comparing the
/// store's whole object set — path, size, and mtime — across the attach,
/// rather than by revoking filesystem permission, which a privileged test
/// runner can override without saying so.
///
/// The follow-latest read-only attach is the control: it records its own
/// checkpoint, so it *does* add a manifest object. That contrast is the
/// point of the option.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn a_pinned_attach_leaves_the_store_untouched() {
    let dir = TempDir::new("ckpt-ro-store");
    let data_dir = TempDir::new("ckpt-ro-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.t (a BIGINT); INSERT INTO lake.main.t VALUES (7);",
    );
    let checkpoint_id = csv_rows(&run_standalone_sql(
        store,
        "SELECT checkpoint_id FROM moraine_create_checkpoint('m');",
    ))[0][0]
        .clone();

    // Spawned directly rather than through `run_session`, whose standalone
    // attach is itself a follow-latest reader — its own checkpoint write
    // would be counted against the pinned one under test.
    let before = store_objects(store);
    let pinned = assert_session_ok(
        Command::new(cli_path())
            .arg("-unsigned")
            .arg("-csv")
            .arg("-c")
            .arg(format!("LOAD '{}';", ext_path().display()))
            .arg("-c")
            .arg(format!(
                "ATTACH 'moraine:{}' AS pinned (READ_ONLY, CHECKPOINT '{checkpoint_id}');",
                store.display()
            ))
            .arg("-c")
            .arg("SELECT count(*) FROM pinned.ducklake_table WHERE end_snapshot IS NULL;")
            .output()
            .expect("failed to spawn duckdb CLI"),
        "checkpoint-pinned attach",
        "pinned read",
    );
    assert_eq!(
        csv_rows(&pinned).last().expect("a row"),
        &vec!["1".to_string()],
        "the pinned attach serves the catalog"
    );
    assert_eq!(
        store_objects(store),
        before,
        "a checkpoint-pinned attach must write nothing"
    );

    run_standalone_read_only_sql(store, "SELECT count(*) FROM m.ducklake_table;");
    assert_ne!(
        store_objects(store),
        before,
        "a follow-latest reader records its own checkpoint, so it does write"
    );
}

/// Every object under the store, as `(path, size, mtime)` — enough that any
/// write, including one that overwrites an existing object in place, shows
/// up as a difference.
fn store_objects(root: &std::path::Path) -> Vec<(std::path::PathBuf, u64, std::time::SystemTime)> {
    let mut objects = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current).expect("read the store dir") {
            let path = entry.expect("read a store dir entry").path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let metadata = std::fs::metadata(&path).expect("stat a store object");
            objects.push((path, metadata.len(), metadata.modified().expect("mtime")));
        }
    }
    objects.sort();
    objects
}
