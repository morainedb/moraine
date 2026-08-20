use std::{net::TcpListener, path::Path, process::Command};

use crate::helpers::*;

/// A free loopback address, resolved by binding `:0` then releasing it so the
/// leader can bind and advertise it. A `:0` bind would advertise port 0, which
/// no client could reach.
fn free_leader_address() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a free loopback port");
    let addr = listener.local_addr().expect("read the bound address");
    drop(listener);
    addr.to_string()
}

/// The leader role composed through the real DuckDB extension. A
/// `META_MAINTENANCE_LEADER` attach starts the role behind the maintenance
/// surface (the designated folder is the leader host): the session's DDL and
/// DML round-trip, `moraine_maintenance_status` reports the role held with its
/// live counters, and the commits are durable to a cold reattach. A second
/// leader attach on the same bucket re-adopts the role — the stand-down at
/// detach and the re-announcement at reattach are the extension-level shape of
/// a leader killed and restarted; the fleet-scale convergence and mid-run kill
/// dynamics are proven over the core (`adaptive_leader_bench`,
/// `two_contending_handles_converge_onto_the_leader`,
/// `a_stale_advert_is_superseded_and_the_client_readopts_the_successor`).
#[test]
#[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
fn ducklake_leader_role_serves_ddl_dml_and_reports_status() {
    let store = TempDir::new("leader-store");
    let data = TempDir::new("leader-data");

    let first = format!(
        ", META_MAINTENANCE_LEADER true, META_MAINTENANCE_LEADER_ADDRESS '{}'",
        free_leader_address()
    );

    // DDL and DML round-trip with the leader enabled, and the status surface
    // reports the role held right now, carrying its live counters.
    let rows = csv_rows(&run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &first,
        "CREATE TABLE lake.t(id BIGINT, v VARCHAR);\
         INSERT INTO lake.t VALUES (1, 'a'), (2, 'b'), (3, 'c');\
         UPDATE lake.t SET v = 'B' WHERE id = 2;\
         DELETE FROM lake.t WHERE id = 3;\
         SELECT 'ROLE' AS tag, status, (detail LIKE 'sessions=%forwarded_commits=%') AS shaped \
         FROM moraine_maintenance_status('lake') WHERE step = 'leader_role';\
         SELECT 'ROW' AS tag, id::VARCHAR, v FROM lake.t ORDER BY id;",
    ));

    let role: Vec<&Vec<String>> = rows
        .iter()
        .filter(|r| r.first().is_some_and(|t| t == "ROLE"))
        .collect();
    assert_eq!(
        role,
        vec![&vec![
            "ROLE".to_string(),
            "held".to_string(),
            "true".to_string()
        ]],
        "the leader role is held and its status detail carries the counters: {rows:?}"
    );
    let data_rows: Vec<Vec<String>> = rows
        .iter()
        .filter(|r| r.first().is_some_and(|t| t == "ROW"))
        .map(|r| vec![r[1].clone(), r[2].clone()])
        .collect();
    assert_eq!(
        data_rows,
        vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "B".to_string()],
        ],
        "DDL and DML round-trip through the leader-hosting session"
    );

    // A cold read-only reattach with no leader sees the durable rows: the
    // leader's own commits landed on the shared bucket and the fleet reads them.
    let cold = csv_rows(&run_ducklake_sql(
        store.path(),
        data.path(),
        "SELECT id, v FROM lake.t ORDER BY id;",
    ));
    assert_eq!(
        cold,
        vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "B".to_string()],
        ],
        "a cold attach past every read window sees the leader session's durable commits"
    );

    // Re-adoption: a fresh leader attach (a new listener, a new announcement)
    // holds the role again and commits on top of the durable state.
    let second = format!(
        ", META_MAINTENANCE_LEADER true, META_MAINTENANCE_LEADER_ADDRESS '{}'",
        free_leader_address()
    );
    let readopted = csv_rows(&run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &second,
        "INSERT INTO lake.t VALUES (4, 'd');\
         SELECT status FROM moraine_maintenance_status('lake') WHERE step = 'leader_role';\
         SELECT id::VARCHAR FROM lake.t ORDER BY id;",
    ));
    assert!(
        readopted.iter().any(|r| r == &vec!["held".to_string()]),
        "a fresh leader attach re-adopts the role: {readopted:?}"
    );
    assert!(
        readopted.iter().any(|r| r == &vec!["4".to_string()]),
        "the re-adopted leader commits on top of the durable state: {readopted:?}"
    );
}

/// Two catalog handles in one process, racing the same slot log through the
/// real DuckDB CLI. One test, four movements:
///
/// 1. A plain `ducklake:moraine:` attach round-trips CREATE/INSERT/SELECT.
/// 2. A cold reattach (a fresh process, empty head cache) still serves the rows
///    — pure tail replay, no maintenance.
/// 3. `moraine_maintenance('lake')` folds the unfolded tail (its status row
///    reports a positive folded count) and truncates; a reattach reads the
///    identical rows off the folded store.
/// 4. A second attach `AS lake2` on the same store commits alongside `lake`.
///    Each handle sees its own writes at once; the handle that commits last
///    sees its peer's rows too, because a commit revalidates the tail before it
///    races. A peer's newest write need not be visible before the read refresh
///    window elapses, so that is never asserted — the durable proof is a fresh
///    cold attach, which sees every row both handles committed.
#[test]
#[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
#[allow(clippy::too_many_lines, clippy::similar_names)]
fn ducklake_multi_writer_round_trip_folds_and_shares() {
    let store = TempDir::new("multi-writer-store");
    let data = TempDir::new("multi-writer-data");

    // Step 1: plain attach round-trips create/insert/select.
    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.t(id BIGINT, v VARCHAR);\
         INSERT INTO lake.t VALUES (1, 'a'), (2, 'b'), (3, 'c');",
    );
    let seeded = csv_rows(&run_ducklake_sql(
        store.path(),
        data.path(),
        "SELECT id, v FROM lake.t ORDER BY id;",
    ));
    assert_eq!(
        seeded,
        vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "b".to_string()],
            vec!["3".to_string(), "c".to_string()],
        ]
    );

    // Step 2: a cold reattach serves the same rows off the tail — no
    // maintenance has run, so nothing is folded yet.
    let after_cold_attach = csv_rows(&run_ducklake_sql(
        store.path(),
        data.path(),
        "SELECT id, v FROM lake.t ORDER BY id;",
    ));
    assert_eq!(
        after_cold_attach, seeded,
        "a cold reattach must replay the tail"
    );

    // Step 3: a maintenance pass folds the unfolded tail and truncates. The
    // `fold_slots` step must have run and folded at least one slot; the
    // comma in its detail string is kept out of the CSV by reducing it to a
    // boolean here.
    let fold = csv_rows(&run_ducklake_sql(
        store.path(),
        data.path(),
        "SELECT status, (detail NOT LIKE 'folded 0 %') AS folded_some \
         FROM moraine_maintenance('lake') WHERE step = 'fold_slots';",
    ));
    assert_eq!(
        fold,
        vec![vec!["ran".to_string(), "true".to_string()]],
        "the fold step must run and fold a positive number of slots"
    );
    let after_fold = csv_rows(&run_ducklake_sql(
        store.path(),
        data.path(),
        "SELECT id, v FROM lake.t ORDER BY id;",
    ));
    assert_eq!(
        after_fold, seeded,
        "folding and truncation must not change what the lake serves"
    );

    // Step 4: two attaches on one store commit alternately. Each handle
    // reads its own writes back at once; `lake2` commits last, so its
    // revalidation absorbs `lake`'s rows before it races.
    let output = run_two_attach_sql(
        store.path(),
        data.path(),
        "INSERT INTO lake.t VALUES (100, 'la');\
         INSERT INTO lake2.t VALUES (200, 'lb');\
         INSERT INTO lake.t VALUES (101, 'lc');\
         INSERT INTO lake2.t VALUES (201, 'ld');\
         SELECT 'LAKE' AS via, id FROM lake.t WHERE id >= 100 ORDER BY id;\
         SELECT 'LAKE2' AS via, id FROM lake2.t WHERE id >= 100 ORDER BY id;",
    );
    let rows = csv_rows(&output);
    let ids_for = |marker: &str| -> Vec<String> {
        rows.iter()
            .filter(|row| row.first().is_some_and(|via| via == marker))
            .map(|row| row[1].clone())
            .collect()
    };
    let lake_ids = ids_for("LAKE");
    let lake2_ids = ids_for("LAKE2");

    // Each handle sees its own two commits — both raced their slots and won.
    for id in ["100", "101"] {
        assert!(
            lake_ids.iter().any(|got| got == id),
            "lake must see its own {id}: {lake_ids:?}"
        );
    }
    for id in ["200", "201"] {
        assert!(
            lake2_ids.iter().any(|got| got == id),
            "lake2 must see its own {id}: {lake2_ids:?}"
        );
    }
    // Cross-visibility, not instantaneous but forced by a commit: `lake`
    // absorbed `lake2`'s first row when it committed 101 after it; `lake2`,
    // committing last, absorbed both of `lake`'s rows.
    assert!(
        lake_ids.iter().any(|got| got == "200"),
        "lake must see the peer row it committed on top of: {lake_ids:?}"
    );
    assert_eq!(
        lake2_ids,
        vec!["100", "101", "200", "201"],
        "the last committer revalidates the whole tail: {lake2_ids:?}"
    );

    // The durable proof: a fresh cold attach, past every read window, sees
    // every row both handles committed.
    let everything = csv_rows(&run_ducklake_sql(
        store.path(),
        data.path(),
        "SELECT id FROM lake.t ORDER BY id;",
    ));
    assert_eq!(
        everything,
        vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()],
            vec!["100".to_string()],
            vec!["101".to_string()],
            vec!["200".to_string()],
            vec!["201".to_string()],
        ],
        "a cold attach must see both handles' durable commits"
    );
}

/// One CLI session with two independent `ducklake:moraine:` attaches on the
/// same store — `lake` and `lake2` — then `sql`. Pinned single-threaded for
/// the same reason [`run_session`] is: DuckLake's post-rename catalog re-read
/// is racy under multiple threads.
fn run_two_attach_sql(store: &Path, data: &Path, sql: &str) -> String {
    let output = Command::new(cli_path())
        .arg("-unsigned")
        .arg("-csv")
        .arg("-c")
        .arg("SET threads=1;")
        .arg("-c")
        .arg(format!(
            "SET extension_directory='{}';",
            ducklake_ext_path()
                .parent()
                .unwrap_or(Path::new("."))
                .display()
        ))
        .arg("-c")
        .arg("INSTALL ducklake;")
        .arg("-c")
        .arg("LOAD ducklake;")
        .arg("-c")
        .arg(format!("LOAD '{}';", ext_path().display()))
        .arg("-c")
        .arg(format!(
            "ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}');",
            store.display(),
            data.display()
        ))
        .arg("-c")
        .arg(format!(
            "ATTACH 'ducklake:moraine:{}' AS lake2 (DATA_PATH '{}');",
            store.display(),
            data.display()
        ))
        .arg("-c")
        .arg(sql)
        .output()
        .expect("failed to spawn duckdb CLI");
    assert_session_ok(output, "two-attach ducklake session", sql)
}
