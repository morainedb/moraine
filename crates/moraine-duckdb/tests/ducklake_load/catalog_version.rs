//! `moraine_raise_catalog_version`: the operator's SQL surface for moving a
//! store to the newest DuckLake catalog version this build serves.
//!
//! The shape a store serves is read once per attach, so a raise shows up in
//! sessions that attach after it rather than the one that made it. That, the
//! dry run that records nothing, the acknowledgement a recording raise
//! takes, and the pinned DuckLake's refusal of a raised store are the
//! properties under test.

use std::path::Path;

use crate::helpers::*;

/// A table the 1.1-dev1 shape declares and the 1.0 shape does not.
const EXTENDED_ONLY_TABLE: &str = "ducklake_view_column_tag";

/// The `from_version`/`to_version` row one raise reports.
fn raise(store: &Path, options: &str) -> Vec<Vec<String>> {
    csv_rows(&run_standalone_sql(
        store,
        &format!(
            "SELECT from_version, to_version FROM moraine_raise_catalog_version('m'{options});"
        ),
    ))
}

/// Whether a fresh session finds the extended shape's marker table.
fn serves_extended_shape(store: &Path) -> bool {
    let sql = format!("SELECT count(*) FROM m.{EXTENDED_ONLY_TABLE};");
    let output = run_session(
        &Attach::Standalone {
            store_dir: store,
            read_only: false,
        },
        &sql,
    );
    if output.status.success() {
        return true;
    }

    // Only a missing table reads as the narrow shape: any other failure
    // would otherwise pass for one and make the assertions vacuous.
    let combined = combined_output(&output);
    assert!(
        combined.contains(EXTENDED_ONLY_TABLE) && combined.contains("does not exist"),
        "expected the narrow shape to lack {EXTENDED_ONLY_TABLE}, got: {combined}"
    );
    false
}

/// Raising reports the move, records it for later sessions, and is
/// idempotent; a dry run reports the same move and records nothing.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extension"]
fn raising_the_catalog_version_widens_the_shape_for_later_sessions() {
    let store = TempDir::new("raise-version-store");
    let data = TempDir::new("raise-version-data");
    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);",
    );

    assert!(
        !serves_extended_shape(store.path()),
        "a freshly created store serves the 1.0 shape, without {EXTENDED_ONLY_TABLE}"
    );

    // Named parameters are matched case-insensitively: a dry run spelled
    // any way is still a dry run, never a one-way raise.
    assert_eq!(
        raise(store.path(), ", DRY_RUN => true"),
        vec![vec!["1.0", "1.1-dev1"]]
    );
    assert!(
        !serves_extended_shape(store.path()),
        "a dry run records nothing"
    );

    assert_eq!(
        raise(store.path(), ", confirm => true"),
        vec![vec!["1.0", "1.1-dev1"]]
    );
    assert!(
        serves_extended_shape(store.path()),
        "the raise lands for sessions that attach after it"
    );
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store.path(),
            "SELECT count(row_group_count) FROM m.ducklake_data_file;",
        )),
        vec![vec!["0"]],
        "the extended shape widens the tables the 1.0 shape already had"
    );

    // Nothing left to raise; the report says so rather than failing.
    assert_eq!(
        raise(store.path(), ", confirm => true"),
        vec![vec!["1.1-dev1", "1.1-dev1"]]
    );
}

/// The pinned DuckLake requires catalog version 1.0 and refuses a raised
/// store outright, at its own version check rather than on a wider table.
/// The raise is a one-way door out of this DuckLake's reach: moraine still
/// serves the store, and nothing but a DuckLake that asks for the wider
/// shape can read the lake again.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extension"]
fn a_raised_store_is_refused_by_the_pinned_ducklake() {
    let store = TempDir::new("raised-lake-store");
    let data = TempDir::new("raised-lake-data");
    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);",
    );
    run_ducklake_sql(
        store.path(),
        data.path(),
        "INSERT INTO lake.main.t VALUES (1), (2);",
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store.path(),
            data.path(),
            "SELECT a FROM lake.main.t ORDER BY a;",
        )),
        vec![vec!["1"], vec!["2"]],
        "the lake reads before the raise"
    );

    assert_eq!(
        raise(store.path(), ", confirm => true"),
        vec![vec!["1.0", "1.1-dev1"]]
    );

    let refused = run_ducklake_sql_expect_err(
        store.path(),
        data.path(),
        "SELECT a FROM lake.main.t ORDER BY a;",
    );
    assert!(
        refused.contains("catalog version mismatch") && refused.contains("1.1-dev1"),
        "expected DuckLake's version refusal, got: {refused}"
    );

    // moraine itself keeps serving the store it raised; only DuckLake's
    // own check stands between the operator and the lake.
    assert!(serves_extended_shape(store.path()));
}

/// A raise that would record refuses without an acknowledgement, and
/// records nothing when it refuses. A dry run needs none: it is the
/// pre-flight for the door.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extension"]
fn a_recording_raise_takes_an_acknowledgement() {
    let store = TempDir::new("raise-confirm-store");
    let data = TempDir::new("raise-confirm-data");
    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);",
    );

    for unacknowledged in ["", ", dry_run => false", ", confirm => false"] {
        let output = run_session(
            &Attach::Standalone {
                store_dir: store.path(),
                read_only: false,
            },
            &format!(
                "SELECT from_version, to_version FROM \
                 moraine_raise_catalog_version('m'{unacknowledged});"
            ),
        );
        let combined = combined_output(&output);
        assert!(
            !output.status.success() && combined.contains("confirm => "),
            "`{unacknowledged}` must refuse and say how to proceed, got: {combined}"
        );
    }

    assert!(
        !serves_extended_shape(store.path()),
        "a refused raise records nothing"
    );

    // The pre-flight itself is unacknowledged, and still reports the move.
    assert_eq!(
        raise(store.path(), ", dry_run => true"),
        vec![vec!["1.0", "1.1-dev1"]]
    );
}
