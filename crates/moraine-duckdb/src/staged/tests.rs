use std::ptr;

use super::*;
use crate::{
    abi::moraine_detach,
    dumps::MoraineDataFileRow,
    test_support::{
        StrArena, TempDir, attach_ok, attach_with_data_path, begin, bool_cell, commit, i64_cell,
        null_cell, stage, u64_cell,
    },
};

/// Stages a full `ducklake_data_file` row (`table_kind` 6,
/// `operation_kind` 0) against `table_id`.
fn stage_data_file_row(
    tx: *mut MoraineTxHandle,
    arena: &mut StrArena,
    data_file_id: u64,
    table_id: u64,
    begin_snapshot: u64,
) {
    stage(
        tx,
        6,
        0,
        &[
            u64_cell(data_file_id),
            u64_cell(table_id),
            u64_cell(begin_snapshot),
            null_cell(),
            null_cell(),
            arena.cell("data-1.parquet"),
            bool_cell(true),
            arena.cell("parquet"),
            u64_cell(10),
            u64_cell(1024),
            u64_cell(64),
            u64_cell(0),
            null_cell(),
            null_cell(),
            null_cell(),
            null_cell(),
        ],
    );
}

/// A commit that registers data files warms the tables it touched.
#[test]
fn a_commit_registering_data_files_warms_those_tables() {
    let lake = TempDir::new("commit-warm");
    let data = TempDir::new("commit-warm-data");
    let handle = attach_with_data_path(lake.path(), data.path());
    // Drained first, so what remains at the end is the commit's own.
    // SAFETY: attached above and not yet detached.
    unsafe { &*handle }.finish_warming();

    let tx = begin(handle);
    let mut arena = StrArena::new();
    stage_table_row(tx, &mut arena, 1, (1, None), 0, "t", "t/");
    stage_data_file_row(tx, &mut arena, 1, 1, 1);
    stage_snapshot_and_changes(tx, &mut arena, 1, 1, 2, "inserted_into_table:1");
    stage(tx, 11, 0, &[u64_cell(1), u64_cell(1), u64_cell(1)]);
    commit(tx);

    // SAFETY: still attached.
    let warming = unsafe { &*handle }.finish_warming();

    assert_eq!(warming, 1, "the commit spawned no warming pass");
    // SAFETY: attached above, detached exactly once.
    unsafe { moraine_detach(handle) };
}

/// A commit that registers no data files spawns no warming.
#[test]
fn a_commit_registering_no_data_files_warms_nothing() {
    let lake = TempDir::new("commit-warm-none");
    let data = TempDir::new("commit-warm-none-data");
    let handle = attach_with_data_path(lake.path(), data.path());
    // SAFETY: attached above and not yet detached.
    unsafe { &*handle }.finish_warming();

    let tx = begin(handle);
    let mut arena = StrArena::new();
    stage_table_row(tx, &mut arena, 1, (1, None), 0, "t", "t/");
    stage_snapshot_and_changes(tx, &mut arena, 1, 1, 2, r#"created_table:"main"."t""#);
    stage(tx, 11, 0, &[u64_cell(1), u64_cell(1), u64_cell(1)]);
    commit(tx);

    // SAFETY: still attached.
    let warming = unsafe { &*handle }.finish_warming();

    assert_eq!(warming, 0);
    // SAFETY: attached above, detached exactly once.
    unsafe { moraine_detach(handle) };
}

/// Stages a full `ducklake_table` row (`table_kind` 3, `operation_kind` 0):
/// id, a synthetic uuid, begin/end snapshot (`lifecycle`), schema id,
/// name, path (always relative) — the shape every test that creates or
/// renames a table needs.
fn stage_table_row(
    tx: *mut MoraineTxHandle,
    arena: &mut StrArena,
    table_id: u64,
    lifecycle: (u64, Option<u64>),
    schema_id: u64,
    name: &str,
    path: &str,
) {
    let (begin_snapshot, end_snapshot) = lifecycle;
    stage(
        tx,
        3,
        0,
        &[
            u64_cell(table_id),
            arena.cell(&format!("uuid-t{table_id}")),
            u64_cell(begin_snapshot),
            end_snapshot.map_or_else(null_cell, u64_cell),
            u64_cell(schema_id),
            arena.cell(name),
            arena.cell(path),
            bool_cell(true),
        ],
    );
}

/// Stages the `ducklake_snapshot` + `ducklake_snapshot_changes` pair
/// every commit in this module bumps: id, time (same value as `id` in
/// every fixture here), schema version, and `next_catalog_id`, plus the
/// changes-made text DuckLake records for the same snapshot.
fn stage_snapshot_and_changes(
    tx: *mut MoraineTxHandle,
    arena: &mut StrArena,
    snapshot_id: u64,
    schema_version: u64,
    next_catalog_id: u64,
    changes_made: &str,
) {
    stage(
        tx,
        0,
        0,
        &[
            u64_cell(snapshot_id),
            i64_cell(i64::try_from(snapshot_id).expect("test snapshot id fits i64")),
            u64_cell(schema_version),
            u64_cell(next_catalog_id),
            u64_cell(0),
        ],
    );
    stage(
        tx,
        1,
        0,
        &[
            u64_cell(snapshot_id),
            arena.cell(changes_made),
            null_cell(),
            null_cell(),
            null_cell(),
        ],
    );
}

/// A DuckLake-shaped snapshot bump plus table create: table `t` (id
/// 1, schema 0 = bootstrap's `main`) with one column, staged and
/// committed over the ABI as one batch, then verified through the
/// dump ABI (the same view the metadata-table scan serves).
#[test]
fn stages_table_create_and_snapshot_bump_over_the_abi() {
    let dir = TempDir::new("create");
    let handle = attach_ok(dir.path());
    let tx = begin(handle);

    let mut arena = StrArena::new();
    stage_table_row(tx, &mut arena, 1, (1, None), 0, "t", "t/");
    // ducklake_column: column_id, begin_snapshot, end_snapshot,
    // table_id, column_order, column_name, column_type,
    // initial_default, default_value, nulls_allowed, parent_column,
    // default_value_type, default_value_dialect.
    stage(
        tx,
        5,
        0,
        &[
            u64_cell(1),
            u64_cell(0),
            null_cell(),
            u64_cell(1),
            u64_cell(0),
            arena.cell("a"),
            arena.cell("BIGINT"),
            null_cell(),
            null_cell(),
            bool_cell(true),
            null_cell(),
            null_cell(),
            null_cell(),
        ],
    );
    stage_snapshot_and_changes(tx, &mut arena, 1, 1, 2, r#"created_table:"main"."t""#);
    // ducklake_schema_versions: begin_snapshot, schema_version,
    // table_id — DuckLake writes one per created table, carrying this
    // commit's own snapshot values.
    stage(tx, 11, 0, &[u64_cell(1), u64_cell(1), u64_cell(1)]);

    let mut snapshot_id: u64 = 0;
    let mut err = MoraineError::default();
    // SAFETY: `tx` is live; outputs are valid local slots.
    let code = unsafe {
        moraine_tx_commit(
            tx,
            &raw mut snapshot_id,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    // SAFETY: `err.message` is null or was just written by `moraine_tx_commit`.
    assert_eq!(code, codes::OK, "commit failed: {:?}", unsafe {
        err.message.as_ref()
    });
    assert_eq!(snapshot_id, 1);

    let mut rows: *mut crate::dumps::MoraineTableRow = ptr::null_mut();
    let mut len: usize = 0;
    let mut dump_err = MoraineError::default();
    // SAFETY: `handle` is attached; outputs are valid local slots.
    let code = unsafe {
        crate::dumps::moraine_dump_tables(
            handle,
            &raw mut rows,
            &raw mut len,
            None,
            ptr::null_mut(),
            &raw mut dump_err,
        )
    };
    assert_eq!(code, codes::OK);
    assert_eq!(len, 1);
    // SAFETY: just populated above.
    let name = unsafe { std::ffi::CStr::from_ptr((*rows).table_name) }
        .to_str()
        .unwrap();
    assert_eq!(name, "t");

    // The schema-version row folded into the snapshot record and
    // flattens back out of the dump verbatim.
    let mut versions: *mut crate::dumps::MoraineSchemaVersionRow = ptr::null_mut();
    let mut versions_len: usize = 0;
    let mut versions_err = MoraineError::default();
    // SAFETY: `handle` is attached; outputs are valid local slots.
    let code = unsafe {
        crate::dumps::moraine_dump_schema_versions(
            handle,
            &raw mut versions,
            &raw mut versions_len,
            None,
            ptr::null_mut(),
            &raw mut versions_err,
        )
    };
    assert_eq!(code, codes::OK);
    assert_eq!(versions_len, 1);
    // SAFETY: just populated above.
    unsafe {
        assert_eq!((*versions).begin_snapshot, 1);
        assert_eq!((*versions).schema_version, 1);
        assert_eq!((*versions).table_id, 1);
    }

    // SAFETY: each from its matching allocator; freed exactly once.
    unsafe {
        crate::dumps::moraine_dump_tables_free(rows, len);
        crate::dumps::moraine_dump_schema_versions_free(versions, versions_len);
        moraine_detach(handle);
    }
}

/// The other two op kinds over the wire: an `update_set_end` row
/// (`operation_kind` 2 — the C++ UPDATE operator's staging for a
/// rename/drop) moves the old table version to history, and a raw
/// `delete` row (`operation_kind` 1) removes an unversioned statistics
/// row. Cell layouts here are exactly what `cpp/staged_write.cpp`'s
/// Sinks emit: key cells in decoder order, plus (for `update_set_end`)
/// the new `end_snapshot`.
#[test]
fn update_set_end_and_stats_delete_over_the_abi() {
    let dir = TempDir::new("end-delete");
    let handle = attach_ok(dir.path());

    // Snapshot 1: table `t` (id 1, in bootstrap's `main`) plus its
    // stats row.
    let setup = begin(handle);
    let mut arena = StrArena::new();
    stage_table_row(setup, &mut arena, 1, (1, None), 0, "t", "t/");
    // ducklake_table_stats: table_id, record_count, next_row_id,
    // file_size_bytes.
    stage(
        setup,
        8,
        0,
        &[u64_cell(1), u64_cell(0), u64_cell(0), u64_cell(0)],
    );
    stage_snapshot_and_changes(setup, &mut arena, 1, 1, 2, r#"created_table:"main"."t""#);
    let mut id: u64 = 0;
    let mut err = MoraineError::default();
    // SAFETY: `setup` is live; outputs are valid local slots.
    let code =
        unsafe { moraine_tx_commit(setup, &raw mut id, None, ptr::null_mut(), &raw mut err) };
    assert_eq!(code, codes::OK);

    // Snapshot 2: end `t`'s live version (rename shape: end + new
    // version) and delete its stats row.
    let tx = begin(handle);
    // update_set_end on ducklake_table: [table_id, new end_snapshot].
    stage(tx, 3, 2, &[u64_cell(1), u64_cell(2)]);
    stage_table_row(tx, &mut arena, 1, (2, None), 0, "t2", "t/");
    // delete on ducklake_table_stats: [table_id].
    stage(tx, 8, 1, &[u64_cell(1)]);
    stage_snapshot_and_changes(tx, &mut arena, 2, 2, 2, "altered_table:1");
    let mut id2: u64 = 0;
    let mut err2 = MoraineError::default();
    // SAFETY: `tx` is live; outputs are valid local slots.
    let code = unsafe { moraine_tx_commit(tx, &raw mut id2, None, ptr::null_mut(), &raw mut err2) };
    // SAFETY: `err2.message` is null or was just written.
    assert_eq!(code, codes::OK, "commit failed: {:?}", unsafe {
        err2.message.as_ref()
    });
    assert_eq!(id2, 2);

    // The dump serves both versions: history `t` ended at 2, current `t2`.
    let mut rows: *mut crate::dumps::MoraineTableRow = ptr::null_mut();
    let mut len: usize = 0;
    let mut dump_err = MoraineError::default();
    // SAFETY: `handle` is attached; outputs are valid local slots.
    let code = unsafe {
        crate::dumps::moraine_dump_tables(
            handle,
            &raw mut rows,
            &raw mut len,
            None,
            ptr::null_mut(),
            &raw mut dump_err,
        )
    };
    assert_eq!(code, codes::OK);
    assert_eq!(len, 2);
    // SAFETY: just populated above with `len` live elements.
    let table_rows = unsafe { std::slice::from_raw_parts(rows, len) };
    let ended = table_rows.iter().find(|r| r.has_end_snapshot).unwrap();
    let live = table_rows.iter().find(|r| !r.has_end_snapshot).unwrap();
    assert_eq!(ended.end_snapshot, 2);
    // SAFETY: owned C strings written above, not yet freed.
    let live_name = unsafe { std::ffi::CStr::from_ptr(live.table_name) }
        .to_str()
        .unwrap();
    assert_eq!(live_name, "t2");

    // The stats row is gone (unversioned raw delete, no history mirror).
    let mut stats: *mut crate::dumps::MoraineTableStatsRow = ptr::null_mut();
    let mut stats_len: usize = 0;
    let mut stats_err = MoraineError::default();
    // SAFETY: `handle` is attached; outputs are valid local slots.
    let code = unsafe {
        crate::dumps::moraine_dump_table_stats(
            handle,
            &raw mut stats,
            &raw mut stats_len,
            None,
            ptr::null_mut(),
            &raw mut stats_err,
        )
    };
    assert_eq!(code, codes::OK);
    assert_eq!(stats_len, 0);

    // SAFETY: each from its matching allocator; freed exactly once.
    unsafe {
        crate::dumps::moraine_dump_tables_free(rows, len);
        crate::dumps::moraine_dump_table_stats_free(stats, stats_len);
        moraine_detach(handle);
    }
}

/// A lost race at commit is never retried: the loser's error carries
/// the literal substring `conflict`, and the store reflects only the
/// winner.
#[test]
fn lost_race_at_commit_is_not_retried_and_carries_conflict_text() {
    let dir = TempDir::new("race");
    let handle = attach_ok(dir.path());

    let tx_a = begin(handle);
    let tx_b = begin(handle);

    for (tx, name) in [(tx_a, "a"), (tx_b, "b")] {
        let mut arena = StrArena::new();
        // ducklake_schema: schema_id, schema_uuid, begin_snapshot,
        // end_snapshot, schema_name, path, path_is_relative.
        stage(
            tx,
            2,
            0,
            &[
                u64_cell(1),
                arena.cell(&format!("uuid-{name}")),
                u64_cell(1),
                null_cell(),
                arena.cell(name),
                arena.cell(&format!("{name}/")),
                bool_cell(true),
            ],
        );
        stage(
            tx,
            0,
            0,
            &[
                u64_cell(1),
                i64_cell(1),
                u64_cell(1),
                u64_cell(2),
                u64_cell(0),
            ],
        );
        stage(
            tx,
            1,
            0,
            &[
                u64_cell(1),
                arena.cell(&format!(r#"created_schema:"{name}""#)),
                null_cell(),
                null_cell(),
                null_cell(),
            ],
        );
        // Leak `arena` to keep its `CString`s alive past every `stage`
        // call's borrowed pointers.
        std::mem::forget(arena);
    }

    let mut id_a: u64 = 0;
    let mut err_a = MoraineError::default();
    // SAFETY: `tx_a` is live; outputs are valid local slots.
    let code_a =
        unsafe { moraine_tx_commit(tx_a, &raw mut id_a, None, ptr::null_mut(), &raw mut err_a) };
    assert_eq!(code_a, codes::OK);

    let mut id_b: u64 = 0;
    let mut err_b = MoraineError::default();
    // SAFETY: `tx_b` is live; outputs are valid local slots.
    let code_b =
        unsafe { moraine_tx_commit(tx_b, &raw mut id_b, None, ptr::null_mut(), &raw mut err_b) };
    assert_eq!(code_b, codes::COMMIT_CONFLICT);
    assert_eq!(err_b.code, codes::COMMIT_CONFLICT);
    assert!(!err_b.message.is_null());
    // SAFETY: just populated above.
    let msg = unsafe { std::ffi::CStr::from_ptr(err_b.message) }
        .to_str()
        .unwrap();
    assert!(msg.contains("conflict"), "message: {msg}");

    // SAFETY: `err_b.message`/`handle` came from the calls above and
    // are each freed exactly once.
    unsafe {
        crate::abi::moraine_error_free(err_b.message);
        moraine_detach(handle);
    }
}

/// `moraine_tx_rollback` discards every staged row: nothing lands.
#[test]
fn rollback_discards_staged_rows() {
    let dir = TempDir::new("rollback");
    let handle = attach_ok(dir.path());
    let tx = begin(handle);

    let mut arena = StrArena::new();
    stage(
        tx,
        2,
        0,
        &[
            u64_cell(1),
            arena.cell("uuid-r"),
            u64_cell(1),
            null_cell(),
            arena.cell("rolled_back"),
            arena.cell("rolled_back/"),
            bool_cell(true),
        ],
    );

    // SAFETY: `tx` is live, not yet committed or rolled back.
    unsafe { moraine_tx_rollback(tx) };

    let mut rows: *mut crate::dumps::MoraineSchemaRow = ptr::null_mut();
    let mut len: usize = 0;
    let mut err = MoraineError::default();
    // SAFETY: `handle` is attached; outputs are valid local slots.
    let code = unsafe {
        crate::dumps::moraine_dump_schemas(
            handle,
            &raw mut rows,
            &raw mut len,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK);
    // Only bootstrap's `main` schema — the rolled-back row never
    // landed.
    assert_eq!(len, 1);

    // SAFETY: each from its matching allocator; freed exactly once.
    unsafe {
        crate::dumps::moraine_dump_schemas_free(rows, len);
        moraine_detach(handle);
    }
}

/// A malformed staged row (wrong cell count) fails loudly as a
/// corruption error at commit, not a panic.
#[test]
fn malformed_row_reports_corruption_not_a_panic() {
    let dir = TempDir::new("malformed");
    let handle = attach_ok(dir.path());
    let tx = begin(handle);

    // Far too few cells for `ducklake_schema`.
    stage(tx, 2, 0, &[u64_cell(1)]);
    stage(
        tx,
        0,
        0,
        &[
            u64_cell(1),
            i64_cell(1),
            u64_cell(1),
            u64_cell(2),
            u64_cell(0),
        ],
    );
    let mut arena = StrArena::new();
    stage(
        tx,
        1,
        0,
        &[
            u64_cell(1),
            arena.cell(""),
            null_cell(),
            null_cell(),
            null_cell(),
        ],
    );

    let mut snapshot_id: u64 = 0;
    let mut err = MoraineError::default();
    // SAFETY: `tx` is live; outputs are valid local slots.
    let code = unsafe {
        moraine_tx_commit(
            tx,
            &raw mut snapshot_id,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::CORRUPTION);

    // SAFETY: `err.message`/`handle` came from the calls above and are
    // each freed exactly once.
    unsafe {
        crate::abi::moraine_error_free(err.message);
        moraine_detach(handle);
    }
}

/// A probe reporting an interrupt cancels BEGIN before anything is
/// staged: out-param unwritten, and the same handle immediately opens
/// a transaction once the probe is quiet.
#[test]
fn probe_cancels_tx_begin_then_quiet_probe_succeeds() {
    unsafe extern "C" fn probe_always(_probe_ctx: *mut c_void) -> bool {
        true
    }
    unsafe extern "C" fn probe_never(_probe_ctx: *mut c_void) -> bool {
        false
    }

    let dir = TempDir::new("probe-tx-begin");
    let handle = attach_ok(dir.path());

    let mut tx: *mut MoraineTxHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: `handle` is attached; `tx`/`err` are valid local slots;
    // the probes accept a null context.
    let code = unsafe {
        moraine_tx_begin(
            handle,
            ptr::null(),
            &raw mut tx,
            Some(probe_always),
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::INTERRUPTED);
    assert_eq!(err.code, codes::INTERRUPTED);
    assert!(tx.is_null());
    // SAFETY: populated by the failed call above, freed exactly once.
    unsafe { crate::abi::moraine_error_free(err.message) };

    let mut tx2: *mut MoraineTxHandle = ptr::null_mut();
    let mut err2 = MoraineError::default();
    // SAFETY: same contracts as above.
    let code2 = unsafe {
        moraine_tx_begin(
            handle,
            ptr::null(),
            &raw mut tx2,
            Some(probe_never),
            ptr::null_mut(),
            &raw mut err2,
        )
    };
    assert_eq!(code2, codes::OK);
    assert!(!tx2.is_null());

    // SAFETY: `tx2` consumed exactly once; `handle` freed exactly once.
    unsafe {
        moraine_tx_rollback(tx2);
        moraine_detach(handle);
    }
}

#[test]
fn begin_on_null_handle_reports_invalid_argument() {
    let mut tx: *mut MoraineTxHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: a null `handle` is exactly the input this test exercises;
    // outputs are valid local slots.
    let code = unsafe {
        moraine_tx_begin(
            ptr::null_mut(),
            ptr::null(),
            &raw mut tx,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::INVALID_ARGUMENT);
    assert!(tx.is_null());
    // SAFETY: `err.message` was just populated above and not yet freed.
    unsafe { crate::abi::moraine_error_free(err.message) };
}

#[test]
fn rollback_on_null_tx_is_a_no_op() {
    // SAFETY: a null `tx` is exactly the input this test exercises,
    // documented as a no-op.
    unsafe { moraine_tx_rollback(ptr::null_mut()) };
}

#[test]
fn stage_rejects_unknown_table_kind() {
    let dir = TempDir::new("bad-kind");
    let handle = attach_ok(dir.path());
    let tx = begin(handle);

    let mut err = MoraineError::default();
    // SAFETY: `tx` is live; empty cells slice; outputs are valid.
    let code = unsafe { moraine_tx_stage(tx, 99, 0, ptr::null(), 0, &raw mut err) };
    assert_eq!(code, codes::INVALID_ARGUMENT);

    // SAFETY: `err.message`/`tx`/`handle` came from the calls above.
    unsafe {
        crate::abi::moraine_error_free(err.message);
        moraine_tx_rollback(tx);
        moraine_detach(handle);
    }
}

/// The tx-aware snapshot dump observes the transaction's own staged
/// snapshot deletes; the plain dump keeps serving committed state.
#[test]
fn tx_dump_snapshots_observes_staged_deletes() {
    let dir = TempDir::new("rywr");
    let handle = attach_ok(dir.path());

    // Seed snapshot 1 so the store holds {0, 1}.
    let tx = begin(handle);
    let mut arena = StrArena::new();
    stage_table_row(tx, &mut arena, 1, (1, None), 0, "t", "t/");
    stage_snapshot_and_changes(tx, &mut arena, 1, 1, 2, "created_table:\"main\".\"t\"");
    let mut snapshot_id: u64 = 0;
    let mut err = MoraineError::default();
    // SAFETY: `tx` is live; outputs are valid local slots.
    let code = unsafe {
        moraine_tx_commit(
            tx,
            &raw mut snapshot_id,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK);

    // Stage (but do not commit) an expiry of snapshot 0.
    let tx = begin(handle);
    stage(tx, 0, 1, &[u64_cell(0)]);

    let mut rows: *mut crate::dumps::MoraineSnapshotRow = ptr::null_mut();
    let mut len: usize = 0;
    let mut dump_err = MoraineError::default();
    // SAFETY: `tx` is live; outputs are valid local slots.
    let code =
        unsafe { moraine_tx_dump_snapshots(tx, &raw mut rows, &raw mut len, &raw mut dump_err) };
    assert_eq!(code, codes::OK);
    assert_eq!(len, 1, "the staged delete must hide snapshot 0");
    // SAFETY: just populated above.
    assert_eq!(unsafe { (*rows).snapshot_id }, 1);
    // SAFETY: freed exactly once.
    unsafe { crate::dumps::moraine_dump_snapshots_free(rows, len) };

    // Outside the transaction, committed state is unchanged.
    let mut committed: *mut crate::dumps::MoraineSnapshotRow = ptr::null_mut();
    let mut committed_len: usize = 0;
    let mut committed_err = MoraineError::default();
    // SAFETY: `handle` is attached; outputs are valid local slots.
    let code = unsafe {
        crate::dumps::moraine_dump_snapshots(
            handle,
            &raw mut committed,
            &raw mut committed_len,
            None,
            ptr::null_mut(),
            &raw mut committed_err,
        )
    };
    assert_eq!(code, codes::OK);
    assert_eq!(committed_len, 2);
    // SAFETY: freed exactly once; `tx` rolled back exactly once.
    unsafe {
        crate::dumps::moraine_dump_snapshots_free(committed, committed_len);
        moraine_tx_rollback(tx);
        moraine_detach(handle);
    }
}

/// A stage and a dump race on one handle without corrupting it: DuckDB
/// drives concurrent pipelines through one staged tx — a Sink staging
/// while another scan's init dumps — and the handle serializes them.
#[test]
fn a_stage_and_a_dump_race_safely_on_one_handle() {
    let lake = TempDir::new("staged-race");
    let handle = attach_ok(lake.path());
    let tx = begin(handle);

    let tx_addr = tx as usize;
    let stager = std::thread::spawn(move || {
        let tx = tx_addr as *mut MoraineTxHandle;
        let mut arena = StrArena::new();
        for i in 0..200 {
            stage_data_file_row(tx, &mut arena, i, 1, 1);
        }
    });

    for _ in 0..200 {
        let mut rows: *mut MoraineDataFileRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `tx` stays live until the join below; outputs are
        // valid local slots.
        let code =
            unsafe { moraine_tx_dump_data_files(tx, &raw mut rows, &raw mut len, &raw mut err) };
        // SAFETY: `err.message` is null or was just written by the dump.
        let err_message = unsafe { err.message.as_ref() };
        assert_eq!(code, codes::OK, "dump failed: {err_message:?}");
        // SAFETY: exactly the array the dump just wrote.
        unsafe { crate::dumps::moraine_dump_data_files_free(rows, len) };
    }
    stager.join().expect("the staging thread panicked");

    // SAFETY: `tx` rolled back exactly once; `handle` detached exactly
    // once.
    unsafe {
        moraine_tx_rollback(tx);
        moraine_detach(handle);
    }
}
