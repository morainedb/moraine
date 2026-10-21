//! Dumps for the snapshot tables: `ducklake_snapshot` (with its merged
//! `ducklake_snapshot_changes` columns) and `ducklake_schema_versions`.

use std::ffi::{c_char, c_void};

use super::{conversion::dump_row, dump_rows, free_rows};
use crate::{
    error::MoraineError,
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One `ducklake_snapshot`/`ducklake_snapshot_changes` row (merged), as
/// returned by [`moraine_dump_snapshots`]. `next_deletion_id` is
/// moraine-internal gc-file counter bookkeeping, not a DuckLake column,
/// and is not carried here.
#[repr(C)]
pub struct MoraineSnapshotRow {
    /// `snapshot_id`.
    pub snapshot_id: u64,
    /// `snapshot_time`, microseconds since the Unix epoch (UTC).
    pub snapshot_time_micros: i64,
    /// `schema_version`.
    pub schema_version: u64,
    /// `next_catalog_id`.
    pub next_catalog_id: u64,
    /// `next_file_id`.
    pub next_file_id: u64,
    /// `changes_made`, owned.
    pub changes_made: *mut c_char,
    /// `author`, owned, null if absent.
    pub author: *mut c_char,
    /// `commit_message`, owned, null if absent.
    pub commit_message: *mut c_char,
    /// `commit_extra_info`, owned, null if absent.
    pub commit_extra_info: *mut c_char,
}

dump_row! {
    snapshot_rows(moraine::ffi_support::SnapshotRecord) -> MoraineSnapshotRow {
        values { snapshot_id, snapshot_time_micros, schema_version, next_catalog_id, next_file_id }
        optional {}
        strings { changes_made }
        optional_strings { author, commit_message, commit_extra_info }
    }
}

/// Dumps every `ducklake_snapshot` row into `*out_items`/`*out_len`.
/// Snapshots carry no begin/end lifecycle of their own — this is the
/// full committed history, not a current/history split.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach_with_wal_store`](crate::abi::moraine_attach_with_wal_store), valid writable
/// `out_items`/`out_len`, a `probe` callable with `probe_ctx` from any
/// thread, and a null-or-writable `err`, all for the duration of the
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_snapshots(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineSnapshotRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        dump_rows(
            handle,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            moraine::ffi_support::dump_snapshots,
            snapshot_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_snapshots`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching [`moraine_dump_snapshots`]
/// call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_snapshots_free(items: *mut MoraineSnapshotRow, len: usize) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}

/// One `ducklake_schema_versions` row, as returned by
/// [`moraine_dump_schema_versions`]: DuckLake's per-table schema-change
/// history, flattened from the snapshot records it folds into (see
/// `moraine::ffi_support::SchemaVersionRow`).
#[repr(C)]
pub struct MoraineSchemaVersionRow {
    /// `begin_snapshot` — the snapshot the schema change landed in.
    pub begin_snapshot: u64,
    /// `schema_version` — that snapshot's schema version.
    pub schema_version: u64,
    /// `table_id` — the created-or-schema-altered table.
    pub table_id: u64,
}

dump_row! {
    schema_version_rows(moraine::ffi_support::SchemaVersionRow) -> MoraineSchemaVersionRow {
        values { begin_snapshot, schema_version, table_id }
        optional {}
        strings {}
        optional_strings {}
    }
}

/// Dumps every `ducklake_schema_versions` row into
/// `*out_items`/`*out_len`, in snapshot order.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach_with_wal_store`](crate::abi::moraine_attach_with_wal_store), valid writable
/// `out_items`/`out_len`, a `probe` callable with `probe_ctx` from any
/// thread, and a null-or-writable `err`, all for the duration of the
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_schema_versions(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineSchemaVersionRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        dump_rows(
            handle,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            moraine::ffi_support::dump_schema_versions,
            schema_version_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_schema_versions`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_schema_versions`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_schema_versions_free(
    items: *mut MoraineSchemaVersionRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}
