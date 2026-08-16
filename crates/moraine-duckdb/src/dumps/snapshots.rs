//! Dumps for the snapshot tables: `ducklake_snapshot` (with its merged
//! `ducklake_snapshot_changes` columns) and `ducklake_schema_versions`.

use std::ffi::{c_char, c_void};

use super::{dump_rows, free_rows, opt_c_string, opt_into_raw};
use crate::{
    abi::{free_c_string, to_c_string},
    error::{AbiError, MoraineError},
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

/// Converts snapshot records to C rows.
pub(crate) fn snapshot_rows(
    rows: Vec<moraine::ffi_support::SnapshotRecord>,
) -> Result<Vec<MoraineSnapshotRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut v| {
            let changes_made = to_c_string(std::mem::take(&mut v.changes_made))?;
            let author = opt_c_string(v.author.as_deref())?;
            let commit_message = opt_c_string(v.commit_message.as_deref())?;
            let commit_extra_info = opt_c_string(v.commit_extra_info.as_deref())?;
            Ok((v, changes_made, author, commit_message, commit_extra_info))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;
    Ok(owned
        .into_iter()
        .map(
            |(v, changes_made, author, commit_message, commit_extra_info)| MoraineSnapshotRow {
                snapshot_id: v.snapshot_id,
                snapshot_time_micros: v.snapshot_time_micros,
                schema_version: v.schema_version,
                next_catalog_id: v.next_catalog_id,
                next_file_id: v.next_file_id,
                changes_made: changes_made.into_raw(),
                author: opt_into_raw(author),
                commit_message: opt_into_raw(commit_message),
                commit_extra_info: opt_into_raw(commit_extra_info),
            },
        )
        .collect())
}

/// Dumps every `ducklake_snapshot` row into `*out_items`/`*out_len`.
/// Snapshots carry no begin/end lifecycle of their own — this is the
/// full committed history, not a current/history split.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach`](crate::abi::moraine_attach), valid writable
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
        free_rows(items, len, |d| {
            free_c_string(d.changes_made);
            free_c_string(d.author);
            free_c_string(d.commit_message);
            free_c_string(d.commit_extra_info);
        });
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

/// Converts core `ducklake_schema_versions` records into the C row shape.
/// Shared by the committed dump and the transaction-aware one, so the two can
/// never drift in what they report.
// Infallible — this kind's row carries no strings — but it keeps the
// fallible signature every converter shares, so `dump_rows` and the
// transaction-aware dumps take them all the same way.
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn schema_version_rows(
    rows: Vec<moraine::ffi_support::SchemaVersionRow>,
) -> Result<Vec<MoraineSchemaVersionRow>, AbiError> {
    Ok(rows
        .into_iter()
        .map(|v| MoraineSchemaVersionRow {
            begin_snapshot: v.begin_snapshot,
            schema_version: v.schema_version,
            table_id: v.table_id,
        })
        .collect())
}

/// Dumps every `ducklake_schema_versions` row into
/// `*out_items`/`*out_len`, in snapshot order.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach`](crate::abi::moraine_attach), valid writable
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
        free_rows(items, len, |_row| {});
    }
}
