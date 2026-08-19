//! The inline read ABI surface: `moraine_inline_scan` materializes
//! DuckLake's four inline scan variants (`SCAN_TABLE`/`SCAN_INSERTIONS`/
//! `SCAN_DELETIONS`/`SCAN_FOR_FLUSH`) over the `inline/*` keyspace;
//! `moraine_inline_schemas`/`moraine_inline_registered_tables` serve the
//! per-table Arrow schema and the `ducklake_inlined_data_tables`
//! projection. Same conventions as [`crate::dumps`]: `catch_unwind`/null
//! discipline via [`guard`](crate::abi), owned-first, one `_free` per
//! array. Write-side staging lives in [`crate::staged`].
//!
//! A scan returns two parallel arrays: the [`MoraineInlineRow`]s and the
//! deduplicated [`MoraineInlineChunk`]s their `chunk_index` points into.

use std::{
    ffi::c_void,
    panic::{AssertUnwindSafe, catch_unwind},
};

use bytes::Bytes;
use moraine::ffi_support::inline::InlineScanKind;

use crate::{
    abi::{free_array, guard, write_array},
    dumps::opt_u64,
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

fn decode_scan_kind(v: i32) -> Result<InlineScanKind, AbiError> {
    match v {
        0 => Ok(InlineScanKind::Table),
        1 => Ok(InlineScanKind::Insertions),
        2 => Ok(InlineScanKind::Deletions),
        3 => Ok(InlineScanKind::ForFlush),
        other => Err(AbiError::invalid_argument(format!(
            "moraine_inline_scan: unknown scan_kind {other}"
        ))),
    }
}

/// Hands a shared `Bytes` to C as `(ptr, len, owner)` without copying;
/// `owner` is the boxed `Bytes`, released via [`release_shared_bytes`].
fn share_bytes(bytes: Bytes) -> (*mut u8, usize, *mut c_void) {
    let data = bytes.as_ptr().cast_mut();
    let len = bytes.len();
    let owner = Box::into_raw(Box::new(bytes)).cast::<c_void>();
    (data, len, owner)
}

/// Releases an `owner` minted by [`share_bytes`], if non-null.
///
/// # Safety
///
/// `owner`, if non-null, must be an `owner` [`share_bytes`] returned, not
/// yet released.
unsafe fn release_shared_bytes(owner: *mut c_void) {
    if owner.is_null() {
        return;
    }
    // SAFETY: caller contract above.
    drop(unsafe { Box::from_raw(owner.cast::<Bytes>()) });
}

/// One inlined row, as returned by [`moraine_inline_scan`]: `chunk_index`
/// names the owning chunk in the scan's parallel [`MoraineInlineChunk`]
/// array.
#[repr(C)]
pub struct MoraineInlineRow {
    /// The row's dense id.
    pub row_id: u64,
    /// The schema version the owning chunk was written under — selects the
    /// `inline/schema` its body decodes against.
    pub schema_version: u64,
    /// The commit snapshot that inserted this row.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present (the row is live for `Table`
    /// scans that return it, or tombstoned for the others).
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// The owning chunk: an index into the scan's chunk array.
    pub chunk_index: usize,
    /// The row's offset within its chunk.
    pub offset_in_chunk: u64,
}

/// One referenced chunk's full Arrow IPC record-batch body, owned;
/// returned once per chunk however many rows reference it.
#[repr(C)]
pub struct MoraineInlineChunk {
    /// The chunk's Arrow IPC record-batch body, owned.
    pub body: *mut u8,
    /// `body`'s length in bytes.
    pub body_len: usize,
    /// Opaque owner of `body`, consumed by Arrow decode or scan cleanup.
    pub owner: *mut c_void,
}

impl MoraineInlineChunk {
    pub(crate) fn from_bytes(body: Bytes) -> Self {
        let (body, body_len, owner) = share_bytes(body);
        Self {
            body,
            body_len,
            owner,
        }
    }

    /// Moves the shared body allocation out of this ABI value.
    ///
    /// # Safety
    ///
    /// `self` must have been minted by [`Self::from_bytes`] and not consumed.
    pub(crate) unsafe fn take_bytes(&mut self) -> Result<Bytes, AbiError> {
        if self.owner.is_null() {
            return Err(AbiError::invalid_argument(
                "inline chunk body has already been consumed",
            ));
        }
        let owner = std::mem::replace(&mut self.owner, std::ptr::null_mut());
        self.body = std::ptr::null_mut();
        self.body_len = 0;
        // SAFETY: caller contract guarantees `owner` came from `from_bytes`
        // and has not been consumed.
        Ok(*unsafe { Box::from_raw(owner.cast::<Bytes>()) })
    }

    /// Releases an unconsumed shared body allocation.
    ///
    /// # Safety
    ///
    /// `self` must have been minted by [`Self::from_bytes`].
    unsafe fn release(&mut self) {
        let owner = std::mem::replace(&mut self.owner, std::ptr::null_mut());
        // SAFETY: caller contract guarantees the owner is live.
        unsafe { release_shared_bytes(owner) };
        self.body = std::ptr::null_mut();
        self.body_len = 0;
    }
}

/// Materializes `table_id`'s inlined rows and selects the `scan_kind`
/// variant (`0` = `SCAN_TABLE`, `1` = `SCAN_INSERTIONS`, `2` =
/// `SCAN_DELETIONS`, `3` = `SCAN_FOR_FLUSH`) at `snapshot`, windowed from
/// `start` for the incremental variants (ignored by `SCAN_TABLE`/
/// `SCAN_FOR_FLUSH`). Only rows written under `schema_version` are
/// selected, and only their chunks' bodies are hauled: every caller
/// serves one `ducklake_inlined_data_<t>_<v>` projection, so a
/// schema-evolved table's other versions cost it nothing.
///
/// Cancellable: races the core read against `probe` (polled immediately,
/// then ~100 ms; a null `probe` disables polling). If a cancellation
/// wins, returns [`codes::INTERRUPTED`] and the out-params are left
/// unwritten.
///
/// # Safety
///
/// `handle` must be a pointer previously returned by
/// [`moraine_attach`](crate::abi::moraine_attach) and not yet detached.
/// `out_items`/`out_len` must be valid, writable pointers. `probe`, if
/// non-null, must be safe to call with `probe_ctx` from any thread.
/// `err`, if non-null, must be a valid, writable [`MoraineError`]. All
/// for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_inline_scan(
    handle: *mut MoraineCatalogHandle,
    table_id: u64,
    scan_kind: i32,
    snapshot: u64,
    start: u64,
    schema_version: u64,
    out_items: *mut *mut MoraineInlineRow,
    out_len: *mut usize,
    out_chunks: *mut *mut MoraineInlineChunk,
    out_chunks_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(Vec<MoraineInlineRow>, Vec<MoraineInlineChunk>), AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null()
            || out_len.is_null()
            || out_chunks.is_null()
            || out_chunks_len.is_null()
        {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        let kind = decode_scan_kind(scan_kind)?;
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let record = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::inline::scan_inline(
                    handle_ref.catalog.reads(),
                    table_id,
                    kind,
                    snapshot,
                    start,
                    Some(schema_version),
                ),
            )
        }?;
        let rows = record
            .rows
            .into_iter()
            .map(|row| {
                let (has_end_snapshot, end_snapshot) = opt_u64(row.end_snapshot);
                let chunk_index = usize::try_from(row.chunk_index).map_err(|_| {
                    AbiError::invalid_argument("inline scan chunk index exceeds usize")
                })?;
                Ok(MoraineInlineRow {
                    row_id: row.row_id,
                    schema_version: row.schema_version,
                    begin_snapshot: row.begin_snapshot,
                    has_end_snapshot,
                    end_snapshot,
                    chunk_index,
                    offset_in_chunk: row.offset_in_chunk,
                })
            })
            .collect::<Result<Vec<_>, AbiError>>()?;
        let chunks = record
            .chunk_bodies
            .into_iter()
            .map(MoraineInlineChunk::from_bytes)
            .collect();
        Ok((rows, chunks))
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok((items, chunks)) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe {
                write_array(items, out_items, out_len);
                write_array(chunks, out_chunks, out_chunks_len);
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Frees the row and chunk arrays returned by [`moraine_inline_scan`].
///
/// # Safety
///
/// `items`/`len` and `chunks`/`chunks_len` must be exactly the pointers
/// and lengths written by one matching [`moraine_inline_scan`] call, not
/// yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_inline_scan_free(
    items: *mut MoraineInlineRow,
    len: usize,
    chunks: *mut MoraineInlineChunk,
    chunks_len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |_row| {});
            free_array(chunks, chunks_len, |c| {
                c.release();
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `(schema_version, arrow_schema)` pair, as returned by
/// [`moraine_inline_schemas`].
#[repr(C)]
pub struct MoraineInlineSchemaRow {
    /// The schema's version.
    pub schema_version: u64,
    /// The Arrow IPC schema message, verbatim; borrowed from `owner`.
    pub arrow_schema: *mut u8,
    /// `arrow_schema`'s length in bytes.
    pub arrow_schema_len: usize,
    /// Opaque owner of `arrow_schema`, released by the array's `_free`.
    pub owner: *mut c_void,
}

/// Dumps every `(schema_version, arrow_schema)` recorded for `table_id`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_inline_scan`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_inline_schemas(
    handle: *mut MoraineCatalogHandle,
    table_id: u64,
    out_items: *mut *mut MoraineInlineSchemaRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineInlineSchemaRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let schemas = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::inline::inline_schemas(handle_ref.catalog.reads(), table_id),
            )
        }?;
        Ok(schemas
            .into_iter()
            .map(|(schema_version, bytes)| {
                let (arrow_schema, arrow_schema_len, owner) = share_bytes(bytes);
                MoraineInlineSchemaRow {
                    schema_version,
                    arrow_schema,
                    arrow_schema_len,
                    owner,
                }
            })
            .collect())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(items) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { write_array(items, out_items, out_len) };
            codes::OK
        }
        Err(code) => code,
    }
}

/// Frees an array returned by [`moraine_inline_schemas`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_inline_schemas`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_inline_schemas_free(
    items: *mut MoraineInlineSchemaRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| release_shared_bytes(d.owner));
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `(table_id, schema_version)` pair, as returned by
/// [`moraine_inline_registered_tables`].
#[repr(C)]
pub struct MoraineInlineTableRow {
    /// The table's id.
    pub table_id: u64,
    /// The recorded schema version.
    pub schema_version: u64,
}

/// Dumps every `(table_id, schema_version)` with a recorded inline
/// schema, across every table: the `ducklake_inlined_data_tables`
/// projection.
///
/// # Safety
///
/// Same pointer contract as [`moraine_inline_scan`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_inline_registered_tables(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineInlineTableRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineInlineTableRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let tables = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::inline::inline_registered_tables(handle_ref.catalog.reads()),
            )
        }?;
        Ok(tables
            .into_iter()
            .map(|(table_id, schema_version)| MoraineInlineTableRow {
                table_id,
                schema_version,
            })
            .collect())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(items) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { write_array(items, out_items, out_len) };
            codes::OK
        }
        Err(code) => code,
    }
}

/// Frees an array returned by [`moraine_inline_registered_tables`]. No
/// owned buffers inside — releases only the backing allocation.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_inline_registered_tables`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_inline_registered_tables_free(
    items: *mut MoraineInlineTableRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// Reports whether `table_id` has at least one recorded `inline/file_delete`
/// record, via `*out_exists`; decides whether
/// `ducklake_inlined_delete_<table_id>` exists at all.
///
/// # Safety
///
/// Same pointer contract as [`moraine_inline_scan`], with `out_exists` in
/// place of `out_items`/`out_len`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_inline_file_delete_table_exists(
    handle: *mut MoraineCatalogHandle,
    table_id: u64,
    out_exists: *mut bool,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<bool, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_exists.is_null() {
            return Err(AbiError::invalid_argument("`out_exists` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::inline::inline_file_delete_table_exists(
                    handle_ref.catalog.reads(),
                    table_id,
                ),
            )
        }
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(exists) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { *out_exists = exists };
            codes::OK
        }
        Err(code) => code,
    }
}

/// One `ducklake_inlined_delete_<t>` row, as returned by
/// [`moraine_inline_file_deletes`].
#[repr(C)]
pub struct MoraineInlineFileDeleteRow {
    /// The targeted data file.
    pub file_id: u64,
    /// The deleted row.
    pub row_id: u64,
    /// The commit snapshot the delete takes effect at.
    pub begin_snapshot: u64,
}

/// Dumps every `inline/file_delete` record for `table_id` in
/// `(file_id, row_id)` order: the `ducklake_inlined_delete_<t>` projection.
///
/// # Safety
///
/// Same pointer contract as [`moraine_inline_scan`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_inline_file_deletes(
    handle: *mut MoraineCatalogHandle,
    table_id: u64,
    out_items: *mut *mut MoraineInlineFileDeleteRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineInlineFileDeleteRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let file_deletes = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::inline::inline_file_deletes(
                    handle_ref.catalog.reads(),
                    table_id,
                ),
            )
        }?;
        Ok(file_deletes
            .into_iter()
            .map(
                |(file_id, row_id, begin_snapshot)| MoraineInlineFileDeleteRow {
                    file_id,
                    row_id,
                    begin_snapshot,
                },
            )
            .collect())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(items) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { write_array(items, out_items, out_len) };
            codes::OK
        }
        Err(code) => code,
    }
}

/// Frees an array returned by [`moraine_inline_file_deletes`]. No owned
/// buffers inside — releases only the backing allocation.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_inline_file_deletes`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_inline_file_deletes_free(
    items: *mut MoraineInlineFileDeleteRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::{
        abi::moraine_detach,
        arrow_ipc::MoraineArrowBytes,
        staged::{
            MoraineTxHandle, moraine_tx_stage_inline_flush_delete,
            moraine_tx_stage_inline_inline_delete, moraine_tx_stage_inline_insert_owned,
            moraine_tx_stage_inline_schema_owned,
        },
        test_support::{
            StrArena, TempDir, attach_ok, begin, commit, i64_cell, null_cell, stage, u64_cell,
        },
    };

    /// The `(table_id, schema_version)` pairs `ducklake_inlined_data_tables`
    /// projects, freed before returning.
    fn registered_table_count(handle: *mut MoraineCatalogHandle) -> usize {
        let mut rows: *mut MoraineInlineTableRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_inline_registered_tables(
                handle,
                &raw mut rows,
                &raw mut len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        // SAFETY: matching allocator, just populated above, not yet freed.
        unsafe { moraine_inline_registered_tables_free(rows, len) };
        len
    }

    /// `ducklake_inlined_data_tables` projects committed `inline/schema`
    /// records: a staged registration is absent until its transaction lands.
    #[test]
    fn an_inline_schema_registers_its_table_only_once_committed() {
        let dir = TempDir::new("registered");
        let handle = attach_ok(dir.path());

        let tx = begin(handle);
        let mut err = MoraineError::default();
        // SAFETY: `tx` is live; the schema bytes are owned by the call; `err`
        // is a valid local slot.
        let code = unsafe {
            moraine_tx_stage_inline_schema_owned(
                tx,
                1,
                0,
                MoraineArrowBytes::from_vec(b"schema".to_vec()),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(registered_table_count(handle), 0);

        commit(tx);
        assert_eq!(registered_table_count(handle), 1);

        // SAFETY: `handle` came from `attach_ok` above and is detached
        // exactly once.
        unsafe { moraine_detach(handle) };
    }

    #[test]
    fn inline_chunk_owner_moves_without_copying_the_body() {
        let body = Bytes::from_static(b"inline body");
        let body_pointer = body.as_ptr();
        let mut chunk = MoraineInlineChunk::from_bytes(body);

        assert_eq!(chunk.body.cast_const(), body_pointer);
        // SAFETY: `chunk` owns one `Bytes` minted by `from_bytes`, consumed
        // exactly once here.
        let moved = unsafe { chunk.take_bytes() }.unwrap();
        assert_eq!(moved.as_ptr(), body_pointer);
        assert!(chunk.body.is_null());
    }

    /// Stages the `ducklake_snapshot` + `ducklake_snapshot_changes` pair
    /// every commit needs, regardless of what else is staged alongside it.
    fn stage_snapshot(tx: *mut MoraineTxHandle, arena: &mut StrArena, snapshot_id: u64) {
        stage(
            tx,
            0,
            0,
            &[
                u64_cell(snapshot_id),
                i64_cell(1),
                u64_cell(0),
                u64_cell(1),
                u64_cell(0),
            ],
        );
        stage(
            tx,
            1,
            0,
            &[
                u64_cell(snapshot_id),
                arena.cell("inlined_insert:1"),
                null_cell(),
                null_cell(),
                null_cell(),
            ],
        );
    }

    /// One representative inline read pins the pull channel for the
    /// family — every inline read routes through the same cancellable
    /// bridge.
    #[test]
    fn probe_cancels_inline_registered_tables_then_quiet_probe_succeeds() {
        unsafe extern "C" fn probe_always(_probe_ctx: *mut c_void) -> bool {
            true
        }
        unsafe extern "C" fn probe_never(_probe_ctx: *mut c_void) -> bool {
            false
        }

        let dir = TempDir::new("probe-inline");
        let handle = attach_ok(dir.path());

        let mut items: *mut MoraineInlineTableRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; out/err slots are valid; the
        // probes accept a null context.
        let code = unsafe {
            moraine_inline_registered_tables(
                handle,
                &raw mut items,
                &raw mut len,
                Some(probe_always),
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::INTERRUPTED);
        assert_eq!(err.code, codes::INTERRUPTED);
        assert!(items.is_null());
        // SAFETY: populated by the failed call above, freed exactly once.
        unsafe { crate::abi::moraine_error_free(err.message) };

        let mut items2: *mut MoraineInlineTableRow = ptr::null_mut();
        let mut len2: usize = 0;
        let mut err2 = MoraineError::default();
        // SAFETY: same contracts as above.
        let code2 = unsafe {
            moraine_inline_registered_tables(
                handle,
                &raw mut items2,
                &raw mut len2,
                Some(probe_never),
                ptr::null_mut(),
                &raw mut err2,
            )
        };
        assert_eq!(code2, codes::OK);

        // SAFETY: freed exactly once each.
        unsafe {
            moraine_inline_registered_tables_free(items2, len2);
            moraine_detach(handle);
        }
    }

    /// End-to-end over the ABI: stage an inline schema + insert, commit;
    /// `moraine_inline_scan` (`Table`) returns the row with the right
    /// `row_id`/`begin_snapshot`/body, and `moraine_inline_schemas`/
    /// `moraine_inline_registered_tables` see the schema. Staging an
    /// `inline/inline_delete` then makes the row disappear from a `Table` scan
    /// at or after its `end_snapshot`. Staging a flush-delete then empties
    /// the scan and drops the table from the registered-tables list.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn stage_scan_inline_delete_and_flush_delete_over_the_abi() {
        let dir = TempDir::new("scan");
        let handle = attach_ok(dir.path());

        let tx = begin(handle);
        let mut arena = StrArena::new();
        let schema_bytes = b"schema";
        let mut err = MoraineError::default();
        // SAFETY: `tx` is live; `schema_bytes` is a valid slice; outputs
        // are valid local slots.
        let code = unsafe {
            moraine_tx_stage_inline_schema_owned(
                tx,
                1,
                0,
                MoraineArrowBytes::from_vec(schema_bytes.to_vec()),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);

        let body = b"chunk-body";
        // SAFETY: `tx` is live; `body` is a valid slice; outputs are
        // valid local slots.
        let code = unsafe {
            moraine_tx_stage_inline_insert_owned(
                tx,
                1,
                0,
                1,
                0,
                2,
                MoraineArrowBytes::from_vec(body.to_vec()),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        stage_snapshot(tx, &mut arena, 1);
        commit(tx);

        let mut rows: *mut MoraineInlineRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut chunks: *mut MoraineInlineChunk = ptr::null_mut();
        let mut chunks_len: usize = 0;
        let mut scan_err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_inline_scan(
                handle,
                1,
                0,
                1,
                0,
                0,
                &raw mut rows,
                &raw mut len,
                &raw mut chunks,
                &raw mut chunks_len,
                None,
                ptr::null_mut(),
                &raw mut scan_err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(len, 2);
        // SAFETY: just populated above with `len` live elements.
        let slice = unsafe { std::slice::from_raw_parts(rows, len) };
        assert_eq!(slice[0].row_id, 0);
        assert_eq!(slice[0].begin_snapshot, 1);
        assert!(!slice[0].has_end_snapshot);
        assert_eq!(slice[0].offset_in_chunk, 0);
        // Both rows reference the one chunk, whose body crossed once.
        assert_eq!(chunks_len, 1);
        assert_eq!(slice[0].chunk_index, 0);
        // SAFETY: just populated above with `chunks_len` live elements.
        let chunk = unsafe { &*chunks };
        // SAFETY: `chunk` was just populated with `body_len` live bytes.
        let body_bytes = unsafe { std::slice::from_raw_parts(chunk.body, chunk.body_len) };
        assert_eq!(body_bytes, body);
        assert_eq!(slice[1].row_id, 1);
        assert_eq!(slice[1].offset_in_chunk, 1);
        // SAFETY: matching allocator, not yet freed.
        unsafe { moraine_inline_scan_free(rows, len, chunks, chunks_len) };

        let mut schema_rows: *mut MoraineInlineSchemaRow = ptr::null_mut();
        let mut schema_len: usize = 0;
        let mut schema_err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_inline_schemas(
                handle,
                1,
                &raw mut schema_rows,
                &raw mut schema_len,
                None,
                ptr::null_mut(),
                &raw mut schema_err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(schema_len, 1);
        // SAFETY: just populated above.
        unsafe {
            assert_eq!((*schema_rows).schema_version, 0);
            let bytes = std::slice::from_raw_parts(
                (*schema_rows).arrow_schema,
                (*schema_rows).arrow_schema_len,
            );
            assert_eq!(bytes, schema_bytes);
        }
        // SAFETY: matching allocator, not yet freed.
        unsafe { moraine_inline_schemas_free(schema_rows, schema_len) };

        let mut table_rows: *mut MoraineInlineTableRow = ptr::null_mut();
        let mut table_len: usize = 0;
        let mut table_err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_inline_registered_tables(
                handle,
                &raw mut table_rows,
                &raw mut table_len,
                None,
                ptr::null_mut(),
                &raw mut table_err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(table_len, 1);
        // SAFETY: just populated above.
        unsafe {
            assert_eq!((*table_rows).table_id, 1);
            assert_eq!((*table_rows).schema_version, 0);
        }
        // SAFETY: matching allocator, not yet freed.
        unsafe { moraine_inline_registered_tables_free(table_rows, table_len) };

        // Tombstone row 0; a `Table` scan at snapshot 2 must no longer
        // return it.
        let inline_delete_tx = begin(handle);
        let mut inline_delete_err = MoraineError::default();
        // SAFETY: `inline_delete_tx` is live; outputs are valid local slots.
        let code = unsafe {
            moraine_tx_stage_inline_inline_delete(
                inline_delete_tx,
                1,
                0,
                2,
                &raw mut inline_delete_err,
            )
        };
        assert_eq!(code, codes::OK);
        let mut inline_delete_arena = StrArena::new();
        stage_snapshot(inline_delete_tx, &mut inline_delete_arena, 2);
        commit(inline_delete_tx);

        let mut rows2: *mut MoraineInlineRow = ptr::null_mut();
        let mut len2: usize = 0;
        let mut chunks2: *mut MoraineInlineChunk = ptr::null_mut();
        let mut chunks_len2: usize = 0;
        let mut scan_err2 = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_inline_scan(
                handle,
                1,
                0,
                2,
                0,
                0,
                &raw mut rows2,
                &raw mut len2,
                &raw mut chunks2,
                &raw mut chunks_len2,
                None,
                ptr::null_mut(),
                &raw mut scan_err2,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(len2, 1);
        // SAFETY: just populated above.
        unsafe {
            assert_eq!((*rows2).row_id, 1);
        }
        // SAFETY: matching allocator, not yet freed.
        unsafe { moraine_inline_scan_free(rows2, len2, chunks2, chunks_len2) };

        // Flush: every chunk begun at or before snapshot 2 is removed,
        // along with its consumed inline delete.
        let flush_tx = begin(handle);
        let mut flush_err = MoraineError::default();
        // SAFETY: `flush_tx` is live; outputs are valid local slots.
        let code =
            unsafe { moraine_tx_stage_inline_flush_delete(flush_tx, 1, 0, 2, &raw mut flush_err) };
        assert_eq!(code, codes::OK);
        let mut flush_arena = StrArena::new();
        stage_snapshot(flush_tx, &mut flush_arena, 3);
        commit(flush_tx);

        let mut rows3: *mut MoraineInlineRow = ptr::null_mut();
        let mut len3: usize = 0;
        let mut chunks3: *mut MoraineInlineChunk = ptr::null_mut();
        let mut chunks_len3: usize = 0;
        let mut scan_err3 = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_inline_scan(
                handle,
                1,
                0,
                3,
                0,
                0,
                &raw mut rows3,
                &raw mut len3,
                &raw mut chunks3,
                &raw mut chunks_len3,
                None,
                ptr::null_mut(),
                &raw mut scan_err3,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(len3, 0, "flushed chunk must be gone from the scan");
        // SAFETY: matching allocator (empty, but still owned per the
        // `write_array` contract), not yet freed.
        unsafe { moraine_inline_scan_free(rows3, len3, chunks3, chunks_len3) };

        let mut table_rows2: *mut MoraineInlineTableRow = ptr::null_mut();
        let mut table_len2: usize = 0;
        let mut table_err2 = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_inline_registered_tables(
                handle,
                &raw mut table_rows2,
                &raw mut table_len2,
                None,
                ptr::null_mut(),
                &raw mut table_err2,
            )
        };
        assert_eq!(code, codes::OK);
        // Only the schema was untouched by flush-delete (drop is a
        // separate op) — `ducklake_inlined_data_tables` still lists it
        // until a `stage_inline_drop`.
        assert_eq!(table_len2, 1);
        // SAFETY: matching allocator, not yet freed.
        unsafe { moraine_inline_registered_tables_free(table_rows2, table_len2) };

        // SAFETY: `handle` came from `attach_ok` above and is detached
        // exactly once.
        unsafe { moraine_detach(handle) };
    }
}
