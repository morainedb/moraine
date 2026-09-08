//! Exact row positioning and located deletion commits.

use std::{
    ffi::{CString, c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
};

use super::{
    borrow_str, free_array, free_c_string, guard, resolve_table, to_c_string, write_array,
};
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One `(row_id, data_file_id)` pair to resolve to an exact file position,
/// as [`moraine_locate_row_positions`] takes them. `has_data_file_id` false
/// means the row's file id is NULL — a lookup's report of a live inlined
/// row.
#[repr(C)]
pub struct MorainePositionPair {
    /// The row id to position.
    pub row_id: u64,
    /// The data file it was located in; meaningful only when
    /// `has_data_file_id`.
    pub data_file_id: u64,
    /// Whether `data_file_id` names a file.
    pub has_data_file_id: bool,
}

/// One data file carrying located positions, as
/// [`moraine_locate_row_positions`] groups its answer. Free with
/// [`moraine_locate_row_positions_free_files`].
#[repr(C)]
pub struct MoraineLocatedFile {
    /// The data file these positions are within.
    pub data_file_id: u64,
    /// The file's recorded path, owned.
    pub file_path: *mut c_char,
    /// Newly requested positions within this file, ascending and
    /// duplicate-free, owned.
    pub positions: *mut u64,
    /// Length of `positions`.
    pub positions_len: usize,
    /// Whether a delete file is currently registered against this data
    /// file; `existing_delete_file_id` and `existing_positions` are
    /// meaningful only when set.
    pub has_existing_delete: bool,
    /// The registered delete file's id — the `expires` a later
    /// [`moraine_commit_located_deletion`] call names to replace it.
    pub existing_delete_file_id: u64,
    /// Positions the registered delete file already carries, ascending
    /// and duplicate-free, owned.
    pub existing_positions: *mut u64,
    /// Length of `existing_positions`.
    pub existing_positions_len: usize,
}

/// One [`MoraineLocatedFile`] with every owned piece still a Rust value,
/// converted to raw pointers only in [`moraine_locate_row_positions`]'s
/// final, infallible pass — so a fallible step failing after this is built
/// leaks nothing.
struct LocatedFileOwned {
    data_file_id: u64,
    file_path: CString,
    positions: Vec<u64>,
    existing: Option<(u64, Vec<u64>)>,
}

impl LocatedFileOwned {
    /// Converts to the C representation, minting every raw pointer this
    /// call owns. Infallible: called only after every fallible step of the
    /// same request has already succeeded.
    fn into_raw(self) -> MoraineLocatedFile {
        let (positions, positions_len) = into_c_array(self.positions);
        let (has_existing_delete, existing_delete_file_id, existing_positions, existing_len) =
            match self.existing {
                Some((delete_file_id, positions)) => {
                    let (ptr, len) = into_c_array(positions);
                    (true, delete_file_id, ptr, len)
                }
                None => (false, 0, ptr::null_mut(), 0),
            };
        MoraineLocatedFile {
            data_file_id: self.data_file_id,
            file_path: self.file_path.into_raw(),
            positions,
            positions_len,
            has_existing_delete,
            existing_delete_file_id,
            existing_positions,
            existing_positions_len: existing_len,
        }
    }
}

/// Hands a `Vec<T>` to C as an owned (pointer, length) pair, without
/// writing through an out-parameter — for an array embedded in a struct
/// field rather than a top-level ABI output. Reclaimed the same way
/// [`write_array`]'s output is, via [`free_array`].
fn into_c_array<T>(items: Vec<T>) -> (*mut T, usize) {
    let boxed = items.into_boxed_slice();
    let len = boxed.len();
    (Box::into_raw(boxed).cast::<T>(), len)
}

/// Resolves located rows to exact file positions for deletion without a
/// scan. `pairs` are `(row_id, data_file_id)` as a lookup reports them,
/// `has_data_file_id` false naming a live inlined row.
///
/// Writes `out_files` (one entry per data file carrying a requested
/// position, each with its own positions and whatever delete file is
/// already registered against it) and `out_inlined` (row ids resolved as
/// live inlined rows, from `pairs` entries naming no file). Both arrays are
/// written even when empty, and each must be freed with its own matching
/// `_free` function exactly once.
///
/// Also writes `out_write_directory`: the absolute directory a new delete
/// file for this table belongs in (see
/// [`moraine::CatalogSnapshot::table_write_directory`]), owned — free with
/// [`super::moraine_string_free`]. Resolved from the same snapshot `out_files`
/// was positioned against, so a concurrent table relocation cannot name a
/// directory a moved table no longer writes under. Written only when
/// `out_files` is non-empty, since only then does a caller need to write
/// anything; null otherwise. A caller that does need to write a file and
/// receives null has a typed error instead: an absent `DATA_PATH`, or a
/// table relocated to an absolute path, both fail the whole call.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `pairs` points to
/// `pairs_len` pairs; every `out_*` pointer must be non-null and writable;
/// `probe`/`probe_ctx` must satisfy the interrupt-probe contract; `err`, if
/// non-null, must be writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_locate_row_positions(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    pairs: *const MorainePositionPair,
    pairs_len: usize,
    out_files: *mut *mut MoraineLocatedFile,
    out_files_len: *mut usize,
    out_inlined: *mut *mut u64,
    out_inlined_len: *mut usize,
    out_write_directory: *mut *mut c_char,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    type Output = (Vec<LocatedFileOwned>, Vec<u64>, Option<CString>);

    let attempt = || -> Result<Output, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_files.is_null()
            || out_files_len.is_null()
            || out_inlined.is_null()
            || out_inlined_len.is_null()
            || out_write_directory.is_null()
        {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        if pairs_len > 0 && pairs.is_null() {
            return Err(AbiError::invalid_argument("`pairs` is null"));
        }

        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: caller contract for the string pointers.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract.
        let table = unsafe { borrow_str(table_name, "table_name") }?;
        // SAFETY: caller contract — `pairs` points to `pairs_len` valid pairs.
        let raw_pairs = unsafe {
            if pairs_len == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(pairs, pairs_len)
            }
        };
        let pairs: Vec<(u64, Option<moraine::DataFileId>)> = raw_pairs
            .iter()
            .map(|pair| {
                (
                    pair.row_id,
                    pair.has_data_file_id
                        .then(|| moraine::DataFileId::new(pair.data_file_id)),
                )
            })
            .collect();

        let reads = handle_ref.catalog.reads();
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let snapshot =
            unsafe { handle_ref.block_on_cancellable(probe, probe_ctx, reads.snapshot()) }?;
        let table_id = resolve_table(&snapshot, schema, table)?;

        let data_store = handle_ref.data_store.clone();
        let locate =
            reads.locate_row_positions(data_store, &handle_ref.data_prefix, table_id, &pairs);
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let located = unsafe { handle_ref.block_on_cancellable(probe, probe_ctx, locate) }?;

        let files = located
            .deletions
            .into_iter()
            .map(|deletion| {
                Ok(LocatedFileOwned {
                    data_file_id: deletion.data_file_id.get(),
                    file_path: to_c_string(deletion.file_path)?,
                    positions: deletion.positions,
                    existing: deletion
                        .existing_delete
                        .map(|existing| (existing.delete_file_id.get(), existing.positions)),
                })
            })
            .collect::<Result<Vec<_>, AbiError>>()?;
        let write_directory = located.write_directory.map(to_c_string).transpose()?;

        Ok((files, located.inlined_rows, write_directory))
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok((files, inlined, write_directory)) => {
            // Every fallible step above already succeeded; only raw
            // pointers are minted from here on, so nothing below can leak.
            let files: Vec<MoraineLocatedFile> =
                files.into_iter().map(LocatedFileOwned::into_raw).collect();
            // SAFETY: caller contract for the output pointers.
            unsafe {
                write_array(files, out_files, out_files_len);
                write_array(inlined, out_inlined, out_inlined_len);
                *out_write_directory = write_directory.map_or(ptr::null_mut(), CString::into_raw);
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Frees the array [`moraine_locate_row_positions`] wrote to
/// `out_files`/`out_files_len`, including each file's nested positions
/// arrays.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written there by a
/// matching call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_locate_row_positions_free_files(
    items: *mut MoraineLocatedFile,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above; each nested array is exactly what
        // this same call's `into_raw` minted via `into_c_array`.
        unsafe {
            free_array(items, len, |item| {
                free_c_string(item.file_path);
                free_array(item.positions, item.positions_len, |_| {});
                free_array(item.existing_positions, item.existing_positions_len, |_| {});
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// Frees the array [`moraine_locate_row_positions`] wrote to
/// `out_inlined`/`out_inlined_len`.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written there by a
/// matching call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_locate_row_positions_free_inlined(items: *mut u64, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above. Plain values own no heap.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One delete file to land through [`moraine_commit_located_deletion`], as
/// [`moraine::DeleteFileRegistration`] takes it.
#[repr(C)]
pub struct MorainePositionedDeleteFile {
    /// The data file this delete file's positions apply to.
    pub data_file_id: u64,
    /// The delete file's bare file name (no path components) within the
    /// table's own data directory, where the caller already wrote it.
    /// Borrowed for the duration of the call.
    pub path: *const c_char,
    /// Total file size in bytes.
    pub file_size: u64,
    /// Parquet footer size in bytes.
    pub footer_size: u64,
    /// Number of positions the file records.
    pub delete_count: u64,
    /// The delete file this one replaces; meaningful only when
    /// `has_expires`.
    pub expires: u64,
    /// Whether `expires` names a delete file to expire in the same commit.
    pub has_expires: bool,
    /// Physical positions within the data file that this registration
    /// newly marks dead, borrowed for the duration of the call — the
    /// positions a `moraine_locate_row_positions` call resolved for this
    /// file, used only to derive index entry removals for the table's
    /// live equality indexes.
    pub new_positions: *const u64,
    /// Length of `new_positions`.
    pub new_positions_len: usize,
}

/// Lands located deletions through the register/expire/inline-delete
/// verbs in one autonomous commit — see
/// [`moraine::Catalog::commit_located_deletion`]. `registrations` are the
/// delete files the caller already wrote; `inlined_rows` are row ids to
/// tombstone directly. Writes the minted snapshot id to `out_snapshot_id`.
///
/// An interrupted call can leave its commit running in the background.
/// On failure, retain the supplied files for orphan cleanup: the error
/// does not establish that no catalog references were committed.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `registrations`
/// points to `registrations_len` descriptors, each with a valid `path` and
/// a `new_positions` valid for `new_positions_len` entries; `inlined_rows`
/// points to `inlined_rows_len` row ids; `out_snapshot_id` must be
/// writable; `probe`/`probe_ctx` must satisfy the interrupt-probe
/// contract; `err`, if non-null, must be writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_commit_located_deletion(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    registrations: *const MorainePositionedDeleteFile,
    registrations_len: usize,
    inlined_rows: *const u64,
    inlined_rows_len: usize,
    out_snapshot_id: *mut u64,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<u64, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_snapshot_id.is_null() {
            return Err(AbiError::invalid_argument("`out_snapshot_id` is null"));
        }
        if registrations_len > 0 && registrations.is_null() {
            return Err(AbiError::invalid_argument("`registrations` is null"));
        }
        if inlined_rows_len > 0 && inlined_rows.is_null() {
            return Err(AbiError::invalid_argument("`inlined_rows` is null"));
        }

        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: caller contract for the string pointers.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract.
        let table = unsafe { borrow_str(table_name, "table_name") }?;

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.reads().snapshot())
        }?;
        let table_id = resolve_table(&snapshot, schema, table)?;

        // SAFETY: caller contract — `registrations` points to
        // `registrations_len` valid descriptors.
        let raw_registrations = unsafe {
            if registrations_len == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(registrations, registrations_len)
            }
        };
        let registrations = raw_registrations
            .iter()
            .map(|registration| {
                // SAFETY: caller contract — each descriptor's `path` is a
                // valid NUL-terminated C string for this call.
                let path = unsafe { borrow_str(registration.path, "registration path") }?;
                let new_positions = if registration.new_positions_len == 0 {
                    &[]
                } else {
                    if registration.new_positions.is_null() {
                        return Err(AbiError::invalid_argument(
                            "registration `new_positions` is null",
                        ));
                    }
                    // SAFETY: caller contract — `new_positions` points to
                    // `new_positions_len` valid positions.
                    unsafe {
                        std::slice::from_raw_parts(
                            registration.new_positions,
                            registration.new_positions_len,
                        )
                    }
                };
                Ok(moraine::DeleteFileRegistration {
                    data_file_id: moraine::DataFileId::new(registration.data_file_id),
                    path: path.to_owned(),
                    file_size: registration.file_size,
                    footer_size: registration.footer_size,
                    delete_count: registration.delete_count,
                    expires: registration
                        .has_expires
                        .then(|| moraine::DeleteFileId::new(registration.expires)),
                    new_positions: new_positions.to_vec(),
                })
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        // SAFETY: caller contract — `inlined_rows` points to
        // `inlined_rows_len` row ids.
        let inlined_rows = unsafe {
            if inlined_rows_len == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(inlined_rows, inlined_rows_len)
            }
        };

        let writer = handle_ref.catalog.writer()?;
        let data_store = handle_ref.data_store.clone();
        let commit = writer.commit_located_deletion(
            data_store,
            &handle_ref.data_prefix,
            table_id,
            &registrations,
            inlined_rows,
        );
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let snapshot_id = unsafe { handle_ref.block_on_commit(probe, probe_ctx, commit) }?;
        Ok(snapshot_id.get())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(snapshot_id) => {
            // SAFETY: caller contract — `out_snapshot_id` is non-null and
            // writable.
            unsafe {
                *out_snapshot_id = snapshot_id;
            }
            codes::OK
        }
        Err(code) => code,
    }
}
