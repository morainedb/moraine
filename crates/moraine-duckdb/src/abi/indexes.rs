//! Index definitions and lifecycle operations.

use std::{
    ffi::{CString, c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
};

use super::{borrow_str, free_array, free_c_string, guard, handle_list, to_c_string};
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// An index's build lifecycle, as [`moraine_indexes`] reports it. `building`
/// collapses everything but `Ready`, so a terminally poisoned index is
/// distinguishable only here.
#[repr(u8)]
#[derive(Clone, Copy)]
pub enum MoraineIndexState {
    /// Serving lookups and enforcing uniqueness.
    Ready = 0,
    /// A staged backfill is in progress; lookups refuse.
    Building = 1,
    /// Awaiting bounded repair after deferred additions; lookups refuse.
    Maintaining = 2,
    /// A duplicate ended the build. Terminal: no progress resumes it.
    Poisoned = 3,
}

/// One index, as returned by [`moraine_indexes`].
#[repr(C)]
pub struct MoraineIndexDesc {
    /// The index's id.
    pub index_id: u64,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
    /// Whether the index is anything but ready. True for a poisoned index,
    /// which no build is advancing — read `state` to tell them apart.
    pub building: bool,
    /// The index's build lifecycle.
    pub state: MoraineIndexState,
    /// The index name, owned — free via [`moraine_indexes_free`].
    pub name: *mut c_char,
}

impl From<moraine::IndexState> for MoraineIndexState {
    fn from(state: moraine::IndexState) -> Self {
        match state {
            moraine::IndexState::Ready => Self::Ready,
            moraine::IndexState::Building => Self::Building,
            moraine::IndexState::Maintaining => Self::Maintaining,
            moraine::IndexState::Poisoned => Self::Poisoned,
        }
    }
}

pub(super) fn resolve_table(
    snapshot: &moraine::CatalogSnapshot,
    schema: &str,
    table: &str,
) -> Result<moraine::TableId, AbiError> {
    let schema = snapshot
        .schema_by_name(schema)
        .ok_or_else(|| AbiError::from(moraine::Error::NotFound(format!("schema {schema}"))))?;
    let table = snapshot
        .table_by_name(schema.id, table)
        .ok_or_else(|| AbiError::from(moraine::Error::NotFound(format!("table {table}"))))?;
    Ok(table.id)
}

/// Borrows an inbound array of C strings.
///
/// # Safety
///
/// `names`/`count` must describe a valid array of `count` non-null,
/// NUL-terminated C strings, valid for the duration of the borrow.
unsafe fn borrow_str_array<'a>(
    names: *const *const c_char,
    count: usize,
    arg: &str,
) -> Result<Vec<&'a str>, AbiError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if names.is_null() {
        return Err(AbiError::invalid_argument(format!("`{arg}` is null")));
    }
    // SAFETY: caller contract that `names`/`count` describe a valid array.
    let slice = unsafe { std::slice::from_raw_parts(names, count) };
    slice
        .iter()
        // SAFETY: each element is a valid C string per the caller contract.
        .map(|&ptr| unsafe { borrow_str(ptr, arg) })
        .collect()
}

/// Builds the per-column [`moraine::ColumnOrder`]s from the ABI's parallel
/// direction / null-placement flag arrays, one `0`/`1` byte per column
/// (never `bool`: reinterpreting a C++ `uint8_t` buffer as `bool` is
/// undefined). Each null pointer defaults its axis (ascending / NULLS
/// LAST); both null yields an empty vec.
///
/// # Safety
///
/// Each non-null pointer must point to `column_count` bytes.
unsafe fn column_orders(
    column_descending: *const u8,
    column_nulls_first: *const u8,
    column_count: usize,
) -> Vec<moraine::ColumnOrder> {
    if column_descending.is_null() && column_nulls_first.is_null() {
        return Vec::new();
    }
    let descending = (!column_descending.is_null()).then(|| {
        // SAFETY: caller contract — non-null points to `column_count` bytes.
        unsafe { std::slice::from_raw_parts(column_descending, column_count) }
    });
    let nulls_first = (!column_nulls_first.is_null()).then(|| {
        // SAFETY: caller contract — non-null points to `column_count` bytes.
        unsafe { std::slice::from_raw_parts(column_nulls_first, column_count) }
    });
    (0..column_count)
        .map(|i| moraine::ColumnOrder {
            direction: if descending.is_some_and(|flags| flags[i] != 0) {
                moraine::Direction::Descending
            } else {
                moraine::Direction::Ascending
            },
            nulls: if nulls_first.is_some_and(|flags| flags[i] != 0) {
                moraine::NullOrder::First
            } else {
                moraine::NullOrder::Last
            },
        })
        .collect()
}

/// Derives the whole backfill and creates the index in one commit.
/// `data_store` is the `DATA_PATH` store when the table holds files to
/// scoped-read, `None` when it holds only inline rows.
///
/// # Safety
///
/// `probe`/`probe_ctx` must satisfy the ABI's cancellation contract.
#[allow(clippy::too_many_arguments)]
unsafe fn create_index_in_one_commit(
    handle: &MoraineCatalogHandle,
    table_id: moraine::TableId,
    def: &moraine::IndexDef,
    orders: &[moraine::ColumnOrder],
    maintenance: moraine::IndexMaintenance,
    data_store: Option<moraine::DataStore>,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
) -> Result<(), AbiError> {
    let reads = handle.catalog.reads();
    let scoped = async {
        match data_store {
            Some(store) => {
                reads
                    .scoped_backfill_entries(store, &handle.data_prefix, table_id, &def.columns)
                    .await
            }
            None => Ok(Vec::new()),
        }
    };
    let inline = reads.inline_backfill_entries(table_id, &def.columns);
    // SAFETY: caller contract for `probe`/`probe_ctx`.
    let (mut backfill, inline) = unsafe {
        handle.block_on_cancellable(probe, probe_ctx, async { tokio::try_join!(scoped, inline) })
    }?;
    backfill.extend(inline);

    // SAFETY: caller contract for `probe`/`probe_ctx`.
    unsafe {
        handle.block_on_commit(
            probe,
            probe_ctx,
            handle.catalog.writer()?.commit(|tx| {
                tx.create_index_ordered_with_maintenance(
                    table_id,
                    def,
                    orders,
                    maintenance,
                    &backfill,
                )?;
                Ok(())
            }),
        )
    }?;
    Ok(())
}

/// Creates an equality index, committing autonomously. With `staged`, runs
/// the multi-commit build — required when the table's backfill exceeds what
/// one commit may stage — and returns once the index is ready; interrupting
/// it leaves the build resumable by the same call.
///
/// `step_entries` and `step_bytes` bound one step of that build (a single
/// object-store request), each `0` for the default; both are ignored
/// without `staged`.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `err`, if non-null,
/// must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_index_create(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    index_name: *const c_char,
    column_names: *const *const c_char,
    column_count: usize,
    column_descending: *const u8,
    column_nulls_first: *const u8,
    unique: bool,
    deferred_maintenance: bool,
    staged: bool,
    step_entries: u64,
    step_bytes: u64,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: caller contract for the string pointers.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract.
        let table = unsafe { borrow_str(table_name, "table_name") }?;
        // SAFETY: caller contract.
        let name = unsafe { borrow_str(index_name, "index_name") }?;
        // SAFETY: caller contract for the column-name array.
        let columns = unsafe { borrow_str_array(column_names, column_count, "column_names") }?;

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.reads().snapshot())
        }?;
        let table_id = resolve_table(&snapshot, schema, table)?;
        let live_columns = snapshot.columns_of(table_id);
        let column_ids = columns
            .iter()
            .map(|column| {
                live_columns
                    .iter()
                    .find(|c| c.name == *column)
                    .map(|found| found.id)
                    .ok_or_else(|| {
                        AbiError::from(moraine::Error::NotFound(format!("column {column}")))
                    })
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        // A table that already holds files must be backfilled from the
        // DATA_PATH store; without one, refuse rather than under-cover.
        let holds_files = !snapshot.data_files_of(table_id).is_empty();
        let data_store = handle_ref.data_store.clone();
        if holds_files && data_store.is_none() {
            return Err(AbiError::from(moraine::Error::Constraint(
                "the table already holds data; attach with META_DATA_PATH so its files can be \
                 scoped-read"
                    .to_owned(),
            )));
        }

        // SAFETY: each non-null orders pointer points to `column_count` bytes,
        // per the caller contract.
        let orders = unsafe { column_orders(column_descending, column_nulls_first, column_count) };

        let def = moraine::IndexDef {
            name: name.to_owned(),
            columns: column_ids,
            unique,
        };
        let maintenance = if deferred_maintenance {
            moraine::IndexMaintenance::Deferred
        } else {
            moraine::IndexMaintenance::Synchronous
        };

        if staged {
            let default = moraine::BuildStep::default();
            let step = moraine::BuildStep {
                entries: if step_entries == 0 {
                    default.entries
                } else {
                    usize::try_from(step_entries).unwrap_or(usize::MAX)
                },
                bytes: if step_bytes == 0 {
                    default.bytes
                } else {
                    step_bytes
                },
            };
            // SAFETY: caller contract for `probe`/`probe_ctx`.
            unsafe {
                handle_ref.block_on_commit(
                    probe,
                    probe_ctx,
                    handle_ref
                        .catalog
                        .writer()?
                        .create_index_staged_with_maintenance(
                            table_id,
                            &def,
                            &orders,
                            maintenance,
                            data_store,
                            &handle_ref.data_prefix,
                            Some(step),
                        ),
                )
            }?;
            return Ok(());
        }

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        unsafe {
            create_index_in_one_commit(
                handle_ref,
                table_id,
                &def,
                &orders,
                maintenance,
                data_store.filter(|_| holds_files),
                probe,
                probe_ctx,
            )
        }
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Drops an equality index by name, committing autonomously.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `err`, if non-null,
/// must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_index_drop(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    index_name: *const c_char,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: caller contract for the string pointers.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract.
        let table = unsafe { borrow_str(table_name, "table_name") }?;
        // SAFETY: caller contract.
        let name = unsafe { borrow_str(index_name, "index_name") }?;

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.reads().snapshot())
        }?;
        let table_id = resolve_table(&snapshot, schema, table)?;
        let index = snapshot
            .index_by_name(table_id, name)
            .ok_or_else(|| AbiError::from(moraine::Error::NotFound(format!("index {name}"))))?;
        let index_id = index.id;
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        unsafe {
            handle_ref.block_on_commit(
                probe,
                probe_ctx,
                handle_ref
                    .catalog
                    .writer()?
                    .commit(move |tx| tx.drop_index(index_id)),
            )
        }?;
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Lists a table's live equality indexes.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `err`, if non-null,
/// must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_indexes(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    out_items: *mut *mut MoraineIndexDesc,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let produce = |handle_ref: &MoraineCatalogHandle| -> Result<Vec<MoraineIndexDesc>, AbiError> {
        // SAFETY: caller contract for the string pointers.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract.
        let table = unsafe { borrow_str(table_name, "table_name") }?;

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.reads().snapshot())
        }?;
        let table_id = resolve_table(&snapshot, schema, table)?;
        let owned: Vec<(u64, bool, moraine::IndexState, CString)> = snapshot
            .indexes_of(table_id)
            .into_iter()
            .map(|index| {
                Ok((
                    index.id.get(),
                    index.unique,
                    index.state,
                    to_c_string(index.name)?,
                ))
            })
            .collect::<Result<_, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|(index_id, unique, state, name)| MoraineIndexDesc {
                index_id,
                unique,
                building: state != moraine::IndexState::Ready,
                state: state.into(),
                name: name.into_raw(),
            })
            .collect())
    };

    // SAFETY: caller contract for the pointers.
    unsafe { handle_list(handle, out_items, out_len, err, produce) }
}

/// Frees the array a [`moraine_indexes`] call returned.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_indexes`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_indexes_free(items: *mut MoraineIndexDesc, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| free_c_string(d.name));
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}
