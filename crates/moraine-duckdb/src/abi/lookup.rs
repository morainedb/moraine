//! Index key conversion and row lookups.

use std::{
    ffi::{c_char, c_void},
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
};

use super::{borrow_bytes, borrow_str, free_array, handle_list, resolve_table};
use crate::{
    error::{AbiError, MoraineError},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One stable row id returned by an index lookup, and the file currently
/// holding it. A row id can appear more than once when more than one
/// current file is a candidate for it.
#[repr(C)]
pub struct MoraineRowId {
    /// The numeric row id.
    pub value: u64,
    /// The file holding it; meaningful only when `has_data_file_id`.
    pub data_file_id: u64,
    /// Whether `data_file_id` names a file. False for a live inlined row
    /// and for one this lookup could not place.
    pub has_data_file_id: bool,
}

/// Runs `lookup` and places the row ids it yields in the table's current
/// files, in one cancellable round trip; falls back to the unplaced form
/// when no `DATA_PATH` store was given at attach.
///
/// # Safety
///
/// `probe`/`probe_ctx` must satisfy the interrupt-probe contract.
unsafe fn abi_located_row_ids(
    handle_ref: &MoraineCatalogHandle,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    table_id: moraine::TableId,
    lookup: impl Future<Output = moraine::Result<Vec<u64>>>,
) -> Result<Vec<MoraineRowId>, AbiError> {
    let reads = handle_ref.catalog.reads();
    // SAFETY: caller contract for `probe`/`probe_ctx`.
    let located = unsafe {
        handle_ref.block_on_cancellable(probe, probe_ctx, async {
            let row_ids = lookup.await?;
            reads
                .locate_row_ids(
                    handle_ref.data_store.clone(),
                    &handle_ref.data_prefix,
                    table_id,
                    row_ids,
                )
                .await
        })
    }?;

    Ok(located
        .into_iter()
        .map(|candidate| MoraineRowId {
            value: candidate.row_id,
            data_file_id: candidate.data_file_id.map_or(0, moraine::DataFileId::get),
            has_data_file_id: candidate.data_file_id.is_some(),
        })
        .collect())
}

/// The table, index, and table columns an index entry point names.
struct ResolvedIndex<'a> {
    name: &'a str,
    table_id: moraine::TableId,
    index: moraine::IndexInfo,
    columns: Vec<moraine::ColumnInfo>,
}

/// Resolves the `schema.table.index` an index entry point names against
/// the current snapshot.
///
/// # Safety
///
/// The name pointers must be valid NUL-terminated C strings for `'a`;
/// `probe`/`probe_ctx` must satisfy the interrupt-probe contract.
unsafe fn resolve_index<'a>(
    handle_ref: &MoraineCatalogHandle,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    schema_name: *const c_char,
    table_name: *const c_char,
    index_name: *const c_char,
) -> Result<ResolvedIndex<'a>, AbiError> {
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
    let columns = snapshot.columns_of(table_id);

    Ok(ResolvedIndex {
        name,
        table_id,
        index,
        columns,
    })
}

/// A value passed to [`moraine_index_lookup`], tagged by kind. The shim
/// fills the field matching `kind`; the ABI coerces it to the indexed
/// column's canonical form.
#[repr(C)]
pub struct MoraineLookupValue {
    /// `0`=IS NULL (a prefix predicate for [`moraine_index_nulls`]), `1`=i64,
    /// `2`=u64, `3`=f64, `4`=bool, `5`=string, `6`=bytes.
    pub kind: i32,
    /// Valid iff `kind == 1`.
    pub i64_value: i64,
    /// Valid iff `kind == 2`.
    pub u64_value: u64,
    /// Valid iff `kind == 3`.
    pub f64_value: f64,
    /// Valid iff `kind == 4`.
    pub bool_value: bool,
    /// Valid iff `kind == 5`: a borrowed, NUL-terminated UTF-8 string.
    pub str_value: *const c_char,
    /// Valid iff `kind == 6`: a borrowed byte buffer of `bytes_len` bytes.
    pub bytes_value: *const u8,
    /// Length of `bytes_value` when `kind == 6`.
    pub bytes_len: usize,
}

/// One complete equality key passed to [`moraine_index_in`].
#[repr(C)]
pub struct MoraineLookupKey {
    /// The key's values, in the index's column order.
    pub values: *const MoraineLookupValue,
    /// Number of entries in `values`.
    pub values_len: usize,
}

/// Coerces a lookup value to the canonical [`IndexKeyValue`] for a column of
/// DuckLake type `ducklake_type`, through the core's coercion table.
///
/// # Safety
///
/// If `raw.kind` is `5` (string) or `6` (bytes), its pointer fields must be
/// valid per the ABI contract for the duration of this call.
pub(super) unsafe fn coerce_lookup_value(
    raw: &MoraineLookupValue,
    ducklake_type: &str,
) -> Result<moraine::IndexKeyValue, AbiError> {
    use moraine::ffi_support::index::{LookupInput, coerce_lookup_value};

    let input = match raw.kind {
        1 => LookupInput::Int(raw.i64_value),
        2 => LookupInput::UInt(raw.u64_value),
        3 => LookupInput::Float(raw.f64_value),
        4 => LookupInput::Bool(raw.bool_value),
        5 => {
            // SAFETY: caller contract — a `kind == 5` value's string pointer
            // is a valid NUL-terminated C string for this call.
            let text = unsafe { borrow_str(raw.str_value, "lookup value") }?;
            LookupInput::Str(text.to_owned())
        }
        6 => {
            // SAFETY: caller contract — a `kind == 6` value's byte pointer is
            // valid for `bytes_len` bytes for this call.
            let bytes = unsafe { borrow_bytes(raw.bytes_value, raw.bytes_len, "lookup value") }?;
            LookupInput::Bytes(bytes.to_vec())
        }
        other => {
            return Err(AbiError::invalid_argument(format!(
                "index lookup: unknown value kind {other}"
            )));
        }
    };
    coerce_lookup_value(&input, ducklake_type).map_err(AbiError::invalid_argument)
}

/// The refusal for a NULL in an equality key or range bound.
fn no_null_in_key() -> AbiError {
    AbiError::invalid_argument(
        "NULL is not a value to match; use moraine_index_nulls for an IS NULL query",
    )
}

/// Coerces a run of ABI values against the index's leading columns, in the
/// index's column order. A `kind == 0` value is the `IS NULL` predicate and
/// yields `None`; callers that admit no NULL reject it before calling.
///
/// # Safety
///
/// Each value's string/bytes fields, where its kind uses them, must be valid
/// for this call.
unsafe fn coerce_index_key(
    index: &moraine::IndexInfo,
    index_name: &str,
    table_id: moraine::TableId,
    columns: &[moraine::ColumnInfo],
    raw: &[MoraineLookupValue],
) -> Result<Vec<Option<moraine::IndexKeyValue>>, AbiError> {
    raw.iter()
        .enumerate()
        .map(|(position, value)| {
            if value.kind == 0 {
                return Ok(None);
            }
            let column_id = index.columns.get(position).ok_or_else(|| {
                AbiError::invalid_argument(format!(
                    "index key of {} values does not fit the {}-column index {index_name}",
                    raw.len(),
                    index.columns.len()
                ))
            })?;
            let column = columns.iter().find(|c| c.id == *column_id).ok_or_else(|| {
                AbiError::from(moraine::Error::Corruption(format!(
                    "index {index_name} covers column {column_id} absent from table {table_id}"
                )))
            })?;
            // SAFETY: caller contract for the value's string/bytes fields.
            unsafe { coerce_lookup_value(value, &column.column_type) }.map(Some)
        })
        .collect()
}

/// Resolves an equality lookup to the rows currently holding `values` — one
/// [`MoraineLookupValue`] per indexed column, in the index's column order,
/// each coerced to its column's type. The count must equal the index's
/// column count: a composite equality key names every column (a leading
/// prefix is not an equality lookup — use [`moraine_index_nulls`] or
/// [`moraine_index_range`] for that).
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `values` points to
/// `values_len` values; `err`, if non-null, must be writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_index_lookup(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    index_name: *const c_char,
    values: *const MoraineLookupValue,
    values_len: usize,
    out_items: *mut *mut MoraineRowId,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let produce = |handle_ref: &MoraineCatalogHandle| -> Result<Vec<MoraineRowId>, AbiError> {
        if values_len == 0 {
            return Err(AbiError::invalid_argument("index lookup: no value given"));
        }
        if values.is_null() {
            return Err(AbiError::invalid_argument("`values` is null"));
        }
        // SAFETY: caller contract for the string pointers and `probe`/`probe_ctx`.
        let ResolvedIndex {
            name,
            table_id,
            index,
            columns,
        } = unsafe {
            resolve_index(
                handle_ref,
                probe,
                probe_ctx,
                schema_name,
                table_name,
                index_name,
            )
        }?;
        if values_len != index.columns.len() {
            return Err(AbiError::invalid_argument(format!(
                "index lookup: {values_len} values do not address the {}-column index {name}; an \
                 equality lookup names every column",
                index.columns.len()
            )));
        }
        // SAFETY: non-null checked; caller contract — `values` points to
        // `values_len` values whose string/bytes fields (if used) are valid.
        let raw_values = unsafe { std::slice::from_raw_parts(values, values_len) };
        // SAFETY: caller contract for each value's string/bytes fields.
        let key = unsafe { coerce_index_key(&index, name, table_id, &columns, raw_values) }?
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(no_null_in_key)?;
        let lookup = handle_ref
            .catalog
            .reads()
            .index_lookup(table_id, index.id, &key);
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        unsafe { abi_located_row_ids(handle_ref, probe, probe_ctx, table_id, lookup) }
    };

    // SAFETY: caller contract for the pointers.
    unsafe { handle_list(handle, out_items, out_len, err, produce) }
}

/// Frees the array a [`moraine_index_lookup`] call returned.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_index_lookup`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_index_lookup_free(items: *mut MoraineRowId, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above. The descriptor owns no heap.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// Resolves an `IN` lookup to the union of rows holding any complete key.
/// Each key is coerced to the indexed columns' canonical types. Duplicate
/// keys are probed once; a key containing NULL matches no row; an empty key
/// list returns no rows after validating the index.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `keys` points to
/// `keys_len` descriptors, and each descriptor's `values` points to
/// `values_len` values. `err`, if non-null, must be writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_index_in(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    index_name: *const c_char,
    keys: *const MoraineLookupKey,
    keys_len: usize,
    out_items: *mut *mut MoraineRowId,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let produce = |handle_ref: &MoraineCatalogHandle| -> Result<Vec<MoraineRowId>, AbiError> {
        if keys.is_null() && keys_len != 0 {
            return Err(AbiError::invalid_argument(
                "`keys` is null but its length is nonzero",
            ));
        }
        // SAFETY: caller contract for the string pointers and `probe`/`probe_ctx`.
        let ResolvedIndex {
            name,
            table_id,
            index,
            columns,
        } = unsafe {
            resolve_index(
                handle_ref,
                probe,
                probe_ctx,
                schema_name,
                table_name,
                index_name,
            )
        }?;
        let raw_keys = if keys_len == 0 {
            &[]
        } else {
            // SAFETY: non-null checked above; caller contract says the array
            // contains `keys_len` readable descriptors.
            unsafe { std::slice::from_raw_parts(keys, keys_len) }
        };
        // A NULL-bearing key matches no row, so it contributes no probe.
        let coerced_keys = raw_keys
            .iter()
            .filter_map(|raw_key| {
                if raw_key.values_len != index.columns.len() {
                    return Some(Err(AbiError::invalid_argument(format!(
                        "index IN: {} values do not address the {}-column index {name}; an \
                         equality lookup names every column",
                        raw_key.values_len,
                        index.columns.len()
                    ))));
                }
                if raw_key.values.is_null() {
                    return Some(Err(AbiError::invalid_argument("`key.values` is null")));
                }
                // SAFETY: caller contract for this descriptor's nested array.
                let raw_values =
                    unsafe { std::slice::from_raw_parts(raw_key.values, raw_key.values_len) };
                // SAFETY: caller contract for each value's string/bytes fields.
                let coerced =
                    unsafe { coerce_index_key(&index, name, table_id, &columns, raw_values) };
                match coerced {
                    Ok(values) => values.into_iter().collect::<Option<Vec<_>>>().map(Ok),
                    Err(error) => Some(Err(error)),
                }
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        let lookup =
            handle_ref
                .catalog
                .reads()
                .index_lookup_many(table_id, index.id, &coerced_keys);
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        unsafe { abi_located_row_ids(handle_ref, probe, probe_ctx, table_id, lookup) }
    };

    // SAFETY: caller contract for the pointers.
    unsafe { handle_list(handle, out_items, out_len, err, produce) }
}

/// Frees the array a [`moraine_index_in`] call returned.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a matching
/// [`moraine_index_in`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_index_in_free(items: *mut MoraineRowId, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above. The descriptor owns no heap.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// Resolves a comparison query to the rows whose leading indexed values fall
/// between the bounds. Each bound is a run of `lower_len`/`upper_len`
/// [`MoraineLookupValue`]s over the index's leading columns — equality on all
/// but the last named column, a comparison on the last; a null pointer or a
/// zero length is an open (unbounded) side. A present bound is `Included`
/// when its `*_inclusive` flag is set, `Excluded` otherwise. Results come
/// back in the index's stored order, or its opposite when `reverse` is set.
///
/// # Safety
///
/// Every non-null pointer must be valid per the ABI contract; `lower_values`
/// points to `lower_len` values and `upper_values` to `upper_len`; `err`, if
/// non-null, must be writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_index_range(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    index_name: *const c_char,
    lower_values: *const MoraineLookupValue,
    lower_len: usize,
    lower_inclusive: bool,
    upper_values: *const MoraineLookupValue,
    upper_len: usize,
    upper_inclusive: bool,
    reverse: bool,
    out_items: *mut *mut MoraineRowId,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    use std::ops::Bound;

    let produce = |handle_ref: &MoraineCatalogHandle| -> Result<Vec<MoraineRowId>, AbiError> {
        let lower_empty = lower_values.is_null() || lower_len == 0;
        let upper_empty = upper_values.is_null() || upper_len == 0;
        if lower_empty && upper_empty {
            return Err(AbiError::invalid_argument(
                "index range: at least one bound must be present",
            ));
        }
        // SAFETY: caller contract for the string pointers and `probe`/`probe_ctx`.
        let ResolvedIndex {
            name,
            table_id,
            index,
            columns,
        } = unsafe {
            resolve_index(
                handle_ref,
                probe,
                probe_ctx,
                schema_name,
                table_name,
                index_name,
            )
        }?;

        let build_bound = |values: *const MoraineLookupValue,
                           len: usize,
                           inclusive: bool|
         -> Result<Bound<Vec<moraine::IndexKeyValue>>, AbiError> {
            if values.is_null() || len == 0 {
                return Ok(Bound::Unbounded);
            }
            if len > index.columns.len() {
                return Err(AbiError::invalid_argument(format!(
                    "index range: a bound of {len} values does not fit the {}-column index {name}",
                    index.columns.len()
                )));
            }
            // SAFETY: non-null checked; caller contract — `values` points to
            // `len` values whose string/bytes fields (if used) are valid.
            let raw = unsafe { std::slice::from_raw_parts(values, len) };
            // SAFETY: caller contract for each value's string/bytes fields.
            let coerced = unsafe { coerce_index_key(&index, name, table_id, &columns, raw) }?
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .ok_or_else(no_null_in_key)?;
            Ok(if inclusive {
                Bound::Included(coerced)
            } else {
                Bound::Excluded(coerced)
            })
        };
        let lower = build_bound(lower_values, lower_len, lower_inclusive)?;
        let upper = build_bound(upper_values, upper_len, upper_inclusive)?;

        let lookup = handle_ref
            .catalog
            .reads()
            .index_range(table_id, index.id, lower, upper, reverse);
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        unsafe { abi_located_row_ids(handle_ref, probe, probe_ctx, table_id, lookup) }
    };

    // SAFETY: caller contract for the pointers.
    unsafe { handle_list(handle, out_items, out_len, err, produce) }
}

/// Frees the array a [`moraine_index_range`] call returned.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a matching
/// [`moraine_index_range`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_index_range_free(items: *mut MoraineRowId, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above. The descriptor owns no heap.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// Resolves an `IS NULL` query on an index to the matching rows. `prefix` is a
/// leading run of predicates over the index's columns: a `MoraineLookupValue`
/// of `kind == 0` is `IS NULL` for that column, any other kind is `= value`.
/// At least one must be `IS NULL`; a bare non-leading `IS NULL` is not
/// expressible (the prefix covers the leading columns).
///
/// # Safety
///
/// Every non-null pointer must be valid per the ABI contract; `prefix` points
/// to `prefix_len` values; `err`, if non-null, must be writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_index_nulls(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    index_name: *const c_char,
    prefix: *const MoraineLookupValue,
    prefix_len: usize,
    reverse: bool,
    out_items: *mut *mut MoraineRowId,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let produce = |handle_ref: &MoraineCatalogHandle| -> Result<Vec<MoraineRowId>, AbiError> {
        if prefix_len == 0 {
            return Err(AbiError::invalid_argument(
                "index nulls: the prefix names no predicate",
            ));
        }
        if prefix.is_null() {
            return Err(AbiError::invalid_argument("`prefix` is null"));
        }
        // SAFETY: caller contract for the string pointers and `probe`/`probe_ctx`.
        let ResolvedIndex {
            name,
            table_id,
            index,
            columns,
        } = unsafe {
            resolve_index(
                handle_ref,
                probe,
                probe_ctx,
                schema_name,
                table_name,
                index_name,
            )
        }?;
        if prefix_len > index.columns.len() {
            return Err(AbiError::invalid_argument(
                "index nulls: the prefix is longer than the index",
            ));
        }
        // SAFETY: non-null checked; caller contract — `prefix` points to
        // `prefix_len` values.
        let prefix_slice = unsafe { std::slice::from_raw_parts(prefix, prefix_len) };
        // SAFETY: caller contract for each predicate's string/bytes fields.
        let values = unsafe { coerce_index_key(&index, name, table_id, &columns, prefix_slice) }?;

        let lookup = handle_ref
            .catalog
            .reads()
            .index_nulls(table_id, index.id, values, reverse);
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        unsafe { abi_located_row_ids(handle_ref, probe, probe_ctx, table_id, lookup) }
    };

    // SAFETY: caller contract for the pointers.
    unsafe { handle_list(handle, out_items, out_len, err, produce) }
}

/// Frees the array a [`moraine_index_nulls`] call returned.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a matching
/// [`moraine_index_nulls`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_index_nulls_free(items: *mut MoraineRowId, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above. The descriptor owns no heap.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}
