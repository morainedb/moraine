//! Translation of scan predicates to core index probes.

use std::ffi::{c_char, c_void};

use moraine::IndexComparison;

use super::{
    MoraineLookupKey, MoraineLookupValue, MoraineRowId, abi_located_row_ids, coerce_lookup_value,
};
use crate::{
    abi::{borrow_str, handle_list, resolve_table},
    error::{AbiError, MoraineError},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One conjunctive comparison on a stored column.
#[repr(C)]
pub struct MoraineIndexPredicate {
    /// Stable catalog field identifier.
    pub field_id: u64,
    /// 0: equality list, 1: IS NULL, 2: >, 3: >=, 4: <, 5: <=, 6: implied
    /// equality to the one value every row holds, which extends a key but
    /// is never residual.
    pub kind: u8,
    /// Constants for the comparison; empty for IS NULL.
    pub values: *const MoraineLookupValue,
    /// Number of constants.
    pub values_len: usize,
}

/// Resolves a conjunction through a ready index, or declines via `out_covered`.
/// `out_certified` means every supplied predicate was answered exactly by a
/// certified index. Original residual predicates remain the caller's
/// responsibility.
///
/// # Safety
/// All input arrays and their nested pointers must be readable for their
/// lengths. Output pointers must be writable; strings and interrupt callbacks
/// satisfy the ABI contract.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_index_predicate_rows(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    predicates: *const MoraineIndexPredicate,
    predicates_len: usize,
    out_items: *mut *mut MoraineRowId,
    out_len: *mut usize,
    out_covered: *mut bool,
    out_certified: *mut bool,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let produce = |handle_ref: &MoraineCatalogHandle| -> Result<(Vec<MoraineRowId>, moraine::MemoryReservation), AbiError> {
        if out_covered.is_null() || out_certified.is_null() || predicates.is_null() {
            return Err(AbiError::invalid_argument(
                "null predicate or coverage pointer",
            ));
        }
        // SAFETY: checked non-null; output validity is the caller's contract.
        unsafe {
            *out_covered = false;
            *out_certified = false;
        }
        // SAFETY: caller contract for strings.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract for strings.
        let table = unsafe { borrow_str(table_name, "table_name") }?;
        let reads = handle_ref.catalog.reads();
        // SAFETY: caller contract for interrupt callbacks.
        let snapshot =
            unsafe { handle_ref.block_on_cancellable(probe, probe_ctx, moraine::ffi_support::catalog_read::index_table(reads, schema, table, false)) }?;
        let table_id = resolve_table(&snapshot, schema, table)?;
        let columns = snapshot.columns_of(table_id);
        // SAFETY: non-null above; caller provides the array length.
        let raw = unsafe { std::slice::from_raw_parts(predicates, predicates_len) };
        let predicates = raw
            .iter()
            .map(|predicate| {
                let column = columns
                    .iter()
                    .find(|column| column.id.get() == predicate.field_id)
                    .ok_or_else(|| AbiError::invalid_argument("unknown predicate column"))?;
                let values = if predicate.values_len == 0 {
                    &[]
                } else {
                    if predicate.values.is_null() {
                        return Err(AbiError::invalid_argument("null predicate values"));
                    }
                    // SAFETY: checked non-null; caller provides readable descriptors.
                    unsafe { std::slice::from_raw_parts(predicate.values, predicate.values_len) }
                };
                let values = values
                    .iter()
                    .filter(|value| value.kind != 0)
                    .map(|value| {
                        // SAFETY: nested constant buffers satisfy the caller's contract.
                        unsafe { coerce_lookup_value(value, &column.column_type) }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let comparison = match predicate.kind {
                    0 => IndexComparison::Any(values),
                    1 => IndexComparison::IsNull,
                    2..=6 if values.len() == 1 => {
                        let value = values
                            .into_iter()
                            .next()
                            .ok_or_else(|| AbiError::invalid_argument("missing bound"))?;
                        match predicate.kind {
                            2 | 3 => IndexComparison::Lower(value, predicate.kind == 3),
                            4 | 5 => IndexComparison::Upper(value, predicate.kind == 5),
                            _ => IndexComparison::Implied(value),
                        }
                    }
                    _ => return Err(AbiError::invalid_argument("invalid predicate comparison")),
                };
                Ok(moraine::IndexPredicate {
                    column: column.id,
                    comparison,
                })
            })
            .collect::<Result<Vec<_>, AbiError>>()?;
        // SAFETY: caller contract for interrupt callbacks.
        let result = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                reads.index_predicate_rows_projected(&snapshot, table_id, &predicates),
            )
        }?;
        let Some(result) = result else {
            return Ok((Vec::new(), moraine::MemoryReservation::reserve(0)?));
        };
        // SAFETY: output pointers checked above.
        unsafe {
            *out_covered = true;
            *out_certified = result.certified_exact;
        }
        // SAFETY: caller contract for interrupt callbacks.
        unsafe {
            abi_located_row_ids(handle_ref, probe, probe_ctx, table_id, async {
                Ok(result.rows)
            })
        }
    };
    // SAFETY: caller contract for handle and output pointers.
    unsafe { handle_list(handle, out_items, out_len, err, produce) }
}

/// Resolves a set of complete equality keys over `columns`, named by field id
/// in any order, through a ready index over those columns, or declines via
/// `out_covered`. Each key lists one value per column in the order of
/// `columns`; a NULL-bearing key matches no row. `implied` holds kind-6
/// predicates, one value each, for columns every row holds one value in; an
/// index may fill its remaining columns from them. `out_certified` means the
/// index is certified or derived-only, so the rows are the exact answer.
/// Free the result with `moraine_index_lookup_free`.
///
/// # Safety
/// All input arrays and their nested pointers must be readable for their
/// lengths. Output pointers must be writable; strings and interrupt callbacks
/// satisfy the ABI contract.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_index_key_rows(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    columns: *const u64,
    columns_len: usize,
    keys: *const MoraineLookupKey,
    keys_len: usize,
    implied: *const MoraineIndexPredicate,
    implied_len: usize,
    out_items: *mut *mut MoraineRowId,
    out_len: *mut usize,
    out_covered: *mut bool,
    out_certified: *mut bool,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let produce = |handle_ref: &MoraineCatalogHandle| -> Result<(Vec<MoraineRowId>, moraine::MemoryReservation), AbiError> {
        if out_covered.is_null() || out_certified.is_null() || columns.is_null() {
            return Err(AbiError::invalid_argument(
                "null column or coverage pointer",
            ));
        }
        if (keys.is_null() && keys_len != 0) || (implied.is_null() && implied_len != 0) {
            return Err(AbiError::invalid_argument(
                "`keys` or `implied` is null but its length is nonzero",
            ));
        }
        // SAFETY: checked non-null; output validity is the caller's contract.
        unsafe {
            *out_covered = false;
            *out_certified = false;
        }
        // SAFETY: caller contract for strings.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract for strings.
        let table = unsafe { borrow_str(table_name, "table_name") }?;
        let reads = handle_ref.catalog.reads();
        // SAFETY: caller contract for interrupt callbacks.
        let snapshot =
            unsafe { handle_ref.block_on_cancellable(probe, probe_ctx, moraine::ffi_support::catalog_read::index_table(reads, schema, table, false)) }?;
        let table_id = resolve_table(&snapshot, schema, table)?;
        let table_columns = snapshot.columns_of(table_id);
        let raw_implied = if implied_len == 0 {
            &[]
        } else {
            // SAFETY: checked non-null; caller provides readable descriptors.
            unsafe { std::slice::from_raw_parts(implied, implied_len) }
        };
        // SAFETY: caller contract for each descriptor's nested buffers.
        let implied = unsafe { implied_constants(raw_implied, &table_columns) }?;
        // SAFETY: non-null above; caller provides the array length.
        let named = unsafe { std::slice::from_raw_parts(columns, columns_len) }
            .iter()
            .map(|field_id| {
                table_columns
                    .iter()
                    .find(|column| column.id.get() == *field_id)
                    .ok_or_else(|| AbiError::invalid_argument("unknown key column"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let raw_keys = if keys_len == 0 {
            &[]
        } else {
            // SAFETY: checked non-null; caller provides readable descriptors.
            unsafe { std::slice::from_raw_parts(keys, keys_len) }
        };
        // SAFETY: caller contract for each key's nested array and buffers.
        let coerced_keys = unsafe { complete_keys(raw_keys, &named) }?;
        let column_ids = named.iter().map(|column| column.id).collect::<Vec<_>>();
        // SAFETY: caller contract for interrupt callbacks.
        let result = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                reads.index_key_rows_projected(
                    &snapshot,
                    table_id,
                    &column_ids,
                    &coerced_keys,
                    &implied,
                ),
            )
        }?;
        let Some(result) = result else {
            return Ok((Vec::new(), moraine::MemoryReservation::reserve(0)?));
        };
        // SAFETY: output pointers checked above.
        unsafe {
            *out_covered = true;
            *out_certified = result.certified_exact;
        }
        // SAFETY: caller contract for interrupt callbacks.
        unsafe {
            abi_located_row_ids(handle_ref, probe, probe_ctx, table_id, async {
                Ok(result.rows)
            })
        }
    };
    // SAFETY: caller contract for handle and output pointers.
    unsafe { handle_list(handle, out_items, out_len, err, produce) }
}

/// Coerces kind-6 predicates to the constants every row holds, one per
/// column.
///
/// # Safety
/// Each descriptor's `values` must point at one readable lookup value whose
/// nested buffers satisfy the ABI contract.
unsafe fn implied_constants(
    raw: &[MoraineIndexPredicate],
    columns: &[moraine::ColumnInfo],
) -> Result<Vec<(moraine::ColumnId, moraine::IndexKeyValue)>, AbiError> {
    raw.iter()
        .map(|predicate| {
            let column = columns
                .iter()
                .find(|column| column.id.get() == predicate.field_id)
                .ok_or_else(|| AbiError::invalid_argument("unknown implied column"))?;
            if predicate.kind != 6 || predicate.values_len != 1 || predicate.values.is_null() {
                return Err(AbiError::invalid_argument(
                    "an implied predicate is kind 6 with one value",
                ));
            }
            // SAFETY: checked non-null with one readable descriptor.
            let value = unsafe { &*predicate.values };
            // SAFETY: nested constant buffers satisfy the caller's contract.
            let value = unsafe { coerce_lookup_value(value, &column.column_type) }?;
            Ok((column.id, value))
        })
        .collect()
}

/// Coerces each key to one value per named column, dropping a NULL-bearing
/// key, which matches no row.
///
/// # Safety
/// Each key's `values` must be readable for `values_len` descriptors whose
/// nested buffers satisfy the ABI contract.
unsafe fn complete_keys(
    raw_keys: &[MoraineLookupKey],
    named: &[&moraine::ColumnInfo],
) -> Result<Vec<Vec<moraine::IndexKeyValue>>, AbiError> {
    raw_keys
        .iter()
        .filter_map(|raw_key| {
            if raw_key.values_len != named.len() {
                return Some(Err(AbiError::invalid_argument(format!(
                    "index key set: a key of {} values does not name the {} columns",
                    raw_key.values_len,
                    named.len()
                ))));
            }
            if raw_key.values.is_null() {
                return Some(Err(AbiError::invalid_argument("`key.values` is null")));
            }
            // SAFETY: caller contract for this descriptor's nested array.
            let raw_values =
                unsafe { std::slice::from_raw_parts(raw_key.values, raw_key.values_len) };
            let coerced = raw_values
                .iter()
                .zip(named)
                .map(|(value, column)| {
                    if value.kind == 0 {
                        return Ok(None);
                    }
                    // SAFETY: nested constant buffers satisfy the caller's contract.
                    unsafe { coerce_lookup_value(value, &column.column_type) }.map(Some)
                })
                .collect::<Result<Vec<_>, AbiError>>();
            match coerced {
                Ok(values) => values.into_iter().collect::<Option<Vec<_>>>().map(Ok),
                Err(error) => Some(Err(error)),
            }
        })
        .collect()
}
