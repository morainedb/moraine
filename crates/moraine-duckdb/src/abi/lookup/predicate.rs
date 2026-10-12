//! Translation of scan predicates to core index probes.

use std::ffi::{c_char, c_void};

use moraine::IndexComparison;

use super::{MoraineLookupValue, MoraineRowId, abi_located_row_ids, coerce_lookup_value};
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
    /// 0: equality list, 1: IS NULL, 2: >, 3: >=, 4: <, 5: <=.
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
    let produce = |handle_ref: &MoraineCatalogHandle| -> Result<Vec<MoraineRowId>, AbiError> {
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
            unsafe { handle_ref.block_on_cancellable(probe, probe_ctx, reads.snapshot()) }?;
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
                    2..=5 if values.len() == 1 => {
                        let value = values
                            .into_iter()
                            .next()
                            .ok_or_else(|| AbiError::invalid_argument("missing bound"))?;
                        if predicate.kind <= 3 {
                            IndexComparison::Lower(value, predicate.kind == 3)
                        } else {
                            IndexComparison::Upper(value, predicate.kind == 5)
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
                reads.index_predicate_rows(&snapshot, table_id, &predicates),
            )
        }?;
        let Some(result) = result else {
            return Ok(Vec::new());
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
