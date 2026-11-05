//! Streaming metadata pages over a retained catalog read scope.

use std::{ffi::c_void, sync::Mutex};

use moraine::ffi_support::{
    metadata_cursor::{MetadataCursor, MetadataPage},
    staged::TableKind,
};

use crate::{
    abi::{guard, write_reserved_array},
    dumps::{
        DumpRow, MoraineColumnMappingRow, MoraineColumnRow, MoraineDataFileRow,
        MoraineDeleteFileRow, MoraineFileColumnStatsRow, MoraineMacroRow, MorainePartitionInfoRow,
        MoraineScheduledDeletionRow, MoraineSchemaRow, MoraineSortInfoRow,
        MoraineTableColumnStatsRow, MoraineTableRow, MoraineTableStatsRow, MoraineViewRow,
        convert_rows,
    },
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// An operation-owned metadata cursor. Its pages use the existing dump free
/// functions.
pub struct MoraineMetadataCursor {
    cursor: Mutex<Option<MetadataCursor>>,
    handle: MoraineCatalogHandle,
}

impl MoraineMetadataCursor {
    pub(crate) fn new(cursor: MetadataCursor, handle: &MoraineCatalogHandle) -> Box<Self> {
        Box::new(Self {
            cursor: Mutex::new(Some(cursor)),
            handle: handle.read_alias(handle.catalog.reads().clone()),
        })
    }
}

/// Restarts a scan with the same read point and frozen authored rows.
///
/// # Safety
/// The cursor must be live; outputs, error, and probe follow the open contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_fork(
    cursor: *mut MoraineMetadataCursor,
    out: *mut *mut MoraineMetadataCursor,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || {
        if cursor.is_null() || out.is_null() {
            return Err(AbiError::invalid_argument(
                "null cursor fork input or output",
            ));
        }
        // SAFETY: caller supplies a live cursor.
        let owned = unsafe { &*cursor };
        let held = owned
            .cursor
            .lock()
            .map_err(|_| AbiError::new(codes::INTERNAL, "metadata cursor poisoned"))?;
        let cursor = held
            .as_ref()
            .ok_or_else(|| AbiError::invalid_argument("metadata cursor failed previously"))?;
        // SAFETY: caller supplies the callback and context lifetime.
        let fork = unsafe {
            owned
                .handle
                .block_on_cancellable(probe, probe_ctx, cursor.fork())
        }?;
        Ok(MoraineMetadataCursor::new(fork, &owned.handle))
    };
    // SAFETY: caller supplies a writable error slot if non-null.
    match unsafe { guard(err, attempt) } {
        Ok(cursor) => {
            // SAFETY: output was checked non-null and is writable by contract.
            unsafe {
                *out = Box::into_raw(cursor);
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Opens a fixed metadata scan; `u64::MAX` means no table or live bound.
///
/// # Safety
/// `handle` must be live; `out` writable; `err` null or writable. The probe
/// must be callable with its context from any thread during this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_open(
    handle: *mut MoraineCatalogHandle,
    table_kind: i32,
    table_id: u64,
    live_bound: u64,
    out: *mut *mut MoraineMetadataCursor,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Box<MoraineMetadataCursor>, AbiError> {
        if handle.is_null() || out.is_null() {
            return Err(AbiError::invalid_argument("null cursor input or output"));
        }
        // SAFETY: the caller supplies a live catalog handle.
        let handle = unsafe { &*handle };
        let kind = TableKind::try_from(table_kind)
            .map_err(|_| AbiError::invalid_argument("unknown metadata kind"))?;
        // SAFETY: the caller supplies the callback and context lifetime.
        let cursor = unsafe {
            handle.block_on_cancellable(
                probe,
                probe_ctx,
                MetadataCursor::open(
                    handle.catalog.reads(),
                    kind,
                    (table_id != u64::MAX).then_some(table_id),
                    (live_bound != u64::MAX).then_some(live_bound),
                ),
            )
        }?;
        Ok(MoraineMetadataCursor::new(cursor, handle))
    };
    // SAFETY: the caller supplies a writable error pointer, if present.
    match unsafe { guard(err, attempt) } {
        Ok(cursor) => {
            // SAFETY: checked non-null; writable by the caller's contract.
            unsafe {
                *out = Box::into_raw(cursor);
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Releases a cursor and its read scope; null is a no-op.
///
/// # Safety
/// `cursor` must be null or an unreleased pointer returned by cursor open.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_free(cursor: *mut MoraineMetadataCursor) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if !cursor.is_null() {
            // SAFETY: the caller transfers ownership of this allocation once.
            drop(unsafe { Box::from_raw(cursor) });
        }
    }));
}

/// Fetches one page, making a cancelled or failed cursor unusable.
///
/// # Safety
/// The cursor must be live, outputs writable, and the probe and error pointers
/// must satisfy the cursor-open contract.
unsafe fn next_page<Row: DumpRow>(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut Row,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
    fetch: impl AsyncFnOnce(&mut MetadataCursor) -> moraine::Result<MetadataPage<Row::Record>>,
) -> i32 {
    let attempt = || {
        if cursor.is_null() || out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("null metadata page argument"));
        }
        // SAFETY: the caller supplies a live cursor.
        let owned = unsafe { &*cursor };
        let mut held = owned
            .cursor
            .lock()
            .map_err(|_| AbiError::new(codes::INTERNAL, "metadata cursor poisoned"))?;
        let mut cursor = held
            .take()
            .ok_or_else(|| AbiError::invalid_argument("metadata cursor failed previously"))?;
        // SAFETY: the caller supplies the callback and context lifetime.
        let page = unsafe {
            owned
                .handle
                .block_on_cancellable(probe, probe_ctx, fetch(&mut cursor))
        }?;
        let rows = convert_rows::<Row>(page.records)?;
        *held = Some(cursor);
        Ok(rows)
    };
    // SAFETY: the caller supplies a writable error pointer, if present.
    match unsafe { guard(err, attempt) } {
        Ok((rows, memory)) => {
            // SAFETY: validated writable output pointers; rows transfer ownership.
            unsafe {
                write_reserved_array(rows, memory, out_items, out_len);
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Reads one page of schemas; the matching dump free function releases it.
///
/// # Safety
/// The live cursor, writable outputs, probe and error follow the cursor-open
/// contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_schemas(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut MoraineSchemaRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::schemas,
        )
    }
}

/// Reads one page of tables; the matching dump free function releases it.
///
/// # Safety
/// The live cursor, writable outputs, probe and error follow the cursor-open
/// contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_tables(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut MoraineTableRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::tables,
        )
    }
}

/// Reads one page of views; the matching dump free function releases it.
///
/// # Safety
/// The live cursor, writable outputs, probe and error follow the cursor-open
/// contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_views(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut MoraineViewRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::views,
        )
    }
}

/// Reads one page of columns; the matching dump free function releases it.
///
/// # Safety
/// The live cursor, writable outputs, probe and error follow the cursor-open
/// contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_columns(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut MoraineColumnRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::columns,
        )
    }
}

/// Reads one page of data files; the matching dump free function releases it.
///
/// # Safety
/// The live cursor, writable outputs, probe and error follow the cursor-open
/// contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_data_files(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut MoraineDataFileRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::data_files,
        )
    }
}

/// Reads one page of delete files; the matching dump free function releases it.
///
/// # Safety
/// The live cursor, writable outputs, probe and error follow the cursor-open
/// contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_delete_files(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut MoraineDeleteFileRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::delete_files,
        )
    }
}

/// Reads one page of file column stats; the matching dump free function
/// releases it.
///
/// # Safety
/// The live cursor, writable outputs, probe and error follow the cursor-open
/// contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_file_column_stats(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut MoraineFileColumnStatsRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::file_column_stats,
        )
    }
}

/// Reads one page of table stats; the matching dump free function releases it.
///
/// # Safety
/// The live cursor, writable outputs, probe and error follow the cursor-open
/// contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_table_stats(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut MoraineTableStatsRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::table_stats,
        )
    }
}

/// Reads one page of table column stats; the matching dump free function
/// releases it.
///
/// # Safety
/// The live cursor, writable outputs, probe and error follow the cursor-open
/// contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_table_column_stats(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut MoraineTableColumnStatsRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::table_column_stats,
        )
    }
}

/// Reads one page of partitions; the matching dump free function releases it.
///
/// # Safety
/// The live cursor, writable outputs, probe and error follow the cursor-open
/// contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_partitions(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut MorainePartitionInfoRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::partitions,
        )
    }
}

/// Reads one page of sorts; the matching dump free function releases it.
///
/// # Safety
/// The live cursor, writable outputs, probe and error follow the cursor-open
/// contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_sorts(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut MoraineSortInfoRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::sorts,
        )
    }
}

/// Reads one page of macros; the matching dump free function releases it.
///
/// # Safety
/// The live cursor, writable outputs, probe and error follow the cursor-open
/// contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_macros(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut MoraineMacroRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::macros,
        )
    }
}

/// Reads one page of mappings; the matching dump free function releases it.
///
/// # Safety
/// The live cursor, writable outputs, probe and error follow the cursor-open
/// contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_mappings(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut MoraineColumnMappingRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::mappings,
        )
    }
}

/// Reads one page of scheduled deletions; the matching dump free function
/// releases it.
///
/// # Safety
/// The live cursor, writable outputs, probe and error follow the cursor-open
/// contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_scheduled_deletions(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut MoraineScheduledDeletionRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::scheduled_deletions,
        )
    }
}

/// Reads a page of tags; the dump free function releases it.
///
/// # Safety
/// The cursor, outputs, probe, and error follow the cursor-open contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_tags(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut crate::dumps::MoraineTagRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::tags,
        )
    }
}

/// Reads a page of partition columns; the dump free function releases it.
///
/// # Safety
/// The cursor, outputs, probe, and error follow the cursor-open contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_partition_columns(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut crate::dumps::MorainePartitionColumnRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::partition_columns,
        )
    }
}

/// Reads a page of sort expressions; the dump free function releases it.
///
/// # Safety
/// The cursor, outputs, probe, and error follow the cursor-open contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_sort_expressions(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut crate::dumps::MoraineSortExpressionRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::sort_expressions,
        )
    }
}

/// Reads a page of file partition values; the dump free function releases it.
///
/// # Safety
/// The cursor, outputs, probe, and error follow the cursor-open contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_file_partition_values(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut crate::dumps::MoraineFilePartitionValueRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::file_partition_values,
        )
    }
}

/// Reads a page of file variant stats; the dump free function releases it.
///
/// # Safety
/// The cursor, outputs, probe, and error follow the cursor-open contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_file_variant_stats(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut crate::dumps::MoraineFileVariantStatsRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::file_variant_stats,
        )
    }
}

/// Reads a page of macro impls; the dump free function releases it.
///
/// # Safety
/// The cursor, outputs, probe, and error follow the cursor-open contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_macro_impls(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut crate::dumps::MoraineMacroImplRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::macro_impls,
        )
    }
}

/// Reads a page of macro parameters; the dump free function releases it.
///
/// # Safety
/// The cursor, outputs, probe, and error follow the cursor-open contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_macro_parameters(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut crate::dumps::MoraineMacroParameterRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::macro_parameters,
        )
    }
}

/// Reads a page of name mappings; the dump free function releases it.
///
/// # Safety
/// The cursor, outputs, probe, and error follow the cursor-open contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_name_mappings(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut crate::dumps::MoraineNameMappingRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::name_mappings,
        )
    }
}

/// Reads a page of options; the dump free function releases it.
///
/// # Safety
/// The cursor, outputs, probe, and error follow the cursor-open contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_options(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut crate::dumps::MoraineOptionRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::options,
        )
    }
}

/// Reads a page of snapshots; the snapshot dump free function releases it.
///
/// # Safety
/// The cursor, outputs, probe, and error follow the cursor-open contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_snapshots(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut crate::dumps::MoraineSnapshotRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::snapshots,
        )
    }
}

/// Reads a page of column tags; the dump free function releases it.
///
/// # Safety
/// The cursor, outputs, probe, and error follow the cursor-open contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_column_tags(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut crate::dumps::MoraineColumnTagRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::column_tags,
        )
    }
}

/// Reads a page of view column tags; the dump free function releases it.
///
/// # Safety
/// The cursor, outputs, probe, and error follow the cursor-open contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_view_column_tags(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut crate::dumps::MoraineViewColumnTagRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::view_column_tags,
        )
    }
}

/// Reads a page of schema versions; the matching dump free function releases
/// it.
///
/// # Safety
/// The cursor, outputs, probe, and error follow the cursor-open contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_metadata_cursor_schema_versions(
    cursor: *mut MoraineMetadataCursor,
    out_items: *mut *mut crate::dumps::MoraineSchemaVersionRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        next_page(
            cursor,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            MetadataCursor::schema_versions,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;

    #[test]
    #[ignore = "reads MORAINE_MEMORY_FIXTURE in an isolated measurement process"]
    fn persisted_metadata_pages_keep_bounded_allowances() {
        let path = std::env::var("MORAINE_MEMORY_FIXTURE").unwrap();
        let handle = crate::test_support::attach_mode(
            std::path::Path::new(&path),
            std::env::var_os("MORAINE_MEMORY_READ_ONLY").is_some(),
        );
        let mut cursor = ptr::null_mut();
        let mut error = MoraineError::default();
        // SAFETY: handle and output slots are live, and arrays are freed once.
        unsafe {
            let mut snapshot = ptr::null_mut();
            assert_eq!(
                crate::abi::moraine_snapshot_scoped(
                    handle,
                    &raw mut snapshot,
                    None,
                    ptr::null_mut(),
                    &raw mut error
                ),
                codes::OK
            );
            let reader = crate::abi::moraine_snapshot_read_handle(snapshot);
            assert_eq!(
                moraine_metadata_cursor_open(
                    reader,
                    10,
                    u64::MAX,
                    u64::MAX,
                    &raw mut cursor,
                    None,
                    ptr::null_mut(),
                    &raw mut error
                ),
                codes::OK
            );
            let baseline = moraine::working_memory_usage().unwrap().0;
            let mut total = 0;
            loop {
                let mut rows = ptr::null_mut();
                let mut length = 0;
                assert_eq!(
                    moraine_metadata_cursor_file_column_stats(
                        cursor,
                        &raw mut rows,
                        &raw mut length,
                        None,
                        ptr::null_mut(),
                        &raw mut error
                    ),
                    codes::OK
                );
                crate::dumps::moraine_dump_file_column_stats_free(rows, length);
                total += length;
                assert!(
                    moraine::working_memory_usage().unwrap().0 <= baseline + 65536,
                    "at {total}: {:?} baseline={baseline}",
                    moraine::working_memory_usage()
                );
                if length == 0 {
                    break;
                }
            }
            moraine_metadata_cursor_free(cursor);
            crate::abi::moraine_snapshot_free(snapshot);
            crate::abi::moraine_detach(handle);
            eprintln!("scanned {total} rows");
        }
    }

    #[test]
    fn metadata_pages_export_reserved_arrays() {
        let directory = crate::test_support::TempDir::new("metadata-pages");
        crate::dumps::test_support::seed(directory.path());
        let handle = crate::test_support::attach_ok(directory.path());
        let mut error = MoraineError::default();
        for _ in 0..100 {
            let mut cursor = ptr::null_mut();
            // SAFETY: the handle is live, output slots writable, and no probe is used.
            unsafe {
                assert_eq!(
                    moraine_metadata_cursor_open(
                        handle,
                        10,
                        u64::MAX,
                        u64::MAX,
                        &raw mut cursor,
                        None,
                        ptr::null_mut(),
                        &raw mut error
                    ),
                    codes::OK
                );
                let mut rows = ptr::null_mut();
                let mut length = 0;
                assert_eq!(
                    moraine_metadata_cursor_file_column_stats(
                        cursor,
                        &raw mut rows,
                        &raw mut length,
                        None,
                        ptr::null_mut(),
                        &raw mut error
                    ),
                    codes::OK
                );
                assert_eq!(length, 1);
                let memory = crate::abi::memory::take(rows as usize).unwrap();
                assert!(memory.bytes() > 0);
                crate::abi::memory::retain(rows as usize, memory);
                crate::dumps::moraine_dump_file_column_stats_free(rows, length);
                moraine_metadata_cursor_free(cursor);
            }
        }
        // SAFETY: the handle is live and all cursors have been freed.
        unsafe {
            crate::abi::moraine_detach(handle);
        }
    }
}
