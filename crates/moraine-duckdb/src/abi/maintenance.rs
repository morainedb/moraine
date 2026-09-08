//! Maintenance operations and storage diagnostics.

use std::{
    ffi::{CStr, CString, c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
    time::Duration,
};

use super::{borrow_str, free_array, free_c_string, guard, handle_list, to_c_string};
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// Runs one moraine-owned maintenance pass, reclaiming the entry ranges
/// of indexes no longer live and the file column statistics of data files
/// no snapshot can still resolve, and writes what it reclaimed to
/// `*indexes_swept`, `*entries_reclaimed`, and `*file_stats_reclaimed`.
/// The pass mints no snapshot and leaves head unchanged. `batch_size`
/// bounds the deletes per commit; 0 takes the core default.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; the out-parameters,
/// if non-null, must be writable, and `err`, if non-null, must be
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_maintain(
    handle: *mut MoraineCatalogHandle,
    batch_size: u64,
    indexes_swept: *mut u64,
    entries_reclaimed: *mut u64,
    file_stats_reclaimed: *mut u64,
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

        // Deferred index additions finish before dead ranges are reclaimed.
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        unsafe {
            handle_ref.block_on_commit(
                probe,
                probe_ctx,
                handle_ref.catalog.writer()?.repair_deferred_indexes(
                    handle_ref.data_store.clone(),
                    &handle_ref.data_prefix,
                    None,
                ),
            )
        }?;

        let mut request = moraine::MaintenanceRequest::default();
        if batch_size > 0 {
            // Refused rather than clamped: saturating would silently
            // unbound the batch.
            request.batch_size = usize::try_from(batch_size).map_err(|_| {
                AbiError::invalid_argument(format!(
                    "batch_size {batch_size} does not fit this platform's pointer width"
                ))
            })?;
        }

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let report = unsafe {
            handle_ref.block_on_commit(
                probe,
                probe_ctx,
                handle_ref.catalog.writer()?.maintain(request),
            )
        }?;

        if !indexes_swept.is_null() {
            // SAFETY: caller contract — non-null means writable.
            unsafe { *indexes_swept = report.indexes_swept };
        }
        if !entries_reclaimed.is_null() {
            // SAFETY: caller contract — non-null means writable.
            unsafe { *entries_reclaimed = report.index_entries_reclaimed };
        }
        if !file_stats_reclaimed.is_null() {
            // SAFETY: caller contract — non-null means writable.
            unsafe { *file_stats_reclaimed = report.file_column_stats_reclaimed };
        }
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// One borrowed step supplied to [`moraine_maintenance_status_record`].
#[repr(C)]
pub struct MoraineMaintenanceStatusStepInput {
    /// Maintenance operation name.
    pub step: *const c_char,
    /// Outcome name.
    pub status: *const c_char,
    /// Human-readable outcome detail.
    pub detail: *const c_char,
}

/// One flattened status row returned by [`moraine_maintenance_status_rows`].
#[repr(C)]
pub struct MoraineMaintenanceStatusRow {
    /// Pass start time, in microseconds from the Unix epoch.
    pub started_at_micros: i64,
    /// Pass trigger, owned — free via [`moraine_maintenance_status_free`].
    pub trigger: *mut c_char,
    /// Maintenance operation name, owned.
    pub step: *mut c_char,
    /// Outcome name, owned.
    pub status: *mut c_char,
    /// Human-readable outcome detail, owned.
    pub detail: *mut c_char,
}

/// Durably records one completed maintenance pass.
///
/// # Safety
///
/// `handle` must be a live writer handle, `trigger` a valid C string,
/// `steps` either null with zero length or point to `steps_len` valid inputs,
/// every string in those inputs must be valid, and `err`, if non-null, must
/// be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_maintenance_status_record(
    handle: *mut MoraineCatalogHandle,
    started_at_micros: i64,
    trigger: *const c_char,
    steps: *const MoraineMaintenanceStatusStepInput,
    steps_len: usize,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        // SAFETY: caller contract.
        let trigger = unsafe { borrow_str(trigger, "trigger") }?.to_owned();
        let raw_steps = if steps.is_null() {
            if steps_len != 0 {
                return Err(AbiError::invalid_argument(
                    "`steps` is null but its length is nonzero",
                ));
            }
            &[]
        } else {
            // SAFETY: caller contract.
            unsafe { std::slice::from_raw_parts(steps, steps_len) }
        };
        let status_steps = raw_steps
            .iter()
            .map(|input| {
                // SAFETY: caller contract covers every input string.
                Ok(moraine::MaintenanceStatusStep::new(
                    unsafe { borrow_str(input.step, "step") }?,
                    unsafe { borrow_str(input.status, "status") }?,
                    unsafe { borrow_str(input.detail, "detail") }?,
                ))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;
        let started_at = moraine::Timestamp::from_micros(started_at_micros);
        let pass = moraine::MaintenanceStatusPass::new(started_at, trigger, status_steps);

        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        handle_ref.block_on(handle_ref.catalog.writer()?.record_maintenance_pass(pass))?;
        Ok(())
    };

    // SAFETY: `err` validity is the caller's contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Lists durable maintenance status, newest pass first and step order within
/// each pass.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; output pointers and
/// `err`, if non-null, must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_maintenance_status_rows(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineMaintenanceStatusRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    let produce =
        |handle_ref: &MoraineCatalogHandle| -> Result<Vec<MoraineMaintenanceStatusRow>, AbiError> {
            let passes = handle_ref.block_on(handle_ref.catalog.reads().maintenance_status())?;
            let owned = passes
                .into_iter()
                .flat_map(|pass| {
                    let started_at_micros = pass.started_at.as_micros();
                    let trigger = pass.trigger;
                    pass.steps.into_iter().map(move |step| {
                        Ok((
                            started_at_micros,
                            to_c_string(trigger.as_str())?,
                            to_c_string(step.step)?,
                            to_c_string(step.status)?,
                            to_c_string(step.detail)?,
                        ))
                    })
                })
                .collect::<Result<Vec<_>, AbiError>>()?;
            Ok(owned
                .into_iter()
                .map(|(started_at_micros, trigger, step, status, detail)| {
                    MoraineMaintenanceStatusRow {
                        started_at_micros,
                        trigger: trigger.into_raw(),
                        step: step.into_raw(),
                        status: status.into_raw(),
                        detail: detail.into_raw(),
                    }
                })
                .collect())
        };

    // SAFETY: caller contract for the pointers.
    unsafe { handle_list(handle, out_items, out_len, err, produce) }
}

/// Frees rows returned by [`moraine_maintenance_status_rows`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a matching
/// status call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_maintenance_status_free(
    items: *mut MoraineMaintenanceStatusRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |row| {
                free_c_string(row.trigger);
                free_c_string(row.step);
                free_c_string(row.status);
                free_c_string(row.detail);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One subspace's row of a store census, as returned by
/// [`moraine_store_census`].
#[repr(C)]
pub struct MoraineSubspaceCensus {
    /// The subspace's name, owned — free via [`moraine_store_census_free`].
    pub subspace: *mut c_char,
    /// Physical bytes across its SSTs.
    pub bytes: u64,
    /// Bloom-filter bytes across its SSTs.
    pub filter_bytes: u64,
    /// Index-block bytes across its SSTs.
    pub index_bytes: u64,
    /// Statistics-block bytes across its SSTs.
    pub stats_bytes: u64,
    /// SSTs not yet merged into a sorted run.
    pub l0_ssts: u32,
    /// Sorted runs. A merge collapses these to one.
    pub sorted_runs: u32,
    /// SSTs across those runs.
    pub sorted_run_ssts: u32,
    /// Whether the live fields carry a count; false unless the census was
    /// asked to scan.
    pub has_live: bool,
    /// Live keys a reader would see.
    pub live_keys: u64,
    /// Encoded bytes of those keys.
    pub live_key_bytes: u64,
    /// Encoded bytes of their values.
    pub live_value_bytes: u64,
    /// Deletion-schedule entries among the live keys.
    pub scheduled_files: u64,
}

/// Store-wide object totals, as returned by [`moraine_store_census`].
#[repr(C)]
pub struct MoraineStoreObjects {
    /// Whether the store could be listed at all; false leaves every other
    /// field zero.
    pub listed: bool,
    /// Every object under the store's prefix.
    pub total_objects: u64,
    /// Bytes across all of them.
    pub total_bytes: u64,
    /// Write-ahead log objects, replayed by an unpinned read attach.
    pub wal_objects: u64,
    /// Bytes across those.
    pub wal_bytes: u64,
    /// Manifest versions.
    pub manifest_objects: u64,
    /// Bytes across those.
    pub manifest_bytes: u64,
    /// Sorted-string tables — the only bytes a merge reclaims.
    pub sst_objects: u64,
    /// Bytes across those.
    pub sst_bytes: u64,
    /// Everything else the layout carries.
    pub other_objects: u64,
    /// Bytes across those.
    pub other_bytes: u64,
}

/// Physical object-store requests one catalog has issued, as returned by
/// [`moraine_catalog_object_store_tally`].
#[repr(C)]
#[derive(Default)]
pub struct MoraineObjectStoreTally {
    /// Reads from the main store.
    pub main_gets: u64,
    /// Summed main-store read latency, in nanoseconds.
    pub main_get_nanoseconds: u64,
    /// Writes to the main store.
    pub main_puts: u64,
    /// Summed main-store write latency, in nanoseconds.
    pub main_put_nanoseconds: u64,
    /// Deletes from the main store.
    pub main_deletes: u64,
    /// Summed main-store delete latency, in nanoseconds.
    pub main_delete_nanoseconds: u64,
    /// Reads from the WAL store.
    pub wal_gets: u64,
    /// Summed WAL-store read latency, in nanoseconds.
    pub wal_get_nanoseconds: u64,
    /// Writes to the WAL store.
    pub wal_puts: u64,
    /// Summed WAL-store write latency, in nanoseconds.
    pub wal_put_nanoseconds: u64,
    /// Deletes from the WAL store.
    pub wal_deletes: u64,
    /// Summed WAL-store delete latency, in nanoseconds.
    pub wal_delete_nanoseconds: u64,
    /// Failed request attempts across both stores, including handled errors.
    pub errors: u64,
}

/// Process-wide cache capacity, occupancy, and eviction counters.
#[repr(C)]
#[derive(Default)]
pub struct MoraineCacheStatus {
    /// Memory reserved for decoded SlateDB metadata.
    pub metadata_capacity_bytes: u64,
    /// Memory currently occupied by decoded SlateDB metadata.
    pub metadata_occupancy_bytes: u64,
    /// Decoded SlateDB metadata entries evicted from memory.
    pub metadata_evictions: u64,
    /// Memory reserved for SlateDB data blocks.
    pub block_capacity_bytes: u64,
    /// Memory currently occupied by SlateDB data blocks.
    pub block_occupancy_bytes: u64,
    /// SlateDB data-block entries evicted from memory.
    pub block_evictions: u64,
    /// Whether a disk tier is configured.
    pub has_block_disk: bool,
    /// Configured disk capacity when `has_block_disk` is true.
    pub block_disk_capacity_bytes: u64,
    /// Memory reserved for parsed Parquet metadata.
    pub auxiliary_metadata_capacity_bytes: u64,
    /// Memory currently occupied by parsed Parquet metadata.
    pub auxiliary_metadata_occupancy_bytes: u64,
    /// Parsed Parquet metadata entries evicted from memory.
    pub auxiliary_metadata_evictions: u64,
}

/// Logical memory attributed to one catalog and the process-shared caches.
#[repr(C)]
#[derive(Default)]
pub struct MoraineMemoryTally {
    /// SlateDB WAL-plus-memtable bytes for this catalog.
    pub slatedb_unflushed_bytes: u64,
    /// Estimated decoded catalog projection bytes for this handle.
    pub projection_bytes: u64,
    /// Process-wide decoded SlateDB metadata cache occupancy.
    pub cache_metadata_bytes: u64,
    /// Process-wide SlateDB data-block cache occupancy.
    pub cache_block_bytes: u64,
    /// Process-wide parsed Parquet metadata occupancy.
    pub auxiliary_metadata_bytes: u64,
    /// Equality-index entries derived by the last staged-row commit.
    pub last_commit_index_entries: u64,
    /// Encoded bytes in the last staged-row commit batch.
    pub last_commit_staged_bytes: u64,
}

fn duration_nanoseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl From<moraine::ObjectStoreTally> for MoraineObjectStoreTally {
    fn from(tally: moraine::ObjectStoreTally) -> Self {
        Self {
            main_gets: tally.main_gets,
            main_get_nanoseconds: duration_nanoseconds(tally.main_get_duration),
            main_puts: tally.main_puts,
            main_put_nanoseconds: duration_nanoseconds(tally.main_put_duration),
            main_deletes: tally.main_deletes,
            main_delete_nanoseconds: duration_nanoseconds(tally.main_delete_duration),
            wal_gets: tally.wal_gets,
            wal_get_nanoseconds: duration_nanoseconds(tally.wal_get_duration),
            wal_puts: tally.wal_puts,
            wal_put_nanoseconds: duration_nanoseconds(tally.wal_put_duration),
            wal_deletes: tally.wal_deletes,
            wal_delete_nanoseconds: duration_nanoseconds(tally.wal_delete_duration),
            errors: tally.errors,
        }
    }
}

/// Measures the store, one row per subspace, and writes the manifest
/// version measured to `*out_manifest_id` and the store-wide object totals
/// to `*out_objects`.
///
/// `count_live_entries` adds a scan of every subspace, which costs a full
/// read of the store; without it the call reads the manifest alone.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; the out-parameters
/// must be writable, and `err`, if non-null, must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_store_census(
    handle: *mut MoraineCatalogHandle,
    count_live_entries: bool,
    out_items: *mut *mut MoraineSubspaceCensus,
    out_len: *mut usize,
    out_manifest_id: *mut u64,
    out_objects: *mut MoraineStoreObjects,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let produce =
        |handle_ref: &MoraineCatalogHandle| -> Result<Vec<MoraineSubspaceCensus>, AbiError> {
            let mut request = moraine::CensusRequest::default();
            request.count_live_entries = count_live_entries;

            // SAFETY: caller contract for `probe`/`probe_ctx`.
            let census = unsafe {
                handle_ref.block_on_cancellable(
                    probe,
                    probe_ctx,
                    handle_ref.catalog.reads().store_census(request),
                )
            }?;

            if !out_manifest_id.is_null() {
                // SAFETY: caller contract — non-null means writable.
                unsafe { *out_manifest_id = census.manifest_id };
            }
            if !out_objects.is_null() {
                let objects = census.objects.unwrap_or_default();
                // SAFETY: caller contract — non-null means writable.
                unsafe {
                    *out_objects = MoraineStoreObjects {
                        listed: census.objects.is_some(),
                        total_objects: objects.total_objects,
                        total_bytes: objects.total_bytes,
                        wal_objects: objects.wal_objects,
                        wal_bytes: objects.wal_bytes,
                        manifest_objects: objects.manifest_objects,
                        manifest_bytes: objects.manifest_bytes,
                        sst_objects: objects.sst_objects,
                        sst_bytes: objects.sst_bytes,
                        other_objects: objects.other_objects,
                        other_bytes: objects.other_bytes,
                    };
                }
            }

            let owned: Vec<(CString, &moraine::SubspaceCensus)> = census
                .subspaces
                .iter()
                .map(|subspace| Ok((to_c_string(subspace.subspace.to_string())?, subspace)))
                .collect::<Result<_, AbiError>>()?;
            Ok(owned
                .into_iter()
                .map(|(name, subspace)| {
                    let live = subspace.live.unwrap_or_default();
                    MoraineSubspaceCensus {
                        subspace: name.into_raw(),
                        bytes: subspace.bytes,
                        filter_bytes: subspace.filter_bytes,
                        index_bytes: subspace.index_bytes,
                        stats_bytes: subspace.stats_bytes,
                        l0_ssts: subspace.l0_ssts,
                        sorted_runs: subspace.sorted_runs,
                        sorted_run_ssts: subspace.sorted_run_ssts,
                        has_live: subspace.live.is_some(),
                        live_keys: live.keys,
                        live_key_bytes: live.key_bytes,
                        live_value_bytes: live.value_bytes,
                        scheduled_files: live.scheduled_files,
                    }
                })
                .collect())
        };

    // SAFETY: caller contract for the pointers.
    unsafe { handle_list(handle, out_items, out_len, err, produce) }
}

/// Frees the array a [`moraine_store_census`] call returned.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_store_census`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_store_census_free(items: *mut MoraineSubspaceCensus, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |row| free_c_string(row.subspace));
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One subspace's merge, as returned by [`moraine_compact_store`].
#[repr(C)]
pub struct MoraineSubspaceMerge {
    /// The subspace merged, owned — free via [`moraine_compact_store_free`].
    pub subspace: *mut c_char,
    /// `"completed"`, `"failed"`, `"pending"`, or `"skipped"`, owned.
    pub outcome: *mut c_char,
    /// The failure message or the skip reason; empty otherwise. Owned.
    pub detail: *mut c_char,
    /// Physical bytes before the merge was submitted.
    pub bytes_before: u64,
    /// Whether `bytes_after` carries a measurement; false unless the merge
    /// committed.
    pub has_bytes_after: bool,
    /// Physical bytes after it committed.
    pub bytes_after: u64,
}

/// Merges each targeted subspace's sorted runs into one.
///
/// `subspace` names one subspace, or is null for every one. `wait_ms` of 0
/// returns as soon as the merges are submitted; otherwise the call waits
/// that long for each to commit, and a merge that outlives the wait keeps
/// running and is reported pending.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; the out-parameters
/// must be writable, and `err`, if non-null, must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_compact_store(
    handle: *mut MoraineCatalogHandle,
    subspace: *const c_char,
    wait_ms: u64,
    require_completed: bool,
    out_items: *mut *mut MoraineSubspaceMerge,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let produce =
        |handle_ref: &MoraineCatalogHandle| -> Result<Vec<MoraineSubspaceMerge>, AbiError> {
            let mut request = moraine::CompactStoreRequest::default();
            if !subspace.is_null() {
                // SAFETY: caller contract for the string pointer.
                let name = unsafe { borrow_str(subspace, "subspace") }?;
                request.target = moraine::CompactionTarget::Subspace(parse_subspace(name)?);
            }
            if wait_ms > 0 {
                request.wait = Some(Duration::from_millis(wait_ms));
            }
            request.require_completed = require_completed;

            // SAFETY: caller contract for `probe`/`probe_ctx`.
            let report = unsafe {
                handle_ref.block_on_commit(
                    probe,
                    probe_ctx,
                    handle_ref.catalog.writer()?.compact_store(request),
                )
            }?;

            let owned: Vec<(CString, CString, CString, &moraine::SubspaceMerge)> = report
                .merges
                .iter()
                .map(|merge| {
                    let (outcome, detail) = match &merge.outcome {
                        moraine::MergeOutcome::Completed => ("completed", String::new()),
                        moraine::MergeOutcome::Failed(why) => ("failed", why.clone()),
                        moraine::MergeOutcome::Pending => ("pending", String::new()),
                        moraine::MergeOutcome::Skipped(why) => ("skipped", (*why).to_string()),
                        // `MergeOutcome` is `#[non_exhaustive]`: a variant
                        // this build does not know still gets a row.
                        _ => ("unknown", String::new()),
                    };
                    Ok((
                        to_c_string(merge.subspace.to_string())?,
                        to_c_string(outcome)?,
                        to_c_string(detail)?,
                        merge,
                    ))
                })
                .collect::<Result<_, AbiError>>()?;
            Ok(owned
                .into_iter()
                .map(|(subspace, outcome, detail, merge)| MoraineSubspaceMerge {
                    subspace: subspace.into_raw(),
                    outcome: outcome.into_raw(),
                    detail: detail.into_raw(),
                    bytes_before: merge.bytes_before,
                    has_bytes_after: merge.bytes_after.is_some(),
                    bytes_after: merge.bytes_after.unwrap_or(0),
                })
                .collect())
        };

    // SAFETY: caller contract for the pointers.
    unsafe { handle_list(handle, out_items, out_len, err, produce) }
}

/// Frees the array a [`moraine_compact_store`] call returned.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_compact_store`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_compact_store_free(items: *mut MoraineSubspaceMerge, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |row| {
                free_c_string(row.subspace);
                free_c_string(row.outcome);
                free_c_string(row.detail);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// Whether `name` is a subspace a merge can target, so an attach can
/// validate its options before any catalog is open.
///
/// # Safety
///
/// `name`, if non-null, must be a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_subspace_is_known(name: *const c_char) -> bool {
    let attempt = || {
        if name.is_null() {
            return false;
        }
        // SAFETY: caller contract for `name`.
        let Ok(name) = (unsafe { CStr::from_ptr(name) }).to_str() else {
            return false;
        };
        parse_subspace(name).is_ok()
    };
    catch_unwind(AssertUnwindSafe(attempt)).unwrap_or(false)
}

/// What the process-wide block cache has served since it was built;
/// zeros before anything has read. Metadata (SST indexes, filters, stats)
/// and data blocks are counted apart. [`moraine_catalog_cache_tally`]
/// reports the same counts for one attach.
///
/// # Safety
///
/// Every out-pointer must be valid and writable for the duration of the
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_cache_tally(
    out_metadata_hits: *mut u64,
    out_metadata_misses: *mut u64,
    out_block_hits: *mut u64,
    out_block_misses: *mut u64,
    out_errors: *mut u64,
    out_preload_metadata_hits: *mut u64,
    out_preload_metadata_misses: *mut u64,
    out_preload_block_hits: *mut u64,
    out_preload_block_misses: *mut u64,
    out_preload_failures: *mut u64,
) -> i32 {
    let attempt = || {
        if out_metadata_hits.is_null()
            || out_metadata_misses.is_null()
            || out_block_hits.is_null()
            || out_block_misses.is_null()
            || out_errors.is_null()
            || out_preload_metadata_hits.is_null()
            || out_preload_metadata_misses.is_null()
            || out_preload_block_hits.is_null()
            || out_preload_block_misses.is_null()
            || out_preload_failures.is_null()
        {
            return codes::INVALID_ARGUMENT;
        }
        let tally = moraine::cache_tally();
        // SAFETY: checked non-null above; caller contract for validity.
        unsafe {
            *out_metadata_hits = tally.metadata_hits;
            *out_metadata_misses = tally.metadata_misses;
            *out_block_hits = tally.block_hits;
            *out_block_misses = tally.block_misses;
            *out_errors = tally.errors;
            *out_preload_metadata_hits = tally.preload_metadata_hits;
            *out_preload_metadata_misses = tally.preload_metadata_misses;
            *out_preload_block_hits = tally.preload_block_hits;
            *out_preload_block_misses = tally.preload_block_misses;
            *out_preload_failures = tally.preload_failures;
        }
        codes::OK
    };
    catch_unwind(AssertUnwindSafe(attempt)).unwrap_or(codes::INTERNAL)
}

/// The counts [`moraine_cache_tally`] reports, narrowed to what the
/// catalog `handle` names has spent since it attached.
///
/// # Safety
///
/// `handle` must be a live handle from [`super::moraine_attach`]. Every
/// out-pointer must be valid and writable for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_catalog_cache_tally(
    handle: *mut MoraineCatalogHandle,
    out_metadata_hits: *mut u64,
    out_metadata_misses: *mut u64,
    out_block_hits: *mut u64,
    out_block_misses: *mut u64,
    out_errors: *mut u64,
    out_preload_metadata_hits: *mut u64,
    out_preload_metadata_misses: *mut u64,
    out_preload_block_hits: *mut u64,
    out_preload_block_misses: *mut u64,
    out_preload_failures: *mut u64,
) -> i32 {
    let attempt = || {
        if handle.is_null()
            || out_metadata_hits.is_null()
            || out_metadata_misses.is_null()
            || out_block_hits.is_null()
            || out_block_misses.is_null()
            || out_errors.is_null()
            || out_preload_metadata_hits.is_null()
            || out_preload_metadata_misses.is_null()
            || out_preload_block_hits.is_null()
            || out_preload_block_misses.is_null()
            || out_preload_failures.is_null()
        {
            return codes::INVALID_ARGUMENT;
        }
        // SAFETY: caller contract for `handle`.
        let tally = unsafe { &*handle }.catalog.reads().cache_tally();
        // SAFETY: checked non-null above; caller contract for validity.
        unsafe {
            *out_metadata_hits = tally.metadata_hits;
            *out_metadata_misses = tally.metadata_misses;
            *out_block_hits = tally.block_hits;
            *out_block_misses = tally.block_misses;
            *out_errors = tally.errors;
            *out_preload_metadata_hits = tally.preload_metadata_hits;
            *out_preload_metadata_misses = tally.preload_metadata_misses;
            *out_preload_block_hits = tally.preload_block_hits;
            *out_preload_block_misses = tally.preload_block_misses;
            *out_preload_failures = tally.preload_failures;
        }
        codes::OK
    };
    catch_unwind(AssertUnwindSafe(attempt)).unwrap_or(codes::INTERNAL)
}

/// Returns process-wide cache capacity, occupancy, and eviction counters.
///
/// # Safety
///
/// `out_status` must be valid and writable for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_cache_status(out_status: *mut MoraineCacheStatus) -> i32 {
    let attempt = || {
        if out_status.is_null() {
            return codes::INVALID_ARGUMENT;
        }
        let status = moraine::cache_status();
        // SAFETY: checked non-null above; caller contract for validity.
        unsafe {
            *out_status = MoraineCacheStatus {
                metadata_capacity_bytes: status.metadata_capacity_bytes,
                metadata_occupancy_bytes: status.metadata_occupancy_bytes,
                metadata_evictions: status.metadata_evictions,
                block_capacity_bytes: status.block_capacity_bytes,
                block_occupancy_bytes: status.block_occupancy_bytes,
                block_evictions: status.block_evictions,
                has_block_disk: status.block_disk_capacity_bytes.is_some(),
                block_disk_capacity_bytes: status.block_disk_capacity_bytes.unwrap_or_default(),
                auxiliary_metadata_capacity_bytes: status.auxiliary_metadata_capacity_bytes,
                auxiliary_metadata_occupancy_bytes: status.auxiliary_metadata_occupancy_bytes,
                auxiliary_metadata_evictions: status.auxiliary_metadata_evictions,
            };
        }
        codes::OK
    };
    catch_unwind(AssertUnwindSafe(attempt)).unwrap_or(codes::INTERNAL)
}

/// Returns logical memory attributed to one attached catalog.
///
/// # Safety
///
/// `handle` must be a live handle from [`super::moraine_attach`] and
/// `out_tally` must be valid and writable for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_catalog_memory_tally(
    handle: *mut MoraineCatalogHandle,
    out_tally: *mut MoraineMemoryTally,
) -> i32 {
    let attempt = || {
        if handle.is_null() || out_tally.is_null() {
            return codes::INVALID_ARGUMENT;
        }
        // SAFETY: caller contract for `handle`.
        let tally = unsafe { &*handle }.catalog.reads().memory_tally();
        // SAFETY: checked non-null above; caller contract for validity.
        unsafe {
            *out_tally = MoraineMemoryTally {
                slatedb_unflushed_bytes: tally.slatedb_unflushed_bytes,
                projection_bytes: tally.projection_bytes,
                cache_metadata_bytes: tally.cache_metadata_bytes,
                cache_block_bytes: tally.cache_block_bytes,
                auxiliary_metadata_bytes: tally.auxiliary_metadata_bytes,
                last_commit_index_entries: tally.last_commit_index_entries,
                last_commit_staged_bytes: tally.last_commit_staged_bytes,
            };
        }
        codes::OK
    };
    catch_unwind(AssertUnwindSafe(attempt)).unwrap_or(codes::INTERNAL)
}

/// Physical object-store requests one attached catalog has issued.
///
/// Counts are the requests SlateDB sent, including retries. Durations are
/// summed request latency in nanoseconds and can exceed wall time when
/// requests overlap.
///
/// # Safety
///
/// `handle` must be a live handle from [`super::moraine_attach`] and
/// `out_tally` must be valid and writable for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_catalog_object_store_tally(
    handle: *mut MoraineCatalogHandle,
    out_tally: *mut MoraineObjectStoreTally,
) -> i32 {
    let attempt = || {
        if handle.is_null() || out_tally.is_null() {
            return codes::INVALID_ARGUMENT;
        }
        // SAFETY: caller contract for `handle`.
        let tally = unsafe { &*handle }.catalog.reads().object_store_tally();
        // SAFETY: checked non-null above; caller contract for validity.
        unsafe {
            *out_tally = tally.into();
        }
        codes::OK
    };
    catch_unwind(AssertUnwindSafe(attempt)).unwrap_or(codes::INTERNAL)
}

/// The store state the catalog's dumps currently serve: the head
/// snapshot id and batch count (a maintenance batch changes the count
/// without minting a snapshot). `out_present` is false on a store with no
/// head yet, where the other outputs are left unwritten.
///
/// # Safety
///
/// `handle` must be a pointer previously returned by [`super::moraine_attach`]
/// and not yet detached. `out_snapshot_id`, `out_batch_seq`, and
/// `out_present` must be valid, writable pointers. `probe`, if non-null,
/// must be safe to call with `probe_ctx` from any thread. `err`, if
/// non-null, must be a valid, writable [`MoraineError`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_head_stamp(
    handle: *mut MoraineCatalogHandle,
    out_snapshot_id: *mut u64,
    out_batch_seq: *mut u64,
    out_present: *mut bool,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Option<(u64, u64)>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_snapshot_id.is_null() || out_batch_seq.is_null() || out_present.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is the caller's contract.
        let head = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::head_stamp(handle_ref.catalog.reads()),
            )
        }?;
        Ok(head.map(|head| (head.snapshot_id, head.batch_seq)))
    };

    // SAFETY: `err` validity is the caller's contract.
    match unsafe { guard(err, attempt) } {
        Ok(stamp) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe {
                match stamp {
                    Some((snapshot_id, batch_seq)) => {
                        *out_snapshot_id = snapshot_id;
                        *out_batch_seq = batch_seq;
                        *out_present = true;
                    }
                    None => *out_present = false,
                }
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// The subspaces a merge can target, comma-separated, for an error
/// message. Owned — free via [`super::moraine_string_free`]; null if allocation
/// fails.
#[unsafe(no_mangle)]
pub extern "C" fn moraine_subspace_names() -> *mut c_char {
    let attempt = || {
        let names: Vec<String> = KNOWN_SUBSPACES.iter().map(ToString::to_string).collect();
        to_c_string(names.join(", ")).map_or(ptr::null_mut(), CString::into_raw)
    };
    catch_unwind(AssertUnwindSafe(attempt)).unwrap_or(ptr::null_mut())
}

/// The subspace `name` refers to, by the name a census prints.
fn parse_subspace(name: &str) -> Result<moraine::SubspaceName, AbiError> {
    KNOWN_SUBSPACES
        .iter()
        .find(|known| known.to_string() == name)
        .cloned()
        .ok_or_else(|| {
            let known: Vec<String> = KNOWN_SUBSPACES.iter().map(ToString::to_string).collect();
            AbiError::invalid_argument(format!(
                "unknown subspace \"{name}\"; known subspaces are: {}",
                known.join(", ")
            ))
        })
}

/// The subspaces a merge target may name.
pub(super) const KNOWN_SUBSPACES: [moraine::SubspaceName; 8] = [
    moraine::SubspaceName::System,
    moraine::SubspaceName::Snapshot,
    moraine::SubspaceName::Current,
    moraine::SubspaceName::History,
    moraine::SubspaceName::Inline,
    moraine::SubspaceName::Index,
    moraine::SubspaceName::SchemaVersion,
    moraine::SubspaceName::Changelog,
];
