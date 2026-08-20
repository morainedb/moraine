//! Per-handle log routing, exercised end to end through the C ABI: each
//! attached catalog's events reach the sink registered for that handle —
//! and no one else's — via the runtime thread tags the attach set up.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    ffi::{CStr, CString, c_char, c_void},
    sync::{Mutex, OnceLock},
};

use moraine_duckdb::{
    abi::{MoraineS3Config, moraine_attach, moraine_detach},
    error::MoraineError,
    logging::{moraine_register_log_sink, moraine_unregister_log_sink},
    runtime::MoraineCatalogHandle,
};

/// What the sinks received, as `(ctx-as-number, message)`.
fn received() -> &'static Mutex<Vec<(usize, String)>> {
    static RECEIVED: OnceLock<Mutex<Vec<(usize, String)>>> = OnceLock::new();
    RECEIVED.get_or_init(|| Mutex::new(Vec::new()))
}

/// A `MoraineLogSink` that appends to `received`, tagged with its `ctx`.
unsafe extern "C" fn collect(ctx: *mut c_void, _level: i32, message: *const c_char) {
    // SAFETY: the ABI documents `message` as a valid NUL-terminated string
    // for the duration of this call.
    let text = unsafe { CStr::from_ptr(message) }
        .to_string_lossy()
        .into_owned();
    received().lock().unwrap().push((ctx as usize, text));
}

fn take_received() -> Vec<(usize, String)> {
    std::mem::take(&mut *received().lock().unwrap())
}

/// Attaches a fresh in-memory catalog through the ABI.
fn attach_memory() -> *mut MoraineCatalogHandle {
    let path = CString::new("memory://").unwrap();
    let mut handle: *mut MoraineCatalogHandle = std::ptr::null_mut();
    let mut error = MoraineError::default();
    // SAFETY: all pointers are valid for this call; null s3/cache_dir/
    // data_path are the documented "none" cases.
    let code = unsafe {
        moraine_attach(
            path.as_ptr(),
            std::ptr::null::<MoraineS3Config>(),
            false,
            false,
            0,
            std::ptr::null(),
            0,
            0,
            0,
            false,
            std::ptr::null(),
            std::ptr::null(),
            0,
            None,
            std::ptr::null_mut(),
            &raw mut handle,
            &raw mut error,
        )
    };
    assert_eq!(code, 0, "attach failed");
    handle
}

/// One test, not several: the registry, buffer, and subscriber are
/// process-wide by design, so parallel tests would receive each other's
/// records.
#[test]
fn each_handles_events_reach_only_its_own_sink() {
    let first = attach_memory();
    // SAFETY: `first` is live; `collect` is a valid sink that never
    // unwinds, emits no events, and re-enters nothing.
    unsafe { moraine_register_log_sink(first, Some(collect), std::ptr::without_provenance_mut(1)) };

    // Registration flushes the attach's own buffered events — emitted on
    // this thread and on the handle's worker threads — through the sink.
    let backlog = take_received();
    assert!(
        backlog
            .iter()
            .any(|(ctx, message)| *ctx == 1 && message.contains("opened catalog read-write")),
        "the first handle's open event should reach its sink: {backlog:?}"
    );
    assert!(
        backlog.iter().all(|(ctx, _)| *ctx == 1),
        "nothing else is registered, so nothing else may receive: {backlog:?}"
    );

    // A second attach emits the same events from its own handle; none of
    // them may surface through the first handle's sink.
    let second = attach_memory();
    assert_eq!(
        take_received(),
        Vec::new(),
        "the second handle's events must not reach the first handle's sink"
    );

    // Its own registration delivers them, to its own context.
    // SAFETY: as above.
    unsafe {
        moraine_register_log_sink(second, Some(collect), std::ptr::without_provenance_mut(2));
    };
    let second_backlog = take_received();
    assert!(
        second_backlog
            .iter()
            .any(|(ctx, message)| *ctx == 2 && message.contains("opened catalog read-write")),
        "the second handle's open event should reach its sink: {second_backlog:?}"
    );
    assert!(
        second_backlog.iter().all(|(ctx, _)| *ctx == 2),
        "the backlog flush must route only to the registering handle: {second_backlog:?}"
    );

    // Mirror the shim's teardown discipline: unregister, then detach.
    // SAFETY: both handles are live and were attached above.
    unsafe {
        moraine_unregister_log_sink(first);
        moraine_unregister_log_sink(second);
        moraine_detach(first);
        moraine_detach(second);
    }
}
