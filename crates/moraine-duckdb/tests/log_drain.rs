//! The `tracing` → DuckDB logging bridge, exercised through the C ABI the
//! shim calls: a real event emitted by the core must reach the sink.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    ffi::{CStr, c_char, c_void},
    sync::{Arc, Mutex, OnceLock},
};

use moraine::{Catalog, CatalogOptions};
use moraine_duckdb::logging::{install, moraine_drain_logs};
use object_store::memory::InMemory;

/// Records the sink received, as `(level, message)`.
fn collected() -> &'static Mutex<Vec<(i32, String)>> {
    static COLLECTED: OnceLock<Mutex<Vec<(i32, String)>>> = OnceLock::new();
    COLLECTED.get_or_init(|| Mutex::new(Vec::new()))
}

/// A `MoraineLogSink` that appends to `collected`.
unsafe extern "C" fn collect(_ctx: *mut c_void, level: i32, message: *const c_char) {
    // SAFETY: the ABI documents `message` as a valid NUL-terminated string
    // for the duration of this call.
    let text = unsafe { CStr::from_ptr(message) }
        .to_string_lossy()
        .into_owned();
    collected().lock().unwrap().push((level, text));
}

fn drain() -> Vec<(i32, String)> {
    // SAFETY: `collect` is a valid sink, does not unwind, and re-enters no
    // `moraine_*` entry point.
    unsafe { moraine_drain_logs(Some(collect), std::ptr::null_mut()) };
    std::mem::take(&mut *collected().lock().unwrap())
}

/// Opening a catalog emits an `info` event; the drain hands it to the sink
/// at DuckDB's `LOG_INFO` (30), tagged with the emitting module, and empties
/// the buffer as it goes.
///
/// One test, not two: the buffer and the subscriber are process-wide by
/// design, so parallel tests would drain each other's records.
#[tokio::test]
async fn catalog_open_reaches_the_sink_through_the_abi() {
    install();
    let _ = drain();

    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();

    let records = drain();
    let opened = records
        .iter()
        .find(|(_, message)| message.contains("opened catalog read-write"))
        .unwrap_or_else(|| panic!("no open event in {records:?}"));
    assert_eq!(opened.0, 30, "expected DuckDB LOG_INFO");
    assert!(
        opened.1.contains("moraine::catalog::handle"),
        "record should name its source module: {}",
        opened.1
    );
    // Structured fields ride along, not just the message.
    assert!(
        opened.1.contains("poll_interval_ms="),
        "record should carry the event's fields: {}",
        opened.1
    );

    // Draining does not replay: the first pass emptied the buffer.
    assert!(drain().is_empty(), "a second drain should find nothing");

    catalog.close().await.unwrap();
}
