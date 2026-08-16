//! Carrying [`moraine`]'s `tracing` events into DuckDB's logger. The
//! extension is a separately loaded library with its own `tracing`, so it
//! consumes its own events.
//!
//! Routing rides on threads: a handle's runtime tags its worker threads at
//! spawn, and its `block_on` wrappers tag the calling thread per call. An
//! event is attributed to whatever handle tagged the thread it fires on
//! and, while that handle has a sink registered, written straight through
//! it as it fires.
//!
//! Events with no attributed sink fall back to a bounded buffering layer
//! (oldest dropped first), which the shim drains at operation boundaries on
//! threads that hold a `ClientContext`.

use std::{
    cell::Cell,
    collections::VecDeque,
    ffi::{CString, c_char, c_void},
    fmt::Write as _,
    sync::{
        Mutex, OnceLock, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use tracing::{Level, Subscriber, field::Visit};
use tracing_subscriber::{layer::Context, prelude::*, registry::LookupSpan};

use crate::runtime::MoraineCatalogHandle;

/// Identifies one attached handle for event routing, assigned at attach.
pub(crate) type HandleId = u64;

fn next_handle_id() -> HandleId {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Allocates a routing identity for a handle about to be attached.
pub(crate) fn allocate_handle_id() -> HandleId {
    next_handle_id()
}

thread_local! {
    /// The handle whose work this thread is currently running, if any.
    static CURRENT_HANDLE: Cell<Option<HandleId>> = const { Cell::new(None) };
}

/// Tags this thread as belonging to `handle_id` for its whole life (for a
/// runtime's worker threads).
pub(crate) fn tag_thread_for_handle(handle_id: HandleId) {
    CURRENT_HANDLE.set(Some(handle_id));
}

/// Tags the current thread as running `handle_id`'s work until the guard
/// drops (for `block_on` callers).
pub(crate) fn enter_handle(handle_id: HandleId) -> HandleGuard {
    let previous = CURRENT_HANDLE.replace(Some(handle_id));
    HandleGuard { previous }
}

/// Restores the thread's previous handle tag on drop.
pub(crate) struct HandleGuard {
    previous: Option<HandleId>,
}

impl Drop for HandleGuard {
    fn drop(&mut self) {
        CURRENT_HANDLE.set(self.previous);
    }
}

/// How many records the buffer holds before dropping the oldest.
const LOG_BUFFER_CAPACITY: usize = 512;

/// DuckDB's `LogLevel` values, which the sink forwards unchanged.
mod levels {
    pub const TRACE: i32 = 10;
    pub const DEBUG: i32 = 20;
    pub const INFO: i32 = 30;
    pub const WARNING: i32 = 40;
    pub const ERROR: i32 = 50;
}

/// One buffered event, attributed to the handle whose thread it fired on
/// (`None` when it fired outside any handle's threads).
struct LogRecord {
    handle: Option<HandleId>,
    level: i32,
    message: String,
}

/// The process-wide buffer, plus how many records were dropped since the
/// last drain.
struct LogBuffer {
    records: VecDeque<LogRecord>,
    dropped: u64,
}

fn buffer() -> &'static Mutex<LogBuffer> {
    static BUFFER: OnceLock<Mutex<LogBuffer>> = OnceLock::new();
    BUFFER.get_or_init(|| {
        Mutex::new(LogBuffer {
            records: VecDeque::new(),
            dropped: 0,
        })
    })
}

/// The callable inside [`MoraineLogSink`], once the null case is peeled.
type LogSinkFunction = unsafe extern "C" fn(ctx: *mut c_void, level: i32, message: *const c_char);

/// One registered push sink: the delivery target for events attributed to
/// `handle`. At most one per handle.
struct RegisteredSink {
    handle: HandleId,
    sink: LogSinkFunction,
    ctx: *mut c_void,
}

// SAFETY: `ctx` is opaque to this module; the registration contract makes
// the shim keep it valid and callable from any thread until unregistered.
unsafe impl Send for RegisteredSink {}
// SAFETY: as above — a shared entry is only ever read, and its `sink` is
// callable from any thread while it is registered.
unsafe impl Sync for RegisteredSink {}

/// Registered sinks. Events take the read side; registration takes the
/// write side.
fn sinks() -> &'static RwLock<Vec<RegisteredSink>> {
    static SINKS: OnceLock<RwLock<Vec<RegisteredSink>>> = OnceLock::new();
    SINKS.get_or_init(|| RwLock::new(Vec::new()))
}

/// The lowest level captured, from `MORAINE_LOG` (`trace`, `debug`, `info`,
/// `warn`, `error`); defaults to `info`. Events below it are never
/// buffered.
fn capture_level() -> Level {
    static LEVEL: OnceLock<Level> = OnceLock::new();
    *LEVEL.get_or_init(|| match std::env::var("MORAINE_LOG") {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "trace" => Level::TRACE,
            "debug" => Level::DEBUG,
            "warn" | "warning" => Level::WARN,
            "error" => Level::ERROR,
            _ => Level::INFO,
        },
        Err(_) => Level::INFO,
    })
}

fn duckdb_level(level: Level) -> i32 {
    match level {
        Level::TRACE => levels::TRACE,
        Level::DEBUG => levels::DEBUG,
        Level::INFO => levels::INFO,
        Level::WARN => levels::WARNING,
        Level::ERROR => levels::ERROR,
    }
}

/// Renders an event's `message` field plus its remaining fields as
/// `message (key=value, key=value)`.
#[derive(Default)]
struct MessageVisitor {
    message: String,
    fields: String,
}

impl MessageVisitor {
    fn record(&mut self, name: &str, value: &dyn std::fmt::Debug) {
        if name == "message" {
            self.message = format!("{value:?}");
            return;
        }
        if !self.fields.is_empty() {
            self.fields.push_str(", ");
        }
        self.fields.push_str(name);
        self.fields.push('=');
        // Writing into a `String` is infallible.
        let _ = write!(self.fields, "{value:?}");
    }

    fn finish(self, target: &str) -> String {
        let Self { message, fields } = self;
        match (message.is_empty(), fields.is_empty()) {
            (true, true) => target.to_string(),
            (true, false) => format!("{target}: {fields}"),
            (false, true) => format!("{target}: {message}"),
            (false, false) => format!("{target}: {message} ({fields})"),
        }
    }
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.record(field.name(), value);
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.record(field.name(), &value);
    }
}

/// Buffers every event at or above [`capture_level`].
struct BufferLayer;

impl<S> tracing_subscriber::Layer<S> for BufferLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _: Context<'_, S>) -> bool {
        metadata.level() <= &capture_level()
    }

    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        let metadata = event.metadata();
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let record = LogRecord {
            handle: CURRENT_HANDLE.get(),
            level: duckdb_level(*metadata.level()),
            message: visitor.finish(metadata.target()),
        };

        // The read lock is held across the sink call, so unregistration
        // (write lock) returning means no call is in flight.
        if let Some(handle) = record.handle
            && let Ok(sinks) = sinks().read()
            && let Some(registered) = sinks.iter().find(|registered| registered.handle == handle)
        {
            // SAFETY: the registration contract keeps `sink` callable
            // with `ctx` from any thread while the entry is present.
            unsafe { write_record(registered.sink, registered.ctx, &record) };
            return;
        }

        let Ok(mut buffer) = buffer().lock() else {
            return;
        };
        if buffer.records.len() >= LOG_BUFFER_CAPACITY {
            buffer.records.pop_front();
            buffer.dropped = buffer.dropped.saturating_add(1);
        }
        buffer.records.push_back(record);
    }
}

/// Hands one record to `sink`, dropping a message that cannot cross as a C
/// string.
///
/// # Safety
///
/// `sink` must be callable with `ctx` on this thread and must not unwind.
unsafe fn write_record(sink: LogSinkFunction, ctx: *mut c_void, record: &LogRecord) {
    let Ok(message) = CString::new(record.message.as_str())
        .or_else(|_| CString::new(record.message.replace('\0', "")))
    else {
        return;
    };
    // SAFETY: caller contract; pointer valid for this call only.
    unsafe { sink(ctx, record.level, message.as_ptr()) };
}

/// Empties the buffer into `sink`: the dropped-count warning first, then
/// every record, oldest first.
///
/// # Safety
///
/// `sink` must be callable with `ctx` on this thread and must not unwind.
unsafe fn drain_buffer_into(sink: LogSinkFunction, ctx: *mut c_void) {
    let Ok(mut buffer) = buffer().lock() else {
        return;
    };
    let dropped = std::mem::take(&mut buffer.dropped);
    let records: Vec<LogRecord> = buffer.records.drain(..).collect();
    // Released before calling out, so a sink that logs cannot deadlock.
    drop(buffer);

    if dropped > 0 {
        let warning = LogRecord {
            handle: None,
            level: levels::WARNING,
            message: format!(
                "moraine: {dropped} diagnostic record(s) dropped; the log buffer filled between drains"
            ),
        };
        // SAFETY: caller contract.
        unsafe { write_record(sink, ctx, &warning) };
    }
    for record in records {
        // SAFETY: caller contract.
        unsafe { write_record(sink, ctx, &record) };
    }
}

/// Installs the buffering subscriber, once per process. Every call after
/// the first is a no-op, and a host that already installed a global
/// subscriber wins.
pub fn install() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        // `try_init` fails only when a global subscriber already exists.
        let _ = tracing_subscriber::registry().with(BufferLayer).try_init();
    });
}

/// Receives one buffered log record: its DuckDB `LogLevel` value and a
/// UTF-8, NUL-terminated message valid only for the duration of the call.
pub type MoraineLogSink =
    Option<unsafe extern "C" fn(ctx: *mut c_void, level: i32, message: *const c_char)>;

/// Drains every buffered log record into `sink`, oldest first. Never
/// fails; each message is borrowed for the duration of its `sink` call.
///
/// # Safety
///
/// `sink`, if non-null, must be callable with `sink_ctx` and must not
/// unwind. It must not re-enter any `moraine_*` entry point.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_drain_logs(sink: MoraineLogSink, sink_ctx: *mut c_void) {
    let Some(sink) = sink else {
        return;
    };
    // SAFETY: caller contract.
    unsafe { drain_buffer_into(sink, sink_ctx) };
}

/// Registers `sink` as the delivery target for `handle`'s events, first
/// handing it whatever the buffer already holds from that handle. While
/// registered, the handle's events bypass the buffer; other handles'
/// events are untouched. Registering again for the same handle replaces
/// its sink.
///
/// # Safety
///
/// `handle` must be a live pointer from `moraine_attach`. `sink`, if
/// non-null, must be callable with `ctx` from any thread until
/// unregistration returns, must not unwind, must not emit `tracing`
/// events, and must not re-enter any `moraine_*` entry point.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_register_log_sink(
    handle: *const MoraineCatalogHandle,
    sink: MoraineLogSink,
    ctx: *mut c_void,
) {
    let Some(sink) = sink else {
        return;
    };
    // SAFETY: caller contract — `handle` is a live attach handle.
    let Some(handle) = (unsafe { handle.as_ref() }) else {
        return;
    };
    // SAFETY: caller contract carried through.
    unsafe { register_sink(handle.log_id, sink, ctx) };
}

/// [`moraine_register_log_sink`] with the routing identity in hand.
///
/// # Safety
///
/// As [`moraine_register_log_sink`], minus the handle-pointer clause.
pub(crate) unsafe fn register_sink(handle: HandleId, sink: LogSinkFunction, ctx: *mut c_void) {
    let Ok(mut sinks) = sinks().write() else {
        return;
    };
    // Flushed under the registry lock, so a concurrent event arrives
    // exactly once, in order.
    // SAFETY: caller contract.
    unsafe { flush_handle_backlog(handle, sink, ctx) };

    if let Some(registered) = sinks
        .iter_mut()
        .find(|registered| registered.handle == handle)
    {
        registered.sink = sink;
        registered.ctx = ctx;
        return;
    }
    sinks.push(RegisteredSink { handle, sink, ctx });
}

/// Removes `handle`'s sink, if one is registered. When it returns, no call
/// to that sink is in flight and none will follow — its `ctx` may be torn
/// down. The handle's later events fall back to the buffer.
///
/// # Safety
///
/// `handle` must be a live pointer from `moraine_attach`. Must not be
/// called from inside a sink.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_unregister_log_sink(handle: *const MoraineCatalogHandle) {
    // SAFETY: caller contract — `handle` is a live attach handle.
    let Some(handle) = (unsafe { handle.as_ref() }) else {
        return;
    };
    unregister_sink(handle.log_id);
}

/// [`moraine_unregister_log_sink`] with the routing identity in hand.
pub(crate) fn unregister_sink(handle: HandleId) {
    let Ok(mut sinks) = sinks().write() else {
        return;
    };
    sinks.retain(|registered| registered.handle != handle);
}

/// Hands every buffered record attributed to `handle` to `sink`, oldest
/// first, leaving other records in the buffer.
///
/// # Safety
///
/// `sink` must be callable with `ctx` on this thread and must not unwind.
unsafe fn flush_handle_backlog(handle: HandleId, sink: LogSinkFunction, ctx: *mut c_void) {
    let Ok(mut buffer) = buffer().lock() else {
        return;
    };
    let (matching, remaining): (VecDeque<LogRecord>, VecDeque<LogRecord>) =
        std::mem::take(&mut buffer.records)
            .into_iter()
            .partition(|record| record.handle == Some(handle));
    buffer.records = remaining;
    // Released before calling out, so a sink that logs cannot deadlock.
    drop(buffer);

    for record in matching {
        // SAFETY: caller contract.
        unsafe { write_record(sink, ctx, &record) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The visitor renders the message and the remaining fields in the
    /// shape the shim forwards to DuckDB.
    #[test]
    fn visitor_renders_message_and_fields() {
        let mut visitor = MessageVisitor::default();
        visitor.record("message", &"commit exhausted its retry budget");
        visitor.record("attempts", &10);
        let rendered = visitor.finish("moraine::transaction::commit");
        assert_eq!(
            rendered,
            "moraine::transaction::commit: \"commit exhausted its retry budget\" (attempts=10)"
        );
    }

    #[test]
    fn visitor_renders_a_bare_message() {
        let mut visitor = MessageVisitor::default();
        visitor.record("message", &"plain");
        assert_eq!(visitor.finish("target"), "target: \"plain\"");
    }

    /// `warn!` maps to DuckDB's `LOG_WARNING`, the level the exhausted-budget
    /// diagnostic is emitted at.
    #[test]
    fn levels_map_to_duckdb_values() {
        assert_eq!(duckdb_level(Level::WARN), 40);
        assert_eq!(duckdb_level(Level::DEBUG), 20);
    }

    /// A drain with no sink is a no-op rather than a crash.
    #[test]
    fn draining_without_a_sink_is_a_no_op() {
        // SAFETY: a null sink is the documented no-op case.
        unsafe { moraine_drain_logs(None, std::ptr::null_mut()) };
    }

    /// What the routing test's sinks received: `(ctx-as-number, message)`.
    fn received() -> &'static Mutex<Vec<(usize, String)>> {
        static RECEIVED: OnceLock<Mutex<Vec<(usize, String)>>> = OnceLock::new();
        RECEIVED.get_or_init(|| Mutex::new(Vec::new()))
    }

    unsafe extern "C" fn receive(ctx: *mut c_void, _level: i32, message: *const c_char) {
        // SAFETY: the sink contract — `message` is a valid NUL-terminated
        // string for the duration of this call.
        let text = unsafe { std::ffi::CStr::from_ptr(message) }
            .to_string_lossy()
            .into_owned();
        if let Ok(mut received) = received().lock() {
            received.push((ctx as usize, text));
        }
    }

    /// Events route by the handle that tagged the emitting thread: a
    /// registered handle's events push straight to its own sink, another
    /// handle's events do not touch it, and unregistering restores
    /// buffering. One test, since the registry and buffer are process-wide.
    #[test]
    fn events_route_to_the_emitting_handles_sink() {
        let ours = allocate_handle_id();
        let other = allocate_handle_id();
        let subscriber = tracing_subscriber::registry().with(super::BufferLayer);

        tracing::subscriber::with_default(subscriber, || {
            // SAFETY: `receive` is a valid sink that never unwinds, emits
            // no events, and re-enters nothing.
            unsafe { register_sink(ours, receive, std::ptr::without_provenance_mut(7)) };

            {
                let _guard = enter_handle(ours);
                tracing::info!("routed to ours");
            }
            {
                let _guard = enter_handle(other);
                tracing::info!("stray from another handle");
            }
            tracing::info!("unattributed event");

            let pushed = std::mem::take(&mut *received().lock().unwrap());
            assert!(
                pushed
                    .iter()
                    .any(|(ctx, message)| *ctx == 7 && message.contains("routed to ours")),
                "the handle's event should push to its sink without a drain: {pushed:?}"
            );
            assert!(
                !pushed
                    .iter()
                    .any(|(_, message)| message.contains("stray")
                        || message.contains("unattributed")),
                "other handles' and unattributed events must not reach this sink: {pushed:?}"
            );

            // Registering for the other handle now flushes its backlog —
            // and only its backlog — from the buffer.
            // SAFETY: as above.
            unsafe { register_sink(other, receive, std::ptr::without_provenance_mut(9)) };
            let flushed = std::mem::take(&mut *received().lock().unwrap());
            assert!(
                flushed.iter().any(
                    |(ctx, message)| *ctx == 9 && message.contains("stray from another handle")
                ),
                "registration should flush the handle's buffered backlog: {flushed:?}"
            );
            assert!(
                !flushed
                    .iter()
                    .any(|(_, message)| message.contains("unattributed")),
                "unattributed records stay for the boundary drains: {flushed:?}"
            );

            // After unregistration the handle's events buffer again.
            unregister_sink(ours);
            {
                let _guard = enter_handle(ours);
                tracing::info!("buffered after unregistration");
            }
            assert!(
                received().lock().unwrap().is_empty(),
                "an unregistered handle's events must not push"
            );

            unregister_sink(other);
        });

        // The buffered leftovers reach a boundary drain.
        // SAFETY: as above.
        unsafe { moraine_drain_logs(Some(receive), std::ptr::null_mut()) };
        let drained = std::mem::take(&mut *received().lock().unwrap());
        assert!(
            drained
                .iter()
                .any(|(_, message)| message.contains("buffered after unregistration")),
            "the boundary drain should pick up unrouted events: {drained:?}"
        );
    }
}
