//! Carrying [`moraine`]'s `tracing` events into DuckDB's logger. The
//! extension is a separately loaded library with its own `tracing`, so it
//! consumes its own events.
//!
//! Routing rides on threads: a handle's runtime tags its worker threads at
//! spawn, and its `block_on` wrappers tag the calling thread per call. An
//! event is attributed to whatever handle tagged the thread it fires on.
//!
//! Nothing is written from the thread that logged. Events go to a bounded
//! queue (oldest dropped first) that a dedicated thread drains, writing
//! each to its handle's sink if one is registered. A sink is host code of
//! unknown duration, and the emitting thread is often a runtime worker
//! mid-task.
//!
//! Events with no attributed sink fall back to a bounded buffer, which the
//! shim drains at operation boundaries on threads that hold a
//! `ClientContext`.

use std::{
    cell::Cell,
    collections::VecDeque,
    ffi::{CString, c_char, c_void},
    fmt::Write as _,
    sync::{
        Condvar, Mutex, OnceLock, RwLock,
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
#[derive(Clone)]
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

/// Parses a level name, falling back to `default` for anything else.
fn level_from(name: &str, default: Level) -> Level {
    match name.trim().to_ascii_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" | "warning" => Level::WARN,
        "error" => Level::ERROR,
        _ => default,
    }
}

/// The lowest level captured for moraine's own events, from `MORAINE_LOG`
/// (`trace`, `debug`, `info`, `warn`, `error`); defaults to `info`.
fn capture_level() -> Level {
    static LEVEL: OnceLock<Level> = OnceLock::new();
    *LEVEL.get_or_init(|| match std::env::var("MORAINE_LOG") {
        Ok(value) => level_from(&value, Level::INFO),
        Err(_) => Level::INFO,
    })
}

/// The lowest level captured for everything moraine depends on, from
/// `MORAINE_LOG_DEPENDENCIES`; defaults to `info`.
///
/// Held separately because the dependencies out-log moraine by orders of
/// magnitude: one HTTP client at `debug` buries a catalog's own records.
fn dependency_capture_level() -> Level {
    static LEVEL: OnceLock<Level> = OnceLock::new();
    *LEVEL.get_or_init(|| match std::env::var("MORAINE_LOG_DEPENDENCIES") {
        Ok(value) => level_from(&value, Level::INFO),
        Err(_) => Level::INFO,
    })
}

/// Whether `target` is moraine's own, rather than a dependency's. A
/// target's first segment is the crate that emitted it.
fn is_moraine_target(target: &str) -> bool {
    matches!(
        target.split("::").next(),
        Some("moraine" | "moraine_duckdb")
    )
}

/// The floor `target`'s events must clear to be captured.
fn floor_for(target: &str) -> Level {
    if is_moraine_target(target) {
        capture_level()
    } else {
        dependency_capture_level()
    }
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

/// Buffers every event at or above the floor its target names.
struct BufferLayer;

impl<S> tracing_subscriber::Layer<S> for BufferLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _: Context<'_, S>) -> bool {
        metadata.level() <= &floor_for(metadata.target())
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

        // Queued, never delivered from here: a sink is host code of
        // unknown duration, and this runs on whatever thread logged --
        // including a runtime worker in the middle of a task.
        enqueue_for_delivery(record);
    }
}

/// Records waiting for the delivery thread, and how many it never saw.
struct PendingDelivery {
    records: VecDeque<LogRecord>,
    dropped: u64,
    /// Totals since the process began; [`flush_delivery`] waits on the gap.
    queued: u64,
    delivered: u64,
}

fn pending() -> &'static (Mutex<PendingDelivery>, Condvar) {
    static PENDING: OnceLock<(Mutex<PendingDelivery>, Condvar)> = OnceLock::new();
    PENDING.get_or_init(|| {
        (
            Mutex::new(PendingDelivery {
                records: VecDeque::new(),
                dropped: 0,
                queued: 0,
                delivered: 0,
            }),
            Condvar::new(),
        )
    })
}

/// Queues `record` and wakes the delivery thread, starting it on first use.
fn enqueue_for_delivery(record: LogRecord) {
    start_delivery_thread();
    let (lock, waiting) = pending();
    let Ok(mut queue) = lock.lock() else {
        return;
    };
    admit(&mut queue, record);

    // Every waiter shares this condvar, so waking just one can wake a
    // flusher and leave the queue undrained.
    waiting.notify_all();
}

/// Admits `record`, evicting the oldest once full: a reader that has
/// stopped consuming costs the oldest records rather than unbounded memory.
///
/// An evicted record counts as delivered. It will never reach a sink, and
/// a flush that waited for it would wait out its whole timeout.
fn admit(queue: &mut PendingDelivery, record: LogRecord) {
    if queue.records.len() >= LOG_BUFFER_CAPACITY {
        queue.records.pop_front();
        queue.dropped = queue.dropped.saturating_add(1);
        queue.delivered = queue.delivered.saturating_add(1);
    }
    queue.records.push_back(record);
    queue.queued = queue.queued.saturating_add(1);
}

/// Waits until every record queued so far has reached a sink or the pull
/// buffer, or until `timeout` passes.
///
/// Delivery runs on a thread of its own, so logging no longer proves
/// arrival.
pub(crate) fn flush_delivery(timeout: std::time::Duration) {
    let (lock, waiting) = pending();
    let Ok(mut queue) = lock.lock() else {
        return;
    };
    let target = queue.queued;
    let deadline = std::time::Instant::now() + timeout;
    while queue.delivered < target {
        let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
            return;
        };
        let Ok((next, _)) = waiting.wait_timeout(queue, remaining) else {
            return;
        };
        queue = next;
    }
}

/// Starts the thread that delivers queued records, once per process.
fn start_delivery_thread() {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        let _ = std::thread::Builder::new()
            .name("moraine-logs".to_owned())
            .spawn(deliver_forever);
    });
}

/// Delivers queued records to their handle's sink, or leaves them for a
/// caller to pull when no sink is registered.
fn deliver_forever() {
    loop {
        let (records, dropped) = {
            let (lock, waiting) = pending();
            let Ok(mut queue) = lock.lock() else {
                return;
            };
            while queue.records.is_empty() {
                let Ok(next) = waiting.wait(queue) else {
                    return;
                };
                queue = next;
            }
            (
                queue.records.drain(..).collect::<Vec<_>>(),
                std::mem::take(&mut queue.dropped),
            )
        };

        if dropped > 0 {
            deliver(&LogRecord {
                handle: None,
                level: levels::WARNING,
                message: format!(
                    "moraine: {dropped} diagnostic record(s) dropped; delivery fell behind"
                ),
            });
        }
        for record in &records {
            deliver(record);
        }
        if let Ok(mut queue) = pending().0.lock() {
            queue.delivered = queue
                .delivered
                .saturating_add(records.len().try_into().unwrap_or(u64::MAX));
            pending().1.notify_all();
        }
    }
}

/// Hands one record to its handle's sink, falling back to the pull buffer
/// when none is registered.
fn deliver(record: &LogRecord) {
    // The read lock is held across the sink call, so unregistration
    // (write lock) returning means no call is in flight.
    if let Some(handle) = record.handle
        && let Ok(sinks) = sinks().read()
        && let Some(registered) = sinks.iter().find(|registered| registered.handle == handle)
    {
        // SAFETY: the registration contract keeps `sink` callable with
        // `ctx` from any thread while the entry is present.
        unsafe { write_record(registered.sink, registered.ctx, record) };
        return;
    }

    let Ok(mut buffer) = buffer().lock() else {
        return;
    };
    if buffer.records.len() >= LOG_BUFFER_CAPACITY {
        buffer.records.pop_front();
        buffer.dropped = buffer.dropped.saturating_add(1);
    }
    buffer.records.push_back(record.clone());
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
    // A drain is a pull of what has been logged so far, and delivery is
    // no longer synchronous with logging. Bounded short: this sits on an
    // operation boundary, and a missed record only waits for the next one.
    flush_delivery(std::time::Duration::from_millis(100));
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
    // Before the registry lock, never under it: delivery takes a read lock,
    // so waiting on it while holding the write lock would deadlock. Queued
    // records reach the backlog buffer first this way, so the flush below
    // sees everything emitted before registration.
    flush_delivery(std::time::Duration::from_secs(2));
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
    // Before the registry lock, as in registration. A record still queued
    // would miss its sink and land in a buffer nobody may drain; the bound
    // keeps a detach from waiting on a reader.
    flush_delivery(std::time::Duration::from_secs(2));
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

    /// A dependency's events are held to their own floor, so raising
    /// moraine's level does not admit an HTTP client's debug traffic.
    #[test]
    fn a_dependency_is_held_to_its_own_floor() {
        for target in [
            "hyper_util::client::legacy::pool",
            "object_store::aws",
            "moraineish",
        ] {
            assert!(
                !is_moraine_target(target),
                "{target} must not be taken for moraine's own"
            );
            assert_eq!(floor_for(target), dependency_capture_level());
        }
    }

    /// The crate's own targets, including the shim's, take `MORAINE_LOG`.
    #[test]
    fn moraines_own_targets_take_the_capture_level() {
        for target in [
            "moraine",
            "moraine::transaction::commit",
            "moraine_duckdb::runtime",
        ] {
            assert!(is_moraine_target(target), "{target} is moraine's own");
            assert_eq!(floor_for(target), capture_level());
        }
    }

    /// An unparseable level leaves the default standing rather than
    /// silently widening or narrowing what is captured.
    #[test]
    fn an_unknown_level_name_keeps_the_default() {
        assert_eq!(level_from("verbose", Level::WARN), Level::WARN);
        assert_eq!(level_from("  DEBUG ", Level::WARN), Level::DEBUG);
        assert_eq!(level_from("warning", Level::INFO), Level::WARN);
    }

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

    /// Eviction keeps `queued - delivered` equal to what is still queued,
    /// so a flush past an overflow finishes instead of waiting out its
    /// timeout.
    #[test]
    fn evicted_records_do_not_leave_a_flush_waiting() {
        let mut queue = PendingDelivery {
            records: VecDeque::new(),
            dropped: 0,
            queued: 0,
            delivered: 0,
        };

        for index in 0..LOG_BUFFER_CAPACITY + 10 {
            admit(
                &mut queue,
                LogRecord {
                    handle: None,
                    level: levels::INFO,
                    message: format!("record {index}"),
                },
            );
        }

        assert_eq!(queue.dropped, 10);
        assert_eq!(queue.records.len(), LOG_BUFFER_CAPACITY);
        assert_eq!(queue.queued - queue.delivered, LOG_BUFFER_CAPACITY as u64);
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

            // Delivery is no longer synchronous with the call that logged.
            flush_delivery(std::time::Duration::from_secs(5));
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
            flush_delivery(std::time::Duration::from_secs(5));
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
