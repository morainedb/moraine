//! Reproducers for runtime-shaped hazards an embedder can hit, where the
//! subject is the store beneath moraine rather than moraine itself.

use std::{sync::Arc, time::Duration};

use object_store::memory::InMemory;

/// How many open/write/close cycles to run before concluding the hang is
/// gone. It is a race, not a fixed count — observed at cycle 8, 47, 50,
/// 87, and 115 across runs — so this is several times the worst seen.
const CYCLES: usize = 300;

/// Long enough that a healthy `close` never trips it, short enough that a
/// wedged one is usually caught rather than hanging the run.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// SlateDB's `Db::close` can wedge on a multi-threaded runtime, and this
/// asserts that it still does: **the test failing is the good news**,
/// meaning the upstream hang is gone and the caveat it documents can go
/// with it.
///
/// The hang needs three things together — a write between the open and the
/// close, repeated cycles, and a multi-threaded runtime. Drop any one and
/// 300 cycles run clean: no write is clean, and the same write-then-close
/// cycle on a current-thread runtime is clean. At the hang every worker is
/// parked at zero CPU with the `close` future suspended and nothing
/// runnable anywhere, which is a lost wakeup in the shutdown path rather
/// than a lock cycle or a spin.
///
/// The timeout below is best-effort, not a guarantee: the wedge sometimes
/// takes the timer with it, and the run then goes quiet indefinitely. That
/// is the same finding — kill it and read it as a reproduction.
///
/// No moraine code is in this chain deliberately. `Catalog::close` is a
/// one-line delegation to `Db::close`, so pinning the subject upstream is
/// the whole point of the test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "upstream reproducer: runs until it wedges, and may need killing"]
#[allow(clippy::unwrap_used)]
async fn slatedb_close_can_hang_on_a_multi_thread_runtime() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let settings = slatedb::config::Settings {
        flush_interval: Some(Duration::from_millis(1)),
        ..Default::default()
    };

    for cycle in 0..CYCLES {
        let db = slatedb::Db::builder("", object_store.clone())
            .with_settings(settings.clone())
            .build()
            .await
            .unwrap();
        db.put(b"k", format!("v{cycle}").as_bytes()).await.unwrap();

        if tokio::time::timeout(CLOSE_TIMEOUT, db.close())
            .await
            .is_err()
        {
            eprintln!("close wedged on cycle {cycle} of {CYCLES}");
            return;
        }
    }

    panic!(
        "{CYCLES} open/write/close cycles all completed: the upstream close hang looks fixed, \
         so drop this test and the multi-threaded caveat it documents"
    );
}
