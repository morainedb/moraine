//! A core-level A/B of the commit paths by object-store request class, the
//! measurement the coalescer's trade rests on. It is `#[ignore]`d: a
//! measurement, not a gate. Run it with
//!
//! ```text
//! cargo test -p moraine --test it -- --ignored --nocapture coalescer_bench
//! ```
//!
//! The number that decides the trade is requests **by class** — PUT and LIST
//! share object storage's expensive tier — counted over `InMemory`, where the
//! one quantity being weighed (a conditional-PUT round trip) is free of
//! latency but still one request. The flip makes the slot log the only commit
//! topology, so two configurations of it are measured: driven commit-at-a-time
//! (the coalescer degenerates to a one-member batch, the ~1-PUT/commit slot
//! floor) and under concurrency (the coalescer batches, recovering the PUT
//! rate). These reproduce the slot-path figures Task 8c's gate recorded.

use std::{sync::Arc, time::Duration};

use moraine::{Catalog, CatalogOptions};
use object_store::ObjectStore;

use crate::fixtures::CountingStore;

/// Commits the workload measures.
const COMMITS: usize = 50;

/// One configuration's request tally, sampled as the delta a workload adds
/// over what opening the catalog already cost.
#[derive(Debug, Default, Clone, Copy)]
struct Sample {
    puts: u64,
    gets: u64,
    heads: u64,
    lists: u64,
    deletes: u64,
}

impl Sample {
    fn of(store: &CountingStore) -> Self {
        Self {
            puts: store.put_count(),
            gets: store.get_count(),
            heads: store.head_count(),
            lists: store.list_count(),
            deletes: store.delete_count(),
        }
    }

    fn since(store: &CountingStore, before: Sample) -> Self {
        let now = Self::of(store);
        Self {
            puts: now.puts - before.puts,
            gets: now.gets - before.gets,
            heads: now.heads - before.heads,
            lists: now.lists - before.lists,
            deletes: now.deletes - before.deletes,
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn report(&self, label: &str, commits: usize) {
        let per = |total: u64| total as f64 / commits as f64;
        println!(
            "{label:<34} PUT={:>4} ({:>5.2}/commit)  LIST={:>4} ({:>5.2})  GET={:>5} ({:>6.2})  \
             HEAD={:>5}  DELETE={:>3}",
            self.puts,
            per(self.puts),
            self.lists,
            per(self.lists),
            self.gets,
            per(self.gets),
            self.heads,
            self.deletes,
        );
    }
}

#[allow(clippy::unwrap_used)]
async fn open(store: &Arc<CountingStore>, options: CatalogOptions) -> Catalog {
    Catalog::open(store.clone() as Arc<dyn ObjectStore>, options)
        .await
        .unwrap()
}

fn slots(window: Duration) -> CatalogOptions {
    let mut options = CatalogOptions::default();
    options.commit_batch_window = window;
    options
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "bench: run with --ignored --nocapture"]
async fn coalescer_bench_counts_requests_by_class() {
    println!("\n=== commit-path request classes over {COMMITS} single-statement commits ===");

    // The slot path, commit-at-a-time: the coalescer forms a one-member batch
    // each time, so this is the slot topology's ~1-PUT/commit floor.
    {
        let store = Arc::new(CountingStore::new());
        let catalog = open(&store, slots(Duration::ZERO)).await;
        let before = Sample::of(&store);
        for i in 0..COMMITS {
            catalog
                .commit(move |tx| tx.create_schema(&format!("s{i}")).map(|_| ()))
                .await
                .unwrap();
        }
        Sample::since(&store, before).report("slot path, serial (no coalescing)", COMMITS);
        catalog.close().await.unwrap();
    }

    // The slot path under concurrency: the coalescer drains the queue into few
    // envelopes, so the PUT rate is bounded by round-trip time, not commit
    // rate. Opportunistic (zero window) and windowed both measured.
    for (label, window) in [
        ("slot path, concurrent (window=0)", Duration::ZERO),
        (
            "slot path, concurrent (window=5ms)",
            Duration::from_millis(5),
        ),
    ] {
        let store = Arc::new(CountingStore::new());
        let catalog = open(&store, slots(window)).await;
        let before = Sample::of(&store);
        let results = futures::future::join_all(
            (0..COMMITS)
                .map(|i| catalog.commit(move |tx| tx.create_schema(&format!("s{i}")).map(|_| ()))),
        )
        .await;
        assert!(results.iter().all(Result::is_ok));
        Sample::since(&store, before).report(label, COMMITS);
        catalog.close().await.unwrap();
    }

    println!();
}
