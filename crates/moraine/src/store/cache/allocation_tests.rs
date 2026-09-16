//! Demand-driven cache allocation through real flushed SlateDB stores.

use slatedb::{
    BlockCachePolicy, CacheTarget, Db, WriteBatch,
    config::{FlushOptions, FlushType, Settings},
};

use super::*;

const CHILD: &str = "MORAINE_CACHE_ALLOCATION_CHILD";

async fn probe(db: &Db, counters: &CacheCounters, version: u8, label: &str) -> u64 {
    let before = counters.object_store_tally();
    let before_cache = counters.tally();
    let started = std::time::Instant::now();
    for key in (0_u64..16384).step_by(17) {
        assert_eq!(
            db.get(key.to_be_bytes()).await.unwrap().unwrap().as_ref(),
            &[version; 128]
        );
    }
    let reads = counters.object_store_tally().since(before).main_gets;
    eprintln!(
        "{label}: {reads} GETs, {:?}, {:?}",
        started.elapsed(),
        counters.tally().since(before_cache)
    );
    reads
}

async fn write(db: &Db, version: u8) {
    let mut batch = WriteBatch::new();
    for key in 0_u64..16384 {
        batch.put(key.to_be_bytes(), [version; 128]);
    }
    db.write(batch).await.unwrap();
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
}

fn assert_budget() {
    let caches = caches();
    let budget = caches.config.as_ref().unwrap().slots().1;
    let capacity: u64 = caches
        .stores
        .values()
        .map(|store| store.capacity.load(Ordering::Relaxed))
        .sum();
    let occupancy: usize = caches.stores.values().map(|store| store.tier.usage()).sum();
    assert!(capacity <= budget, "allocated {capacity}, budget {budget}");
    assert!(
        as_bytes(occupancy) <= budget,
        "occupied {occupancy}, budget {budget}"
    );
}

async fn scenario(disk: bool, admission: bool, root: &Path) {
    let config = CacheConfig {
        memory: Some(8 * 1024 * 1024),
        dir: disk.then(|| root.to_owned()),
        disk_size: Some(64 * 1024 * 1024),
    };
    let objects = Arc::new(object_store::memory::InMemory::new());
    let identity = crate::CacheIdentity::new("allocation-test");
    let location = |path: &str| StoreLocation {
        identity,
        path: path.to_owned(),
    };
    let settings = Settings {
        flush_interval: None,
        compactor_options: None,
        // Keep background manifest reads outside the measured probe windows.
        manifest_poll_interval: Duration::from_secs(60),
        ..Settings::default()
    };
    let cache = shared(&config, location("active"), store_counters())
        .await
        .unwrap();
    let counters = store_counters();
    let db = Db::builder("active", objects.clone())
        .with_settings(settings.clone())
        .with_block_cache_policy(flush_policy(admission))
        .with_db_cache(cache)
        .with_metrics_recorder(recorder(counters.clone()))
        .build()
        .await
        .unwrap();
    write(&db, 1).await;
    probe(&db, &counters, 1, "initial").await;
    assert_eq!(probe(&db, &counters, 1, "warm alone").await, 0);

    let mut idle = Vec::new();
    for number in 0..8 {
        let path = format!("idle-{number}");
        let counters = store_counters();
        idle.push((
            Db::builder(path.as_str(), objects.clone())
                .with_settings(settings.clone())
                .with_db_cache(
                    shared(&config, location(&path), store_counters())
                        .await
                        .unwrap(),
                )
                .with_metrics_recorder(recorder(counters.clone()))
                .build()
                .await
                .unwrap(),
            counters,
        ));
        assert_budget();
    }
    assert_eq!(
        probe(&db, &counters, 1, "warm with eight idle stores").await,
        0,
        "empty attaches evicted the active working set"
    );

    write(&db, 2).await;
    let first_updated = probe(&db, &counters, 2, "updated first touch").await;
    if admission {
        assert_eq!(first_updated, 0, "flush admission lost updated blocks");
    }
    assert_eq!(probe(&db, &counters, 2, "updated warm").await, 0);
    write(&idle[0].0, 3).await;
    probe(&idle[0].0, &idle[0].1, 3, "newly busy first touch").await;
    assert_eq!(probe(&idle[0].0, &idle[0].1, 3, "newly busy warm").await, 0);
    probe(&db, &counters, 2, "original borrower refill").await;
    assert_eq!(probe(&db, &counters, 2, "two busy stores warm").await, 0);
    assert_budget();

    futures::future::join_all(idle[1..].iter().map(|(writer, _)| write(writer, 4))).await;
    assert_budget();
    let eviction_reads = probe(&db, &counters, 2, "all stores busy").await;
    if !disk {
        assert!(
            eviction_reads > 0,
            "oversubscribed stores never evicted blocks"
        );
    }
    assert_budget();
    let status = cache_status();
    assert!(
        status.metadata_occupancy_bytes + status.block_occupancy_bytes
            <= status.block_capacity_bytes,
        "{status:?}"
    );
    for (db, _) in idle {
        db.close().await.unwrap();
    }
    db.close().await.unwrap();
    drop(db);
    assert_budget();
    assert!(
        caches()
            .stores
            .values()
            .all(|store| store.capacity.load(Ordering::Relaxed) == 0)
    );
}

fn flush_policy(admission: bool) -> BlockCachePolicy {
    let targets = [
        CacheTarget::data::<&[u8], _>(..),
        CacheTarget::Index,
        CacheTarget::Filters,
        CacheTarget::Stats,
    ];
    BlockCachePolicy::default().with_flush_targets(if admission { &targets } else { &[] })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_attaches_do_not_evict_an_active_working_set() {
    if let Ok(mode) = std::env::var(CHILD) {
        let root = PathBuf::from(std::env::var_os("MORAINE_CACHE_ALLOCATION_ROOT").unwrap());
        scenario(mode.contains("disk"), mode.contains("puts"), &root).await;
        return;
    }
    let root = std::env::temp_dir().join(format!("moraine-allocation-{}", uuid::Uuid::new_v4()));
    let name = "store::cache::allocation_tests::idle_attaches_do_not_evict_an_active_working_set";
    for mode in ["memory-reads", "memory-puts", "disk-reads", "disk-puts"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"])
            .env(CHILD, mode)
            .env("MORAINE_CACHE_ALLOCATION_ROOT", root.join(mode))
            .output()
            .unwrap();
        eprintln!("{mode}:\n{}", String::from_utf8_lossy(&output.stderr));
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}
