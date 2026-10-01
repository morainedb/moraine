//! What a warm disk tier is written for, through the production cache on
//! real SSTs.

use slatedb::config::{FlushOptions, FlushType};

use super::*;

/// Polls until the disk tier's write counter stops moving, so a later
/// sample is not racing the flusher finishing earlier work.
async fn settled_disk_writes(cache: &cache::TestCache) -> usize {
    let mut stable = 0;
    let mut last = cache.disk_write_bytes().unwrap();
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let now = cache.disk_write_bytes().unwrap();
        if now == last {
            stable += 1;
            if stable == 4 {
                return now;
            }
        } else {
            stable = 0;
            last = now;
        }
    }
    panic!("the disk tier never stopped writing");
}

/// Absent-key lookups the memory tier answers never rewrite the disk
/// tier. foyer sizes a disk write by serializing the whole entry on the
/// caller's task, which for an SST filter is megabytes a lookup.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lookups_served_from_memory_never_rewrite_the_disk_tier() {
    let directory = std::env::temp_dir().join(format!("moraine-disk-tier-{}", Uuid::new_v4()));
    let cache = cache::TestCache::new(64 * 1024 * 1024, Some(directory.as_path())).await;

    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let options = StoreBuilder::new("disk-tier", Arc::clone(&objects));
    let db = Db::builder("disk-tier", Arc::clone(&objects))
        .with_settings(options.settings())
        .with_sst_block_size(SST_BLOCK_SIZE)
        .with_block_cache_policy(options.block_cache_policy())
        .with_db_cache(cache.handle.clone(), 1)
        .with_metrics_recorder(cache::recorder(Arc::clone(&cache.counters)))
        .build()
        .await
        .unwrap();

    // Only even keys, so an odd one is absent but inside the SST's key
    // range: a key outside it is refused by the range check and never
    // reaches a filter.
    for key in (0_u64..8192).step_by(2) {
        db.put(key.to_be_bytes(), [7_u8; 64]).await.unwrap();
    }
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();

    // Warm every filter, then let the fills' own writes drain.
    for key in (1_u64..64).step_by(2) {
        assert!(db.get(key.to_be_bytes()).await.unwrap().is_none());
    }
    let warm = settled_disk_writes(&cache).await;
    assert!(warm > 0, "the disk tier was never written at all");

    let before = cache.counters.tally();
    for key in (1025_u64..1537).step_by(2) {
        assert!(db.get(key.to_be_bytes()).await.unwrap().is_none());
    }
    let served = cache.counters.tally().since(before);
    assert!(
        served.metadata_hits >= 256,
        "the lookups never reached a filter: {served:?}"
    );
    assert_eq!(served.metadata_misses, 0, "the probes were not warm");
    assert_eq!(served.metadata_disk_hits, 0, "the probes left memory");

    assert_eq!(
        settled_disk_writes(&cache).await,
        warm,
        "lookups the memory tier answered rewrote the disk tier"
    );

    db.close().await.unwrap();
    let _ = std::fs::remove_dir_all(&directory);
}
