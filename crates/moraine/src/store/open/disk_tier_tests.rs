//! What a warm disk tier is written for, through the production cache on
//! real SSTs.

use slatedb::config::{FlushOptions, FlushType};

use super::*;

/// Non-admitting scans reuse disk entries but leave cold data blocks uncached.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_admitting_scans_reuse_disk_blocks_but_do_not_fill_misses() {
    let directory = std::env::temp_dir().join(format!("moraine-scan-tier-{}", Uuid::new_v4()));
    let cache = cache::TestCache::new(64 * 1024 * 1024, Some(directory.as_path())).await;
    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let options = StoreBuilder::new("scan-tier", Arc::clone(&objects)).cache_puts(false);
    let db = Db::builder("scan-tier", objects)
        .with_settings(Settings {
            compactor_options: None,
            ..options.settings()
        })
        .with_sst_block_size(SST_BLOCK_SIZE)
        .with_block_cache_policy(options.block_cache_policy())
        .with_db_cache(cache.handle.clone(), 1)
        .with_metrics_recorder(cache::recorder(Arc::clone(&cache.counters)))
        .build()
        .await
        .unwrap();
    let mut batch = slatedb::WriteBatch::new();
    for key in 0_u64..4096 {
        batch.put(key.to_be_bytes(), [7_u8; 64]);
    }
    db.write(batch).await.unwrap();
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();

    for (pass, admit) in [false, false, true, false].into_iter().enumerate() {
        if pass == 3 {
            settled_disk_writes(&cache).await;
            cache.resize(0);
            cache.resize(64 * 1024 * 1024);
        }
        let before = cache.counters.tally();
        let mut scan = db
            .scan_with_options(
                ..,
                &slatedb::config::ScanOptions {
                    cache_blocks: admit,
                    read_ahead_bytes: 256 * 1024,
                    max_fetch_tasks: 2,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut rows = 0;
        while scan.next().await.unwrap().is_some() {
            rows += 1;
        }
        assert_eq!(rows, 4096);
        let served = cache.counters.tally().since(before);
        eprintln!("scan {pass}, admit={admit}: {served:?}");
        if pass < 3 {
            assert!(served.block_misses > 0, "cold blocks must reach storage");
            assert_eq!(served.block_hits, 0, "earlier scans admitted nothing");
        } else {
            assert_eq!(
                served.block_misses, 0,
                "the admitted blocks survived on disk"
            );
            assert!(
                served.block_disk_hits > 0,
                "the scan must consult the disk tier"
            );
        }
    }
    db.close().await.unwrap();
    let _ = std::fs::remove_dir_all(directory);
}

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

/// A filter the disk tier answers counts as a metadata disk hit, not a
/// block one. The probe window reads these to tell a resident filter from
/// one it had to fetch and decode.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filter_read_back_from_disk_counts_as_metadata() {
    let directory = std::env::temp_dir().join(format!("moraine-filter-tier-{}", Uuid::new_v4()));
    let cache = cache::TestCache::new(64 * 1024 * 1024, Some(directory.as_path())).await;

    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let options = StoreBuilder::new("filter-tier", Arc::clone(&objects));
    let db = Db::builder("filter-tier", Arc::clone(&objects))
        .with_settings(options.settings())
        .with_sst_block_size(SST_BLOCK_SIZE)
        .with_block_cache_policy(options.block_cache_policy())
        .with_db_cache(cache.handle.clone(), 1)
        .with_metrics_recorder(cache::recorder(Arc::clone(&cache.counters)))
        .build()
        .await
        .unwrap();

    // Many SSTs, so their filters together outweigh what the metadata
    // pool defends once the budget shrinks.
    for sst in 0_u64..12 {
        let base = sst * 100_000;
        for key in (base..base + 8192).step_by(2) {
            db.put(key.to_be_bytes(), [7_u8; 64]).await.unwrap();
        }
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
    }
    for sst in 0_u64..12 {
        let base = sst * 100_000;
        for key in (base + 1..base + 64).step_by(2) {
            assert!(db.get(key.to_be_bytes()).await.unwrap().is_none());
        }
    }

    // Evicted from memory, so the next lookup must come off the device.
    cache.resize(32 * 1024);

    let before = cache.counters.tally();
    for sst in 0_u64..12 {
        let base = sst * 100_000;
        for key in (base + 1025..base + 1101).step_by(2) {
            assert!(db.get(key.to_be_bytes()).await.unwrap().is_none());
        }
    }
    let served = cache.counters.tally().since(before);

    assert!(
        served.metadata_disk_hits > 0,
        "a filter off the device is a metadata disk hit: {served:?}"
    );
    assert_eq!(
        served.metadata_misses, 0,
        "the device answered, so nothing went to the object store: {served:?}"
    );

    db.close().await.unwrap();
    let _ = std::fs::remove_dir_all(&directory);
}
