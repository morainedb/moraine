//! Compaction admission and eviction through the production cache on real SSTs.

use slatedb::{
    WriteBatch,
    config::{CompactorOptions, FlushOptions, FlushType},
};

use super::*;

async fn read_rows(db: &Db) {
    for key in (0_u64..8192).step_by(43) {
        assert_eq!(
            db.get(key.to_be_bytes()).await.unwrap().unwrap().as_ref(),
            &[2; 128]
        );
    }
    assert!(db.get(8192_u64.to_be_bytes()).await.unwrap().is_none());
}

async fn compacted_fixture(
    admission: bool,
    cache: &cache::TestCache,
) -> (Db, Arc<cache::CacheCounters>) {
    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let options = StoreBuilder::new("compaction-cache", objects.clone())
        .cache_puts(false)
        .cache_compaction_puts(admission);
    let settings = Settings {
        compactor_options: Some(CompactorOptions {
            poll_interval: Duration::from_millis(20),
            scheduler_options: [("min_compaction_sources".into(), "2".into())].into(),
            ..CompactorOptions::default()
        }),
        ..options.settings()
    };
    let counters = cache::store_counters();
    let db = Db::builder("compaction-cache", objects.clone())
        .with_settings(settings)
        .with_sst_block_size(SST_BLOCK_SIZE)
        .with_block_cache_policy(options.block_cache_policy())
        .with_db_cache(cache.handle.clone())
        .with_metrics_recorder(cache::recorder(counters.clone()))
        .build()
        .await
        .unwrap();
    for version in [1_u8, 2] {
        let mut batch = WriteBatch::new();
        for key in 0_u64..8193 {
            if version == 2 && key == 8192 {
                batch.delete(key.to_be_bytes());
            } else {
                batch.put(key.to_be_bytes(), [version; 128]);
            }
        }
        db.write(batch).await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
    }
    let admin = AdminBuilder::new("compaction-cache", objects).build();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(compactions) = admin.read_compactions(None).await.unwrap()
                && compactions
                    .recent_compactions()
                    .any(|job| !job.output_ssts().is_empty() && !job.active())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    db.refresh_manifest().await.unwrap();
    (db, counters)
}

/// Compaction admission warms rewritten data independently of flush admission;
/// a reduced memory budget evicts it without changing update/delete visibility.
#[tokio::test]
async fn compaction_admission_and_eviction() {
    for disk in [false, true] {
        let directory =
            std::env::temp_dir().join(format!("moraine-compaction-cache-{}", Uuid::new_v4()));
        for admission in [false, true] {
            let path = directory.join(admission.to_string());
            let cache =
                cache::TestCache::new(16 * 1024 * 1024, disk.then_some(path.as_path())).await;
            let (db, counters) = compacted_fixture(admission, &cache).await;
            let before = counters.object_store_tally();
            let before_cache = counters.tally();
            read_rows(&db).await;
            let reads = counters.object_store_tally().since(before).main_gets;
            let cache_reads = counters.tally().since(before_cache);
            eprintln!(
                "compaction disk={disk} admission={admission}: first-touch {reads} GETs, {cache_reads:?}"
            );
            // Compactor manifest polling also contributes object-store GETs.
            assert_eq!(
                cache_reads.metadata_misses + cache_reads.block_misses == 0,
                admission
            );

            cache.resize(512 * 1024);
            assert!(
                cache.usage() <= 512 * 1024,
                "resize did not evict resident entries: {}",
                cache.usage()
            );
            let before = counters.object_store_tally();
            let before_cache = counters.tally();
            read_rows(&db).await;
            let reads = counters.object_store_tally().since(before).main_gets;
            assert!(
                cache.usage() <= 512 * 1024,
                "memory admission exceeded its resized budget: {}",
                cache.usage()
            );
            if !disk {
                assert!(
                    counters.tally().since(before_cache).block_misses > 0,
                    "admitted blocks never evicted"
                );
            }
            eprintln!(
                "after eviction disk={disk} admission={admission}: {reads} GETs, {} resident bytes",
                cache.usage()
            );
            db.close().await.unwrap();
        }
        if disk {
            std::fs::remove_dir_all(directory).unwrap();
        }
    }
}
