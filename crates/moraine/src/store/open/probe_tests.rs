//! Cold and warm non-unique probes against real, overlapping SSTs.

use std::time::Instant;

use futures::{StreamExt, stream};
use object_store::memory::InMemory;
use slatedb::{
    BloomFilterPolicy, WriteBatch,
    config::{FlushOptions, FlushType},
};

use super::*;
use crate::store::{
    index_encoding::{CanonicalKey, IndexKeyValue, IntWidth, encode_key},
    key::{IndexKey, Key},
};

fn value(number: u64) -> CanonicalKey {
    encode_key(&[IndexKeyValue::UInt {
        value: u128::from(number),
        width: IntWidth::I64,
    }])
    .unwrap()
}

async fn probe(reader: ReadHandle<'_>, keys: &[CanonicalKey]) -> Vec<Vec<u64>> {
    stream::iter(keys.iter().map(|key| async move {
        crate::transaction::index_maintenance::lookup_row_ids(reader, 1, false, key)
            .await
            .unwrap()
    }))
    .buffered(192)
    .collect()
    .await
}

async fn fixture(
    prefix_filters: bool,
    cache_puts: bool,
) -> (Db, Arc<cache::CacheCounters>, Arc<dyn ObjectStore>) {
    fixture_with_cache(prefix_filters, cache_puts, None).await
}

async fn fixture_with_cache(
    prefix_filters: bool,
    cache_puts: bool,
    isolated: Option<Arc<dyn slatedb::db_cache::DbCache>>,
) -> (Db, Arc<cache::CacheCounters>, Arc<dyn ObjectStore>) {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let options = StoreBuilder::new("probe", objects.clone()).cache_puts(cache_puts);
    let settings = Settings {
        manifest_poll_interval: std::time::Duration::from_secs(60),
        compactor_options: None,
        l0_max_ssts: 32,
        l0_max_ssts_per_key: 32,
        ..options.settings()
    };
    let counters = cache::store_counters();
    let policies: Vec<Arc<dyn slatedb::FilterPolicy>> = if prefix_filters {
        crate::store::index_filter::policies()
    } else {
        vec![Arc::new(BloomFilterPolicy::new(10))]
    };
    let db = Db::builder("probe", objects.clone())
        .with_settings(settings)
        .with_sst_block_size(SST_BLOCK_SIZE)
        .with_segment_extractor(Arc::new(TagSegmentExtractor))
        .with_filter_policies(policies)
        .with_block_cache_policy(options.block_cache_policy())
        .with_metrics_recorder(cache::recorder(counters.clone()))
        .with_db_cache(match isolated {
            Some(cache) => cache,
            None => cache::shared(&options.cache_config(), options.location())
                .await
                .unwrap(),
        })
        .build()
        .await
        .unwrap();
    for shard in 0..8 {
        let mut batch = WriteBatch::new();
        for number in (shard..262_144).step_by(8) {
            for row_id in [number * 2, number * 2 + 1] {
                batch.put(
                    Key::Index(IndexKey::Multi {
                        index_id: 1,
                        key: value(number),
                        row_id,
                    })
                    .encode(),
                    b"row",
                );
            }
        }
        db.write(batch).await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
    }
    (db, counters, objects)
}

/// Small and high-fanout equality prefixes retain every result with bounded
/// read-ahead.
#[tokio::test]
async fn equality_read_ahead_sweep() {
    use slatedb::config::ScanOptions;

    let cache = cache::TestCache::new(128 * 1024 * 1024, None).await;
    let (db, counters, _) = fixture_with_cache(true, false, Some(cache.handle.clone())).await;
    let mut batch = WriteBatch::new();
    for row_id in 0..131_072 {
        batch.put(
            Key::Index(IndexKey::Multi {
                index_id: 1,
                key: value(u64::MAX),
                row_id,
            })
            .encode(),
            b"row",
        );
    }
    db.write(batch).await.unwrap();
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    for (bytes, tasks) in [
        (8 * 1024 * 1024, 32),
        (4096, 1),
        (256 * 1024, 2),
        (1024 * 1024, 2),
    ] {
        for hot in [false, true] {
            cache.resize(0);
            cache.resize(128 * 1024 * 1024);
            let keys: Vec<_> = if hot {
                vec![value(u64::MAX)]
            } else {
                (0..192).map(|i| value(i * 1307 + 50)).collect()
            };
            let options = ScanOptions {
                read_ahead_bytes: bytes,
                max_fetch_tasks: tasks,
                cache_blocks: true,
                ..ScanOptions::default()
            };
            for warm in [false, true] {
                let before = counters.object_store_tally();
                let start = Instant::now();
                let counts: Vec<_> = stream::iter(keys.iter().map(|key| {
                    let db = &db;
                    let options = &options;
                    async move {
                        let prefix = crate::store::key::index_multi_value_prefix(1, key);
                        let mut scan = db
                            .scan_prefix_with_options(prefix, .., options)
                            .await
                            .unwrap();
                        let mut count = 0;
                        while let Some(entry) = scan.next().await.unwrap() {
                            assert_eq!(entry.value.as_ref(), b"row");
                            count += 1;
                        }
                        count
                    }
                }))
                .buffered(192)
                .collect()
                .await;
                assert!(
                    counts
                        .iter()
                        .all(|&count| count == if hot { 131_072 } else { 2 })
                );
                let gets = counters.object_store_tally().since(before).main_gets;
                eprintln!(
                    "equality read_ahead={bytes} tasks={tasks} hot={hot} warm={warm}: {gets} GETs {:?}",
                    start.elapsed()
                );
                if warm {
                    assert_eq!(gets, 0);
                }
            }
        }
    }
    db.close().await.unwrap();
}

/// Prefix filtering reduces cold block reads without changing duplicate-key
/// results.
#[tokio::test]
async fn equality_probe_cold_and_warm_reads() {
    let keys: Vec<_> = (0..192).map(|i| value(i * 1307 + 50)).collect();
    let expected: Vec<_> = (0..192)
        .map(|i| vec![(i * 1307 + 50) * 2, (i * 1307 + 50) * 2 + 1])
        .collect();
    let mut cold_reads = Vec::new();
    for prefix_filters in [false, true] {
        let (db, counters, objects) = fixture(prefix_filters, false).await;
        let before = counters.object_store_tally();
        let start = Instant::now();
        assert_eq!(probe(ReadHandle::Writer(&db), &keys).await, expected);
        let cold = counters.object_store_tally().since(before).main_gets;
        let cold_elapsed = start.elapsed();
        let before = counters.object_store_tally();
        let start = Instant::now();
        assert_eq!(probe(ReadHandle::Writer(&db), &keys).await, expected);
        let warm = counters.object_store_tally().since(before).main_gets;
        eprintln!(
            "192 keys, 8 overlapping SSTs, prefix_filters={prefix_filters}: cold={cold} GETs/{cold_elapsed:?}, warm={warm} GETs/{:?}",
            start.elapsed()
        );
        assert_eq!(warm, 0);
        cold_reads.push(cold);

        // The current reader must also understand legacy SSTs.
        let (reader, _) = StoreBuilder::new("probe", objects.clone())
            .open_reader()
            .await
            .unwrap();
        assert_eq!(probe(ReadHandle::Reader(&reader), &keys).await, expected);
        reader.close().await.unwrap();
        // Older readers ignore the additional named filter safely.
        let reader = DbReader::builder("probe", objects)
            .with_segment_extractor(Arc::new(TagSegmentExtractor))
            .build()
            .await
            .unwrap();
        assert_eq!(probe(ReadHandle::Reader(&reader), &keys).await, expected);
        reader.close().await.unwrap();
        db.close().await.unwrap();
    }
    assert!(
        cold_reads[1] * 3 < cold_reads[0],
        "prefix filtering did not reduce cold reads: {cold_reads:?}"
    );
}

/// Cache puts make the first probe of freshly flushed index data a cache hit.
#[tokio::test]
async fn cache_puts_warms_first_touch_data_blocks() {
    let (db, counters, _) = fixture(true, true).await;
    let keys: Vec<_> = (0..192).map(|i| value(i * 1307 + 50)).collect();
    let before = counters.object_store_tally();
    let results = probe(ReadHandle::Writer(&db), &keys).await;
    assert!(results.iter().all(|rows| rows.len() == 2));
    let reads = counters.object_store_tally().since(before).main_gets;
    eprintln!("192 first-touch keys after CACHE_PUTS flush: {reads} GETs");
    assert_eq!(reads, 0);
    let old = Key::Index(IndexKey::Multi {
        index_id: 1,
        key: keys[0].clone(),
        row_id: 100,
    })
    .encode();
    db.delete(old).await.unwrap();
    db.put(
        Key::Index(IndexKey::Multi {
            index_id: 1,
            key: keys[0].clone(),
            row_id: 900_000,
        })
        .encode(),
        b"updated",
    )
    .await
    .unwrap();
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    let before = counters.object_store_tally();
    assert_eq!(
        probe(ReadHandle::Writer(&db), &keys[..1]).await,
        vec![vec![101, 900_000]]
    );
    assert_eq!(
        counters.object_store_tally().since(before).main_gets,
        0,
        "updated blocks were not admitted"
    );
    db.close().await.unwrap();
}
