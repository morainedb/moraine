//! Experimental probe policies on real SlateDB; no production defaults change.

use std::{collections::BTreeMap, ops::Bound, time::Instant};

use futures::{StreamExt, stream};
use slatedb::{
    WriteBatch,
    config::{FlushOptions, FlushType},
};
use tokio::sync::Semaphore;

use super::*;
use crate::store::{
    handle::ScanShape,
    index_encoding::{CanonicalKey, IndexKeyValue, IntWidth, encode_key},
    key::{IndexKey, IndexKind, Key, index_index_prefix, index_value_above, index_value_suffix},
};

const FANOUT: u64 = 17;
const CACHE_BYTES: u64 = 128 * 1024 * 1024;

mod measure;
mod read_batch;
mod transport;

fn value(number: u64) -> CanonicalKey {
    encode_key(&[IndexKeyValue::UInt {
        value: u128::from(number),
        width: IntWidth::I64,
    }])
    .unwrap()
}

async fn fixture(keys: u64) -> (Db, cache::TestCache, Arc<transport::Transport>) {
    let objects = transport::Transport::new();
    let cache = cache::TestCache::new(CACHE_BYTES, None).await;
    let options = StoreBuilder::new("probe-policy", objects.clone()).cache_puts(false);
    let db = Db::builder("probe-policy", read_batch::ReadBatch::new(objects.clone()))
        .with_settings(Settings {
            compactor_options: None,
            l0_max_ssts: 32,
            l0_max_ssts_per_key: 32,
            manifest_poll_interval: Duration::from_secs(3600),
            ..options.settings()
        })
        .with_sst_block_size(SST_BLOCK_SIZE)
        .with_segment_extractor(Arc::new(TagSegmentExtractor))
        .with_filter_policies(crate::store::index_filter::policies())
        .with_block_cache_policy(options.block_cache_policy())
        .with_db_cache(cache.handle.clone())
        .build()
        .await
        .unwrap();
    for shard in 0..8 {
        let mut batch = WriteBatch::new();
        for number in (shard..keys).step_by(8) {
            for offset in 0..FANOUT {
                batch.put(
                    Key::Index(IndexKey::Multi {
                        index_id: 1,
                        key: value(number),
                        row_id: number * FANOUT + offset,
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
    (db, cache, objects)
}

fn groups(numbers: &[u64], group_size: usize) -> Vec<Vec<CanonicalKey>> {
    let mut buckets = BTreeMap::<u64, Vec<CanonicalKey>>::new();
    for &number in numbers {
        // Numeric proximity is only a prototype for physical block locality.
        buckets
            .entry(if group_size > 1 { number / 256 } else { number })
            .or_default()
            .push(value(number));
    }
    buckets
        .into_values()
        .flat_map(|mut keys| {
            keys.sort_unstable();
            keys.dedup();
            keys.chunks(group_size)
                .map(<[CanonicalKey]>::to_vec)
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn scan_group(db: &Db, keys: &[CanonicalKey]) -> Vec<u64> {
    if keys.len() == 1 {
        return crate::transaction::index_maintenance::lookup_row_ids(
            ReadHandle::Writer(db),
            1,
            false,
            &keys[0],
        )
        .await
        .unwrap();
    }
    let start = Bound::Included(index_value_suffix(IndexKind::Multi, 1, &keys[0]));
    let end =
        Bound::Excluded(index_value_above(IndexKind::Multi, 1, keys.last().unwrap()).unwrap());
    let mut scan = ReadHandle::Writer(db)
        .scan_prefix(
            index_index_prefix(IndexKind::Multi, 1),
            (start, end),
            ScanShape::Equality,
        )
        .await
        .unwrap();
    let mut rows = Vec::new();
    while let Some(entry) = scan.next().await.unwrap() {
        let Key::Index(IndexKey::Multi { key, row_id, .. }) = Key::decode(&entry.key).unwrap()
        else {
            panic!("unexpected index entry");
        };
        if keys.binary_search(&key).is_ok() {
            rows.push(row_id);
        }
    }
    rows
}

async fn lookup(
    db: &Db,
    numbers: &[u64],
    group_size: usize,
    limit: Option<Arc<Semaphore>>,
) -> Vec<u64> {
    let batches: Vec<_> = stream::iter(groups(numbers, group_size).into_iter().map(|keys| {
        let limit = limit.clone();
        async move {
            let _permit = match &limit {
                Some(limit) => Some(limit.acquire().await.unwrap()),
                None => None,
            };
            scan_group(db, &keys).await
        }
    }))
    .buffer_unordered(512)
    .collect()
    .await;
    let mut rows: Vec<_> = batches.into_iter().flatten().collect();
    rows.sort_unstable();
    rows.dedup();
    rows
}

/// Grouped scans preserve sparse membership, duplicate input and absent keys.
#[tokio::test]
async fn grouped_probe_results_are_exact() {
    let (db, _, _) = fixture(512).await;
    let numbers = [1, 3, 4, 16, 254, 255, 256, 257, 511, 512, 700, 3];
    let mut expected: Vec<_> = numbers
        .iter()
        .filter(|&&number| number < 512)
        .flat_map(|number| number * FANOUT..(number + 1) * FANOUT)
        .collect();
    expected.sort_unstable();
    expected.dedup();
    for group_size in [1, 16, 64, 192] {
        assert_eq!(
            lookup(&db, &numbers, group_size, Some(Arc::new(Semaphore::new(2)))).await,
            expected
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn exact_batched_reads_preserve_pinned_deletes_and_updates() {
    let (db, cache, objects) = fixture(512).await;
    let pinned = db.begin(slatedb::IsolationLevel::Snapshot).await.unwrap();
    let key = |row_id| {
        Key::Index(IndexKey::Multi {
            index_id: 1,
            key: value(7),
            row_id,
        })
        .encode()
    };
    let old = 7 * FANOUT;
    db.delete(key(old)).await.unwrap();
    db.put(key(90_000), b"new").await.unwrap();
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    cache.resize(0);
    cache.resize(CACHE_BYTES);
    objects
        .coalescing
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let key = value(7);
    let (historical, current) = tokio::join!(
        crate::transaction::index_maintenance::lookup_row_ids(
            ReadHandle::Tx(&pinned),
            1,
            false,
            &key
        ),
        crate::transaction::index_maintenance::lookup_row_ids(
            ReadHandle::Writer(&db),
            1,
            false,
            &key
        )
    );
    assert_eq!(historical.unwrap(), (old..old + FANOUT).collect::<Vec<_>>());
    let mut expected: Vec<_> = (old + 1..old + FANOUT).collect();
    expected.push(90_000);
    assert_eq!(current.unwrap(), expected);
    pinned.rollback();
    objects.settle().await;
    db.close().await.unwrap();
}
