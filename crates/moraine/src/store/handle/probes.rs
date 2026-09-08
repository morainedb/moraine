//! Batched point reads within one transaction.

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use slatedb::DbTransaction;

/// Resolves keys in input order, including duplicates, at the transaction's
/// cut. Callers must bound transaction-local writes in the key span because
/// SlateDB copies them when opening the scan.
pub(crate) async fn get_many(
    transaction: &DbTransaction,
    keys: &[Bytes],
) -> Result<Vec<Option<Bytes>>, slatedb::Error> {
    match keys {
        [] => return Ok(Vec::new()),
        [key] => return Ok(vec![transaction.get(key).await?]),
        _ => {}
    }
    let mut ordered: Vec<_> = keys.iter().enumerate().collect();
    ordered.sort_unstable_by_key(|(_, key)| *key);
    let mut values = vec![None; keys.len()];
    if let (Some((_, first)), Some((_, last))) = (ordered.first(), ordered.last()) {
        let mut iterator = transaction
            .scan_with_options(
                first.as_ref()..=last.as_ref(),
                &slatedb::config::ScanOptions {
                    cache_blocks: true,
                    ..Default::default()
                },
            )
            .await?;
        let mut row = iterator.next().await?;
        let mut remaining = ordered.into_iter();
        while let Some((position, key)) = remaining.next() {
            if row.as_ref().is_some_and(|row| row.key < *key) {
                row = iterator.next().await?;
                if row.as_ref().is_some_and(|row| row.key < *key) {
                    // A gap should not serialize remote seeks; resolve the rest in parallel.
                    drop(iterator);
                    let point_reads = std::iter::once((position, key)).chain(remaining);
                    let reads: Vec<_> = point_reads
                        .map(|(position, key)| async move {
                            transaction.get(key).await.map(|value| (position, value))
                        })
                        .collect();
                    let found: Vec<_> = stream::iter(reads)
                        .buffer_unordered(128)
                        .try_collect()
                        .await?;
                    for (position, value) in found {
                        values[position] = value;
                    }
                    break;
                }
            }
            if let Some(row) = &row
                && row.key == *key
            {
                values[position] = Some(row.value.clone());
            }
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::IsolationLevel;

    use super::*;
    use crate::store::open::StoreBuilder;

    #[tokio::test]
    async fn batches_match_point_reads_with_local_writes_and_later_commits() {
        let store = Arc::new(InMemory::new());
        let (db, _) = StoreBuilder::new("batch-probes", store.clone())
            .open_writer()
            .await
            .unwrap();
        let seed = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for id in 0..2048u64 {
            seed.put(id.to_be_bytes(), id.to_be_bytes()).unwrap();
        }
        seed.commit().await.unwrap();
        db.close().await.unwrap();
        let (db, _) = StoreBuilder::new("batch-probes", store)
            .open_writer()
            .await
            .unwrap();
        let held = db.begin(IsolationLevel::Snapshot).await.unwrap();
        held.delete(2u64.to_be_bytes()).unwrap();
        held.put(3u64.to_be_bytes(), b"local").unwrap();
        held.put(3000u64.to_be_bytes(), b"new").unwrap();
        let later = db.begin(IsolationLevel::Snapshot).await.unwrap();
        later.put(4u64.to_be_bytes(), b"later").unwrap();
        later.delete(5u64.to_be_bytes()).unwrap();
        later.commit().await.unwrap();
        let keys: Vec<_> = [3000u64, 3, 2, 5, 3, 2047, 4000, 0, 4, 3001, 1]
            .into_iter()
            .map(|key| Bytes::copy_from_slice(&key.to_be_bytes()))
            .collect();
        let values = get_many(&held, &keys).await.unwrap();
        for (key, value) in keys.iter().zip(&values) {
            assert_eq!(*value, held.get(key).await.unwrap());
        }
        assert_eq!(values[0].as_deref(), Some(b"new".as_slice()));
        assert_eq!(values[1].as_deref(), Some(b"local".as_slice()));
        assert!(values[2].is_none());
        assert_eq!(get_many(&held, &[]).await.unwrap(), vec![]);
        assert_eq!(get_many(&held, &keys[..1]).await.unwrap(), values[..1]);
        held.rollback();
        db.close().await.unwrap();
    }
}
