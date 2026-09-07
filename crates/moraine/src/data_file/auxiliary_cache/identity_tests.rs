use std::sync::Arc;

use object_store::{ObjectStoreExt, memory::InMemory, path::Path};

use super::{AuxiliaryCache, Tier};
use crate::{CacheIdentity, DataStore, data_file::ParquetFile};

#[tokio::test]
async fn disk_recovery_keeps_object_namespaces_separate() {
    let directory =
        std::env::temp_dir().join(format!("moraine-cache-identity-{}", uuid::Uuid::new_v4()));
    let path = Path::from("same-file");
    let first = Arc::new(InMemory::new());
    let second = Arc::new(InMemory::new());
    let objects = [first, second];
    let namespaces = ["https://one.example/bucket", "https://two.example/bucket"];
    let values = [b"left", b"rght"];
    let cache = AuxiliaryCache::hybrid(1 << 20, &directory, 64 << 20)
        .await
        .unwrap();
    for index in 0..2 {
        objects[index]
            .put(&path, values[index].to_vec().into())
            .await
            .unwrap();
        let store = DataStore::with_cache_identity(
            objects[index].clone(),
            CacheIdentity::new(namespaces[index]),
        );
        let file = ParquetFile::new(store, path.clone(), 4, 0);
        assert_eq!(
            cache.range(&file, 0..4).await.unwrap().as_ref(),
            values[index]
        );
    }
    if let Tier::Hybrid(hybrid) = &cache.tier {
        hybrid.close().await.unwrap();
    }
    drop(cache);

    // With the source objects removed, successful reads must come from disk.
    for object in &objects {
        object.delete(&path).await.unwrap();
    }
    let cache = AuxiliaryCache::hybrid(1 << 20, &directory, 64 << 20)
        .await
        .unwrap();
    for index in [1, 0] {
        let store = DataStore::with_cache_identity(
            objects[index].clone(),
            CacheIdentity::new(namespaces[index]),
        );
        let file = ParquetFile::new(store, path.clone(), 4, 0);
        assert_eq!(
            cache.range(&file, 0..4).await.unwrap().as_ref(),
            values[index]
        );
    }
    let unnamed = ParquetFile::new(DataStore::new(objects[0].clone()), path, 4, 0);
    assert!(cache.range(&unnamed, 0..4).await.is_err());
    if let Tier::Hybrid(hybrid) = &cache.tier {
        hybrid.close().await.unwrap();
    }
    drop(cache);
    std::fs::remove_dir_all(directory).unwrap();
}
