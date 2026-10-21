//! Byte-bounded retention for file and inline lookup directories.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::catalog::TableId;

/// Table directories kept per source kind.
const DIRECTORY_CAPACITY: usize = 256;
const DIRECTORY_BYTES: u64 = 64 * 1024 * 1024;

pub(super) trait Directory {
    fn retained_bytes(&self) -> u64;
}

/// Byte- and count-bounded directories, evicted by least recent use.
pub(super) struct Directories<T> {
    entries: HashMap<TableId, Held<T>>,
    clock: AtomicU64,
    bytes: u64,
    capacity: u64,
}

struct Held<T> {
    directory: Arc<T>,
    used: AtomicU64,
    bytes: u64,
}

impl<T: Directory> Default for Directories<T> {
    fn default() -> Self {
        Self::new(DIRECTORY_BYTES)
    }
}

impl<T: Directory> Directories<T> {
    pub(super) fn new(capacity: u64) -> Self {
        Self {
            entries: HashMap::new(),
            clock: AtomicU64::new(0),
            bytes: 0,
            capacity,
        }
    }

    pub(super) fn get(&self, table: TableId) -> Option<Arc<T>> {
        let held = self.entries.get(&table)?;
        held.used.store(
            self.clock.fetch_add(1, Ordering::Relaxed),
            Ordering::Relaxed,
        );
        Some(Arc::clone(&held.directory))
    }

    fn remove(&mut self, table: TableId) {
        if let Some(held) = self.entries.remove(&table) {
            self.bytes -= held.bytes;
        }
    }

    pub(super) fn insert(&mut self, table: TableId, directory: Arc<T>) {
        self.remove(table);
        let Some(bytes) = directory
            .retained_bytes()
            .checked_add(size_of::<(TableId, Held<T>)>() as u64)
        else {
            return;
        };
        if bytes > self.capacity {
            return;
        }
        while self.entries.len() >= DIRECTORY_CAPACITY || self.bytes > self.capacity - bytes {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, held)| held.used.load(Ordering::Relaxed))
                .map(|(table, _)| *table)
            else {
                break;
            };
            self.remove(oldest);
        }
        let used = AtomicU64::new(self.clock.fetch_add(1, Ordering::Relaxed));
        self.entries.insert(
            table,
            Held {
                directory,
                used,
                bytes,
            },
        );
        self.bytes += bytes;
    }

    pub(super) fn estimated_bytes(&self) -> u64 {
        let spare = self.entries.capacity() - self.entries.len();
        self.bytes
            .saturating_add((spare * size_of::<(TableId, Held<T>)>()) as u64)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use super::{
        super::{install, lookup},
        DIRECTORY_CAPACITY, Directories,
    };
    use crate::catalog::TableId;

    impl super::Directory for u64 {
        fn retained_bytes(&self) -> u64 {
            *self
        }
    }

    impl super::Directory for usize {
        fn retained_bytes(&self) -> u64 {
            *self as u64
        }
    }

    #[test]
    fn an_oversized_directory_is_not_retained() {
        let mut directories = Directories::default();
        directories.insert(TableId::new(1), Arc::new(64 * 1024 * 1024 + 1_u64));
        assert!(directories.get(TableId::new(1)).is_none());
    }

    #[test]
    fn byte_pressure_evicts_oldest_entries_but_preserves_readers() {
        let header = size_of::<(TableId, super::Held<u64>)>() as u64;
        let mut cache: Directories<u64> = Directories::new(3 * (header + 10));
        for table in 1..=3 {
            cache.insert(TableId::new(table), Arc::new(10));
        }
        let reader = cache.get(TableId::new(2)).unwrap();
        let weak = Arc::downgrade(&reader);
        cache.get(TableId::new(3));
        cache.get(TableId::new(1));
        cache.insert(TableId::new(4), Arc::new(30));

        assert!(cache.get(TableId::new(1)).is_some());
        assert!(cache.get(TableId::new(2)).is_none());
        assert!(cache.get(TableId::new(3)).is_none());
        assert_eq!(cache.get(TableId::new(4)).as_deref(), Some(&30));
        assert!(cache.bytes <= cache.capacity);
        assert_eq!(*reader, 10);
        drop(reader);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn replacement_releases_its_old_charge_even_when_oversized() {
        let header = size_of::<(TableId, super::Held<u64>)>() as u64;
        let capacity = 2 * (header + 10);
        let mut cache: Directories<u64> = Directories::new(capacity);
        cache.insert(TableId::new(1), Arc::new(10));
        cache.insert(TableId::new(2), Arc::new(10));
        cache.insert(TableId::new(1), Arc::new(1));
        assert_eq!(cache.bytes, 2 * header + 11);

        cache.insert(TableId::new(1), Arc::new(capacity + 1));
        assert!(cache.get(TableId::new(1)).is_none());
        assert_eq!(cache.get(TableId::new(2)).as_deref(), Some(&10));
        assert_eq!(cache.bytes, header + 10);
    }

    #[test]
    fn zero_capacity_and_overflowing_weights_are_not_admitted() {
        let mut cache: Directories<u64> = Directories::new(0);
        cache.insert(TableId::new(1), Arc::new(0_u64));
        assert!(cache.get(TableId::new(1)).is_none());
        let mut cache: Directories<u64> = Directories::new(u64::MAX);
        cache.insert(TableId::new(1), Arc::new(u64::MAX));
        assert!(cache.get(TableId::new(1)).is_none());
    }

    /// Past capacity, the directory unused for longest is the one evicted.
    #[test]
    fn eviction_drops_the_least_recently_used_directory() {
        let cache = RwLock::new(Directories::default());
        let table = |id: usize| TableId::new(u64::try_from(id).unwrap());
        for id in 0..DIRECTORY_CAPACITY {
            install(&cache, table(id), Arc::new(id));
        }

        assert!(lookup(&cache, table(0)).is_some());
        install(
            &cache,
            table(DIRECTORY_CAPACITY),
            Arc::new(DIRECTORY_CAPACITY),
        );

        assert_eq!(cache.read().unwrap().len(), DIRECTORY_CAPACITY);
        assert!(
            lookup(&cache, table(0)).is_some(),
            "the directory just used was evicted"
        );
        assert!(
            lookup(&cache, table(1)).is_none(),
            "the directory unused for longest survived"
        );
        assert!(lookup(&cache, table(DIRECTORY_CAPACITY)).is_some());
    }
}
