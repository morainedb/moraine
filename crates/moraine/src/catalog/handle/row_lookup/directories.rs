//! Byte-bounded retention for file and inline lookup directories.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{
    catalog::TableId,
    store::cache::retention::{Budget, Charge, Shared, hash_bytes},
};

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
    budget: Arc<Budget>,
    storage: Charge,
}

struct Held<T> {
    directory: Shared<T>,
    used: AtomicU64,
    bytes: u64,
}

#[cfg(test)]
impl<T: Directory> Default for Directories<T> {
    fn default() -> Self {
        Self::new(DIRECTORY_BYTES)
    }
}

impl<T: Directory> Directories<T> {
    #[cfg(test)]
    pub(super) fn new(capacity: u64) -> Self {
        Self::with_budget(capacity, Budget::new(u64::MAX))
    }

    pub(super) fn shared() -> Self {
        Self::with_budget(DIRECTORY_BYTES, crate::store::cache::lookup_budget())
    }

    fn with_budget(capacity: u64, budget: Arc<Budget>) -> Self {
        Self {
            entries: HashMap::new(),
            clock: AtomicU64::new(0),
            bytes: 0,
            capacity,
            storage: Charge::new(Arc::clone(&budget)),
            budget,
        }
    }

    pub(super) fn get(&self, table: TableId) -> Option<Shared<T>> {
        let held = self.entries.get(&table)?;
        held.used.store(
            self.clock.fetch_add(1, Ordering::Relaxed),
            Ordering::Relaxed,
        );
        Some(held.directory.clone())
    }

    fn remove(&mut self, table: TableId) {
        if let Some(held) = self.entries.remove(&table) {
            self.bytes -= held.bytes;
        }
    }

    pub(super) fn insert(&mut self, table: TableId, directory: Shared<T>) {
        self.remove(table);
        let Some(bytes) = directory
            .retained_bytes()
            .checked_add(Shared::<T>::overhead() as u64)
            .and_then(|bytes| bytes.checked_add(size_of::<(TableId, Held<T>)>() as u64))
        else {
            return;
        };
        if bytes > self.capacity {
            return;
        }
        while self.entries.len() >= DIRECTORY_CAPACITY
            || self.bytes > self.capacity - bytes
            || !directory.try_charge(&self.budget, bytes - size_of::<(TableId, Held<T>)>() as u64)
        {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, held)| held.used.load(Ordering::Relaxed))
                .map(|(table, _)| *table)
            else {
                return;
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
        if !self
            .storage
            .resize(hash_bytes::<(TableId, Held<T>)>(self.entries.capacity()))
        {
            self.entries = HashMap::new();
            self.bytes = 0;
            self.storage.resize(0);
        }
    }

    pub(super) fn estimated_bytes(&self) -> u64 {
        self.bytes
            .saturating_sub((self.entries.len() * size_of::<(TableId, Held<T>)>()) as u64)
            .saturating_add(self.storage.bytes())
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
    use crate::{
        catalog::TableId,
        store::cache::retention::{Budget, Shared},
    };

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
        directories.insert(TableId::new(1), Shared::new(64 * 1024 * 1024 + 1_u64));
        assert!(directories.get(TableId::new(1)).is_none());
    }

    #[test]
    fn byte_pressure_evicts_oldest_entries_but_preserves_readers() {
        let header = (size_of::<(TableId, super::Held<u64>)>() + Shared::<u64>::overhead()) as u64;
        let mut cache: Directories<u64> = Directories::new(3 * (header + 10));
        for table in 1..=3 {
            cache.insert(TableId::new(table), Shared::new(10));
        }
        let reader = cache.get(TableId::new(2)).unwrap();

        cache.get(TableId::new(3));
        cache.get(TableId::new(1));
        cache.insert(TableId::new(4), Shared::new(30));

        assert!(cache.get(TableId::new(1)).is_some());
        assert!(cache.get(TableId::new(2)).is_none());
        assert!(cache.get(TableId::new(3)).is_none());
        assert_eq!(cache.get(TableId::new(4)).as_deref(), Some(&30));
        assert!(cache.bytes <= cache.capacity);
        assert_eq!(*reader, 10);
        drop(reader);
    }

    #[test]
    fn replacement_releases_its_old_charge_even_when_oversized() {
        let header = (size_of::<(TableId, super::Held<u64>)>() + Shared::<u64>::overhead()) as u64;
        let capacity = 2 * (header + 10);
        let mut cache: Directories<u64> = Directories::new(capacity);
        cache.insert(TableId::new(1), Shared::new(10));
        cache.insert(TableId::new(2), Shared::new(10));
        cache.insert(TableId::new(1), Shared::new(1));
        assert_eq!(cache.bytes, 2 * header + 11);

        cache.insert(TableId::new(1), Shared::new(capacity + 1));
        assert!(cache.get(TableId::new(1)).is_none());
        assert_eq!(cache.get(TableId::new(2)).as_deref(), Some(&10));
        assert_eq!(cache.bytes, header + 10);
    }

    #[test]
    fn zero_capacity_and_overflowing_weights_are_not_admitted() {
        let mut cache: Directories<u64> = Directories::new(0);
        cache.insert(TableId::new(1), Shared::new(0_u64));
        assert!(cache.get(TableId::new(1)).is_none());
        let mut cache: Directories<u64> = Directories::new(u64::MAX);
        cache.insert(TableId::new(1), Shared::new(u64::MAX));
        assert!(cache.get(TableId::new(1)).is_none());
    }

    /// Past capacity, the directory unused for longest is the one evicted.
    #[test]
    fn eviction_drops_the_least_recently_used_directory() {
        let cache = RwLock::new(Directories::default());
        let table = |id: usize| TableId::new(u64::try_from(id).unwrap());
        for id in 0..DIRECTORY_CAPACITY {
            install(&cache, table(id), Shared::new(id));
        }

        assert!(lookup(&cache, table(0)).is_some());
        install(
            &cache,
            table(DIRECTORY_CAPACITY),
            Shared::new(DIRECTORY_CAPACITY),
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
    #[test]
    fn directory_budget_counts_readers_after_cache_drop() {
        let budget = Budget::new(4096);
        let mut cache = Directories::with_budget(4096, Arc::clone(&budget));
        cache.insert(TableId::new(1), Shared::new(1024_u64));
        let reader = cache.get(TableId::new(1)).unwrap();
        let occupied = budget.used();
        drop(cache);
        assert_eq!(budget.used(), 1024 + Shared::<u64>::overhead() as u64);
        assert!(budget.used() < occupied);
        drop(reader);
        assert_eq!(budget.used(), 0);
    }
}
