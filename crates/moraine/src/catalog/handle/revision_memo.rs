//! Byte- and count-bounded results shared at one store revision.

use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash},
    sync::{Arc, Mutex},
};

use hashbrown::HashTable;
use lru_slab::LruSlab;

use crate::{
    TableId,
    store::cache::retention::{Budget, Charge, Shared},
};

struct Entry<K, V> {
    hash: u64,
    revision: u64,
    table: TableId,
    key: K,
    value: Shared<V>,
    _key_charge: Charge,
    bytes: usize,
}

struct Entries<K, V> {
    index: HashTable<u32>,
    values: LruSlab<Entry<K, V>>,
    bytes: usize,
    storage: Charge,
}

impl<K, V> Entries<K, V> {
    fn find(
        &self,
        hash: u64,
        revision: u64,
        table: TableId,
        matches: impl Fn(&K) -> bool,
    ) -> Option<u32> {
        self.index
            .find(hash, |&slot| {
                let entry = self.values.peek(slot);
                entry.revision == revision && entry.table == table && matches(&entry.key)
            })
            .copied()
    }

    fn remove(&mut self, slot: u32) {
        let hash = self.values.peek(slot).hash;
        if let Ok(indexed) = self.index.find_entry(hash, |&candidate| candidate == slot) {
            indexed.remove();
            self.bytes -= self.values.remove(slot).bytes;
        }
    }
}

/// Least-recently-used entries under a byte and count ceiling. A value that
/// alone exceeds the ceiling is not admitted.
pub(super) struct RevisionMemo<K, V> {
    entries: Mutex<Entries<K, V>>,
    hasher: RandomState,
    capacity: usize,
    max_entries: usize,
    budget: Arc<Budget>,
}

impl<K: Eq + Hash, V> RevisionMemo<K, V> {
    #[cfg(test)]
    pub(super) fn new(capacity: usize, max_entries: usize) -> Self {
        Self::with_budget(capacity, max_entries, Budget::new(u64::MAX))
    }

    pub(super) fn shared(capacity: usize, max_entries: usize) -> Self {
        Self::with_budget(capacity, max_entries, crate::store::cache::lookup_budget())
    }

    pub(super) fn with_budget(capacity: usize, max_entries: usize, budget: Arc<Budget>) -> Self {
        Self {
            entries: Mutex::new(Entries {
                index: HashTable::new(),
                values: LruSlab::new(),
                bytes: 0,
                storage: Charge::new(Arc::clone(&budget)),
            }),
            hasher: RandomState::new(),
            capacity,
            max_entries: max_entries.min(usize::try_from(u32::MAX - 1).unwrap_or(usize::MAX)),
            budget,
        }
    }

    // Each slab slot holds an optional entry and two link indices.
    fn slot_bytes() -> usize {
        size_of::<(Option<Entry<K, V>>, u32, u32)>()
    }

    fn entry_bytes() -> usize {
        Self::slot_bytes() + size_of::<u32>() + 1
    }

    pub(super) fn estimated_bytes(&self) -> u64 {
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let payload = entries
            .bytes
            .saturating_sub(entries.index.len() * Self::entry_bytes());
        u64::try_from(payload)
            .unwrap_or(u64::MAX)
            .saturating_add(entries.storage.bytes())
    }

    pub(super) fn get(&self, revision: u64, table: TableId, key: &K) -> Option<Shared<V>> {
        self.get_matching(revision, table, key, |candidate| candidate == key)
    }

    /// Looks up a borrowed key whose hash must match the owned key's hash.
    pub(super) fn get_matching<Q: Hash>(
        &self,
        revision: u64,
        table: TableId,
        key: &Q,
        matches: impl Fn(&K) -> bool,
    ) -> Option<Shared<V>> {
        let hash = self.hasher.hash_one((revision, table, key));
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let slot = entries.find(hash, revision, table, matches)?;
        Some(entries.values.get_mut(slot).value.clone())
    }

    /// Whether `bytes`, plus the entry header, fits the retention limits.
    pub(super) fn can_hold(&self, bytes: usize) -> bool {
        self.max_entries != 0
            && bytes.checked_add(Self::entry_bytes()).is_some_and(|total| {
                total <= self.capacity
                    && u64::try_from(total).unwrap_or(u64::MAX) <= self.budget.capacity()
            })
    }

    /// Reserves result storage before making a cache-only copy.
    pub(super) fn put_cloned<Q>(
        &self,
        revision: u64,
        table: TableId,
        key: K,
        value: &Q,
        key_bytes: usize,
        value_bytes: usize,
    ) where
        Q: ToOwned<Owned = V> + ?Sized,
    {
        let Some(weight) = value_bytes
            .checked_add(size_of::<V>())
            .and_then(|bytes| bytes.checked_add(Shared::<V>::overhead()))
        else {
            return;
        };
        if !self.can_hold(weight.saturating_add(key_bytes)) {
            return;
        }
        let mut charge = Charge::new(Arc::clone(&self.budget));
        {
            let mut entries = self
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while !charge.resize(weight as u64) {
                let Some(oldest) = entries.values.lru() else {
                    return;
                };
                entries.remove(oldest);
            }
        }
        self.put(
            revision,
            table,
            key,
            Shared::charged(value.to_owned(), charge),
            key_bytes,
            value_bytes,
        );
    }

    /// Weights describe dynamic allocations; value and container headers are
    /// added here.
    pub(super) fn put(
        &self,
        revision: u64,
        table: TableId,
        key: K,
        value: Shared<V>,
        key_bytes: usize,
        value_bytes: usize,
    ) {
        let Some(value_bytes) = value_bytes
            .checked_add(size_of::<V>())
            .and_then(|bytes| bytes.checked_add(Shared::<V>::overhead()))
        else {
            return;
        };
        let Some(payload) = key_bytes.checked_add(value_bytes) else {
            return;
        };
        if !self.can_hold(payload) {
            return;
        }
        let bytes = Self::entry_bytes() + payload;
        let hash = self.hasher.hash_one((revision, table, &key));
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(slot) = entries.find(hash, revision, table, |candidate| candidate == &key) {
            entries.remove(slot);
        }
        let mut key_charge = Charge::new(Arc::clone(&self.budget));
        loop {
            let fits =
                entries.bytes <= self.capacity - bytes && entries.index.len() < self.max_entries;
            if fits
                && value.try_charge(&self.budget, value_bytes as u64)
                && key_charge.resize(key_bytes as u64)
            {
                break;
            }
            let Some(oldest) = entries.values.lru() else {
                return;
            };
            entries.remove(oldest);
        }
        let Entries { index, values, .. } = &mut *entries;
        let slot = values.insert(Entry {
            hash,
            revision,
            table,
            key,
            value,
            _key_charge: key_charge,
            bytes,
        });
        index.insert_unique(hash, slot, |&slot| values.peek(slot).hash);
        entries.bytes += bytes;
        let storage_bytes = (entries.values.capacity() as usize)
            .saturating_mul(Self::slot_bytes())
            .saturating_add(entries.index.allocation_size());
        if !entries
            .storage
            .resize(u64::try_from(storage_bytes).unwrap_or(u64::MAX))
        {
            entries.index = HashTable::new();
            entries.values = LruSlab::new();
            entries.bytes = 0;
            entries.storage.resize(0);
        }
    }
}

#[cfg(test)]
mod tests;
