//! Byte- and count-bounded results shared at one store revision.

use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash},
    sync::{Arc, Mutex},
};

use hashbrown::HashTable;
use lru_slab::LruSlab;

use crate::TableId;

struct Entry<K, V> {
    hash: u64,
    revision: u64,
    table: TableId,
    key: K,
    value: Arc<V>,
    bytes: usize,
}

struct Entries<K, V> {
    index: HashTable<u32>,
    values: LruSlab<Entry<K, V>>,
    bytes: usize,
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
}

impl<K: Eq + Hash, V> RevisionMemo<K, V> {
    pub(super) fn new(capacity: usize, max_entries: usize) -> Self {
        Self {
            entries: Mutex::new(Entries {
                index: HashTable::new(),
                values: LruSlab::new(),
                bytes: 0,
            }),
            hasher: RandomState::new(),
            capacity,
            max_entries: max_entries.min(usize::try_from(u32::MAX - 1).unwrap_or(usize::MAX)),
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
        let spare_slots =
            (entries.values.capacity() - entries.values.len()) as usize * Self::slot_bytes();
        let spare_index = entries
            .index
            .allocation_size()
            .saturating_sub(entries.index.len() * (size_of::<u32>() + 1));
        u64::try_from(
            entries
                .bytes
                .saturating_add(spare_slots)
                .saturating_add(spare_index),
        )
        .unwrap_or(u64::MAX)
    }

    pub(super) fn get(&self, revision: u64, table: TableId, key: &K) -> Option<Arc<V>> {
        self.get_matching(revision, table, key, |candidate| candidate == key)
    }

    /// Looks up a borrowed key whose hash must match the owned key's hash.
    pub(super) fn get_matching<Q: Hash>(
        &self,
        revision: u64,
        table: TableId,
        key: &Q,
        matches: impl Fn(&K) -> bool,
    ) -> Option<Arc<V>> {
        let hash = self.hasher.hash_one((revision, table, key));
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let slot = entries.find(hash, revision, table, matches)?;
        Some(Arc::clone(&entries.values.get_mut(slot).value))
    }

    /// Whether `bytes`, plus the entry header, fits the retention limits.
    pub(super) fn can_hold(&self, bytes: usize) -> bool {
        self.max_entries != 0
            && bytes
                .checked_add(Self::entry_bytes())
                .is_some_and(|total| total <= self.capacity)
    }

    /// `bytes` is what the entry costs to retain, beyond its header.
    pub(super) fn put(&self, revision: u64, table: TableId, key: K, value: Arc<V>, bytes: usize) {
        if !self.can_hold(bytes) {
            return;
        }
        let bytes = Self::entry_bytes() + bytes;
        let hash = self.hasher.hash_one((revision, table, &key));
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(slot) = entries.find(hash, revision, table, |candidate| candidate == &key) {
            entries.remove(slot);
        }
        while entries.bytes > self.capacity - bytes || entries.index.len() >= self.max_entries {
            let Some(oldest) = entries.values.lru() else {
                break;
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
            bytes,
        });
        index.insert_unique(hash, slot, |&slot| values.peek(slot).hash);
        entries.bytes += bytes;
    }
}

#[cfg(test)]
mod tests;
