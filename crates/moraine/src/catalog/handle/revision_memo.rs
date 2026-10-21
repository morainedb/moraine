//! Byte- and count-bounded results shared at one store revision.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use crate::TableId;

struct Entry<K, V> {
    revision: u64,
    table: TableId,
    key: K,
    value: Arc<V>,
    bytes: usize,
}

struct Entries<K, V> {
    values: VecDeque<Entry<K, V>>,
    bytes: usize,
}

/// Least-recently-used entries under a byte and count ceiling. A value that
/// alone exceeds the ceiling is not admitted.
pub(super) struct RevisionMemo<K, V> {
    entries: Mutex<Entries<K, V>>,
    capacity: usize,
    max_entries: usize,
}

impl<K: Eq, V> RevisionMemo<K, V> {
    pub(super) fn new(capacity: usize, max_entries: usize) -> Self {
        Self {
            entries: Mutex::new(Entries {
                values: VecDeque::new(),
                bytes: 0,
            }),
            capacity,
            max_entries,
        }
    }

    pub(super) fn estimated_bytes(&self) -> u64 {
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let spare = (entries.values.capacity() - entries.values.len()) * size_of::<Entry<K, V>>();
        u64::try_from(entries.bytes.saturating_add(spare)).unwrap_or(u64::MAX)
    }

    pub(super) fn get(&self, revision: u64, table: TableId, key: &K) -> Option<Arc<V>> {
        self.get_matching(revision, table, |candidate| candidate == key)
    }

    /// Looks up a composite key without allocating an owned copy of it.
    pub(super) fn get_matching(
        &self,
        revision: u64,
        table: TableId,
        matches: impl Fn(&K) -> bool,
    ) -> Option<Arc<V>> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let position = entries.values.iter().position(|entry| {
            entry.revision == revision && entry.table == table && matches(&entry.key)
        })?;
        let entry = entries.values.remove(position)?;
        let value = Arc::clone(&entry.value);
        entries.values.push_front(entry);
        Some(value)
    }

    /// Whether `bytes`, plus the entry header, fits the retention limits.
    pub(super) fn can_hold(&self, bytes: usize) -> bool {
        self.max_entries != 0
            && bytes
                .checked_add(size_of::<Entry<K, V>>())
                .is_some_and(|total| total <= self.capacity)
    }

    /// `bytes` is what the entry costs to retain, beyond its header.
    pub(super) fn put(&self, revision: u64, table: TableId, key: K, value: Arc<V>, bytes: usize) {
        if !self.can_hold(bytes) {
            return;
        }
        let bytes = size_of::<Entry<K, V>>() + bytes;
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(position) = entries.values.iter().position(|entry| {
            entry.revision == revision && entry.table == table && entry.key == key
        }) && let Some(previous) = entries.values.remove(position)
        {
            entries.bytes -= previous.bytes;
        }
        while entries.bytes > self.capacity - bytes || entries.values.len() >= self.max_entries {
            let Some(oldest) = entries.values.pop_back() else {
                break;
            };
            entries.bytes -= oldest.bytes;
        }
        entries.values.push_front(Entry {
            revision,
            table,
            key,
            value,
            bytes,
        });
        entries.bytes += bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_zero_entry_limit_disables_retention() {
        let memo = RevisionMemo::new(1024, 0);
        memo.put(1, TableId::new(1), 1, Arc::new(7), 8);
        assert!(memo.get(1, TableId::new(1), &1).is_none());
    }

    #[test]
    fn near_limit_weights_do_not_overflow_accounting() {
        let memo: RevisionMemo<u8, u64> = RevisionMemo::new(usize::MAX, 8);
        let weight = usize::MAX - size_of::<Entry<u8, u64>>();
        memo.put(1, TableId::new(1), 1, Arc::new(1), weight);
        assert_eq!(memo.estimated_bytes(), usize::MAX as u64);
        memo.put(1, TableId::new(1), 2, Arc::new(2), weight);
        assert!(memo.get(1, TableId::new(1), &1).is_none());
        assert!(memo.get(1, TableId::new(1), &2).is_some());
    }

    #[test]
    fn revisions_tables_and_keys_do_not_alias() {
        let memo: RevisionMemo<u8, u64> = RevisionMemo::new(1024, 8);
        let table = TableId::new(1);
        memo.put(1, table, 7, Arc::new(70), 8);
        assert_eq!(memo.get(1, table, &7).as_deref(), Some(&70));
        assert_eq!(memo.get(2, table, &7), None);
        assert_eq!(memo.get(1, TableId::new(2), &7), None);
        assert_eq!(memo.get(1, table, &8), None);
    }

    #[test]
    fn eviction_holds_the_byte_and_entry_ceilings() {
        let memo: RevisionMemo<u8, u64> = RevisionMemo::new(256, 2);
        let table = TableId::new(1);
        memo.put(1, table, 1, Arc::new(1), 8);
        memo.put(1, table, 2, Arc::new(2), 8);
        memo.put(1, table, 3, Arc::new(3), 8);
        assert_eq!(
            memo.get(1, table, &1),
            None,
            "the count ceiling evicts the oldest"
        );
        assert!(memo.get(1, table, &3).is_some());

        // Below the ceiling with its header, so admitted; everything older
        // goes to make room.
        memo.put(1, table, 4, Arc::new(4), 200);
        assert_eq!(memo.get(1, table, &4).as_deref(), Some(&4));
        assert_eq!(memo.get(1, table, &3), None);
        assert!(memo.entries.lock().unwrap().bytes <= 256);

        memo.put(1, table, 5, Arc::new(5), 1024);
        assert_eq!(
            memo.get(1, table, &5),
            None,
            "an oversized value is not admitted"
        );
    }
}
