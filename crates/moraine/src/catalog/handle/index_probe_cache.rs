//! Bounded probe results shared by read scopes at the same store revision.

use std::{collections::VecDeque, ops::Bound, sync::Mutex};

use crate::{IndexId, TableId, store::index_encoding::CanonicalKey};

const CAPACITY: usize = 8 * 1024 * 1024;
const MAX_ENTRIES: usize = 4096;

#[derive(Clone, PartialEq, Eq)]
pub(super) enum Probe {
    Many(Vec<CanonicalKey>),
    Range(Bound<CanonicalKey>, Bound<CanonicalKey>, bool),
    Nulls(CanonicalKey, bool),
}

impl Probe {
    fn bytes(&self) -> usize {
        let bound_size = |bound: &Bound<CanonicalKey>| match bound {
            Bound::Included(key) | Bound::Excluded(key) => key.retained_bytes(),
            Bound::Unbounded => 0,
        };
        match self {
            Self::Many(keys) => {
                keys.capacity() * size_of::<CanonicalKey>()
                    + keys.iter().map(CanonicalKey::retained_bytes).sum::<usize>()
            }
            Self::Range(lower, upper, _) => bound_size(lower) + bound_size(upper),
            Self::Nulls(key, _) => key.retained_bytes(),
        }
    }
}

struct Entry {
    revision: u64,
    table: TableId,
    index: IndexId,
    probe: Probe,
    rows: Vec<u64>,
    bytes: usize,
}

#[derive(Default)]
struct Entries {
    values: VecDeque<Entry>,
    bytes: usize,
}

#[derive(Default)]
pub(super) struct IndexProbeCache(Mutex<Entries>);

impl IndexProbeCache {
    pub(super) fn estimated_bytes(&self) -> u64 {
        let entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let spare = (entries.values.capacity() - entries.values.len()) * size_of::<Entry>();
        u64::try_from(entries.bytes + spare).unwrap_or(u64::MAX)
    }

    pub(super) fn get(
        &self,
        revision: u64,
        table: TableId,
        index: IndexId,
        probe: &Probe,
    ) -> Option<Vec<u64>> {
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let position = entries.values.iter().position(|entry| {
            entry.revision == revision
                && entry.table == table
                && entry.index == index
                && &entry.probe == probe
        })?;
        let entry = entries.values.remove(position)?;
        let rows = entry.rows.clone();
        entries.values.push_front(entry);
        drop(entries);
        tracing::debug!(
            table_id = table.get(),
            index_id = index.get(),
            "index probe reused"
        );
        Some(rows)
    }

    pub(super) fn put(
        &self,
        revision: u64,
        table: TableId,
        index: IndexId,
        probe: Probe,
        rows: &[u64],
    ) {
        let bytes = size_of::<Entry>() + probe.bytes() + size_of_val(rows);
        if bytes > CAPACITY {
            return;
        }
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(position) = entries.values.iter().position(|entry| {
            entry.revision == revision
                && entry.table == table
                && entry.index == index
                && entry.probe == probe
        }) && let Some(previous) = entries.values.remove(position)
        {
            entries.bytes -= previous.bytes;
        }
        while entries.bytes + bytes > CAPACITY || entries.values.len() >= MAX_ENTRIES {
            let Some(oldest) = entries.values.pop_back() else {
                break;
            };
            entries.bytes -= oldest.bytes;
        }
        entries.values.push_front(Entry {
            revision,
            table,
            index,
            probe,
            rows: rows.to_vec(),
            bytes,
        });
        entries.bytes += bytes;
        let retained_bytes = entries.bytes;
        drop(entries);
        tracing::debug!(
            table_id = table.get(),
            index_id = index.get(),
            retained_bytes,
            "index probe cached"
        );
    }
}

impl super::ReadOnlyCatalog {
    pub(super) async fn scoped_probe(
        &self,
        table: TableId,
        index: IndexId,
        probe: Probe,
        read: impl AsyncFnOnce() -> crate::Result<Vec<u64>>,
    ) -> crate::Result<Vec<u64>> {
        let revision = self
            .pinned
            .as_ref()
            .map(|pinned| pinned.transaction.seqnum());
        if let Some(revision) = revision
            && let Some(rows) = self.index_probes.get(revision, table, index, &probe)
        {
            return Ok(rows);
        }
        let rows = read().await?;
        tracing::debug!(
            table_id = table.get(),
            index_id = index.get(),
            "index probe resolved"
        );
        if let Some(revision) = revision {
            self.index_probes.put(revision, table, index, probe, &rows);
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_modes_and_eviction_do_not_alias() {
        let cache = IndexProbeCache::default();
        let table = TableId::new(1);
        let index = IndexId::new(1);
        let probe = Probe::Many(vec![]);
        cache.put(1, table, index, probe.clone(), &[7]);
        assert_eq!(cache.get(1, table, index, &probe), Some(vec![7]));
        assert_eq!(cache.get(2, table, index, &probe), None);
        assert_eq!(cache.get(1, TableId::new(2), index, &probe), None);
        assert_eq!(cache.get(1, table, IndexId::new(2), &probe), None);
        assert_eq!(
            cache.get(
                1,
                table,
                index,
                &Probe::Range(Bound::Unbounded, Bound::Unbounded, false)
            ),
            None
        );
        let rows = vec![0; CAPACITY / 16];
        cache.put(2, table, index, probe.clone(), &rows);
        cache.put(3, table, index, probe.clone(), &rows);
        assert_eq!(cache.get(1, table, index, &probe), None);
        assert!(cache.0.lock().unwrap().bytes <= CAPACITY);
        assert_eq!(cache.get(3, table, index, &probe), Some(rows));
    }
}
