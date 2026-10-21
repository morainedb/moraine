//! Bounded probe results shared by read scopes at the same store revision.

use std::{ops::Bound, sync::Arc};

use super::revision_memo::RevisionMemo;
use crate::{IndexId, TableId, store::index_encoding::CanonicalKey};

const CAPACITY: usize = 8 * 1024 * 1024;
const MAX_ENTRIES: usize = 4096;

#[derive(Clone, PartialEq, Eq)]
pub(super) enum Probe {
    Many(Vec<CanonicalKey>),
    Range(Bound<CanonicalKey>, Bound<CanonicalKey>, bool),
    Nulls(CanonicalKey, bool),
    Limited(Box<Self>, usize),
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
            Self::Limited(probe, _) => size_of::<Self>() + probe.bytes(),
        }
    }
}

pub(super) struct IndexProbeCache(RevisionMemo<(IndexId, Probe), Vec<u64>>);

impl Default for IndexProbeCache {
    fn default() -> Self {
        Self(RevisionMemo::new(CAPACITY, MAX_ENTRIES))
    }
}

impl IndexProbeCache {
    pub(super) fn estimated_bytes(&self) -> u64 {
        self.0.estimated_bytes()
    }

    pub(super) fn get(
        &self,
        revision: u64,
        table: TableId,
        index: IndexId,
        probe: &Probe,
    ) -> Option<Vec<u64>> {
        let rows = self
            .0
            .get_matching(revision, table, |(candidate_index, candidate)| {
                *candidate_index == index && candidate == probe
            })?;
        tracing::debug!(
            table_id = table.get(),
            index_id = index.get(),
            "index probe reused"
        );
        Some(rows.as_ref().clone())
    }

    pub(super) fn put(
        &self,
        revision: u64,
        table: TableId,
        index: IndexId,
        probe: Probe,
        rows: &[u64],
    ) {
        let bytes = probe
            .bytes()
            .saturating_add(size_of::<Vec<u64>>())
            .saturating_add(size_of_val(rows));
        if !self.0.can_hold(bytes) {
            return;
        }
        self.0.put(
            revision,
            table,
            (index, probe),
            Arc::new(rows.to_vec()),
            bytes,
        );
        tracing::debug!(
            table_id = table.get(),
            index_id = index.get(),
            retained_bytes = self.estimated_bytes(),
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
        let revision = self.pinned_revision();
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
        assert!(cache.estimated_bytes() <= CAPACITY as u64);
        assert_eq!(cache.get(3, table, index, &probe), Some(rows));
    }
}
