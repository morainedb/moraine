//! Snapshot-scoped directories for selective row lookups.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use imbl::OrdMap;

use crate::{
    CacheIdentity,
    catalog::TableId,
    store::{
        inline::InlineChunkLocator,
        proto::{DataFileValue, HeadValue},
    },
};

pub(super) mod files;
pub(super) mod inline;

#[derive(Default)]
pub(super) struct RowLookupCache {
    files: RwLock<HashMap<TableId, Arc<FileDirectory>>>,
    inline: RwLock<HashMap<TableId, Arc<InlineDirectory>>>,
}

impl RowLookupCache {
    pub(super) fn estimated_bytes(&self) -> u64 {
        let files = self
            .files
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let inline = self
            .inline
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        files
            .values()
            .map(|directory| directory.bytes)
            .sum::<u64>()
            .saturating_add(
                inline
                    .values()
                    .map(|directory| directory.ranges.estimated_bytes())
                    .sum(),
            )
    }
}

/// Keeps at most 64 table directories per source kind; active readers own their
/// copies.
fn install<T>(cache: &RwLock<HashMap<TableId, Arc<T>>>, table: TableId, directory: Arc<T>) {
    let mut cache = cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if cache.len() >= 64
        && !cache.contains_key(&table)
        && let Some(evicted) = cache.keys().next().copied()
    {
        cache.remove(&evicted);
    }
    cache.insert(table, directory);
}

struct FileDirectory {
    identity: CacheIdentity,
    data_prefix: String,
    table_prefix: String,
    files: OrdMap<u64, DataFileValue>,
    ranges: Intervals<u64>,
    arbitrary: Vec<u64>,
    bytes: u64,
}

struct InlineDirectory {
    head: HeadValue,
    ranges: Intervals<InlineChunkLocator>,
}

/// Balanced intervals ordered by start, with each subtree's greatest end.
struct Intervals<T> {
    entries: Vec<Interval<T>>,
}

struct Interval<T> {
    start: u64,
    end: u64,
    maximum_end: u64,
    value: T,
}

impl<T> Intervals<T> {
    fn estimated_bytes(&self) -> u64 {
        u64::try_from(
            self.entries
                .capacity()
                .saturating_mul(std::mem::size_of::<Interval<T>>()),
        )
        .unwrap_or(u64::MAX)
    }

    fn new(ranges: impl IntoIterator<Item = (u64, u64, T)>) -> Self {
        let mut entries: Vec<_> = ranges
            .into_iter()
            .map(|(start, end, value)| Interval {
                start,
                end,
                maximum_end: end,
                value,
            })
            .collect();
        entries.sort_unstable_by_key(|entry| entry.start);
        Self::augment(&mut entries);
        Self { entries }
    }

    fn augment(entries: &mut [Interval<T>]) -> u64 {
        if entries.is_empty() {
            return 0;
        }
        let middle = entries.len() / 2;
        let (left, rest) = entries.split_at_mut(middle);
        let (root, right) = rest.split_at_mut(1);
        root[0].maximum_end = root[0]
            .end
            .max(Self::augment(left))
            .max(Self::augment(right));
        root[0].maximum_end
    }

    fn visit(&self, row: u64, mut matched: impl FnMut(&T)) {
        Self::visit_slice(&self.entries, row, &mut matched);
    }

    fn visit_slice(entries: &[Interval<T>], row: u64, matched: &mut impl FnMut(&T)) {
        if entries.is_empty() {
            return;
        }
        let middle = entries.len() / 2;
        let root = &entries[middle];
        if root.maximum_end < row {
            return;
        }
        Self::visit_slice(&entries[..middle], row, matched);
        if root.start > row {
            return;
        }
        if row <= root.end {
            matched(&root.value);
        }
        Self::visit_slice(&entries[middle + 1..], row, matched);
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::Intervals;

    #[test]
    fn interval_lookup_keeps_nested_ranges_and_domain_endpoints() {
        let ranges = Intervals::new([
            (0, u64::MAX, 0),
            (5, 5, 1),
            (5, 10, 2),
            (u64::MAX, u64::MAX, 3),
        ]);
        for (row, expected) in [
            (0, vec![0]),
            (5, vec![0, 1, 2]),
            (10, vec![0, 2]),
            (u64::MAX, vec![0, 3]),
        ] {
            let mut found = Vec::new();
            ranges.visit(row, |id| found.push(*id));
            found.sort_unstable();
            assert_eq!(found, expected);
        }
    }

    proptest! {
        #[test]
        fn interval_lookup_matches_exhaustive_membership(
            ranges in prop::collection::vec((any::<u64>(), any::<u64>()), 0..200), row in any::<u64>()
        ) {
            let ranges: Vec<_> = ranges.into_iter().map(|(a,b)| (a.min(b), a.max(b))).collect();
            let index = Intervals::new(ranges.iter().enumerate().map(|(id, &(start, end))| (start,end,id)));
            let mut actual = Vec::new();
            index.visit(row, |id| actual.push(*id));
            actual.sort_unstable();
            let expected: Vec<_> = ranges.iter().enumerate().filter_map(|(id, &(start,end))|
                (start <= row && row <= end).then_some(id)).collect();
            prop_assert_eq!(actual, expected);
        }
    }
}
