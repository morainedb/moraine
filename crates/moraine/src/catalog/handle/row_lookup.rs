//! Snapshot-scoped directories for selective row lookups.

use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use imbl::OrdMap;

use crate::{
    CacheIdentity,
    catalog::TableId,
    data_file::FileSummary,
    store::{
        inline::InlineChunkLocator,
        proto::{DataFileValue, HeadValue},
    },
};

pub(super) mod files;
pub(super) mod inline;

/// Table directories kept per source kind.
const DIRECTORY_CAPACITY: usize = 256;

#[derive(Default)]
pub(super) struct RowLookupCache {
    files: RwLock<Directories<FileDirectory>>,
    inline: RwLock<Directories<InlineDirectory>>,
    /// Files sent for a summary read while building or refreshing a file
    /// directory.
    summarized_files: AtomicU64,
}

impl RowLookupCache {
    fn note_summarized(&self, files: usize) {
        self.summarized_files
            .fetch_add(u64::try_from(files).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(super) fn summarized_files(&self) -> u64 {
        self.summarized_files.load(Ordering::Relaxed)
    }

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
            .directories()
            .map(|directory| directory.bytes)
            .sum::<u64>()
            .saturating_add(
                inline
                    .directories()
                    .map(|directory| directory.ranges.estimated_bytes())
                    .sum(),
            )
    }
}

/// At most `DIRECTORY_CAPACITY` table directories, each stamped with its
/// last use so the least recently used one is evicted first.
struct Directories<T> {
    entries: HashMap<TableId, Held<T>>,
    clock: AtomicU64,
}

struct Held<T> {
    directory: Arc<T>,
    used: AtomicU64,
}

impl<T> Default for Directories<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            clock: AtomicU64::new(0),
        }
    }
}

impl<T> Directories<T> {
    /// The directory held for `table`, stamped as just used.
    fn get(&self, table: TableId) -> Option<Arc<T>> {
        let held = self.entries.get(&table)?;
        held.used.store(
            self.clock.fetch_add(1, Ordering::Relaxed),
            Ordering::Relaxed,
        );

        Some(Arc::clone(&held.directory))
    }

    /// Holds `directory` for `table`, evicting the least recently used one
    /// when at capacity.
    fn insert(&mut self, table: TableId, directory: Arc<T>) {
        if self.entries.len() >= DIRECTORY_CAPACITY
            && !self.entries.contains_key(&table)
            && let Some(evicted) = self
                .entries
                .iter()
                .min_by_key(|(_, held)| held.used.load(Ordering::Relaxed))
                .map(|(table, _)| *table)
        {
            self.entries.remove(&evicted);
        }

        let used = AtomicU64::new(self.clock.fetch_add(1, Ordering::Relaxed));
        self.entries.insert(table, Held { directory, used });
    }

    fn directories(&self) -> impl Iterator<Item = &T> {
        self.entries.values().map(|held| held.directory.as_ref())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// The directory held for `table`, if any.
fn lookup<T>(cache: &RwLock<Directories<T>>, table: TableId) -> Option<Arc<T>> {
    cache
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(table)
}

/// Holds `directory` for `table`; active readers own their copies.
fn install<T>(cache: &RwLock<Directories<T>>, table: TableId, directory: Arc<T>) {
    cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(table, directory);
}

struct FileDirectory {
    identity: CacheIdentity,
    data_prefix: String,
    table_prefix: String,
    files: OrdMap<u64, DataFileValue>,
    ranges: Intervals<u64>,
    /// Summaries of files holding arbitrary ids, kept across lookups.
    arbitrary: HashMap<u64, FileSummary>,
    /// Files that could not be summarized; every lookup retries them.
    failed: Vec<u64>,
    /// Encoded size of `files`, carried across refreshes.
    file_bytes: u64,
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

    /// Every interval as `(start, end, value)`.
    fn iter(&self) -> impl Iterator<Item = (u64, u64, &T)> {
        self.entries
            .iter()
            .map(|entry| (entry.start, entry.end, &entry.value))
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
    use std::sync::{Arc, RwLock};

    use proptest::prelude::*;

    use super::{DIRECTORY_CAPACITY, Directories, Intervals, install, lookup};
    use crate::catalog::TableId;

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
