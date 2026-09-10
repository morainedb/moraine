//! The keys a staged build has already staged, as a bloom filter.

use fastbloom::BloomFilter;

/// Target false-positive rate; every positive still probes the store.
const FALSE_POSITIVE_RATE: f64 = 0.001;

/// Fewest keys a filter is sized for.
const MIN_EXPECTED_KEYS: u64 = 65_536;

/// Most keys a filter is sized for: about 64 MiB at the target rate. Past
/// it the false-positive rate rises rather than the memory.
const MAX_EXPECTED_KEYS: u64 = 35_000_000;

/// A bloom filter over physical entry keys: a key it has not seen cannot
/// be in the index, and a key it has seen must be probed.
pub(super) struct BuildFilter {
    inner: BloomFilter,
}

impl BuildFilter {
    /// A filter sized for `expected` keys.
    pub(super) fn for_entries(expected: u64) -> Self {
        let expected = expected.clamp(MIN_EXPECTED_KEYS, MAX_EXPECTED_KEYS);
        Self {
            inner: BloomFilter::with_false_pos(FALSE_POSITIVE_RATE)
                .expected_items(usize::try_from(expected).unwrap_or(usize::MAX)),
        }
    }

    pub(super) fn insert(&mut self, key: &[u8]) {
        self.inner.insert(key);
    }

    /// False only when `key` was never inserted.
    pub(super) fn may_contain(&self, key: &[u8]) -> bool {
        self.inner.contains(key)
    }

    /// The filter's size in bits.
    pub(super) fn bits(&self) -> u64 {
        u64::try_from(self.inner.num_bits()).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: u64) -> Vec<u8> {
        format!("entry-{n}").into_bytes()
    }

    /// A filter never forgets a key it was given.
    #[test]
    fn inserted_keys_are_always_positive() {
        let mut filter = BuildFilter::for_entries(100_000);
        for n in 0..100_000 {
            filter.insert(&key(n));
        }
        assert!((0..100_000).all(|n| filter.may_contain(&key(n))));
    }

    /// A filter sized for its keys answers absent keys as absent nearly
    /// always.
    #[test]
    fn absent_keys_are_rarely_positive() {
        let mut filter = BuildFilter::for_entries(100_000);
        for n in 0..100_000 {
            filter.insert(&key(n));
        }
        let false_positives = (100_000..200_000)
            .filter(|n| filter.may_contain(&key(*n)))
            .count();
        assert!(
            false_positives < 500,
            "{false_positives} false positives in 100,000 absent keys"
        );
    }

    /// Sizing is bounded at both ends.
    #[test]
    fn sizing_is_clamped() {
        assert_eq!(
            BuildFilter::for_entries(0).bits(),
            BuildFilter::for_entries(MIN_EXPECTED_KEYS).bits()
        );
        assert_eq!(
            BuildFilter::for_entries(u64::MAX).bits(),
            BuildFilter::for_entries(MAX_EXPECTED_KEYS).bits()
        );
    }
}
