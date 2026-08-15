//! Adaptive exact membership for one immutable data file's row ids.

use std::mem::size_of;

use roaring::RoaringTreemap;

use super::usize_as_u64;
use crate::error::{Error, Result};

/// Approximate allocator and container metadata retained per Roaring
/// container. The representation decision only needs a conservative ordering;
/// the cache accounts the same estimate when enforcing its byte budget.
const ROARING_CONTAINER_OVERHEAD_BYTES: u64 = 64;

/// Approximate tree-node overhead for one high-32-bit partition.
const ROARING_PARTITION_OVERHEAD_BYTES: u64 = 64;

/// Divisors correcting the container statistics to the heap they describe.
///
/// `n_bytes_bitset_containers` reports a container's *bit* capacity
/// (`1024 * 64`) where the container is a `Box<[u64; 1024]>` — 8 KiB, not
/// 64 KiB. `n_bytes_array_containers` charges `size_of::<u32>()` against a
/// vector of `u16`. Taken at face value both inflate a Roaring set past the
/// raw ids it replaces, which picks the sorted form for exactly the
/// moderately sparse files Roaring is smallest on.
const ROARING_BITSET_STATISTIC_PER_BYTE: u64 = 8;
const ROARING_ARRAY_STATISTIC_PER_BYTE: u64 = 2;

/// The representation selected, as the tests assert it.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FileRowSetKind {
    /// One contiguous half-open range.
    Range,
    /// A run-optimized 64-bit Roaring set.
    Roaring,
    /// Sorted raw row ids, selected when Roaring fragments.
    Sorted,
}

/// Exact physical row-id membership for one immutable file.
#[derive(Debug, Clone)]
pub(super) enum FileRowSet {
    /// A half-open dense range.
    Range { start: u64, end: u64 },
    /// A compressed sparse set.
    Roaring(RoaringTreemap),
    /// Predictable storage for highly fragmented ids.
    Sorted(Vec<u64>),
}

impl FileRowSet {
    /// Constructs the smallest estimated resident representation from strictly
    /// ascending, unique row ids.
    pub(super) fn from_sorted(row_ids: Vec<u64>) -> Result<Self> {
        if row_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::Constraint(
                "file row ids must be strictly ascending and unique".to_owned(),
            ));
        }

        // Check if the row ids form a dense range
        if let (Some(&start), Some(&last)) = (row_ids.first(), row_ids.last()) {
            let count = usize_as_u64(row_ids.len());
            if start
                .checked_add(count)
                .is_some_and(|end| end == last.saturating_add(1))
            {
                return Ok(Self::Range {
                    start,
                    end: last.saturating_add(1),
                });
            }
        }

        let raw_bytes = usize_as_u64(row_ids.len()).saturating_mul(size_of::<u64>() as u64);
        let unoptimized = row_ids.iter().copied().collect::<RoaringTreemap>();
        let roaring =
            RoaringTreemap::from_bitmaps(unoptimized.bitmaps().map(|(partition, bitmap)| {
                let mut optimized = bitmap.clone();
                optimized.optimize();
                (partition, optimized)
            }));

        if roaring_estimated_bytes(&roaring) < raw_bytes {
            Ok(Self::Roaring(roaring))
        } else {
            Ok(Self::Sorted(row_ids))
        }
    }

    /// Constructs a dense range, refusing an unrepresentable end.
    pub(super) fn range(start: u64, count: u64) -> Result<Self> {
        let end = start.checked_add(count).ok_or_else(|| {
            Error::Corruption(format!(
                "file row-id range {start} + {count} exceeds the u64 domain"
            ))
        })?;

        Ok(Self::Range { start, end })
    }

    /// Whether this file physically contains `row_id`.
    pub(super) fn contains(&self, row_id: u64) -> bool {
        match self {
            Self::Range { start, end } => (*start..*end).contains(&row_id),
            Self::Roaring(rows) => rows.contains(row_id),
            Self::Sorted(rows) => rows.binary_search(&row_id).is_ok(),
        }
    }

    /// Requested row ids physically present in this file, preserving request
    /// order.
    pub(super) fn matching(&self, requested: &[u64]) -> Vec<u64> {
        requested
            .iter()
            .copied()
            .filter(|row_id| self.contains(*row_id))
            .collect()
    }

    /// Estimated resident bytes charged to the cache.
    pub(super) fn estimated_bytes(&self) -> u64 {
        match self {
            Self::Range { .. } => size_of::<Self>() as u64,
            Self::Roaring(rows) => roaring_estimated_bytes(rows),
            Self::Sorted(rows) => usize_as_u64(rows.len()).saturating_mul(size_of::<u64>() as u64),
        }
    }

    #[cfg(test)]
    pub(super) const fn kind(&self) -> FileRowSetKind {
        match self {
            Self::Range { .. } => FileRowSetKind::Range,
            Self::Roaring(_) => FileRowSetKind::Roaring,
            Self::Sorted(_) => FileRowSetKind::Sorted,
        }
    }
}

/// Approximates the heap retained by the Rust Roaring implementation from its
/// exposed container statistics.
fn roaring_estimated_bytes(rows: &RoaringTreemap) -> u64 {
    rows.bitmaps().fold(0_u64, |total, (_, bitmap)| {
        let stats = bitmap.statistics();
        let payload = (stats.n_bytes_array_containers / ROARING_ARRAY_STATISTIC_PER_BYTE)
            .saturating_add(stats.n_bytes_run_containers)
            .saturating_add(stats.n_bytes_bitset_containers / ROARING_BITSET_STATISTIC_PER_BYTE);

        total
            .saturating_add(payload)
            .saturating_add(
                u64::from(stats.n_containers).saturating_mul(ROARING_CONTAINER_OVERHEAD_BYTES),
            )
            .saturating_add(ROARING_PARTITION_OVERHEAD_BYTES)
    })
}
