//! Adaptive exact membership for one immutable data file's row ids.

use std::mem::size_of;

use roaring::{RoaringBitmap, RoaringTreemap};

use super::usize_as_u64;
use crate::error::{Error, Result};

/// Approximate allocator and container metadata retained per Roaring
/// container.
const ROARING_CONTAINER_OVERHEAD_BYTES: u64 = 64;

/// Approximate tree-node overhead for one high-32-bit partition.
const ROARING_PARTITION_OVERHEAD_BYTES: u64 = 64;

/// Divisors correcting the container statistics to the heap they describe:
/// `n_bytes_bitset_containers` reports bit capacity (8x the bytes) and
/// `n_bytes_array_containers` charges `u32` for a `u16` vector.
const ROARING_BITSET_STATISTIC_PER_BYTE: u64 = 8;
const ROARING_ARRAY_STATISTIC_PER_BYTE: u64 = 2;

/// The representation a summary took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FileRowSetKind {
    /// One contiguous half-open range.
    Range,
    /// A run-optimized 64-bit Roaring set.
    Roaring,
    /// Sorted raw row ids.
    Sorted,
}

/// Exact physical row-id membership for one immutable file.
#[derive(Debug, Clone)]
pub(super) enum FileRowSet {
    /// A half-open dense range.
    Range { start: u64, end: u64 },
    /// A compressed sparse set.
    Roaring(RoaringTreemap),
    /// Sorted raw row ids.
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
        let bitmaps = row_ids
            .chunk_by(|left, right| high_word(*left) == high_word(*right))
            .map(|partition| {
                let mut bitmap = RoaringBitmap::from_sorted_iter(
                    partition.iter().map(|row_id| low_word(*row_id)),
                )
                .map_err(|_| {
                    Error::Constraint("file row ids must be strictly ascending".to_owned())
                })?;
                bitmap.optimize();
                Ok((high_word(partition[0]), bitmap))
            })
            .collect::<Result<Vec<_>>>()?;
        let roaring = RoaringTreemap::from_bitmaps(bitmaps);

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

    pub(super) const fn kind(&self) -> FileRowSetKind {
        match self {
            Self::Range { .. } => FileRowSetKind::Range,
            Self::Roaring(_) => FileRowSetKind::Roaring,
            Self::Sorted(_) => FileRowSetKind::Sorted,
        }
    }
}

#[allow(clippy::cast_possible_truncation)]
const fn high_word(row_id: u64) -> u32 {
    (row_id >> 32) as u32
}

#[allow(clippy::cast_possible_truncation)]
const fn low_word(row_id: u64) -> u32 {
    row_id as u32
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
