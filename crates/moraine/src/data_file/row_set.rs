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

    /// Every member, ascending. Materializes whatever the representation
    /// compressed, so a caller wanting the set itself pays for it once.
    pub(super) fn to_sorted_vec(&self) -> Vec<u64> {
        match self {
            Self::Range { start, end } => (*start..*end).collect(),
            Self::Roaring(rows) => rows.iter().collect(),
            Self::Sorted(rows) => rows.clone(),
        }
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

/// How a summary's ascending-id rank maps to a row's physical file
/// position.
#[derive(Debug, Clone)]
pub(super) enum RowOrder {
    /// File order is ascending id order: position = rank - 1.
    Ascending,
    /// `positions[k]` is the file position of the k-th smallest id.
    ///
    /// Never pairs with `FileRowSet::Range`: a range answers positions by
    /// arithmetic alone, which is only correct when file order is
    /// ascending order, so a permutation over a contiguous id set is kept
    /// as `Roaring` rather than collapsed to a range.
    Permuted(Vec<u32>),
}

/// A membership set plus how to read file positions off it.
#[derive(Debug, Clone)]
pub(super) struct PositionedRowSet {
    pub(super) rows: FileRowSet,
    pub(super) order: RowOrder,
}

impl PositionedRowSet {
    /// Estimated resident bytes charged to the cache: the set itself plus
    /// any permutation riding beside it.
    pub(super) fn estimated_bytes(&self) -> u64 {
        let permutation_bytes = match &self.order {
            RowOrder::Ascending => 0,
            RowOrder::Permuted(positions) => {
                usize_as_u64(positions.len()).saturating_mul(size_of::<u32>() as u64)
            }
        };
        self.rows
            .estimated_bytes()
            .saturating_add(permutation_bytes)
    }

    /// File positions of `requested` rows; `None` for absent rows.
    pub(super) fn positions_of(&self, requested: &[u64]) -> Vec<Option<u64>> {
        requested
            .iter()
            .map(|&row_id| {
                self.rank_of(row_id)
                    .and_then(|rank| self.order.position_of(rank))
            })
            .collect()
    }

    /// This row id's ascending-id rank among the set's members, if present.
    fn rank_of(&self, row_id: u64) -> Option<u64> {
        if !self.rows.contains(row_id) {
            return None;
        }

        match &self.rows {
            FileRowSet::Range { start, .. } => Some(row_id - start),
            FileRowSet::Roaring(rows) => rows.rank(row_id).checked_sub(1),
            FileRowSet::Sorted(rows) => rows.binary_search(&row_id).ok().map(usize_as_u64),
        }
    }

    /// Constructs from row ids in file order; duplicate ids are
    /// [`Error::Constraint`].
    pub(super) fn from_file_order(row_ids: Vec<u64>) -> Result<Self> {
        if is_strictly_ascending(&row_ids) {
            return Ok(Self {
                rows: FileRowSet::from_sorted(row_ids)?,
                order: RowOrder::Ascending,
            });
        }

        let mut ascending: Vec<u64> = row_ids.clone();
        ascending.sort_unstable();
        if ascending.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(Error::Constraint("file row ids must be unique".to_owned()));
        }

        let mut permutation = vec![0_u32; ascending.len()];
        for (position, row_id) in row_ids.iter().enumerate() {
            let rank = ascending
                .binary_search(row_id)
                .map_err(|_| Error::Corruption("row id vanished while sorting".to_owned()))?;
            permutation[rank] = u32::try_from(position).map_err(|_| {
                Error::Constraint("file position exceeds the u32 domain".to_owned())
            })?;
        }

        let rows = FileRowSet::from_sorted(ascending)?;
        // A permutation never pairs with Range: demote a contiguous
        // permuted set to Roaring so position resolution still consults
        // the permutation instead of arithmetic.
        let rows = match rows {
            FileRowSet::Range { start, end } => {
                let mut treemap = RoaringTreemap::new();
                treemap.insert_range(start..end);
                FileRowSet::Roaring(treemap)
            }
            other => other,
        };

        Ok(Self {
            rows,
            order: RowOrder::Permuted(permutation),
        })
    }
}

impl RowOrder {
    /// The file position for a set member's ascending-id rank.
    fn position_of(&self, rank: u64) -> Option<u64> {
        match self {
            Self::Ascending => Some(rank),
            Self::Permuted(positions) => usize::try_from(rank)
                .ok()
                .and_then(|rank| positions.get(rank))
                .map(|&position| u64::from(position)),
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

/// Whether `row_ids` is strictly increasing (and therefore also unique).
fn is_strictly_ascending(row_ids: &[u64]) -> bool {
    row_ids.windows(2).all(|pair| pair[0] < pair[1])
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
