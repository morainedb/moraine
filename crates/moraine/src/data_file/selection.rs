//! Which of a file's rows a read decodes, and where each emitted row sits
//! in the file. Positions arrive sorted and duplicate-free, so building a
//! row selection never repeats a sort.

use object_store::path::Path;
use parquet::{
    arrow::arrow_reader::RowSelection,
    file::metadata::{PageIndexPolicy, ParquetMetaData},
};

use crate::{
    data_file::usize_as_u64,
    error::{Error, Result},
};

/// Which of a file's rows a scoped read decodes.
///
/// [`At`](Self::At) becomes a Parquet row selection, so deriving the entries
/// of a few rows out of a large file costs those rows rather than the file —
/// the difference between a delete that pays for what it deletes and one
/// that pays for the table.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ScopedRows<'a> {
    /// Every row, in file order.
    All,
    /// Only the rows at these physical positions. Entries come back in
    /// position order, and the set's ordering is what lets the selection be
    /// built without sorting.
    At(&'a RowPositions),
    /// Every row from this physical position through the end of the file.
    From(u64),
}

impl ScopedRows<'_> {
    /// Whether the read can be answered without touching the store.
    pub(super) fn is_empty(self) -> bool {
        matches!(self, Self::At(wanted) if wanted.is_empty())
    }

    /// A selection reads the page index so it can skip whole pages; a full
    /// read has nothing to skip and does not pay for it.
    pub(super) fn page_index_policy(self) -> PageIndexPolicy {
        match self {
            Self::All => PageIndexPolicy::Skip,
            Self::At(_) | Self::From(_) => PageIndexPolicy::Optional,
        }
    }
}

/// Physical Parquet row positions, sorted and duplicate-free. Every selected
/// read shares this representation, so constructing a row selection never
/// repeats a tree-to-vector copy.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RowPositions(Vec<u64>);

impl RowPositions {
    /// Establishes the sorted/unique invariant for positions collected from
    /// one or more delete sources.
    pub(crate) fn from_unsorted(mut positions: Vec<u64>) -> Self {
        positions.sort_unstable();
        positions.dedup();
        Self(positions)
    }

    /// The ordered positions used by Parquet row selection and ordinal
    /// resolution.
    pub(crate) fn as_slice(&self) -> &[u64] {
        &self.0
    }

    /// Whether no physical row is selected.
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether the selected set contains `position`.
    pub(crate) fn contains(&self, position: u64) -> bool {
        self.0.binary_search(&position).is_ok()
    }
}

impl FromIterator<u64> for RowPositions {
    fn from_iter<T: IntoIterator<Item = u64>>(iter: T) -> Self {
        Self::from_unsorted(iter.into_iter().collect())
    }
}

/// Maps the nth row a read emits to the file ordinal it sits at.
#[derive(Debug, Clone, Copy)]
pub(super) enum Ordinals<'a> {
    /// Every row was read, so the nth emitted row is the nth row.
    Dense,
    /// A full-file read resumed at this physical position.
    Offset(u64),
    /// Only these positions were read, in this order.
    Selected(&'a [u64]),
}

impl Ordinals<'_> {
    /// The file ordinal of the `nth` row emitted.
    pub(super) fn at(self, nth: usize) -> Result<u64> {
        match self {
            Self::Dense => Ok(usize_as_u64(nth)),
            Self::Offset(start) => Ok(start.saturating_add(usize_as_u64(nth))),
            Self::Selected(positions) => positions.get(nth).copied().ok_or_else(|| {
                Error::Corruption(
                    "scoped read: the reader emitted more rows than the selection named".to_owned(),
                )
            }),
        }
    }
}

/// Owned ordinal mapping retained by a returned stream.
pub(super) enum OwnedOrdinals {
    Dense,
    Offset(u64),
    Selected(Vec<u64>),
}

impl OwnedOrdinals {
    pub(super) fn borrowed(&self) -> Ordinals<'_> {
        match self {
            Self::Dense => Ordinals::Dense,
            Self::Offset(start) => Ordinals::Offset(*start),
            Self::Selected(positions) => Ordinals::Selected(positions),
        }
    }
}

pub(super) fn scoped_selection(
    rows: ScopedRows<'_>,
    total: usize,
) -> Result<(Option<RowSelection>, OwnedOrdinals)> {
    match rows {
        ScopedRows::All => Ok((None, OwnedOrdinals::Dense)),
        ScopedRows::At(positions) => Ok((
            Some(row_selection(positions.as_slice(), total)?),
            OwnedOrdinals::Selected(positions.as_slice().to_vec()),
        )),
        ScopedRows::From(start_ordinal) => {
            let start = usize::try_from(start_ordinal)
                .ok()
                .filter(|start| *start <= total)
                .ok_or_else(|| {
                    Error::Corruption(format!(
                        "scoped read: resume row {start_ordinal} is past the file's {total} rows"
                    ))
                })?;
            Ok((
                Some(RowSelection::from_consecutive_ranges(
                    std::iter::once(start..total),
                    total,
                )),
                OwnedOrdinals::Offset(start_ordinal),
            ))
        }
    }
}

/// The row selection naming `positions` in a file of `total_rows`.
///
/// Positions arrive ascending and unique (they come from [`RowPositions`]) and
/// are bounds-checked here, so the consecutive ranges handed over are
/// ordered and in range — the two things the builder requires of them.
fn row_selection(positions: &[u64], total_rows: usize) -> Result<RowSelection> {
    let mut ranges = Vec::with_capacity(positions.len());
    for &position in positions {
        let start = usize::try_from(position)
            .ok()
            .filter(|start| *start < total_rows)
            .ok_or_else(|| {
                Error::Corruption(format!(
                    "scoped read: selected row {position} is past the file's {total_rows} rows"
                ))
            })?;
        ranges.push(start..start.saturating_add(1));
    }
    Ok(RowSelection::from_consecutive_ranges(
        ranges.into_iter(),
        total_rows,
    ))
}

/// A file's row count, as the selection builder counts it.
pub(super) fn total_rows(metadata: &ParquetMetaData, path: &Path) -> Result<usize> {
    usize::try_from(metadata.file_metadata().num_rows())
        .map_err(|_| Error::Corruption(format!("scoped read: {path} reports a negative row count")))
}
