//! Equality, range, and NULL index lookups.

use std::ops::Bound;

use futures::{StreamExt, TryStreamExt, stream};

use super::ReadOnlyCatalog;
use crate::{
    catalog::{IndexId, IndexInfo, IndexState, TableId},
    error::{Error, Result},
    store::{
        handle::ScanOrder,
        index_encoding::{
            CanonicalKey, Direction, IndexKeyValue, NullOrder, encode_ordered_values,
        },
        read,
    },
    transaction::index_maintenance,
};

/// Exact probes kept in flight by one batched index lookup.
const INDEX_LOOKUP_CONCURRENCY: usize = 32;

impl ReadOnlyCatalog {
    /// Resolves an equality lookup to the rows currently holding `values`.
    ///
    /// Head-only: the lookup materializes the current head and scans the
    /// `index` subspace under one read session, so the entries and the catalog
    /// they resolve against are one consistent cut. Entries are live-only,
    /// so there is no time-travel variant. Returns stable row ids; the caller
    /// applies delete files as any DuckLake scan does.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the index does not exist,
    /// [`Error::IndexBuilding`] if its staged backfill has not completed,
    /// [`Error::Constraint`] if a value exceeds the size cap, or a store
    /// error if the scan fails.
    pub async fn index_lookup(
        &self,
        table: TableId,
        index: IndexId,
        values: &[IndexKeyValue],
    ) -> Result<Vec<u64>> {
        self.index_lookup_many(table, index, &[values.to_vec()])
            .await
    }

    /// Resolves an `IN` lookup to the union of rows currently holding any
    /// complete equality key in `keys`.
    ///
    /// Duplicate keys are probed once and duplicate row ids are returned
    /// once. An empty key set returns no rows after validating that the index
    /// exists and is ready. The whole batch uses one read session and one head
    /// view, so every result belongs to the same consistent cut.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the index does not exist,
    /// [`Error::IndexBuilding`] if its staged backfill has not completed,
    /// [`Error::Constraint`] if a key is partial or exceeds the size cap, or
    /// a store error if a probe fails.
    pub async fn index_lookup_many(
        &self,
        table: TableId,
        index: IndexId,
        keys: &[Vec<IndexKeyValue>],
    ) -> Result<Vec<u64>> {
        let session = self.begin_read().await?;
        let handle = session.handle();

        let index_row_ids = read::consistent(handle, || async {
            let view = self.head_view(handle).await?;
            let info = view
                .index_by_id(table, index)
                .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;

            match info.state {
                IndexState::Ready => {}
                IndexState::Building | IndexState::Maintaining => {
                    return Err(Error::IndexBuilding(format!(
                        "index {index} is still building"
                    )));
                }
                IndexState::Poisoned => {
                    return Err(Error::NotFound(format!("index {index} was poisoned")));
                }
            }

            let mut encoded = keys.iter().map(|key| {
                if key.len() != info.columns.len() {
                    return Err(Error::Constraint(format!(
                        "index lookup: {} values do not address the {}-column index {index}; an \
                         equality lookup names every column",
                        key.len(),
                        info.columns.len()
                    )));
                }
                encode_ordered_values(
                    &key.iter().cloned().map(Some).collect::<Vec<_>>(),
                    &info.directions,
                    &info.nulls,
                )
            }).collect::<Result<Vec<_>>>()?;
            encoded.sort_unstable();
            encoded.dedup();

            let mut row_ids = stream::iter(encoded)
                .map(|key| async move {
                    index_maintenance::lookup_row_ids(handle, index.get(), info.unique, &key).await
                })
                .buffer_unordered(INDEX_LOOKUP_CONCURRENCY)
                .try_concat()
                .await?;
            row_ids.dedup();
            row_ids.sort_unstable();

            Ok(row_ids)
        })
        .await;
        session.finish();

        index_row_ids
    }

    /// Resolves a comparison query to the rows whose indexed value falls
    /// between `lower` and `upper` (`<`, `<=`, `>`, `>=`, `BETWEEN`, and their
    /// half-open forms via [`Bound::Unbounded`]). Each bound names the leading
    /// columns' values; equality is the degenerate closed `[v, v]` range.
    ///
    /// Head-only and candidate-returning, exactly like
    /// [`index_lookup`](Self::index_lookup): the scan and the catalog it
    /// resolves against are one consistent cut, and the caller applies delete
    /// files. Results are in the index's stored order, or its exact opposite
    /// when `reverse` is set. Both directions stream from the store in the
    /// requested order.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if the index does not exist,
    /// [`Error::IndexBuilding`] if its staged backfill has not completed,
    /// [`Error::Constraint`] if a bound value exceeds the size cap, or a
    /// store error if the scan fails.
    pub async fn index_range(
        &self,
        table: TableId,
        index: IndexId,
        lower: Bound<Vec<IndexKeyValue>>,
        upper: Bound<Vec<IndexKeyValue>>,
        reverse: bool,
    ) -> Result<Vec<u64>> {
        let session = self.begin_read().await?;
        let handle = session.handle();

        let view = self.head_view(handle).await?;
        let info = view
            .index_by_id(table, index)
            .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;

        match info.state {
            IndexState::Ready => {}
            IndexState::Building | IndexState::Maintaining => {
                return Err(Error::IndexBuilding(format!(
                    "index {index} is still building"
                )));
            }
            IndexState::Poisoned => {
                return Err(Error::NotFound(format!("index {index} was poisoned")));
            }
        }

        let (byte_lower, byte_upper) = encode_range_bounds(&info, index, lower, upper)?;
        let leading_nulls = info.nulls.first().copied().unwrap_or(NullOrder::Last);

        let range_row_ids = index_maintenance::range_row_ids(
            handle,
            index.get(),
            info.unique,
            leading_nulls,
            byte_lower,
            byte_upper,
            ScanOrder::from_reverse(reverse),
        )
        .await;
        session.finish();

        range_row_ids
    }

    /// Resolves an `IS NULL` query to the rows whose leading indexed columns
    /// match `prefix` — a leading run of `Some(value)` (equality) and `None`
    /// (`IS NULL`) predicates. The prefix must cover leading columns
    /// contiguously and name at least one `IS NULL`.
    ///
    /// Head-only and candidate-returning like
    /// [`index_lookup`](Self::index_lookup).
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if the index does not exist,
    /// [`Error::IndexBuilding`] while its staged backfill runs, or
    /// [`Error::Constraint`] if the prefix is empty, longer than the index,
    /// or names no `IS NULL`.
    pub async fn index_nulls(
        &self,
        table: TableId,
        index: IndexId,
        prefix: Vec<Option<IndexKeyValue>>,
        reverse: bool,
    ) -> Result<Vec<u64>> {
        let session = self.begin_read().await?;
        let handle = session.handle();

        let view = self.head_view(handle).await?;
        let info = view
            .index_by_id(table, index)
            .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;

        match info.state {
            IndexState::Ready => {}
            IndexState::Building | IndexState::Maintaining => {
                return Err(Error::IndexBuilding(format!(
                    "index {index} is still building"
                )));
            }
            IndexState::Poisoned => {
                return Err(Error::NotFound(format!("index {index} was poisoned")));
            }
        }

        if prefix.is_empty() || prefix.len() > info.columns.len() {
            return Err(Error::Constraint(format!(
                "index_nulls: a prefix of {} predicates does not fit the {}-column index \
                     {index}",
                prefix.len(),
                info.columns.len()
            )));
        }
        if prefix.iter().all(Option::is_some) {
            return Err(Error::Constraint(
                "index_nulls: the prefix names no IS NULL; use index_lookup for pure equality"
                    .to_owned(),
            ));
        }

        let key = encode_ordered_values(&prefix, &info.directions, &info.nulls)?;

        let null_prefix_row_ids = index_maintenance::null_prefix_row_ids(
            handle,
            index.get(),
            &key,
            ScanOrder::from_reverse(reverse),
        )
        .await;

        session.finish();

        null_prefix_row_ids
    }
}

/// Maps a value window onto the byte bounds one index scan answers.
fn encode_range_bounds(
    info: &IndexInfo,
    index: IndexId,
    lower: Bound<Vec<IndexKeyValue>>,
    upper: Bound<Vec<IndexKeyValue>>,
) -> Result<(Bound<CanonicalKey>, Bound<CanonicalKey>)> {
    let named_len = |bound: &Bound<Vec<IndexKeyValue>>| match bound {
        Bound::Included(values) | Bound::Excluded(values) => Some(values.len()),
        Bound::Unbounded => None,
    };
    let (lower_len, upper_len) = (named_len(&lower), named_len(&upper));

    if lower_len == Some(0) || upper_len == Some(0) {
        return Err(Error::Constraint(format!(
            "index_range: a bound of index {index} must name at least one column"
        )));
    }

    let widest = lower_len.unwrap_or(0).max(upper_len.unwrap_or(0));
    if widest > info.columns.len() {
        return Err(Error::Constraint(format!(
            "index_range: a bound of {widest} values does not fit the {}-column index {index}",
            info.columns.len()
        )));
    }

    let range_column = widest.saturating_sub(1);
    let mixed_directions = info
        .directions
        .iter()
        .take(widest)
        .any(|direction| Some(direction) != info.directions.first());
    let pinned_prefix = match (&lower, &upper) {
        (
            Bound::Included(low) | Bound::Excluded(low),
            Bound::Included(high) | Bound::Excluded(high),
        ) => low.len() == high.len() && low[..range_column] == high[..range_column],
        _ => false,
    };
    if mixed_directions && !pinned_prefix {
        return Err(Error::Constraint(format!(
            "index_range: index {index} names columns of differing sort directions, so both \
             bounds must pin the same leading values and compare on the last column"
        )));
    }

    let encode_bound = |bound: Bound<Vec<IndexKeyValue>>| -> Result<Bound<_>> {
        let encode = |values: Vec<IndexKeyValue>| {
            encode_ordered_values(
                &values.into_iter().map(Some).collect::<Vec<_>>(),
                &info.directions,
                &info.nulls,
            )
        };
        Ok(match bound {
            Bound::Included(values) => Bound::Included(encode(values)?),
            Bound::Excluded(values) => Bound::Excluded(encode(values)?),
            Bound::Unbounded => Bound::Unbounded,
        })
    };

    let descending = info.directions.get(range_column).copied() == Some(Direction::Descending);
    let (byte_lower, byte_upper) = if descending {
        (upper, lower)
    } else {
        (lower, upper)
    };
    Ok((encode_bound(byte_lower)?, encode_bound(byte_upper)?))
}
