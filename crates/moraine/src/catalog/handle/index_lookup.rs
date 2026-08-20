//! Equality, range, and NULL index lookups.

use std::{
    ops::Bound,
    time::{Duration, Instant},
};

use futures::{StreamExt, stream::FuturesUnordered};
use tracing::debug;

use super::ReadOnlyCatalog;
use crate::{
    catalog::{CatalogSnapshot, IndexId, IndexInfo, IndexState, TableId},
    error::{Error, Result},
    store::{
        cache::{CacheTally, ObjectStoreTally},
        handle::{ReadHandle, ScanOrder},
        index_encoding::{
            CanonicalKey, Direction, IndexKeyValue, NullOrder, encode_ordered_values,
        },
    },
    telemetry::milliseconds,
    transaction::index_maintenance,
};

/// Exact probes kept in flight by one batched index lookup.
const INDEX_LOOKUP_CONCURRENCY: usize = 512;

#[derive(Default)]
struct LookupMetrics {
    unique_keys: u64,
    head: Duration,
    probe_window: Duration,
    probe_service: Duration,
    hits: u64,
    misses: u64,
    peak_in_flight: u64,
}

struct LookupResolution {
    row_ids: Vec<u64>,
    metrics: LookupMetrics,
}

async fn resolve_encoded(
    handle: ReadHandle<'_>,
    tail: Option<&moraine_wal::Overlay>,
    index_id: u64,
    unique: bool,
    encoded: Vec<CanonicalKey>,
) -> Result<LookupResolution> {
    let mut metrics = LookupMetrics {
        unique_keys: u64::try_from(encoded.len()).unwrap_or(u64::MAX),
        ..LookupMetrics::default()
    };
    let mut pending = encoded.into_iter();
    let mut probes = FuturesUnordered::new();
    let mut row_ids = Vec::new();
    let mut first_probe = None;
    let mut last_completion = None;
    let mut exhausted = false;

    loop {
        while !exhausted && probes.len() < INDEX_LOOKUP_CONCURRENCY {
            let Some(key) = pending.next() else {
                exhausted = true;
                break;
            };
            first_probe.get_or_insert_with(Instant::now);
            probes.push(async move {
                let started = Instant::now();
                let found =
                    index_maintenance::lookup_row_ids(handle, tail, index_id, unique, &key).await;
                (found, started.elapsed(), Instant::now())
            });
        }
        metrics.peak_in_flight = metrics
            .peak_in_flight
            .max(u64::try_from(probes.len()).unwrap_or(u64::MAX));

        let Some((found, service, completed)) = probes.next().await else {
            break;
        };
        let found = found?;
        metrics.probe_service = metrics.probe_service.saturating_add(service);
        last_completion = Some(completed);
        if found.is_empty() {
            metrics.misses = metrics.misses.saturating_add(1);
        } else {
            metrics.hits = metrics.hits.saturating_add(1);
            row_ids.extend(found);
        }
    }

    if let (Some(first), Some(last)) = (first_probe, last_completion) {
        metrics.probe_window = last.saturating_duration_since(first);
    }
    row_ids.sort_unstable();
    row_ids.dedup();
    Ok(LookupResolution { row_ids, metrics })
}

fn log_lookup(
    table: TableId,
    index: IndexId,
    requested_keys: usize,
    elapsed: Duration,
    metrics: &LookupMetrics,
    cache: CacheTally,
    store: ObjectStoreTally,
) {
    debug!(
        table_id = table.get(),
        index_id = index.get(),
        lookup_keys = requested_keys,
        lookup_unique_keys = metrics.unique_keys,
        lookup_ms = milliseconds(elapsed),
        lookup_head_ms = milliseconds(metrics.head),
        lookup_probe_window_ms = milliseconds(metrics.probe_window),
        lookup_probe_service_ms = milliseconds(metrics.probe_service),
        lookup_hits = metrics.hits,
        lookup_misses = metrics.misses,
        lookup_peak_in_flight = metrics.peak_in_flight,
        lookup_metadata_hits = cache.metadata_hits,
        lookup_metadata_misses = cache.metadata_misses,
        lookup_block_hits = cache.block_hits,
        lookup_block_misses = cache.block_misses,
        lookup_cache_errors = cache.errors,
        lookup_gets = store.main_gets,
        lookup_get_ms = milliseconds(store.main_get_duration),
        lookup_store_errors = store.errors,
        "index lookup resolved"
    );
}

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
        let started = Instant::now();
        let cache_before = self.cache_tally();
        let store_before = self.object_store_tally();
        // The probe materializes head and unfolded tail as one cut, so the
        // entries and the catalog they resolve against agree without a
        // stable-read retry.
        let head_started = Instant::now();
        let read = self.begin_probe().await?;
        let head = head_started.elapsed();

        let index_row_ids = async {
            let info = ready_index(read.view(), table, index)?;

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

            let mut resolution = resolve_encoded(
                read.handle(),
                read.tail(),
                index.get(),
                info.unique,
                encoded,
            )
            .await?;
            resolution.metrics.head = head;
            Ok(resolution)
        }
        .await;
        read.finish().await;

        match index_row_ids {
            Ok(resolution) => {
                log_lookup(
                    table,
                    index,
                    keys.len(),
                    started.elapsed(),
                    &resolution.metrics,
                    self.cache_tally().since(cache_before),
                    self.object_store_tally().since(store_before),
                );
                Ok(resolution.row_ids)
            }
            Err(error) => Err(error),
        }
    }

    /// Resolves a comparison query to the rows whose indexed value falls
    /// between `lower` and `upper` (`<`, `<=`, `>`, `>=`, `BETWEEN`, and their
    /// half-open forms via [`Bound::Unbounded`]). Each bound names the leading
    /// columns' values; equality is the degenerate closed `[v, v]` range.
    ///
    /// Head-only and candidate-returning, exactly like
    /// [`index_lookup`](Self::index_lookup). Results are in the index's
    /// stored order, or its exact opposite when `reverse` is set.
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
        let read = self.begin_probe().await?;

        let outcome = async {
            let info = ready_index(read.view(), table, index)?;

            let (byte_lower, byte_upper) = encode_range_bounds(&info, index, lower, upper)?;
            let leading_nulls = info.nulls.first().copied().unwrap_or(NullOrder::Last);

            index_maintenance::range_row_ids(
                read.handle(),
                read.tail(),
                index.get(),
                info.unique,
                leading_nulls,
                byte_lower,
                byte_upper,
                ScanOrder::from_reverse(reverse),
            )
            .await
        }
        .await;
        read.finish().await;

        outcome
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
        let read = self.begin_probe().await?;

        let outcome = async {
            let info = ready_index(read.view(), table, index)?;

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

            index_maintenance::null_prefix_row_ids(
                read.handle(),
                read.tail(),
                index.get(),
                &key,
                ScanOrder::from_reverse(reverse),
            )
            .await
        }
        .await;
        read.finish().await;

        outcome
    }
}

/// The index `index` of `table` in `view`, once it is ready to serve.
fn ready_index(view: &CatalogSnapshot, table: TableId, index: IndexId) -> Result<IndexInfo> {
    let info = view
        .index_by_id(table, index)
        .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;

    match info.state {
        IndexState::Ready => Ok(info),
        IndexState::Building | IndexState::Maintaining => Err(Error::IndexBuilding(format!(
            "index {index} is still building"
        ))),
        IndexState::Poisoned => Err(Error::NotFound(format!("index {index} was poisoned"))),
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
