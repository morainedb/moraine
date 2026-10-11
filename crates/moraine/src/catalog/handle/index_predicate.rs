//! Index selection for conjunctive SQL predicates.

use std::ops::Bound;

use super::{ReadOnlyCatalog, index_lookup::encode_range_bounds, index_probe_cache::Probe};
use crate::store::index_encoding::encode_ordered_values;

/// Maximum candidates worth collecting during query planning.
const MAX_PREDICATE_ROWS: usize = 4096;
use crate::{CatalogSnapshot, ColumnId, IndexInfo, IndexKeyValue, IndexState, Result, TableId};

/// A supported predicate on one stored column.
#[derive(Clone, Debug)]
pub struct IndexPredicate {
    /// Stable catalog column identifier.
    pub column: ColumnId,
    /// The comparison applied to that column.
    pub comparison: IndexComparison,
}

/// Comparisons an index can use to narrow a scan.
#[derive(Clone, Debug)]
pub enum IndexComparison {
    /// Equality to any listed value. NULL keys have already been removed.
    Any(Vec<IndexKeyValue>),
    /// SQL `IS NULL`.
    IsNull,
    /// A lower bound, inclusive when the boolean is true.
    Lower(IndexKeyValue, bool),
    /// An upper bound, inclusive when the boolean is true.
    Upper(IndexKeyValue, bool),
}

/// Candidate rows returned by an automatically selected index.
pub struct IndexPredicateRows {
    /// Stable row identifiers; the caller retains residual predicates.
    pub rows: Vec<u64>,
    /// All supplied predicates were answered exactly by a certified index.
    pub certified_exact: bool,
}

struct Plan {
    index: IndexInfo,
    prefixes: Vec<Vec<Option<IndexKeyValue>>>,
    used: usize,
    lower: Option<(IndexKeyValue, bool)>,
    upper: Option<(IndexKeyValue, bool)>,
}

fn plan(index: IndexInfo, predicates: &[IndexPredicate]) -> Option<Plan> {
    let mut result = Plan {
        index,
        prefixes: vec![Vec::new()],
        used: 0,
        lower: None,
        upper: None,
    };
    for column in &result.index.columns {
        let comparisons: Vec<_> = predicates
            .iter()
            .filter(|p| p.column == *column)
            .map(|p| &p.comparison)
            .collect();
        let values = comparisons.iter().find_map(|comparison| match comparison {
            IndexComparison::Any(values) => {
                Some(values.iter().cloned().map(Some).collect::<Vec<_>>())
            }
            IndexComparison::IsNull => Some(vec![None]),
            _ => None,
        });
        if let Some(values) = values {
            // Bound expansion of independent IN lists across composite columns.
            if result.prefixes.len().saturating_mul(values.len()) > 4096 {
                break;
            }
            result.prefixes = result
                .prefixes
                .into_iter()
                .flat_map(|prefix| {
                    values.iter().map(move |value| {
                        let mut key = prefix.clone();
                        key.push(value.clone());
                        key
                    })
                })
                .collect();
            result.used += 1;
            continue;
        }
        // A NULL prefix can still prune, but cannot serve an ordered range.
        if result
            .prefixes
            .iter()
            .any(|prefix| prefix.iter().any(Option::is_none))
        {
            break;
        }
        for comparison in comparisons {
            match comparison {
                IndexComparison::Lower(value, inclusive) if result.lower.is_none() => {
                    result.lower = Some((value.clone(), *inclusive));
                    result.used += 1;
                }
                IndexComparison::Upper(value, inclusive) if result.upper.is_none() => {
                    result.upper = Some((value.clone(), *inclusive));
                    result.used += 1;
                }
                _ => {}
            }
        }
        break;
    }
    // Unique indexes store NULL-bearing keys separately. An incomplete
    // non-null prefix cannot omit that region when trailing columns are free.
    let range_columns = usize::from(result.lower.is_some() || result.upper.is_some());
    if result.index.unique
        && result.prefixes.iter().any(|prefix| {
            prefix.iter().all(Option::is_some)
                && prefix.len() + range_columns < result.index.columns.len()
        })
    {
        return None;
    }
    (result.used > 0).then_some(result)
}

fn range_bound(
    prefix: &[IndexKeyValue],
    edge: Option<&(IndexKeyValue, bool)>,
) -> Bound<Vec<IndexKeyValue>> {
    match edge {
        Some((value, inclusive)) => {
            let mut values = prefix.to_vec();
            values.push(value.clone());
            if *inclusive {
                Bound::Included(values)
            } else {
                Bound::Excluded(values)
            }
        }
        None if prefix.is_empty() => Bound::Unbounded,
        None => Bound::Included(prefix.to_vec()),
    }
}

impl ReadOnlyCatalog {
    /// Selects a ready index for a conjunction, returning candidates or `None`.
    /// Retain the original predicates unless `certified_exact` is true.
    /// Call on a pinned read scope with that scope's snapshot. Probes that
    /// exceed 4,096 entries decline before row location.
    ///
    ///
    /// ```no_run
    /// # async fn example(catalog: &moraine::Catalog, table: moraine::TableId,
    /// # column: moraine::ColumnId) -> moraine::Result<()> {
    /// if let Some(scope) = catalog.index_read_scope().await? {
    ///     let snapshot = scope.reads().snapshot().await?;
    ///     let predicate = moraine::IndexPredicate {
    ///         column,
    ///         comparison: moraine::IndexComparison::IsNull,
    ///     };
    ///     let candidates = scope
    ///         .reads()
    ///         .index_predicate_rows(&snapshot, table, &[predicate])
    ///         .await?;
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    /// Returns lookup errors when a selected index cannot be read.
    pub async fn index_predicate_rows(
        &self,
        snapshot: &CatalogSnapshot,
        table: TableId,
        predicates: &[IndexPredicate],
    ) -> Result<Option<IndexPredicateRows>> {
        let Some(plan) = snapshot
            .indexes_of(table)
            .into_iter()
            .filter(|index| index.state == IndexState::Ready)
            .filter_map(|index| plan(index, predicates))
            .max_by_key(|plan| plan.used)
        else {
            return Ok(None);
        };
        let full_keys = plan.lower.is_none()
            && plan.upper.is_none()
            && plan.prefixes.iter().all(|prefix| {
                prefix.len() == plan.index.columns.len() && prefix.iter().all(Option::is_some)
            });
        let certified_exact =
            full_keys && plan.used == predicates.len() && plan.index.verified_at_snapshot.is_some();
        let probes = if full_keys {
            vec![Probe::Many(
                plan.prefixes
                    .iter()
                    .map(|prefix| {
                        encode_ordered_values(prefix, &plan.index.directions, &plan.index.nulls)
                    })
                    .collect::<Result<Vec<_>>>()?,
            )]
        } else {
            plan.prefixes
                .into_iter()
                .map(|prefix| {
                    if prefix.iter().any(Option::is_none) {
                        Ok(Probe::Nulls(
                            encode_ordered_values(
                                &prefix,
                                &plan.index.directions,
                                &plan.index.nulls,
                            )?,
                            false,
                        ))
                    } else {
                        let prefix = prefix.into_iter().flatten().collect::<Vec<_>>();
                        let (lower, upper) = encode_range_bounds(
                            &plan.index,
                            plan.index.id,
                            range_bound(&prefix, plan.lower.as_ref()),
                            range_bound(&prefix, plan.upper.as_ref()),
                        )?;
                        Ok(Probe::Range(lower, upper, false))
                    }
                })
                .collect::<Result<Vec<_>>>()?
        };
        let mut rows = Vec::new();
        for probe in probes {
            rows.extend(
                self.probe_limited(
                    table,
                    plan.index.id,
                    probe,
                    MAX_PREDICATE_ROWS + 1 - rows.len(),
                )
                .await?,
            );
            if rows.len() > MAX_PREDICATE_ROWS {
                return Ok(None);
            }
        }
        rows.sort_unstable();
        rows.dedup();
        Ok(Some(IndexPredicateRows {
            rows,
            certified_exact,
        }))
    }
}

#[cfg(test)]
mod tests;
