// The staged-row write path: `duckdb::PhysicalOperator`s that translate
// every row a DuckDB DML statement feeds them into `moraine_tx_stage`
// calls against the DuckDB transaction's lazily-opened staged tx
// (transaction_manager.hpp's `MoraineTransaction::StagedTx`).
//
// Sink-chunk layouts the operators depend on:
//
// - INSERT: the child chunk is the row's values in declared column order
//   (`LogicalInsert::expected_types`).
// - UPDATE: the child is a projection emitting [one column per SET
//   expression (in `LogicalUpdate::columns` order), any extra columns
//   constraints demanded, then the row-id column(s) appended last].
//   `LogicalUpdate::expressions[i]` is (after column-binding resolution) a
//   `BoundReferenceExpression` into that chunk, or a `BoundDefaultExpression`
//   for `SET col = DEFAULT` (which DuckLake never issues — rejected at plan
//   time). The row id is the last column.
// - DELETE: the child chunk is the scan/filter output;
//   `LogicalDelete::expressions[0]` is a `BoundReferenceExpression` whose
//   `.index` locates the row-id column in it.
//
// Row identity: these tables have no physical row ids, so the base
// `TableCatalogEntry::GetRowIdColumns()` default applies — one virtual
// `rowid` BIGINT — and the metadata scan serves it as the row's index into
// the scan's materialized row set (metadata_tables.cpp). The UPDATE/DELETE
// Sinks resolve that index back to the row's key cells by re-materializing
// the same provider: deterministic for a fixed committed head, which cannot
// move between a statement's scan and its Sink in any supported topology
// (one metadata connection per attach, statements sequential; a second
// writer is excluded by the store's single-writer fencing).
#pragma once

#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb.hpp"

#include "moraine_abi.h"

namespace duckdb {
class LogicalDelete;
class LogicalUpdate;
} // namespace duckdb

namespace moraine_duckdb {

struct MetadataTableSpec;

// Builds the physical operator `MoraineCatalog::PlanInsert` returns for an
// INSERT into a writable `MoraineMetadataTableEntry` (`spec.write_table_kind
// != kNotWritable`): a Sink-only translator feeding rows to
// `moraine_tx_stage`, dual-rooted as a Source emitting the DuckDB-standard
// one-row `Count` result once Sink's input is exhausted. `op.table.catalog`
// supplies the `MoraineCatalog` the Sink fetches the active
// `MoraineTransaction` (and its lazily-opened staged tx) from at
// execution time — the operator itself is stateless, per DuckDB's Sink
// convention.
duckdb::PhysicalOperator &PlanMetadataInsert(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalInsert &op,
                                             const MetadataTableSpec &spec);

// The UPDATE operator, same Sink+Source shape. Exactly two UPDATE forms
// are translatable — the only two DuckLake issues against its metadata
// catalog:
// - `SET end_snapshot = <v>` on a versioned kind (`spec.end_key_columns`
//   non-empty): staged as the update-set-end lifecycle op, the one
//   interpreted convention on this path.
// - any SET subset on an unversioned statistics kind
//   (`spec.delete_key_columns` non-empty): staged as an insert of the full
//   updated row (the old row overlaid with the SET values), which the core
//   applies as the in-place overwrite unversioned kinds define.
// Anything else throws NotImplementedException at plan time.
duckdb::PhysicalOperator &PlanMetadataUpdate(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalUpdate &op,
                                             const MetadataTableSpec &spec);

// The DELETE operator, same Sink+Source shape. Translatable only for the
// three unversioned statistics kinds (`spec.delete_key_columns`
// non-empty) — the only raw DELETEs the staged-row contract defines; a
// DELETE against a versioned kind (snapshot-expiry cleanup, deferred this
// slice) throws NotImplementedException at plan time.
duckdb::PhysicalOperator &PlanMetadataDelete(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalDelete &op,
                                             const MetadataTableSpec &spec);

} // namespace moraine_duckdb
