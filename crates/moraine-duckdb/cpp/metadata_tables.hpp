// Synthesized DuckLake metadata tables, with column shapes matching its catalog schema.
#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "duckdb.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "moraine_abi.h"

namespace moraine_duckdb {

// SQL rows in column-declaration order.
using MetadataRows = std::vector<std::vector<duckdb::Value>>;

// A declared column; ducklake_type is parsed by MapColumnType.
struct MetadataColumnSpec {
	const char *name;
	const char *ducklake_type;
	bool not_null;
};

// Read and shape committed rows, forwarding the cancellation probe to the core.
using MetadataRowProvider = MetadataRows (*)(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                             void *probe_ctx);

// As `MetadataRowProvider`, for one table's rows of a narrowable kind, in
// the order the whole-kind provider emits them.
using MetadataScopedRowProvider = MetadataRows (*)(MoraineCatalogHandle *handle, uint64_t table_id,
                                                   MoraineInterruptProbe probe, void *probe_ctx);

// `moraine_tx_stage`'s "not writable" `table_kind` sentinel (moraine_abi.h),
// mirrored here so this spec and the staged-write Sink (staged_write.cpp)
// share one source of truth.
constexpr int32_t kNotWritable = -1;

// `moraine_tx_dump_*_of`'s "every version" `filter_snapshot` sentinel
// (moraine_abi.h), mirrored here. Zero is a real snapshot id, so the
// sentinel is the one value no snapshot can carry.
constexpr uint64_t kEveryVersion = UINT64_MAX;

// Accept inline-registry INSERTs without staging them twice: the paired CREATE TABLE
// already stages the registration.
constexpr int32_t kVoidInsertable = -2;

// As `MetadataRowProvider`, through an open staged transaction: the rows it
// sees at its pinned read point, its own staged rows overlaid. `handle` and
// the probe pair reach only `ducklake_metadata`'s provider, whose fixed rows
// are store facts no transaction stages.
using MetadataTxRowProvider = std::function<MetadataRows(MoraineTxHandle *tx, MoraineCatalogHandle *handle,
                                                         MoraineInterruptProbe probe, void *probe_ctx)>;

// As `MetadataTxRowProvider`, for a reader that keeps a row only while
// `live_bound < end_snapshot` (or it is null): the ended half is skipped
// once `live_bound` reaches the transaction's read point.
using MetadataTxLiveRowProvider = std::function<MetadataRows(MoraineTxHandle *tx, uint64_t live_bound)>;

// As `MetadataTxRowProvider`, narrowed to one table and, unless
// `filter_snapshot` is `kEveryVersion`, to the versions live at it.
using MetadataTxScopedRowProvider =
    std::function<MetadataRows(MoraineTxHandle *tx, uint64_t table_id, uint64_t filter_snapshot)>;

// Staged readers share one row shape. Unsupported readers are empty.
struct MetadataTxProviders {
	MetadataTxRowProvider provider;
	MetadataTxLiveRowProvider live_provider;
	MetadataTxScopedRowProvider scoped_provider;
};

struct MetadataTableSpec {
	const char *name;
	std::vector<MetadataColumnSpec> columns;
	MetadataRowProvider provider;
	// The readers a scan inside a write transaction goes through, so it
	// observes the rows that transaction has staged: DuckLake's expiry and
	// cleanup cascades re-read these tables after staging their own deletes.
	MetadataTxProviders tx;
	// Staged ABI table kind, or a sentinel for read-only/no-op projections.
	int32_t write_table_kind = kNotWritable;
	// Lifecycle-update key columns in ABI decoder order, which can differ from
	// declaration order. Empty means end_snapshot UPDATE is unsupported.
	std::vector<duckdb::idx_t> end_key_columns;
	// Lifecycle column; meaningful only when end_key_columns is nonempty.
	duckdb::idx_t end_snapshot_column = 0;
	// Delete key columns in ABI decoder order. Empty rejects matching deletes.
	std::vector<duckdb::idx_t> delete_key_columns;
	// Whether an UPDATE with an arbitrary SET list overlays the row in
	// place (the unversioned statistics kinds only). Distinct from
	// `delete_key_columns`: reclamation gave most kinds a delete key, but
	// overlay updates stay a statistics-table convention.
	bool overlay_updatable = false;
	// Table ID column for scoped dumps, or -1 to materialize the whole kind.
	int32_t scope_column = -1;
	// Whether a scan of this kind may be narrowed to the versions live at
	// a filter's snapshot, skipping the ended half. Set on every versioned
	// kind the core exposes a live-bounded transactional dump for.
	bool live_narrowable = false;
	// Materializes one table's rows. Set exactly where `scope_column` is;
	// without it a narrowable scan falls back to the whole kind.
	//
	// Preserve field order: table specs use positional initializers.
	MetadataScopedRowProvider scoped_provider = nullptr;
};

// Cache rows for this transaction so scans and staged writes resolve the same row IDs.
// Opening or ending a staged transaction clears the cache; staging invalidates the affected table.
// A live_bound narrows versioned reads only through an open staged transaction.
std::shared_ptr<const MetadataRows> MetadataRowsFor(duckdb::ClientContext &context, duckdb::Catalog &catalog,
                                                    const MetadataTableSpec &spec,
                                                    duckdb::optional_idx live_bound = duckdb::optional_idx());

// Cache one table's rows, including staged changes, keyed by table_id.
// Fall back to MetadataRowsFor when a scoped provider is unavailable.
std::shared_ptr<const MetadataRows> ScopedMetadataRowsFor(duckdb::ClientContext &context, duckdb::Catalog &catalog,
                                                          const MetadataTableSpec &spec, uint64_t table_id,
                                                          duckdb::optional_idx live_bound = duckdb::optional_idx());

// Whether this store records the catalog version whose shape adds
// 1.1's columns and tables. One read of the store's recorded version.
bool MoraineServesExtendedCatalog(MoraineCatalogHandle *handle);

// The synthesized tables, in the order they're registered. `extended`
// selects the shape catalog version 1.1 declares. Built once per
// shape; returns the same static instance every call for a given shape.
const std::vector<MetadataTableSpec> &MoraineMetadataTableSpecs(bool extended = false);

// A synthesized metadata table. Scans materialize rows through its spec;
// staged writes use the same spec to decode row IDs and columns.
class MoraineMetadataTableEntry : public duckdb::TableCatalogEntry {
public:
	MoraineMetadataTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
	                          duckdb::CreateTableInfo &info, const MetadataTableSpec &spec,
	                          MoraineCatalogHandle *handle);

	duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(duckdb::ClientContext &context,
	                                                         duckdb::column_t column_id) override;
	duckdb::TableFunction GetScanFunction(duckdb::ClientContext &context,
	                                      duckdb::unique_ptr<duckdb::FunctionData> &bind_data) override;
	duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext &context) override;

	// Exposed for the staged-write path (staged_write.cpp): the column
	// shape and `table_kind` needed to translate an incoming DataChunk row
	// into a `moraine_tx_stage` call, and the catalog handle
	// `moraine_tx_begin` opens against.
	const MetadataTableSpec &Spec() const {
		return spec_;
	}
	MoraineCatalogHandle *Handle() const {
		return handle_;
	}

private:
	const MetadataTableSpec &spec_;
	MoraineCatalogHandle *handle_;

public:
	// 2.0's `TableCatalogEntry` declares `GetColumns` pure and stores no
	// columns of its own, so every entry owns its list.
	const duckdb::ColumnList &GetColumns() const override {
		return columns_;
	}

private:
	duckdb::ColumnList columns_;
};

// Builds a `MoraineMetadataTableEntry` for every table in
// `MoraineMetadataTableSpecs()` and adds it to `tables` (keyed by name, via
// `emplace` — a same-named entry already present wins, never overwritten).
void PopulateMetadataTables(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, MoraineCatalogHandle *handle,
                            duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CatalogEntry>> &tables);

// Bind data for a metadata-shaped scan: every row is materialized up front
// (these tables are metadata/inline-registry sized, not data-sized). Shared
// by the synthesized `ducklake_*` tables (this file) and the dynamic
// inline-table entries (inline_tables.cpp), which scan the same way.
struct MetadataScanBindData : public duckdb::FunctionData {
	// Shared, never copied: a scan emitting row ids registers this exact
	// list with its transaction, and the staged-write Sink resolves each id
	// back into it.
	//
	// Null when `spec` names a narrowable kind: that scan materializes in
	// `InitGlobal` instead, because the filter deciding how much to build
	// has not been pushed down yet at bind.
	std::shared_ptr<const MetadataRows> rows;
	// The spec this scan reads and the catalog it reads through. Null for
	// the inline-table entries, whose rows have no spec and whose Sinks
	// resolve row ids their own way.
	//
	// A raw pointer, not `optional_ptr`: `InitGlobal` reads the bind data as
	// const and must still reach a mutable catalog. Bind data outlives its
	// bind (a prepared statement holds it across a DETACH), so `InitGlobal`
	// re-resolves `catalog_name` and compares before dereferencing; the
	// pointer is an identity, never a borrow.
	const MetadataTableSpec *spec = nullptr;
	duckdb::Catalog *catalog = nullptr;
	std::string catalog_name;
	// The snapshot a `live_bound < end_snapshot OR end_snapshot IS NULL`
	// filter pinned this scan to, set by `pushdown_complex_filter` before
	// `InitGlobal` runs. Unset means the scan reads every version.
	duckdb::optional_idx live_bound;
	// The `table_id` an equality filter pinned this scan to, set by
	// `pushdown_complex_filter` before `InitGlobal` runs. Unset means the
	// scan reads its kind whole.
	duckdb::optional_idx scope;
	// The synthesized entry this scan reads, exposed through the table
	// function's `get_bind_info` so `LogicalGet::GetTable()` resolves it:
	// the binder's UPDATE/DELETE paths require a resolvable base table.
	duckdb::optional_ptr<duckdb::TableCatalogEntry> table_entry;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override;
	bool Equals(const duckdb::FunctionData &other) const override;
};

// Builds the reusable eager-materialized-rows TableFunction. No `bind`
// callback (as in `MoraineScanFunction`, scan.hpp): the caller already
// produces complete `MetadataScanBindData` itself.
duckdb::TableFunction MetadataScanTableFunction();

} // namespace moraine_duckdb
