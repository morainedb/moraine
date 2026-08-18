// Synthesized `ducklake_*` catalog tables: DuckLake's metadata connection
// speaks generic SQL against these, not the store's real user tables. Each
// column shape matches the corresponding DuckLake `CREATE TABLE`.
// `ducklake_metadata` is the one exception: it has no store-modeled source
// of truth, so its rows are synthesized in-process rather than read from the
// dump ABI (see metadata_tables.cpp).
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "duckdb.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "moraine_abi.h"

namespace moraine_duckdb {

// Every row of one synthesized `ducklake_*` table, each row's cells in
// column-declaration order.
using MetadataRows = std::vector<std::vector<duckdb::Value>>;

// One column of a synthesized `ducklake_*` table: `ducklake_type` is a
// DuckLake column-type string (fed through the existing `MapColumnType`,
// same as real user-table columns) so both paths share one type mapper.
struct MetadataColumnSpec {
	const char *name;
	const char *ducklake_type;
	bool not_null;
};

// Fetches every row of one `ducklake_*` table, already converted to typed
// `duckdb::Value`s in column-declaration order. Reads through the
// dump ABI for store-backed tables, forwarding the probe pair so a blocked
// read cancels; `ducklake_metadata`'s provider ignores `handle` and the
// probe pair and returns fixed rows instead (see metadata_tables.cpp).
using MetadataRowProvider = MetadataRows (*)(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                             void *probe_ctx);

// `moraine_tx_stage`'s "not writable" `table_kind` sentinel (moraine_abi.h),
// mirrored here so this spec and the staged-write Sink (staged_write.cpp)
// share one source of truth.
constexpr int32_t kNotWritable = -1;

// `ducklake_inlined_data_tables`'s sentinel: DuckLake's own inlined-table
// registration batch always pairs `INSERT INTO ducklake_inlined_data_tables
// VALUES (...)` with the `CREATE TABLE ducklake_inlined_data_<t>_<v>(...)`
// this shim intercepts (inline_tables.cpp's `CreateInlineDataTable`, which
// already stages `inline/schema` — the source `ProvideInlinedDataTables`
// projects from). The INSERT still has to be *accepted* (this table is a
// real read projection, not `kNotWritable`), but staging it too would
// double-register; it lands here as a no-op instead.
constexpr int32_t kVoidInsertable = -2;

struct MetadataTableSpec {
	const char *name;
	std::vector<MetadataColumnSpec> columns;
	MetadataRowProvider provider;
	// `moraine_tx_stage`'s `table_kind` for this table, or `kNotWritable`
	// for the always-empty stand-ins and `ducklake_metadata` (writes to
	// those are out of scope this slice — DDL/unsupported-DML naming the
	// statement kind, per PlanInsert's NotImplementedException).
	int32_t write_table_kind = kNotWritable;
	// Indices into `columns` of the ended row's entity-key columns, in
	// exactly the order the staged ABI's update-set-end decoder consumes
	// them — NOT necessarily this table's declared column order (e.g.
	// `ducklake_column`'s key is read as `table_id` (col 3) then
	// `column_id` (col 0)). Non-empty only for the six versioned kinds;
	// `UPDATE ... SET end_snapshot` against any other table is not
	// translatable and throws at plan time.
	std::vector<duckdb::idx_t> end_key_columns;
	// Index into `columns` of `end_snapshot`; meaningful only when
	// `end_key_columns` is non-empty. Verifies an UPDATE's single SET
	// target is exactly the lifecycle column — the one interpreted
	// convention on the staged-row path.
	duckdb::idx_t end_snapshot_column = 0;
	// Indices into `columns` of a removed row's key columns, in exactly
	// the order the staged ABI's raw-delete decoder consumes them. Empty
	// means raw DELETEs are not translatable for this table: they plan as
	// void-deletes that throw if a row ever actually matches.
	std::vector<duckdb::idx_t> delete_key_columns;
	// Whether an UPDATE with an arbitrary SET list overlays the row in
	// place (the unversioned statistics kinds only). Distinct from
	// `delete_key_columns`: reclamation gave most kinds a delete key, but
	// overlay updates stay a statistics-table convention.
	bool overlay_updatable = false;
	// Index into `columns` of the `table_id` a scan may narrow to, or -1 for
	// a kind always materialized whole. Set only where the dump is large
	// enough to be worth narrowing *and* keyed by that column first, so one
	// table's rows are a contiguous run of the whole and narrowing does not
	// renumber them — `ducklake_file_column_stats` alone.
	//
	// Last, and it must stay last: these specs are initialized positionally,
	// so a field added anywhere earlier silently shifts every entry that
	// relies on declaration order.
	int32_t scope_column = -1;
};

// The rows of `spec` as the calling DuckDB transaction sees them. Every
// reader of one table in one transaction gets one list, which is what makes
// a rowid mean something: the scan emits an index into it and the
// staged-write Sink (staged_write.cpp) resolves that index against the same
// list, rather than against a second materialization that a commit landing
// in between could have reordered.
//
// Which list depends on whether the transaction has opened a staged tx:
//
//   - **Before it has** — a reader — each table is dumped once and held for
//     the transaction. This is what a plain read needs: DuckLake resolves
//     its snapshot by reading `ducklake_snapshot` twice in one statement
//     (`WHERE snapshot_id = (SELECT MAX(snapshot_id) ...)`), and two dumps
//     observe two heads (dumps.rs), so a commit or an expiry landing
//     between them yields a maximum the other half has never heard of —
//     which DuckLake reports as "No snapshot found".
//   - **After it has** — a writer — every read goes to the staged tx, whose
//     own read point pins it and whose dumps overlay the rows staged so
//     far. That read point holds for the tx's life, so a dump is held here
//     too, and dropped by the only two things that can outdate it: staging
//     into the same table, which moves its overlay, and the tx ending,
//     which retires its read point. A commit attempt takes the staged tx,
//     so the retry DuckLake drives between attempts re-reads through a
//     fresh one — never the premise its last attempt lost on.
//
// One limit remains: tables are pinned independently, each at the head its
// first scan observed, so a statement joining two of them can still
// straddle a commit.
std::shared_ptr<const MetadataRows> MetadataRowsFor(duckdb::ClientContext &context, duckdb::Catalog &catalog,
                                                    const MetadataTableSpec &spec);

// One table's rows of a narrowable `spec`.
//
// Uncached, unlike `MetadataRowsFor`: a narrowed set is small, built once per
// statement, and keyed by a value the transaction's cache is not keyed by.
// Falls back to the whole set when the kind cannot be narrowed, or mid-write,
// where only the unscoped dump carries the staged overlay a read then owes.
std::shared_ptr<const MetadataRows> ScopedMetadataRowsFor(duckdb::ClientContext &context, duckdb::Catalog &catalog,
                                                          const MetadataTableSpec &spec, uint64_t table_id);

// The fixed list of synthesized tables, in the order they're registered.
// Built once; returns the same static instance every call.
const std::vector<MetadataTableSpec> &MoraineMetadataTableSpecs();

// A synthesized `ducklake_*` table entry: pure read, materializes every row
// up front (metadata-sized, not data-sized) at scan time via `spec`'s
// provider.
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
