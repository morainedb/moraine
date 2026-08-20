// A duckdb::Catalog backed by moraine over the C ABI in moraine_abi.h.
// Translate-only: every callback turns a listing-ABI call into a DuckDB
// catalog entry, or (every write path, out of scope this slice) throws
// duckdb::NotImplementedException.
#pragma once

#include "duckdb.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/logging/logging.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "maintenance.hpp"
#include "metadata_tables.hpp"
#include "moraine_abi.h"

#include <mutex>
#include <unordered_map>

namespace moraine_duckdb {

// Maps a DuckLake column-type string (e.g. "BIGINT", "DECIMAL(18,3)") to a
// DuckDB LogicalType. Scalar types only this slice; an unrecognized or
// nested type throws duckdb::NotImplementedException naming the type
// string verbatim.
duckdb::LogicalType MapColumnType(const std::string &ducklake_type);

// Translates a MoraineError into the matching DuckDB exception (NotFound/
// AlreadyExists/Constraint -> CatalogException, CommitConflict ->
// TransactionException, Corruption/Store/internal -> IOException/
// InternalException) and throws it. Frees `err.message` first if non-null.
[[noreturn]] void ThrowMoraineError(MoraineError &err);

// Throws a MORAINE_MIGRATION refusal from an attach of `path`, naming the
// SQL function that repairs it. The core diagnoses the store and names its
// own Rust verb; only the attach holds the path, and only the shim may name
// a DuckDB function, so the two halves of the remedy meet here. Frees
// `err.message` first if non-null.
[[noreturn]] void ThrowMigrationRefusal(MoraineError &err, const std::string &path);

// Drains the core's buffered `tracing` events into DuckDB's logger under
// the `moraine` log type, so they surface in `duckdb_logs`. Events are
// emitted on the core's own worker threads, where no ClientContext is in
// scope; this runs on the calling thread, which has one. Safe to call when
// nothing was buffered, and never throws — losing a diagnostic must not
// fail the operation that produced it.
void DrainMoraineLogs(duckdb::ClientContext &context) noexcept;

// The same drain through a database-scoped logger, for callers with no
// ClientContext — the maintenance scheduler runs on a thread of its own.
void DrainMoraineLogs(duckdb::DatabaseInstance &db) noexcept;

// Writes one shim-originated record under the `moraine` log type through a
// database-scoped logger. For the shim's own diagnostics (e.g. a failed
// maintenance step); core diagnostics arrive via DrainMoraineLogs instead.
void WriteMoraineLog(duckdb::DatabaseInstance &db, duckdb::LogLevel level, const std::string &message) noexcept;

// The shim's MoraineInterruptProbe: reports whether the query driving
// `client_context` (an opaque duckdb::ClientContext*) has been
// interrupted. One atomic load — the same flag DuckDB's executor polls —
// so it is safe from any thread. C linkage to match the ABI's C function
// pointer type.
extern "C" bool moraine_shim_is_interrupted(void *client_context);

// A moraine-backed table entry. Column/schema translation happens in
// MoraineSchemaEntry; this class only supplies the pure virtuals
// TableCatalogEntry still needs. The scan function binds normally (so
// DESCRIBE/EXPLAIN work) but always redirects to the DuckLake attach at
// execution time (see scan.hpp).
class MoraineTableEntry : public duckdb::TableCatalogEntry {
public:
	MoraineTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, duckdb::CreateTableInfo &info,
	                  MoraineSnapshotHandle *snapshot, uint64_t table_id);

	duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(duckdb::ClientContext &context,
	                                                         duckdb::column_t column_id) override;
	duckdb::TableFunction GetScanFunction(duckdb::ClientContext &context,
	                                      duckdb::unique_ptr<duckdb::FunctionData> &bind_data) override;
	duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext &context) override;

private:
	MoraineSnapshotHandle *snapshot_;
	uint64_t table_id_;
};

// A moraine-backed view entry. Cataloging (name/schema lookup, `DESCRIBE`)
// works; binding the defining query is deferred and throws
// duckdb::NotImplementedException instead of dereferencing a null query,
// which the base class would otherwise do.
class MoraineViewEntry : public duckdb::ViewCatalogEntry {
public:
	MoraineViewEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, duckdb::CreateViewInfo &info);

	const duckdb::SelectStatement &GetQuery() override;
	void BindView(duckdb::ClientContext &context, duckdb::BindViewAction action) override;
	std::string ToSQL() const override;
};

// A moraine-backed schema entry: table/view lookup and enumeration
// translate directly to the listing ABI over the snapshot captured at
// construction (one snapshot per DuckDB transaction). Every write
// callback throws duckdb::NotImplementedException.
class MoraineSchemaEntry : public duckdb::SchemaCatalogEntry {
public:
	MoraineSchemaEntry(duckdb::Catalog &catalog, duckdb::CreateSchemaInfo &info, MoraineSnapshotHandle *snapshot,
	                   uint64_t schema_id);

	void Scan(duckdb::ClientContext &context, duckdb::CatalogType type,
	          const std::function<void(duckdb::CatalogEntry &)> &callback) override;
	void Scan(duckdb::CatalogType type, const std::function<void(duckdb::CatalogEntry &)> &callback) override;

	duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntry(duckdb::CatalogTransaction transaction,
	                                                       const duckdb::EntryLookupInfo &lookup_info) override;

	duckdb::optional_ptr<duckdb::CatalogEntry> CreateIndex(duckdb::CatalogTransaction transaction,
	                                                       duckdb::CreateIndexInfo &info,
	                                                       duckdb::TableCatalogEntry &table) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateFunction(duckdb::CatalogTransaction transaction,
	                                                          duckdb::CreateFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateTable(duckdb::CatalogTransaction transaction,
	                                                       duckdb::BoundCreateTableInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateView(duckdb::CatalogTransaction transaction,
	                                                      duckdb::CreateViewInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateSequence(duckdb::CatalogTransaction transaction,
	                                                          duckdb::CreateSequenceInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateTableFunction(duckdb::CatalogTransaction transaction,
	                                                               duckdb::CreateTableFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateCopyFunction(duckdb::CatalogTransaction transaction,
	                                                              duckdb::CreateCopyFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreatePragmaFunction(duckdb::CatalogTransaction transaction,
	                                                                duckdb::CreatePragmaFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateCollation(duckdb::CatalogTransaction transaction,
	                                                           duckdb::CreateCollationInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateType(duckdb::CatalogTransaction transaction,
	                                                      duckdb::CreateTypeInfo &info) override;
	void DropEntry(duckdb::ClientContext &context, duckdb::DropInfo &info) override;
	void Alter(duckdb::CatalogTransaction transaction, duckdb::AlterInfo &info) override;

private:
	MoraineSnapshotHandle *snapshot_;
	uint64_t schema_id_;
	bool tables_loaded_ = false;
	bool views_loaded_ = false;
	// Keyed by name (DuckDB catalog lookups are case-insensitive); built
	// lazily and cached for this schema entry's lifetime.
	duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CatalogEntry>> tables_;
	duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CatalogEntry>> views_;

	void EnsureTablesLoaded();
	void EnsureViewsLoaded();
};

// A moraine-backed Catalog: ATTACH opens the store via moraine_attach;
// DETACH (or database shutdown) closes it via moraine_detach. Schema
// lookup/enumeration delegate to the active transaction's cached
// MoraineSchemaEntry set; every write path throws
// duckdb::NotImplementedException.
class MoraineCatalog : public duckdb::Catalog {
public:
	MoraineCatalog(duckdb::AttachedDatabase &db, duckdb::ClientContext &context,
	               MoraineCatalogHandle *handle, std::string path, MaintenanceConfig maintenance);
	~MoraineCatalog() override;

	// The attach_function_t the storage extension registers.
	static duckdb::unique_ptr<duckdb::Catalog> Attach(duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
	                                                  duckdb::ClientContext &context, duckdb::AttachedDatabase &db,
	                                                  const std::string &name, duckdb::AttachInfo &info,
	                                                  duckdb::AttachOptions &options);

	void Initialize(bool load_builtin) override;
	std::string GetCatalogType() override;

	duckdb::optional_ptr<duckdb::CatalogEntry> CreateSchema(duckdb::CatalogTransaction transaction,
	                                                        duckdb::CreateSchemaInfo &info) override;
	duckdb::optional_ptr<duckdb::SchemaCatalogEntry> LookupSchema(duckdb::CatalogTransaction transaction,
	                                                              const duckdb::EntryLookupInfo &schema_lookup,
	                                                              duckdb::OnEntryNotFound if_not_found) override;
	void ScanSchemas(duckdb::ClientContext &context,
	                 std::function<void(duckdb::SchemaCatalogEntry &)> callback) override;

	duckdb::PhysicalOperator &PlanCreateTableAs(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner,
	                                            duckdb::LogicalCreateTable &op,
	                                            duckdb::PhysicalOperator &plan) override;
	duckdb::PhysicalOperator &PlanInsert(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner,
	                                     duckdb::LogicalInsert &op,
	                                     duckdb::optional_ptr<duckdb::PhysicalOperator> plan) override;
	duckdb::PhysicalOperator &PlanDelete(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner,
	                                     duckdb::LogicalDelete &op, duckdb::PhysicalOperator &plan) override;
	duckdb::PhysicalOperator &PlanUpdate(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner,
	                                     duckdb::LogicalUpdate &op, duckdb::PhysicalOperator &plan) override;

	duckdb::DatabaseSize GetDatabaseSize(duckdb::ClientContext &context) override;
	bool InMemory() override;
	std::string GetDBPath() override;

	void OnDetach(duckdb::ClientContext &context) override;

	// Private pure virtual in duckdb::Catalog itself; a derived class's
	// access specifier for an override is independent of the base's.
	void DropSchema(duckdb::ClientContext &context, duckdb::DropInfo &info) override;

	MoraineCatalogHandle *Handle() const {
		return handle_;
	}

	// The store path this catalog was attached at.
	const std::string &StorePath() const {
		return path_;
	}

	// The maintenance driver for this attach. Always present — it serves
	// the on-demand trigger even when no interval was configured.
	MaintenanceScheduler &Scheduler() const {
		return *scheduler_;
	}

	// The rows this attach last dumped for `spec`, if the store still
	// stands where they were dumped from. A miss (nothing held, or the
	// stamp moved) returns null and the caller re-dumps.
	std::shared_ptr<const MetadataRows> HeldMetadataRows(const MetadataTableSpec &spec, uint64_t snapshot_id,
	                                                     uint64_t batch_seq) const;

	// Holds `rows` as this attach's rows for `spec` at the given stamp,
	// replacing whatever it held. Shared by every connection on the
	// attach, so it is guarded.
	void HoldMetadataRows(const MetadataTableSpec &spec, uint64_t snapshot_id, uint64_t batch_seq,
	                      std::shared_ptr<const MetadataRows> rows);

private:
	MoraineCatalogHandle *handle_;
	std::string path_;
	duckdb::shared_ptr<MaintenanceScheduler> scheduler_;

	// One dumped row set per synthesized table, stamped with the store
	// state it was dumped at. DuckLake re-reads its metadata at every
	// transaction start and autocommit makes every statement a
	// transaction, so without this every statement re-pays the ABI
	// crossing for rows that are byte-identical whenever no commit landed
	// between them. Shared across the attach's connections, hence the
	// mutex; the per-transaction pin above it is what makes a row index
	// mean something within one transaction.
	struct HeldRows {
		uint64_t snapshot_id;
		uint64_t batch_seq;
		std::shared_ptr<const MetadataRows> rows;
	};
	mutable std::mutex held_rows_lock_;
	std::unordered_map<const MetadataTableSpec *, HeldRows> held_rows_;

	// Ensures the active transaction's schema cache is populated from the
	// listing ABI, then returns it.
	static duckdb::vector<duckdb::reference<duckdb::SchemaCatalogEntry>> LoadedSchemas(duckdb::Catalog &catalog,
	                                                                                   duckdb::Transaction &tx);
};

// Resolves the moraine catalog behind `catalog_name`, accepting either
// the DuckLake lake name or the name of its metadata catalog.
//
// The two are not related by name in general: an attach may pass
// `METADATA_CATALOG` and name the metadata catalog itself, so DuckLake's
// default `__ducklake_metadata_<lake>` is only one case. What does hold
// is that the lake attaches `moraine:<store path>` over a catalog that
// reports `<store path>`, so the fallback matches on that. Throws
// `InvalidInputException` when neither names a moraine catalog.
MoraineCatalog &ResolveMoraineCatalog(duckdb::ClientContext &context, const std::string &catalog_name);

} // namespace moraine_duckdb
