// Dynamic inline-table catalog surface. Recognizes two per-table name
// families and routes CREATE/INSERT/UPDATE/SELECT against them into the
// `inline/*` keyspace over the staged-tx ABI instead of materializing real
// tables (translate-only):
//
//   - `ducklake_inlined_data_<table_id>_<schema_version>` — inlined
//     inserts. Columns `(row_id BIGINT, begin_snapshot BIGINT,
//     end_snapshot BIGINT, <the table's user columns>)`.
//   - `ducklake_inlined_delete_<table_id>` — inlined deletes against
//     already-flushed Parquet rows. Columns `(file_id BIGINT, row_id
//     BIGINT, begin_snapshot BIGINT)`.
//
// Existence discipline: the delete family must NOT be recognized for every
// table id — DuckLake probes it with `SELECT NULL FROM ... LIMIT 1` and
// treats a bind error as "does not exist", so this shim consults
// `moraine_inline_file_delete_table_exists` and reports it missing until a first
// `inline/fdel` has been staged. The data family is recognized purely by
// whether `moraine_inline_schemas` has a matching `(table_id,
// schema_version)` record.
//
// Wire format for `inline/schema` and `inline/insert` bytes (opaque to the
// store, encoded and decoded only here): both are Arrow IPC. An
// `inline/schema` record is a schema-only stream (no batches); an
// `inline/insert` chunk is a record-batch **body** only (no schema message)
// over the table's user columns, decoded against its version's
// `inline/schema` so the schema is not re-serialized per chunk. DuckDB
// converts a `DataChunk` to the Arrow C Data Interface and the Rust bridge
// serializes/deserializes the IPC, so nested user column types round-trip.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "duckdb.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "moraine_abi.h"

namespace duckdb {
class LogicalDelete;
class LogicalInsert;
class LogicalUpdate;
} // namespace duckdb

namespace moraine_duckdb {

std::string InlinedDataTableName(uint64_t table_id, uint64_t schema_version);
std::string InlinedDeleteTableName(uint64_t table_id);

struct InlinedDataTableId {
	uint64_t table_id;
	uint64_t schema_version;
};

// Parses `ducklake_inlined_data_<table_id>_<schema_version>`, or nullopt
// if `name` doesn't match the pattern.
std::optional<InlinedDataTableId> ParseInlinedDataTableName(const std::string &name);

// Parses `ducklake_inlined_delete_<table_id>`, or nullopt if `name`
// doesn't match the pattern.
std::optional<uint64_t> ParseInlinedDeleteTableName(const std::string &name);

// One user column, name + fully resolved type.
struct DecodedInlineColumn {
	std::string name;
	duckdb::LogicalType type;
};

// Serializes `user_columns` as a schema-only Arrow IPC stream.
MoraineArrowBytes EncodeInlineSchema(duckdb::ClientContext &context,
                                     const std::vector<DecodedInlineColumn> &user_columns);

// Inverse of `EncodeInlineSchema`: reconstructs each column's name and
// DuckDB type from the stream's Arrow schema. Throws on malformed bytes or
// an Arrow type DuckDB cannot map back.
std::vector<DecodedInlineColumn> DecodeInlineSchema(duckdb::ClientContext &context, const uint8_t *data, size_t len);

// Serializes columns `[user_col_start, chunk.ColumnCount())` of rows
// `[row_offset, row_offset + row_count)` as one Arrow IPC record-batch body
// (no schema message).
MoraineArrowBytes EncodeInlineChunkRows(duckdb::ClientContext &context, duckdb::DataChunk &chunk,
                                        duckdb::idx_t user_col_start, duckdb::idx_t row_offset,
                                        duckdb::idx_t row_count);

// A version's schema-only stream (`inline/schema`) parsed once by the Rust
// side, reused for every chunk of that version. Throws on malformed bytes.
class DecodedInlineSchema {
public:
	DecodedInlineSchema(const uint8_t *schema_ipc, size_t schema_ipc_len);
	~DecodedInlineSchema();
	DecodedInlineSchema(const DecodedInlineSchema &) = delete;
	DecodedInlineSchema &operator=(const DecodedInlineSchema &) = delete;

	const MoraineArrowSchema *Get() const {
		return handle_;
	}

private:
	MoraineArrowSchema *handle_;
};

// Inverse of `EncodeInlineChunkRows`, imported through DuckDB's Arrow
// reader and left as the `DataChunk`s that reader produced — one per
// `STANDARD_VECTOR_SIZE` rows, in order, so the chunk's row `o` is row
// `o % STANDARD_VECTOR_SIZE` of piece `o / STANDARD_VECTOR_SIZE`. Columnar
// on both sides: nothing is transcoded through `duckdb::Value`.
// `schema` is the version's decoded schema, against which the body-only
// chunk decodes. Throws InternalException on malformed bytes or a column
// count mismatch against `user_types`.
std::vector<duckdb::unique_ptr<duckdb::DataChunk>>
DecodeInlineChunkPieces(duckdb::ClientContext &context, const DecodedInlineSchema &schema, MoraineInlineChunk &chunk,
                        const std::vector<duckdb::LogicalType> &user_types);

// A synthesized `ducklake_inlined_data_<t>_<v>` entry: columns are
// `(row_id, begin_snapshot, end_snapshot, <user columns>)`.
class MoraineInlineDataTableEntry : public duckdb::TableCatalogEntry {
public:
	MoraineInlineDataTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
	                            duckdb::CreateTableInfo &info, MoraineCatalogHandle *handle, uint64_t table_id,
	                            uint64_t schema_version);

	duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(duckdb::ClientContext &context,
	                                                         duckdb::column_t column_id) override;
	duckdb::TableFunction GetScanFunction(duckdb::ClientContext &context,
	                                      duckdb::unique_ptr<duckdb::FunctionData> &bind_data) override;
	duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext &context) override;

	uint64_t TableId() const {
		return table_id_;
	}
	uint64_t SchemaVersion() const {
		return schema_version_;
	}
	MoraineCatalogHandle *Handle() const {
		return handle_;
	}
	// This entry's user columns' types only (skips the three system
	// columns).
	std::vector<duckdb::LogicalType> UserColumnTypes() const;

private:
	MoraineCatalogHandle *handle_;
	uint64_t table_id_;
	uint64_t schema_version_;
};

// A synthesized `ducklake_inlined_delete_<t>` entry: fixed columns
// `(file_id, row_id, begin_snapshot)`.
//
// It exists from the table's first inlined deletion until the table is
// dropped — emptying it does not remove it. DuckLake caches the table's
// existence for the life of the catalog and never re-probes, so an
// existence that tracked whether any deletion is currently recorded would
// vanish under it the moment a flush cleared them.
class MoraineInlineDeleteTableEntry : public duckdb::TableCatalogEntry {
public:
	MoraineInlineDeleteTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
	                              duckdb::CreateTableInfo &info, MoraineCatalogHandle *handle, uint64_t table_id);

	duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(duckdb::ClientContext &context,
	                                                         duckdb::column_t column_id) override;
	duckdb::TableFunction GetScanFunction(duckdb::ClientContext &context,
	                                      duckdb::unique_ptr<duckdb::FunctionData> &bind_data) override;
	duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext &context) override;

	uint64_t TableId() const {
		return table_id_;
	}
	MoraineCatalogHandle *Handle() const {
		return handle_;
	}

private:
	MoraineCatalogHandle *handle_;
	uint64_t table_id_;
};

// Builds a fresh data-table entry's full column list (system three +
// `user_columns`).
duckdb::unique_ptr<MoraineInlineDataTableEntry>
MakeInlineDataTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, MoraineCatalogHandle *handle,
                         uint64_t table_id, uint64_t schema_version,
                         const std::vector<DecodedInlineColumn> &user_columns);

duckdb::unique_ptr<MoraineInlineDeleteTableEntry> MakeInlineDeleteTableEntry(duckdb::Catalog &catalog,
                                                                             duckdb::SchemaCatalogEntry &schema,
                                                                             MoraineCatalogHandle *handle,
                                                                             uint64_t table_id);

// Recognizes `name` against the store's persisted state and synthesizes
// the matching entry (see the module doc for each family's existence
// rule), or returns null if `name` matches neither pattern, or matches a
// pattern but nothing has been staged for it yet — the CREATE path, or an
// existence-probe DuckLake expects to fail.
duckdb::unique_ptr<duckdb::CatalogEntry> LookupInlineTableEntry(duckdb::ClientContext &context,
                                                                duckdb::Catalog &catalog,
                                                                duckdb::SchemaCatalogEntry &schema,
                                                                MoraineCatalogHandle *handle, const std::string &name);

// Handles `CREATE TABLE [IF NOT EXISTS] ducklake_inlined_data_<t>_<v>(...)`:
// stages `inline/schema` from `info`'s bound columns (skipping the three
// system columns) and returns the new entry. Returns null if a schema is
// already recorded for `(t, v)` and `on_conflict == IGNORE_ON_CONFLICT`;
// throws CatalogException if already recorded and `ERROR_ON_CONFLICT`.
duckdb::unique_ptr<duckdb::CatalogEntry> CreateInlineDataTable(duckdb::ClientContext &context, duckdb::Catalog &catalog,
                                                               duckdb::SchemaCatalogEntry &schema,
                                                               MoraineCatalogHandle *handle, MoraineTxHandle *tx,
                                                               duckdb::BoundCreateTableInfo &info, uint64_t table_id,
                                                               uint64_t schema_version);

// Physical operator builders: a translate-only Sink feeding the inline
// staged-write ABI, dual-rooted as a Source emitting the one-row `Count`
// result.
duckdb::PhysicalOperator &PlanInlineDataInsert(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalInsert &op,
                                               MoraineInlineDataTableEntry &table_entry);

// The only translatable UPDATE shape: `SET end_snapshot = <v> WHERE
// row_id = r`. Throws NotImplementedException at plan time for any other
// SET target.
duckdb::PhysicalOperator &PlanInlineDataUpdate(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalUpdate &op,
                                               MoraineInlineDataTableEntry &table_entry);

// `DELETE FROM ducklake_inlined_data_<t>_<v> WHERE begin_snapshot <=
// {flush_snap}` — the flush's chunk-removal step. One
// `stage_inline_flush_delete` call at the maximum `begin_snapshot` among the
// deleted rows.
duckdb::PhysicalOperator &PlanInlineDataDelete(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalDelete &op,
                                               MoraineInlineDataTableEntry &table_entry);

// `INSERT INTO ducklake_inlined_delete_<t> VALUES (file_id, row_id,
// {snap}), ...` — one `stage_inline_fdel` call per row.
duckdb::PhysicalOperator &PlanInlineDeleteInsert(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalInsert &op,
                                                 MoraineInlineDeleteTableEntry &table_entry);

// `DELETE FROM ducklake_inlined_delete_<t>` — the flush's clean-up step,
// once those inlined deletions have been written out as a real delete
// file. One `stage_inline_file_delete_remove` call per matched row.
duckdb::PhysicalOperator &PlanInlineDeleteDelete(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalDelete &op,
                                                 MoraineInlineDeleteTableEntry &table_entry);

} // namespace moraine_duckdb
