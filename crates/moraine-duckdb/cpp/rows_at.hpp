// `moraine_rows_at`: reads rows already resolved to a data file or to
// inline data (typically from an index lookup) back whole, without a table
// scan, at the snapshot the current transaction pinned.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "duckdb.hpp"

#include "functions/ducklake_table_functions.hpp"

#include "catalog.hpp"
#include "moraine_abi.h"

namespace moraine_duckdb {

// The metadata catalog's view for the current DuckDB transaction, taken
// after DuckLake's transaction on `catalog_name` has pinned its own.
struct PinnedSnapshot {
	MoraineCatalog *catalog = nullptr;
	MoraineSnapshotHandle *snapshot = nullptr;
	uint64_t snapshot_id = 0;
};

PinnedSnapshot PinTransactionSnapshot(duckdb::ClientContext &context, const std::string &catalog_name,
                                      const std::string &schema_name, const std::string &table_name);

// Parses a located-rows argument: a LIST of STRUCT(row_id BIGINT,
// data_file_id UBIGINT) with the fields resolved by name; a NULL file id
// names an inlined row. `caller` prefixes the error messages.
//
// `payload`, when given, collects the names of any further fields in
// caller order and permits them; without it a third field is an error.
std::vector<MorainePositionPair> ParseLocatedPairs(const duckdb::Value &rows, const char *caller,
                                                   std::vector<std::string> *payload = nullptr);

// Parses `rows` and registers it on the current transaction unpositioned,
// returning the token DuckLake carries in place of the positions.
// `payload` behaves as it does for [`ParseLocatedPairs`].
uint64_t RegisterLocatedRows(duckdb::ClientContext &context, const std::string &catalog_name,
                             const std::string &schema_name, const std::string &table_name,
                             const duckdb::Value &rows, const char *caller,
                             std::vector<std::string> *payload = nullptr);

// The empty `files` a token-carrying call passes positionally, typed as the
// pair struct rather than left as an untyped empty list.
duckdb::Value EmptyLocatedFiles();

// Positions `pairs` against the snapshot the current transaction pinned,
// as DuckLake's own request: no Value is built for a position, and the
// committed delete file's positions the locate decoded are handed over as
// they were decoded.
duckdb::PositionalDeletes LocatedPositionalDeletes(duckdb::ClientContext &context, const std::string &catalog_name,
                                                   const std::string &schema_name, const std::string &table_name,
                                                   const std::vector<MorainePositionPair> &pairs);

// Installs the resolver DuckLake answers a `positions_token` with, so a
// located change positions its rows when it runs rather than when it binds.
void RegisterMorainePositionResolver();

// The table's top-level columns at `snapshot`, in catalog order, typed as
// the storage extension binds them.
void TableColumns(MoraineSnapshotHandle *snapshot, const std::string &schema_name, const std::string &table_name,
                  duckdb::vector<duckdb::LogicalType> &types, duckdb::vector<std::string> &names);

void RegisterMoraineRowsAtFunction(duckdb::ExtensionLoader &loader);

// Consumes both Arrow structs, including when import throws.
std::vector<duckdb::unique_ptr<duckdb::DataChunk>> ImportLocatedBatch(duckdb::ClientContext &context,
                                                                      ArrowSchema &schema, ArrowArray &array);

} // namespace moraine_duckdb
