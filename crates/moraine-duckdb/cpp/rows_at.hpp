// `moraine_rows_at`: reads rows already resolved to a data file or to
// inline data (typically from an index lookup) back whole, without a table
// scan, at the snapshot the current transaction pinned.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "duckdb.hpp"

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
std::vector<MorainePositionPair> ParseLocatedPairs(const duckdb::Value &rows, const char *caller);

// Located rows resolved to DuckLake's own identifiers at the pinned
// snapshot: `files` is a LIST of STRUCT(data_file_id, positions),
// `inlined_rows` a LIST of row ids, and `snapshot_id` the view they were
// resolved in, which DuckLake refuses to stage against an older one.
struct LocatedArguments {
	duckdb::Value files;
	duckdb::Value inlined_rows;
	uint64_t snapshot_id = 0;
};

LocatedArguments ResolveLocatedArguments(duckdb::ClientContext &context, const std::string &catalog_name,
                                         const std::string &schema_name, const std::string &table_name,
                                         const duckdb::Value &rows, const char *caller);

// The table's top-level columns at `snapshot`, in catalog order, typed as
// the storage extension binds them.
void TableColumns(MoraineSnapshotHandle *snapshot, const std::string &schema_name, const std::string &table_name,
                  duckdb::vector<duckdb::LogicalType> &types, duckdb::vector<std::string> &names);

void RegisterMoraineRowsAtFunction(duckdb::ExtensionLoader &loader);

} // namespace moraine_duckdb
