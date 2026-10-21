// What the optimizer needs to read out of a bound index lookup: the rows it
// resolved, and where they sit in its result.
#pragma once

#include <deque>
#include <string>
#include <vector>

#include "duckdb.hpp"

#include "moraine_abi.h"

namespace moraine_duckdb {

// Backing storage for a `MoraineLookupValue`'s string/bytes fields, which
// point into these members and so must outlive any use of the value.
struct LookupValueBacking {
	std::string str;
	std::vector<uint8_t> bytes;
};

using LookupValueArena = std::deque<LookupValueBacking>;

// Translates a DuckDB `Value` into the tagged `MoraineLookupValue` the ABI
// coerces to the indexed column's type. `caller` names the SQL function in
// any error. Throws on a type equality indexes do not cover.
MoraineLookupValue BuildLookupValue(const duckdb::Value &value, LookupValueArena &arena, const char *caller);

// Column positions shared by every index read's result.
constexpr duckdb::idx_t INDEX_READ_ROW_ID_COLUMN = 0;
constexpr duckdb::idx_t INDEX_READ_DATA_FILE_ID_COLUMN = 1;

// Whether `name` is one of the table functions that resolves index entries to
// located rows.
bool IsIndexRead(const std::string &name);

// The rows an index read resolved while binding, or nullptr if `function` is
// not an index read. Borrows from `bind_data`.
const std::vector<MoraineRowId> *IndexReadRows(const duckdb::TableFunction &function,
                                               const duckdb::FunctionData *bind_data);

bool IndexReadTable(const duckdb::TableFunction &function, const duckdb::FunctionData *bind_data, std::string &catalog,
                    std::string &schema, std::string &table);

} // namespace moraine_duckdb
