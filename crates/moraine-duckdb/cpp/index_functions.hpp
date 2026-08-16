// What the optimizer needs to read out of a bound index lookup: the rows it
// resolved, and where they sit in its result.
#pragma once

#include <string>
#include <vector>

#include "duckdb.hpp"

#include "moraine_abi.h"

namespace moraine_duckdb {

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

} // namespace moraine_duckdb
