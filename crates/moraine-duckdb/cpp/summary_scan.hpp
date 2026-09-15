#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "moraine_abi.h"

namespace moraine_duckdb {

// Replaces a same-table, current-snapshot scan after both location join keys are verified.
bool UseSummaryScan(duckdb::ClientContext &context, duckdb::LogicalGet &scan, const duckdb::LogicalGet &index,
                    const std::vector<MoraineRowId> &rows);

} // namespace moraine_duckdb
