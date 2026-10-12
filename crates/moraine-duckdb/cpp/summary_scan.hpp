#pragma once

#include <string>
#include <vector>

#include "duckdb.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "moraine_abi.h"

namespace moraine_duckdb {

// Replaces a same-table, current-snapshot scan after both location join keys are verified.
bool UseSummaryScan(duckdb::ClientContext &context, duckdb::LogicalGet &scan, const duckdb::LogicalGet &index,
                    const std::vector<MoraineRowId> &rows);

// The same for rows resolved against the table named directly, which is how
// a pushed predicate names it: `catalog_name` is the lake or its metadata
// catalog. The scan's pushed filters stay with it and run over what it reads.
bool UseSummaryScan(duckdb::ClientContext &context, duckdb::LogicalGet &scan, const std::string &catalog_name,
                    const std::string &schema, const std::string &table, const std::vector<MoraineRowId> &rows);

} // namespace moraine_duckdb
