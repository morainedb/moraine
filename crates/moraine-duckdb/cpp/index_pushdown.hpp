// The files an index says a pushed-down filter can match, for a scan to
// prune its file list by.
#pragma once

#include <cstdint>
#include <optional>
#include <unordered_set>

#include "duckdb.hpp"

#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"

namespace moraine_duckdb {

// The committed files that can hold a row matching every filter in
// `filter_info`, or nothing when that cannot be answered from an index at
// `snapshot` — no equality filter on an indexed column, a shape the index
// cannot answer, a row the lookup could not place, or a file list being
// built for another snapshot than the one this transaction reads. A scan
// prunes to the returned set; an empty set prunes every committed file.
std::optional<std::unordered_set<uint64_t>>
IndexCandidateFiles(duckdb::DuckLakeTransaction &transaction, duckdb::DuckLakeTableEntry &table,
                    duckdb::DuckLakeSnapshot snapshot, const duckdb::FilterPushdownInfo *filter_info);

} // namespace moraine_duckdb
