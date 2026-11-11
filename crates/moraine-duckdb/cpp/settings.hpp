// Process-wide settings: the budgets one process shares across every lake
// it attaches, registered as DuckDB settings so a session sets them with
// `SET` before its first attach, as it does `memory_limit`.
#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "duckdb.hpp"
#include "moraine_abi.h"

namespace moraine_duckdb {

void RegisterMoraineSettings(duckdb::DBConfig &config);

// A memory-size setting (`'512MB'`, `'1GiB'`) in bytes; unset or empty is
// `nullopt`, and anything else that is not a size is refused.
std::optional<uint64_t> SettingBytes(duckdb::ClientContext &context, const char *name);

std::string SettingString(duckdb::ClientContext &context, const char *name);

uint64_t SettingUnsigned(duckdb::ClientContext &context, const char *name);

// The memory budgets the session's settings name; zero where a setting is
// unset, which the core reads as its environment variable or default.
MoraineMemoryBudgets MemoryBudgetSettings(duckdb::ClientContext &context);

} // namespace moraine_duckdb
