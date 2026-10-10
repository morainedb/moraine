#pragma once

#include "duckdb/main/client_context.hpp"

namespace moraine_duckdb {

// Consumes only the driving session's armed read stall.
uint64_t TakeMoraineReadStall(duckdb::ClientContext &context);

} // namespace moraine_duckdb
