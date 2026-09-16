#pragma once

#include "duckdb.hpp"
#include "moraine_abi.h"

namespace moraine_duckdb {

class MoraineCatalog;

// Registers a whole-plan dependency and reads through DuckLake's metadata transaction.
MoraineCatalogHandle *BindIndexRead(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                    bool &cacheable);
duckdb::optional_idx IndexCatalogVersion(duckdb::ClientContext &context, MoraineCatalog &catalog);

} // namespace moraine_duckdb
