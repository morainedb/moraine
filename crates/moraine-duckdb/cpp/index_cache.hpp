#pragma once

#include <string>

#include "duckdb.hpp"
#include "moraine_abi.h"

namespace moraine_duckdb {

class MoraineCatalog;

// Registers a whole-plan dependency and reads through DuckLake's metadata transaction.
MoraineCatalogHandle *BindIndexRead(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                    bool &cacheable);
// As above for a read the optimizer issues against `catalog_name` — a lake or
// its metadata catalog — with the statement's binder, or none outside one.
MoraineCatalogHandle *BindIndexReadFor(duckdb::ClientContext &context, const std::string &catalog_name,
                                       duckdb::optional_ptr<duckdb::Binder> binder, bool &cacheable);
duckdb::optional_idx IndexCatalogVersion(duckdb::ClientContext &context, MoraineCatalog &catalog);

} // namespace moraine_duckdb
