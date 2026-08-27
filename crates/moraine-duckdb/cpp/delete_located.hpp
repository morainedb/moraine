// `moraine_delete_located`: deletes rows already resolved to a data file
// (typically from an index lookup) without a table scan.
#pragma once

#include "duckdb.hpp"

namespace moraine_duckdb {

void RegisterMoraineDeleteLocatedFunction(duckdb::ExtensionLoader &loader);

} // namespace moraine_duckdb
