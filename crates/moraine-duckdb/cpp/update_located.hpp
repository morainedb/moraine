// `moraine_update`: updates rows already resolved to a data file or to
// inline data (typically from an index lookup) in one statement, without
// a table scan: the located rows are deleted and their replacements, with
// the given assignments applied, inserted in the current transaction.
#pragma once

#include "duckdb.hpp"

namespace moraine_duckdb {

void RegisterMoraineUpdateFunction(duckdb::ExtensionLoader &loader);

} // namespace moraine_duckdb
