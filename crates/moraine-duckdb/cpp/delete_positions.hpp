#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace moraine_duckdb {

// `moraine_delete_positions(catalog, schema, table, files, snapshot := NULL)`
// stages deletes of rows already located, as positions within committed data
// files, in the current DuckLake transaction without scanning the table.
// Built on DuckLake's public transaction API: the deletes are staged exactly
// as `DELETE` stages its own, so COMMIT publishes them and ROLLBACK discards
// them.
class MoraineDeletePositionsFunction : public duckdb::TableFunction {
public:
	MoraineDeletePositionsFunction();
};

void RegisterMoraineDeletePositionsFunction(duckdb::ExtensionLoader &loader);

} // namespace moraine_duckdb
