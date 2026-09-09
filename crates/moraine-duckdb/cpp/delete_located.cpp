// moraine_delete_located(catalog, schema, table, rows): resolves
// already-located `rows` — a LIST of STRUCT(row_id BIGINT, data_file_id
// UBIGINT), as an index lookup reports them — to exact file positions and
// hands them to DuckLake's `ducklake_delete_positions`, which stages the
// deletion in the current DuckLake transaction with no scan.
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "catalog.hpp"
#include "delete_located.hpp"
#include "moraine_abi.h"
#include "rows_at.hpp"

namespace moraine_duckdb {
namespace {

// Resolves the located rows to `(data_file_id, positions)` at the snapshot
// the current DuckDB transaction pinned on the metadata catalog, and
// rewrites the call into DuckLake's transaction-aware function, naming that
// snapshot so DuckLake can refuse a mismatch. A file the snapshot does not
// hold, or a row its named file does not hold, fails here.
duckdb::unique_ptr<duckdb::TableRef> DeleteLocatedReplace(duckdb::ClientContext &context,
                                                          duckdb::TableFunctionBindInput &input) {
	auto catalog_name = input.inputs[0].GetValue<std::string>();
	auto schema_name = input.inputs[1].GetValue<std::string>();
	auto table_name = input.inputs[2].GetValue<std::string>();
	auto located =
	    ResolveLocatedArguments(context, catalog_name, schema_name, table_name, input.inputs[3], "moraine_delete_located");

	duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arguments;
	for (duckdb::idx_t i = 0; i < 3; i++) {
		arguments.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(input.inputs[i]));
	}
	arguments.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(located.files));
	auto inlined_rows = duckdb::make_uniq<duckdb::ConstantExpression>(located.inlined_rows);
	inlined_rows->SetAlias("inlined_rows");
	arguments.push_back(std::move(inlined_rows));
	auto snapshot = duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::UBIGINT(located.snapshot_id));
	snapshot->SetAlias("snapshot");
	arguments.push_back(std::move(snapshot));

	auto result = duckdb::make_uniq<duckdb::TableFunctionRef>();
	result->function = duckdb::make_uniq<duckdb::FunctionExpression>("ducklake_delete_positions", std::move(arguments));
	return std::move(result);
}

} // namespace

void RegisterMoraineDeleteLocatedFunction(duckdb::ExtensionLoader &loader) {
	using duckdb::LogicalType;

	// (catalog, schema, table, rows), where `rows` is a LIST of
	// STRUCT(row_id BIGINT, data_file_id UBIGINT); DuckDB's ANY element
	// type accepts the struct shape without pinning field names or order —
	// the replacement above resolves each field by name.
	duckdb::TableFunction delete_located(
	    "moraine_delete_located",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	    nullptr);
	delete_located.bind_replace = DeleteLocatedReplace;
	loader.RegisterFunction(delete_located);
}

} // namespace moraine_duckdb
