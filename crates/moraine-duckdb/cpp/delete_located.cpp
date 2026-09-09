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
#include "owned_array.hpp"

namespace moraine_duckdb {
namespace {

std::vector<MorainePositionPair> ParsePairs(const duckdb::Value &rows) {
	std::vector<MorainePositionPair> pairs;
	if (rows.IsNull()) {
		throw duckdb::InvalidInputException("moraine_delete_located: `rows` cannot be NULL");
	}

	// The element struct's field names are validated once here, by type,
	// rather than assumed positional: a caller naming `data_file_id` before
	// `row_id` must still resolve to the right row.
	auto element_type = duckdb::ListType::GetChildType(rows.type());
	if (element_type.id() != duckdb::LogicalTypeId::STRUCT) {
		throw duckdb::InvalidInputException(
		    "moraine_delete_located: each `rows` entry must be STRUCT(row_id BIGINT, data_file_id UBIGINT)");
	}
	auto &children = duckdb::StructType::GetChildTypes(element_type);
	duckdb::optional_idx row_id_index;
	duckdb::optional_idx data_file_id_index;
	for (duckdb::idx_t i = 0; i < children.size(); i++) {
		auto name = duckdb::StringUtil::Lower(children[i].first);
		if (name == "row_id") {
			row_id_index = i;
		} else if (name == "data_file_id") {
			data_file_id_index = i;
		}
	}
	if (children.size() != 2 || !row_id_index.IsValid() || !data_file_id_index.IsValid()) {
		throw duckdb::InvalidInputException(
		    "moraine_delete_located: each `rows` entry must be exactly STRUCT(row_id BIGINT, data_file_id UBIGINT)");
	}

	for (auto &row : duckdb::ListValue::GetChildren(rows)) {
		if (row.IsNull()) {
			throw duckdb::InvalidInputException("moraine_delete_located: a `rows` entry cannot be NULL");
		}
		auto &fields = duckdb::StructValue::GetChildren(row);
		auto &row_id_value = fields[row_id_index.GetIndex()];
		auto &data_file_id_value = fields[data_file_id_index.GetIndex()];
		if (row_id_value.IsNull()) {
			throw duckdb::InvalidInputException("moraine_delete_located: `row_id` cannot be NULL");
		}
		MorainePositionPair pair {};
		pair.row_id = row_id_value.DefaultCastAs(duckdb::LogicalType::UBIGINT).GetValue<uint64_t>();
		if (!data_file_id_value.IsNull()) {
			pair.has_data_file_id = true;
			pair.data_file_id = data_file_id_value.DefaultCastAs(duckdb::LogicalType::UBIGINT).GetValue<uint64_t>();
		}
		pairs.push_back(pair);
	}

	return pairs;
}

duckdb::Value Positions(const uint64_t *positions, size_t length) {
	duckdb::vector<duckdb::Value> values;
	values.reserve(length);
	for (size_t i = 0; i < length; i++) {
		values.push_back(duckdb::Value::UBIGINT(positions[i]));
	}
	return duckdb::Value::LIST(duckdb::LogicalType::UBIGINT, std::move(values));
}

// Resolves the located rows to `(data_file_id, positions)` at the catalog
// head and rewrites the call into DuckLake's transaction-aware function.
// A file the head no longer holds, or a row its named file does not hold,
// fails here; a file the transaction's snapshot lacks fails in DuckLake.
duckdb::unique_ptr<duckdb::TableRef> DeleteLocatedReplace(duckdb::ClientContext &context,
                                                          duckdb::TableFunctionBindInput &input) {
	auto catalog_name = input.inputs[0].GetValue<std::string>();
	auto schema_name = input.inputs[1].GetValue<std::string>();
	auto table_name = input.inputs[2].GetValue<std::string>();
	auto pairs = ParsePairs(input.inputs[3]);

	auto handle = ResolveMoraineCatalog(context, catalog_name).Handle();
	OwnedArray<MoraineLocatedFile> files(moraine_locate_row_positions_free_files);
	OwnedArray<uint64_t> inlined(moraine_locate_row_positions_free_inlined);
	char *raw_write_directory = nullptr;
	MoraineError error {};
	auto code = moraine_locate_row_positions(handle, schema_name.c_str(), table_name.c_str(), pairs.data(),
	                                         pairs.size(), files.OutItems(), files.OutLen(), inlined.OutItems(),
	                                         inlined.OutLen(), &raw_write_directory, moraine_shim_is_interrupted,
	                                         &context, &error);
	if (code != MORAINE_OK) {
		ThrowMoraineError(error);
	}
	// DuckLake composes the delete file's directory itself.
	moraine_string_free(raw_write_directory);

	using duckdb::LogicalType;
	using duckdb::Value;
	duckdb::child_list_t<LogicalType> file_fields {{"data_file_id", LogicalType::UBIGINT},
	                                               {"positions", LogicalType::LIST(LogicalType::UBIGINT)}};
	auto file_type = LogicalType::STRUCT(file_fields);
	duckdb::vector<Value> file_values;
	file_values.reserve(files.size());
	for (auto &file : files) {
		duckdb::child_list_t<Value> fields {{"data_file_id", Value::UBIGINT(file.data_file_id)},
		                                    {"positions", Positions(file.positions, file.positions_len)}};
		file_values.push_back(Value::STRUCT(std::move(fields)));
	}
	duckdb::vector<Value> inlined_values;
	inlined_values.reserve(inlined.size());
	for (auto row_id : inlined) {
		inlined_values.push_back(Value::UBIGINT(row_id));
	}

	duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arguments;
	for (duckdb::idx_t i = 0; i < 3; i++) {
		arguments.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(input.inputs[i]));
	}
	arguments.push_back(
	    duckdb::make_uniq<duckdb::ConstantExpression>(Value::LIST(file_type, std::move(file_values))));
	auto inlined_rows = duckdb::make_uniq<duckdb::ConstantExpression>(
	    Value::LIST(LogicalType::UBIGINT, std::move(inlined_values)));
	inlined_rows->SetAlias("inlined_rows");
	arguments.push_back(std::move(inlined_rows));

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
