// moraine_update(catalog, schema, table, rows, assignments): a located
// update in one statement. `rows` is a LIST of STRUCT(row_id BIGINT,
// data_file_id UBIGINT), as an index lookup reports them; `assignments` is
// SET-clause text such as 'b = b + 1, c = 0'. The call rewrites into
// DuckLake's `ducklake_update_positions`, whose replacement rows are
// `moraine_rows_at` with the assignments applied, so the old values come
// from exactly the located rows' pages and chunks.
#include <unordered_map>

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "rows_at.hpp"
#include "update_located.hpp"

namespace moraine_duckdb {
namespace {

// The assigned expression per lowercase column name.
std::unordered_map<std::string, std::string> ParseAssignments(const std::string &assignments) {
	std::unordered_map<std::string, std::string> result;
	duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> expressions;
	try {
		expressions = duckdb::Parser::ParseExpressionList(assignments);
	} catch (std::exception &error) {
		throw duckdb::InvalidInputException("moraine_update: `assignments` must be `column = expression` pairs "
		                                    "separated by commas: %s",
		                                    duckdb::ErrorData(error).RawMessage());
	}
	for (auto &expression : expressions) {
		if (expression->GetExpressionType() != duckdb::ExpressionType::COMPARE_EQUAL) {
			throw duckdb::InvalidInputException(
			    "moraine_update: `assignments` must be `column = expression` pairs separated by commas");
		}
		auto &comparison = expression->Cast<duckdb::ComparisonExpression>();
		if (comparison.left->GetExpressionType() != duckdb::ExpressionType::COLUMN_REF) {
			throw duckdb::InvalidInputException("moraine_update: an assignment must name a column on its left side");
		}
		auto &column = comparison.left->Cast<duckdb::ColumnRefExpression>();
		if (column.IsQualified()) {
			throw duckdb::InvalidInputException("moraine_update: assigned column \"%s\" must not be qualified",
			                                    column.ToString());
		}
		auto name = duckdb::StringUtil::Lower(column.GetColumnName());
		if (!result.emplace(name, comparison.right->ToString()).second) {
			throw duckdb::InvalidInputException("moraine_update: column \"%s\" is assigned twice",
			                                    column.GetColumnName());
		}
	}
	return result;
}

// The replacement query: every column of the table in order, assigned
// columns replaced by their expression, then the row id, read from the
// located rows. DuckLake writes the id back, so the rows keep their ids as
// an UPDATE's would.
std::string ReplacementQuery(duckdb::ClientContext &context, const std::string &catalog_name,
                             const std::string &schema_name, const std::string &table_name,
                             const duckdb::Value &rows, const std::string &assignments) {
	auto assigned = ParseAssignments(assignments);
	auto pinned = PinTransactionSnapshot(context, catalog_name, schema_name, table_name);
	duckdb::vector<duckdb::LogicalType> types;
	duckdb::vector<std::string> names;
	TableColumns(pinned.snapshot, schema_name, table_name, types, names);

	std::unordered_map<std::string, bool> known;
	std::string projection;
	for (auto &name : names) {
		auto quoted = duckdb::KeywordHelper::WriteOptionallyQuoted(name);
		auto lower = duckdb::StringUtil::Lower(name);
		known.emplace(lower, true);
		if (!projection.empty()) {
			projection += ", ";
		}
		auto assignment = assigned.find(lower);
		projection += assignment == assigned.end() ? quoted : "(" + assignment->second + ") AS " + quoted;
	}
	for (auto &assignment : assigned) {
		if (!known.count(assignment.first)) {
			throw duckdb::InvalidInputException("moraine_update: table \"%s\" has no column \"%s\"", table_name,
			                                    assignment.first);
		}
	}

	return duckdb::StringUtil::Format("SELECT %s, row_id FROM moraine_rows_at(%s, %s, %s, %s)", projection,
	                                  duckdb::KeywordHelper::WriteQuoted(catalog_name),
	                                  duckdb::KeywordHelper::WriteQuoted(schema_name),
	                                  duckdb::KeywordHelper::WriteQuoted(table_name), rows.ToSQLString());
}

duckdb::unique_ptr<duckdb::TableRef> UpdateReplace(duckdb::ClientContext &context,
                                                   duckdb::TableFunctionBindInput &input) {
	auto catalog_name = input.inputs[0].GetValue<std::string>();
	auto schema_name = input.inputs[1].GetValue<std::string>();
	auto table_name = input.inputs[2].GetValue<std::string>();
	if (input.inputs[4].IsNull()) {
		throw duckdb::InvalidInputException("moraine_update: `assignments` cannot be NULL");
	}
	auto replacement = ReplacementQuery(context, catalog_name, schema_name, table_name, input.inputs[3],
	                                    input.inputs[4].GetValue<std::string>());
	auto located =
	    ResolveLocatedArguments(context, catalog_name, schema_name, table_name, input.inputs[3], "moraine_update");

	duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arguments;
	for (duckdb::idx_t i = 0; i < 3; i++) {
		arguments.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(input.inputs[i]));
	}
	arguments.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(located.files));
	arguments.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value(replacement)));
	auto inlined_rows = duckdb::make_uniq<duckdb::ConstantExpression>(located.inlined_rows);
	inlined_rows->SetAlias("inlined_rows");
	arguments.push_back(std::move(inlined_rows));
	auto snapshot = duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::UBIGINT(located.snapshot_id));
	snapshot->SetAlias("snapshot");
	arguments.push_back(std::move(snapshot));

	auto result = duckdb::make_uniq<duckdb::TableFunctionRef>();
	result->function = duckdb::make_uniq<duckdb::FunctionExpression>("ducklake_update_positions", std::move(arguments));
	return std::move(result);
}

} // namespace

void RegisterMoraineUpdateFunction(duckdb::ExtensionLoader &loader) {
	using duckdb::LogicalType;

	duckdb::TableFunction update("moraine_update",
	                             {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                              LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR},
	                             nullptr);
	update.bind_replace = UpdateReplace;
	loader.RegisterFunction(update);
}

} // namespace moraine_duckdb
