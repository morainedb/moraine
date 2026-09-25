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

// The located rows and the payload, as the replacement query names them.
// `new` is the caller's handle on a payload field; the other only has to
// differ from it and from any column a caller writes unqualified.
constexpr auto LOCATED_ALIAS = "moraine_located";
constexpr auto PAYLOAD_ALIAS = "new";

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

	// A payload joins beside the row, so an unassigned column is qualified
	// to stay unambiguous against a payload field of the same name — which
	// is the ordinary case, a column updated from a field named for it.
	std::vector<std::string> payload;
	auto pairs = ParseLocatedPairs(rows, "moraine_update", &payload);
	auto located = payload.empty() ? std::string() : std::string(LOCATED_ALIAS) + ".";

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
		projection += assignment == assigned.end() ? located + quoted
		                                           : "(" + assignment->second + ") AS " + quoted;
	}
	for (auto &assignment : assigned) {
		if (!known.count(assignment.first)) {
			throw duckdb::InvalidInputException("moraine_update: table \"%s\" has no column \"%s\"", table_name,
			                                    assignment.first);
		}
	}

	// `moraine_rows_at` takes the pair shape and nothing else, so the read
	// side gets the pairs back without their payload.
	duckdb::child_list_t<duckdb::LogicalType> pair_fields {{"row_id", duckdb::LogicalType::BIGINT},
	                                                       {"data_file_id", duckdb::LogicalType::UBIGINT}};
	duckdb::vector<duckdb::Value> pair_values;
	pair_values.reserve(pairs.size());
	for (auto &pair : pairs) {
		duckdb::child_list_t<duckdb::Value> fields {
		    {"row_id", duckdb::Value::BIGINT(static_cast<int64_t>(pair.row_id))},
		    {"data_file_id", pair.has_data_file_id ? duckdb::Value::UBIGINT(pair.data_file_id)
		                                           : duckdb::Value(duckdb::LogicalType::UBIGINT)}};
		pair_values.push_back(duckdb::Value::STRUCT(std::move(fields)));
	}
	auto located_rows = duckdb::Value::LIST(duckdb::LogicalType::STRUCT(pair_fields), std::move(pair_values));

	// An empty list renders as `[]`, which carries no element type, and the
	// replacement query is re-bound from this text — so both lists are cast
	// back to the type they were built with.
	auto typed = [](const duckdb::Value &value) {
		return value.ToSQLString() + "::" + value.type().ToString();
	};

	auto source = duckdb::StringUtil::Format("moraine_rows_at(%s, %s, %s, %s)",
	                                         duckdb::KeywordHelper::WriteQuoted(catalog_name),
	                                         duckdb::KeywordHelper::WriteQuoted(schema_name),
	                                         duckdb::KeywordHelper::WriteQuoted(table_name),
	                                         typed(located_rows));
	if (payload.empty()) {
		return duckdb::StringUtil::Format("SELECT %s, row_id FROM %s", projection, source);
	}
	// `unnest` at depth two spreads the struct's fields into columns, so the
	// payload is addressable as `new.<field>` without naming any of them here.
	return duckdb::StringUtil::Format(
	    "SELECT %s, %s.row_id FROM %s AS %s JOIN (SELECT unnest(%s, max_depth := 2)) AS %s "
	    "ON %s.row_id = %s.row_id",
	    projection, LOCATED_ALIAS, source, LOCATED_ALIAS, typed(rows), PAYLOAD_ALIAS, PAYLOAD_ALIAS,
	    LOCATED_ALIAS);
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
	auto located = ResolveLocatedArguments(context, catalog_name, schema_name, table_name, input.inputs[3],
	                                       "moraine_update", /* allow_payload */ true);

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
