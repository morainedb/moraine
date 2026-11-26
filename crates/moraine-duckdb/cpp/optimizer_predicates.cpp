#include "optimizer_predicates.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

#include <algorithm>

namespace moraine_duckdb {

void PredicateArena::Add(uint64_t column, uint8_t kind, const duckdb::vector<duckdb::Value> &values) {
	std::vector<MoraineLookupValue> converted;
	for (auto &value : values) {
		converted.push_back(BuildLookupValue(value, arena, "moraine index predicate"));
	}
	constants.push_back(std::move(converted));
	auto &stored = constants.back();
	predicates.push_back({column, kind, stored.data(), stored.size()});
}

bool EqualityValues(const duckdb::Expression &expression, duckdb::vector<duckdb::Value> &values) {
	if (expression.GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_OR) {
		for (auto &child : expression.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
			if (!EqualityValues(*child, values)) {
				return false;
			}
		}
		return true;
	}
	if (expression.GetExpressionType() == duckdb::ExpressionType::COMPARE_IN) {
		auto &children = expression.Cast<duckdb::BoundOperatorExpression>().GetChildren();
		if (children.empty() || !duckdb::ExpressionFilter::IsSimpleFilterColumnRef(*children[0])) {
			return false;
		}
		for (duckdb::idx_t i = 1; i < children.size(); i++) {
			if (children[i]->GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
				return false;
			}
			values.push_back(children[i]->Cast<duckdb::BoundConstantExpression>().GetValue());
		}
		return true;
	}
	if (!duckdb::BoundComparisonExpression::IsComparison(expression)) {
		return false;
	}
	auto type = duckdb::ExpressionType::INVALID;
	auto value = duckdb::ExpressionFilter::TryGetColumnConstantComparison(
	    expression.Cast<duckdb::BoundFunctionExpression>(), type);
	if (!value || type != duckdb::ExpressionType::COMPARE_EQUAL) {
		return false;
	}
	values.push_back(value->GetValue());
	return true;
}

bool ExtractPredicates(const duckdb::Expression &expression, uint64_t column, PredicateArena &output) {
	if (auto child = duckdb::ExpressionFilter::GetOptionalFilterChild(expression)) {
		ExtractPredicates(*child, column, output);
		return false;
	}
	if (expression.GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_AND) {
		bool complete = true;
		for (auto &child : expression.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
			complete = ExtractPredicates(*child, column, output) && complete;
		}
		return complete;
	}
	duckdb::vector<duckdb::Value> values;
	if (EqualityValues(expression, values)) {
		output.Add(column, 0, values);
		return true;
	}
	if (expression.GetExpressionType() == duckdb::ExpressionType::OPERATOR_IS_NULL) {
		auto &children = expression.Cast<duckdb::BoundOperatorExpression>().GetChildren();
		if (children.size() != 1 || !duckdb::ExpressionFilter::IsSimpleFilterColumnRef(*children[0])) {
			return false;
		}
		output.Add(column, 1, {});
		return true;
	}
	if (!duckdb::BoundComparisonExpression::IsComparison(expression)) {
		return false;
	}
	auto type = duckdb::ExpressionType::INVALID;
	auto value = duckdb::ExpressionFilter::TryGetColumnConstantComparison(
	    expression.Cast<duckdb::BoundFunctionExpression>(), type);
	if (!value || value->GetValue().IsNull()) {
		return false;
	}
	uint8_t kind;
	switch (type) {
	case duckdb::ExpressionType::COMPARE_GREATERTHAN:
		kind = 2;
		break;
	case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		kind = 3;
		break;
	case duckdb::ExpressionType::COMPARE_LESSTHAN:
		kind = 4;
		break;
	case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
		kind = 5;
		break;
	default:
		return false;
	}
	output.Add(column, kind, {value->GetValue()});
	return true;
}

namespace {

bool HasDuckLakeScan(const duckdb::LogicalOperator &plan) {
	if (plan.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
		auto &get = plan.Cast<duckdb::LogicalGet>();
		return get.function.GetDefinition() && get.function.GetDefinition()->name == "ducklake_scan";
	}
	for (auto &child : plan.children) {
		if (HasDuckLakeScan(*child)) {
			return true;
		}
	}
	return false;
}

// Collect constant row choices before DuckDB rewrites a long IN into a join.
bool TupleChoices(duckdb::ClientContext &context, const duckdb::Expression &expression,
                  const duckdb::Expression *&subject, duckdb::vector<duckdb::Value> &values) {
	if (expression.GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_OR) {
		for (auto &child : expression.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
			if (!TupleChoices(context, *child, subject, values)) {
				return false;
			}
		}
		return subject != nullptr && !values.empty();
	}
	const duckdb::Expression *candidate = nullptr;
	duckdb::vector<const duckdb::Expression *> constants;
	if (expression.GetExpressionType() == duckdb::ExpressionType::COMPARE_IN) {
		auto &children = expression.Cast<duckdb::BoundOperatorExpression>().GetChildren();
		if (children.empty()) {
			return false;
		}
		candidate = children[0].get();
		for (duckdb::idx_t i = 1; i < children.size(); i++) {
			constants.push_back(children[i].get());
		}
	} else if (duckdb::BoundComparisonExpression::IsComparison(expression) &&
	           expression.GetExpressionType() == duckdb::ExpressionType::COMPARE_EQUAL) {
		auto &comparison = expression.Cast<duckdb::BoundFunctionExpression>();
		candidate = &duckdb::BoundComparisonExpression::Left(comparison);
		constants.push_back(&duckdb::BoundComparisonExpression::Right(comparison));
		if (candidate->IsFoldable()) {
			std::swap(candidate, constants[0]);
		}
	} else {
		return false;
	}
	if (candidate->GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &row = candidate->Cast<duckdb::BoundFunctionExpression>();
	if (row.Function().GetName() != "row" || row.GetChildren().empty()) {
		return false;
	}
	for (auto &child : row.GetChildren()) {
		if (child->GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
	}
	if (subject && !subject->Equals(*candidate)) {
		return false;
	}
	subject = candidate;
	for (auto constant : constants) {
		duckdb::Value value;
		if (!constant->IsFoldable() || !duckdb::ExpressionExecutor::TryEvaluateScalar(context, *constant, value) ||
		    value.IsNull() || !duckdb::StructType::IsStruct(value.type()) ||
		    duckdb::StructValue::GetChildren(value).size() != row.GetChildren().size()) {
			return false;
		}
		values.push_back(std::move(value));
		if (values.size() > 4096) {
			return false;
		}
	}
	return !values.empty();
}

} // namespace

void ExtractTuplePredicates(duckdb::ClientContext &context, const duckdb::Expression &expression,
                            const duckdb::LogicalGet &get, const duckdb::DuckLakeTableEntry &table,
                            PredicateArena &output) {
	if (expression.GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_AND) {
		for (auto &child : expression.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
			ExtractTuplePredicates(context, *child, get, table, output);
		}
		return;
	}
	const duckdb::Expression *subject = nullptr;
	duckdb::vector<duckdb::Value> choices;
	if (!TupleChoices(context, expression, subject, choices)) {
		return;
	}
	auto &columns = subject->Cast<duckdb::BoundFunctionExpression>().GetChildren();
	auto &column_ids = get.GetColumnIds();
	for (duckdb::idx_t position = 0; position < columns.size(); position++) {
		auto &binding = columns[position]->Cast<duckdb::BoundColumnRefExpression>().Binding();
		if (binding.table_index != get.table_index || binding.column_index >= column_ids.size()) {
			continue;
		}
		auto column = column_ids[binding.column_index].GetPrimaryIndex();
		if (column >= table.GetColumns().PhysicalColumnCount()) {
			continue;
		}
		duckdb::vector<duckdb::Value> values;
		for (auto &choice : choices) {
			auto &value = duckdb::StructValue::GetChildren(choice)[position];
			if (value.IsNull()) {
				values.clear();
				break;
			}
			values.push_back(value);
		}
		if (!values.empty()) {
			output.Add(table.GetFieldId(duckdb::PhysicalIndex(column)).GetFieldIndex().index, 0, values);
		}
	}
}

void ExposeTuplePredicates(duckdb::ClientContext &context, duckdb::LogicalOperator &plan) {
	for (auto &child : plan.children) {
		ExposeTuplePredicates(context, *child);
	}
	if (plan.type != duckdb::LogicalOperatorType::LOGICAL_FILTER || !HasDuckLakeScan(plan)) {
		return;
	}
	auto &filter = plan.Cast<duckdb::LogicalFilter>();
	auto count = filter.expressions.size();
	for (duckdb::idx_t i = 0; i < count; i++) {
		const duckdb::Expression *subject = nullptr;
		duckdb::vector<duckdb::Value> choices;
		if (!TupleChoices(context, *filter.expressions[i], subject, choices)) {
			continue;
		}
		auto &columns = subject->Cast<duckdb::BoundFunctionExpression>().GetChildren();
		for (duckdb::idx_t column = 0; column < columns.size(); column++) {
			duckdb::vector<duckdb::Value> values;
			bool has_null = false;
			for (auto &choice : choices) {
				auto &value = duckdb::StructValue::GetChildren(choice)[column];
				if (value.IsNull()) {
					has_null = true;
				} else {
					values.push_back(value);
				}
			}
			duckdb::unique_ptr<duckdb::Expression> derived;
			if (!values.empty()) {
				derived = duckdb::ExpressionFilter::CreateInExpression(columns[column]->Copy(), std::move(values));
			}
			if (has_null) {
				auto nulls = duckdb::ExpressionFilter::CreateNullCheckExpression(
				    columns[column]->Copy(), duckdb::ExpressionType::OPERATOR_IS_NULL);
				derived = derived ? duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
				                        duckdb::ExpressionType::CONJUNCTION_OR, std::move(derived), std::move(nulls))
				                  : std::move(nulls);
			}
			// These are implied predicates only. The tuple predicate still
			// rejects cross-products and preserves DuckDB's row-NULL semantics.
			if (derived) {
				filter.expressions.push_back(std::move(derived));
			}
		}
	}
}

namespace {

constexpr size_t MAX_KEY_SET = 4096;

// The physical column of `get` that `expression` names directly.
bool ScanColumn(const duckdb::Expression &expression, const duckdb::LogicalGet &get, duckdb::idx_t &column) {
	if (expression.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &binding = expression.Cast<duckdb::BoundColumnRefExpression>().Binding();
	auto &column_ids = get.GetColumnIds();
	if (binding.table_index != get.table_index || binding.column_index >= column_ids.size()) {
		return false;
	}
	column = column_ids[binding.column_index].GetPrimaryIndex();
	return true;
}

// The distinct columns of `get` a `row(...)` of column references names.
bool TupleColumns(const duckdb::Expression &expression, const duckdb::LogicalGet &get,
                  std::vector<duckdb::idx_t> &columns) {
	if (expression.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &row = expression.Cast<duckdb::BoundFunctionExpression>();
	if (row.Function().GetName() != "row" || row.GetChildren().empty()) {
		return false;
	}
	for (auto &child : row.GetChildren()) {
		duckdb::idx_t column;
		if (!ScanColumn(*child, get, column) || std::find(columns.begin(), columns.end(), column) != columns.end()) {
			return false;
		}
		columns.push_back(column);
	}
	return true;
}

// Names the columns `subject` keys: one column, or the distinct columns of a
// `row(...)`, which `tuple` reports.
bool SubjectColumns(const duckdb::Expression &subject, const duckdb::LogicalGet &get, KeySet &result, bool &tuple) {
	duckdb::idx_t column;
	if (ScanColumn(subject, get, column)) {
		result.columns.push_back(column);
		tuple = false;
		return true;
	}
	tuple = true;
	return TupleColumns(subject, get, result.columns);
}

// Adds one constant as a key: a value for a column, a struct for a tuple. A
// NULL constant names no row and is dropped; a NULL field inside a tuple
// declines.
bool AppendKey(KeySet &result, bool tuple, const duckdb::Value &value) {
	if (value.IsNull()) {
		return true;
	}
	if (!tuple) {
		result.keys.push_back({value});
		return true;
	}
	if (!duckdb::StructType::IsStruct(value.type())) {
		return false;
	}
	auto &fields = duckdb::StructValue::GetChildren(value);
	if (fields.size() != result.columns.size()) {
		return false;
	}
	for (auto &field : fields) {
		if (field.IsNull()) {
			return false;
		}
	}
	result.keys.push_back(fields);
	return true;
}

// The keys `subject` takes from `constants`.
std::optional<KeySet> SubjectKeys(duckdb::ClientContext &context, const duckdb::Expression &subject,
                                  const duckdb::vector<const duckdb::Expression *> &constants,
                                  const duckdb::LogicalGet &get) {
	KeySet result;
	bool tuple = false;
	if (!SubjectColumns(subject, get, result, tuple)) {
		return std::nullopt;
	}
	for (auto constant : constants) {
		duckdb::Value value;
		if (!constant->IsFoldable() || !duckdb::ExpressionExecutor::TryEvaluateScalar(context, *constant, value) ||
		    !AppendKey(result, tuple, value)) {
			return std::nullopt;
		}
	}
	if (result.keys.size() > MAX_KEY_SET) {
		return std::nullopt;
	}
	return result;
}

// The keys in either set, which must name the same columns.
std::optional<KeySet> UniteKeySets(KeySet left, const KeySet &right) {
	if (left.columns.size() != right.columns.size()) {
		return std::nullopt;
	}
	std::vector<duckdb::idx_t> positions;
	for (auto column : left.columns) {
		auto found = std::find(right.columns.begin(), right.columns.end(), column);
		if (found == right.columns.end()) {
			return std::nullopt;
		}
		positions.push_back(static_cast<duckdb::idx_t>(found - right.columns.begin()));
	}
	for (auto &key : right.keys) {
		duckdb::vector<duckdb::Value> reordered;
		for (auto position : positions) {
			reordered.push_back(key[position]);
		}
		left.keys.push_back(std::move(reordered));
		if (left.keys.size() > MAX_KEY_SET) {
			return std::nullopt;
		}
	}
	return left;
}

} // namespace

std::optional<KeySet> ConjoinKeySets(const KeySet &left, const KeySet &right) {
	KeySet result;
	result.columns = left.columns;
	std::vector<std::pair<duckdb::idx_t, duckdb::idx_t>> shared;
	std::vector<duckdb::idx_t> appended;
	for (duckdb::idx_t position = 0; position < right.columns.size(); position++) {
		auto found = std::find(left.columns.begin(), left.columns.end(), right.columns[position]);
		if (found == left.columns.end()) {
			result.columns.push_back(right.columns[position]);
			appended.push_back(position);
		} else {
			shared.emplace_back(static_cast<duckdb::idx_t>(found - left.columns.begin()), position);
		}
	}
	for (auto &left_key : left.keys) {
		for (auto &right_key : right.keys) {
			bool agree =
			    std::all_of(shared.begin(), shared.end(), [&](const std::pair<duckdb::idx_t, duckdb::idx_t> &pair) {
				    return duckdb::Value::NotDistinctFrom(left_key[pair.first], right_key[pair.second]);
			    });
			if (!agree) {
				continue;
			}
			auto key = left_key;
			for (auto position : appended) {
				key.push_back(right_key[position]);
			}
			result.keys.push_back(std::move(key));
			if (result.keys.size() > MAX_KEY_SET) {
				return std::nullopt;
			}
		}
	}
	return result;
}

std::optional<KeySet> ExtractKeySet(duckdb::ClientContext &context, const duckdb::Expression &expression,
                                    const duckdb::LogicalGet &get) {
	switch (expression.GetExpressionType()) {
	case duckdb::ExpressionType::CONJUNCTION_AND: {
		KeySet result;
		result.keys.push_back({});
		for (auto &child : expression.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
			auto part = ExtractKeySet(context, *child, get);
			if (!part) {
				return std::nullopt;
			}
			auto joined = ConjoinKeySets(result, *part);
			if (!joined) {
				return std::nullopt;
			}
			result = std::move(*joined);
		}
		return result;
	}
	case duckdb::ExpressionType::CONJUNCTION_OR: {
		std::optional<KeySet> result;
		for (auto &child : expression.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
			auto part = ExtractKeySet(context, *child, get);
			if (!part) {
				return std::nullopt;
			}
			result = result ? UniteKeySets(std::move(*result), *part) : std::move(part);
			if (!result) {
				return std::nullopt;
			}
		}
		return result;
	}
	case duckdb::ExpressionType::COMPARE_IN: {
		auto &children = expression.Cast<duckdb::BoundOperatorExpression>().GetChildren();
		if (children.empty()) {
			return std::nullopt;
		}
		duckdb::vector<const duckdb::Expression *> constants;
		for (duckdb::idx_t i = 1; i < children.size(); i++) {
			constants.push_back(children[i].get());
		}
		return SubjectKeys(context, *children[0], constants, get);
	}
	case duckdb::ExpressionType::COMPARE_EQUAL: {
		if (!duckdb::BoundComparisonExpression::IsComparison(expression)) {
			return std::nullopt;
		}
		auto &comparison = expression.Cast<duckdb::BoundFunctionExpression>();
		const duckdb::Expression *subject = &duckdb::BoundComparisonExpression::Left(comparison);
		const duckdb::Expression *constant = &duckdb::BoundComparisonExpression::Right(comparison);
		if (subject->IsFoldable()) {
			std::swap(subject, constant);
		}
		return SubjectKeys(context, *subject, {constant}, get);
	}
	default:
		return std::nullopt;
	}
}

std::optional<KeySet> MarkJoinKeySet(duckdb::ClientContext &context, const duckdb::LogicalComparisonJoin &join,
                                     const duckdb::LogicalGet &get) {
	if (join.join_type != duckdb::JoinType::MARK || join.children.size() != 2 ||
	    join.children[1]->type != duckdb::LogicalOperatorType::LOGICAL_CHUNK_GET || join.conditions.size() != 1) {
		return std::nullopt;
	}
	auto &chunk = join.children[1]->Cast<duckdb::LogicalColumnDataGet>();
	auto &condition = join.conditions[0];
	if (condition.GetComparisonType() != duckdb::ExpressionType::COMPARE_EQUAL || !chunk.collection ||
	    chunk.collection->ColumnCount() != 1) {
		return std::nullopt;
	}
	auto &constants = condition.GetRHS();
	if (constants.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
		return std::nullopt;
	}
	auto &binding = constants.Cast<duckdb::BoundColumnRefExpression>().Binding();
	if (binding.table_index != chunk.table_index || binding.column_index != 0) {
		return std::nullopt;
	}
	KeySet result;
	bool tuple = false;
	if (!SubjectColumns(condition.GetLHS(), get, result, tuple)) {
		return std::nullopt;
	}
	auto count = chunk.collection->Count();
	if (count > MAX_KEY_SET) {
		return std::nullopt;
	}
	for (duckdb::idx_t i = 0; i < count; i++) {
		if (!AppendKey(result, tuple, chunk.collection->GetValue(0, i))) {
			return std::nullopt;
		}
	}
	return result;
}

std::optional<KeySet> TableFilterKeySet(const duckdb::TableFilter &filter, duckdb::idx_t column) {
	if (filter.filter_type != duckdb::TableFilterType::EXPRESSION_FILTER) {
		return std::nullopt;
	}
	auto &expression = filter.Cast<duckdb::ExpressionFilter>();
	if (!expression.column_indexes.empty()) {
		return std::nullopt;
	}
	const duckdb::Expression *inner = expression.expr.get();
	if (auto child = duckdb::ExpressionFilter::GetOptionalFilterChild(*inner)) {
		inner = child.get();
	}
	duckdb::vector<duckdb::Value> values;
	if (!EqualityValues(*inner, values)) {
		return std::nullopt;
	}
	KeySet result;
	result.columns.push_back(column);
	for (auto &value : values) {
		if (!value.IsNull()) {
			result.keys.push_back({value});
		}
	}
	return result;
}

} // namespace moraine_duckdb
