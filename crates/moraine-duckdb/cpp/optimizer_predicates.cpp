#include "optimizer_predicates.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

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
		return true;
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

} // namespace moraine_duckdb
