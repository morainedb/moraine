#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "index_functions.hpp"

#include <set>

namespace moraine_duckdb {

namespace {

void ReplaceEmptyIndexReads(duckdb::ClientContext &context, duckdb::unique_ptr<duckdb::LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		ReplaceEmptyIndexReads(context, child);
	}

	if (plan->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = plan->Cast<duckdb::LogicalGet>();
	if (!IsIndexRead(get.function.name) || !get.function.cardinality) {
		return;
	}
	auto cardinality = get.function.cardinality(context, get.bind_data.get());
	if (!cardinality || !cardinality->has_max_cardinality || cardinality->max_cardinality != 0) {
		return;
	}
	plan = duckdb::make_uniq<duckdb::LogicalEmptyResult>(std::move(plan));
}

// An index read resolves its rows while binding, so a join against one is a
// join against a list of constants the planner can already see. Restating that
// list as a filter on the other side lets DuckDB's own pushdown reach the scan,
// which a join condition alone does not: a hash join's runtime filters arrive
// after the file list is built, and null-safe equality generates none at all.
//
// The filter only repeats what the join already enforces, so it is added
// beside the join rather than replacing it, and holds whatever the query
// projects from either side.

// Most distinct values one derived filter will list. Past this a lookup is a
// scan in disguise, and evaluating the list costs more than the files it could
// still exclude are worth.
constexpr duckdb::idx_t MAX_DERIVED_FILTER_VALUES = 256;

duckdb::LogicalGet *IndexReadGet(duckdb::LogicalOperator &op) {
	if (op.type != duckdb::LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	auto &get = op.Cast<duckdb::LogicalGet>();
	return IsIndexRead(get.function.name) ? &get : nullptr;
}

// The column of `get` that `expression` names, if it names one directly.
bool ReferencedColumn(const duckdb::Expression &expression, const duckdb::LogicalGet &get, duckdb::idx_t &column) {
	if (expression.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &reference = expression.Cast<duckdb::BoundColumnRefExpression>();
	if (reference.binding.table_index != get.table_index || !get.projection_ids.empty()) {
		return false;
	}
	auto &column_ids = get.GetColumnIds();
	if (reference.binding.column_index >= column_ids.size()) {
		return false;
	}
	column = column_ids[reference.binding.column_index].GetPrimaryIndex();
	return true;
}

// The distinct values `rows` carries in `column`, and whether any was NULL.
// False if the column is not one the located rows describe, or if there are
// more values than a filter should list.
bool ResolvedValues(const std::vector<MoraineRowId> &rows, duckdb::idx_t column,
                    duckdb::vector<duckdb::Value> &values, bool &any_null) {
	std::set<uint64_t> seen;
	any_null = false;
	for (auto &row : rows) {
		if (column == INDEX_READ_ROW_ID_COLUMN) {
			if (seen.insert(row.value).second) {
				values.push_back(duckdb::Value::BIGINT(static_cast<int64_t>(row.value)));
			}
		} else if (column == INDEX_READ_DATA_FILE_ID_COLUMN) {
			if (!row.has_data_file_id) {
				any_null = true;
			} else if (seen.insert(row.data_file_id).second) {
				values.push_back(duckdb::Value::UBIGINT(row.data_file_id));
			}
		} else {
			return false;
		}
		if (values.size() > MAX_DERIVED_FILTER_VALUES) {
			return false;
		}
	}
	return true;
}

// `expression comparison value` for each resolved value, OR-ed together.
// Under plain equality a NULL value matches nothing and so contributes no
// disjunct; under null-safe equality it matches a NULL column.
duckdb::unique_ptr<duckdb::Expression> DerivedFilter(const duckdb::Expression &expression,
                                                     duckdb::ExpressionType comparison,
                                                     const duckdb::vector<duckdb::Value> &values, bool any_null) {
	duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> disjuncts;

	if (!values.empty()) {
		auto in_list = duckdb::make_uniq<duckdb::BoundOperatorExpression>(duckdb::ExpressionType::COMPARE_IN,
		                                                                 duckdb::LogicalType::BOOLEAN);
		in_list->children.push_back(expression.Copy());
		for (auto &value : values) {
			in_list->children.push_back(duckdb::make_uniq<duckdb::BoundConstantExpression>(value));
		}
		disjuncts.push_back(std::move(in_list));
	}
	if (any_null && comparison == duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		auto is_null = duckdb::make_uniq<duckdb::BoundOperatorExpression>(duckdb::ExpressionType::OPERATOR_IS_NULL,
		                                                                 duckdb::LogicalType::BOOLEAN);
		is_null->children.push_back(expression.Copy());
		disjuncts.push_back(std::move(is_null));
	}

	if (disjuncts.empty()) {
		return nullptr;
	}
	if (disjuncts.size() == 1) {
		return std::move(disjuncts[0]);
	}
	return duckdb::make_uniq<duckdb::BoundConjunctionExpression>(duckdb::ExpressionType::CONJUNCTION_OR,
	                                                             std::move(disjuncts[0]), std::move(disjuncts[1]));
}

void FilterJoinsAgainstIndexReads(duckdb::unique_ptr<duckdb::LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		FilterJoinsAgainstIndexReads(child);
	}

	if (plan->type != duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return;
	}
	auto &join = plan->Cast<duckdb::LogicalComparisonJoin>();
	// An inner join is the only shape whose conditions restrict both sides;
	// an outer one keeps rows that meet none of them.
	if (join.join_type != duckdb::JoinType::INNER || join.children.size() != 2) {
		return;
	}

	duckdb::idx_t index_side = 0;
	duckdb::LogicalGet *index_get = nullptr;
	for (duckdb::idx_t side = 0; side < join.children.size(); side++) {
		auto *candidate = IndexReadGet(*join.children[side]);
		if (!candidate) {
			continue;
		}
		// Two index reads leave no scan to prune.
		if (index_get) {
			return;
		}
		index_get = candidate;
		index_side = side;
	}
	if (!index_get) {
		return;
	}
	auto *rows = IndexReadRows(index_get->function, index_get->bind_data.get());
	if (!rows || rows->empty()) {
		return;
	}

	duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> filters;
	for (auto &condition : join.conditions) {
		if (condition.comparison != duckdb::ExpressionType::COMPARE_EQUAL &&
		    condition.comparison != duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			continue;
		}
		auto &index_expression = index_side == 0 ? *condition.left : *condition.right;
		auto &other_expression = index_side == 0 ? *condition.right : *condition.left;

		duckdb::idx_t column = 0;
		if (!ReferencedColumn(index_expression, *index_get, column)) {
			continue;
		}
		// The filter repeats the other side's expression, so only a column
		// reference is certain to be cheap and free of side effects.
		if (other_expression.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
			continue;
		}

		duckdb::vector<duckdb::Value> values;
		bool any_null = false;
		if (!ResolvedValues(*rows, column, values, any_null)) {
			continue;
		}
		if (auto filter = DerivedFilter(other_expression, condition.comparison, values, any_null)) {
			filters.push_back(std::move(filter));
		}
	}
	if (filters.empty()) {
		return;
	}

	auto other_side = 1 - index_side;
	auto filter = duckdb::make_uniq<duckdb::LogicalFilter>();
	filter->expressions = std::move(filters);
	filter->children.push_back(std::move(join.children[other_side]));
	filter->ResolveOperatorTypes();
	join.children[other_side] = std::move(filter);
}

class MoraineOptimizer : public duckdb::OptimizerExtension {
public:
	MoraineOptimizer() {
		pre_optimize_function = Optimize;
	}

	static void Optimize(duckdb::OptimizerExtensionInput &input, duckdb::unique_ptr<duckdb::LogicalOperator> &plan) {
		ReplaceEmptyIndexReads(input.context, plan);
		FilterJoinsAgainstIndexReads(plan);
	}
};

} // namespace

void RegisterMoraineOptimizer(duckdb::DBConfig &config) {
	duckdb::OptimizerExtension::Register(config, MoraineOptimizer());
}

} // namespace moraine_duckdb
