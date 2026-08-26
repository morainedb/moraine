#include "duckdb.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

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
// list as a filter on the other side reaches the scan, which a join condition
// alone does not: a hash join's runtime filters arrive after the file list is
// built, and null-safe equality generates none at all.
//
// The pass runs after DuckDB's own optimizers, because that is when every
// qualifying join exists: DELETE and UPDATE bind their join through the WHERE
// clause as a filter over a cross product, and an EXISTS probe as a dependent
// join, both formed into comparison joins only during optimization. The
// built-in filter pushdown has already run by then, so the pass pushes the
// derived filter down its own subtree itself.
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

// The index read beneath `op`, looked through projections and filters — a
// flattened EXISTS leaves both over the lookup. Neither adds rows, so the
// resolved rows stay a superset of what the join side produces, and a filter
// derived from them stays implied by the join.
duckdb::LogicalGet *IndexReadGetBeneath(duckdb::LogicalOperator &op) {
	if (op.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION ||
	    op.type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
		return IndexReadGetBeneath(*op.children[0]);
	}
	return IndexReadGet(op);
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

// As `ReferencedColumn`, but resolving `expression` through the projections
// between `op` and `get` — each rebinds the columns it forwards.
bool ReferencedColumnBeneath(const duckdb::Expression &expression, duckdb::LogicalOperator &op,
                             const duckdb::LogicalGet &get, duckdb::idx_t &column) {
	if (op.type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
		return ReferencedColumnBeneath(expression, *op.children[0], get, column);
	}
	if (op.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
		if (expression.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		auto &reference = expression.Cast<duckdb::BoundColumnRefExpression>();
		auto &projection = op.Cast<duckdb::LogicalProjection>();
		if (reference.binding.table_index != projection.table_index ||
		    reference.binding.column_index >= projection.expressions.size()) {
			return false;
		}
		return ReferencedColumnBeneath(*projection.expressions[reference.binding.column_index], *op.children[0], get,
		                               column);
	}
	return ReferencedColumn(expression, get, column);
}

// The distinct values `rows` carries in `column`, and whether any was NULL.
// False if the column is not one the located rows describe, or if there are
// more values than a filter should list.
bool ResolvedValues(const std::vector<MoraineRowId> &rows, duckdb::idx_t column, duckdb::vector<duckdb::Value> &values,
                    bool &any_null) {
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

void FilterJoinsAgainstIndexReads(duckdb::Optimizer &optimizer, duckdb::unique_ptr<duckdb::LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		FilterJoinsAgainstIndexReads(optimizer, child);
	}

	if (plan->type != duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return;
	}
	auto &join = plan->Cast<duckdb::LogicalComparisonJoin>();
	// Inner and semi joins are the shapes whose conditions every surviving
	// row must meet; an outer join keeps rows that meet none of them.
	if ((join.join_type != duckdb::JoinType::INNER && join.join_type != duckdb::JoinType::SEMI) ||
	    join.children.size() != 2) {
		return;
	}

	duckdb::idx_t index_side = 0;
	duckdb::LogicalGet *index_get = nullptr;
	for (duckdb::idx_t side = 0; side < join.children.size(); side++) {
		auto *candidate = IndexReadGetBeneath(*join.children[side]);
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
		if (!ReferencedColumnBeneath(index_expression, *join.children[index_side], *index_get, column)) {
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
	// The built-in pushdown already ran; carry the filter down to the scan.
	duckdb::FilterPushdown pushdown(optimizer);
	join.children[other_side] = pushdown.Rewrite(std::move(filter));
}

class MoraineOptimizer : public duckdb::OptimizerExtension {
public:
	MoraineOptimizer() {
		pre_optimize_function = PreOptimize;
		optimize_function = PostOptimize;
	}

	static void PreOptimize(duckdb::OptimizerExtensionInput &input, duckdb::unique_ptr<duckdb::LogicalOperator> &plan) {
		ReplaceEmptyIndexReads(input.context, plan);
	}

	static void PostOptimize(duckdb::OptimizerExtensionInput &input,
	                         duckdb::unique_ptr<duckdb::LogicalOperator> &plan) {
		FilterJoinsAgainstIndexReads(input.optimizer, plan);
	}
};

} // namespace

void RegisterMoraineOptimizer(duckdb::DBConfig &config) {
	duckdb::OptimizerExtension::Register(config, MoraineOptimizer());
}

} // namespace moraine_duckdb
