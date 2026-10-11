#include "duckdb/planner/filter/list.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "catalog.hpp"
#include "index_cache.hpp"
#include "index_functions.hpp"
#include "owned_array.hpp"
#include "optimizer_predicates.hpp"
#include "summary_scan.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_stats.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_transaction.hpp"

#include <set>
#include <algorithm>

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
	if (!IsIndexRead(get.function.GetDefinition()->name.GetIdentifierName()) || !get.function.cardinality) {
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

// Bounds the row-id pruning list and its planning cost.
constexpr duckdb::idx_t MAX_DERIVED_ROW_ID_VALUES = 4096;

// Larger lists prune scan statistics only; the join performs exact matching.
constexpr duckdb::idx_t MAX_EXACT_ROW_ID_VALUES = 256;

// DuckLake expands these into metadata predicates as well as scan filters.
constexpr duckdb::idx_t MAX_ROW_PRUNING_RANGES = 16;

// Most distinct file ids one derived filter will list. The list dedups to the
// files holding the rows and is checked against each file's constant id, not
// against every row, so it stays worthwhile far past the row-id bound; this
// only keeps a pathological plan bounded.
constexpr duckdb::idx_t MAX_DERIVED_FILE_ID_VALUES = 16384;

duckdb::LogicalGet *IndexReadGet(duckdb::LogicalOperator &op) {
	if (op.type != duckdb::LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	auto &get = op.Cast<duckdb::LogicalGet>();
	return IsIndexRead(get.function.GetDefinition()->name.GetIdentifierName()) ? &get : nullptr;
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
	if (reference.Binding().table_index != get.table_index) {
		return false;
	}
	auto &column_ids = get.GetColumnIds();
	if (reference.Binding().column_index >= column_ids.size()) {
		return false;
	}
	column = column_ids[reference.Binding().column_index].GetPrimaryIndex();
	return true;
}

// Where `column` sits in this scan's projection. A pushed filter is keyed by
// that position, and the scan resolves it back through its own column list,
// so a key carrying the table's column index names the wrong column.
duckdb::optional_idx ProjectedColumn(const duckdb::LogicalGet &get, duckdb::idx_t column) {
	auto &column_ids = get.GetColumnIds();
	for (duckdb::idx_t i = 0; i < column_ids.size(); i++) {
		if (column_ids[i].GetPrimaryIndex() == column) {
			return duckdb::optional_idx(i);
		}
	}
	return duckdb::optional_idx();
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
		if (reference.Binding().table_index != projection.table_index ||
		    reference.Binding().column_index >= projection.expressions.size()) {
			return false;
		}
		return ReferencedColumnBeneath(*projection.expressions[reference.Binding().column_index], *op.children[0], get,
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
	auto most_values =
	    column == INDEX_READ_ROW_ID_COLUMN ? MAX_DERIVED_ROW_ID_VALUES : MAX_DERIVED_FILE_ID_VALUES;
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
		if (values.size() > most_values) {
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
		in_list->GetChildrenMutable().push_back(expression.Copy());
		for (auto &value : values) {
			in_list->GetChildrenMutable().push_back(duckdb::make_uniq<duckdb::BoundConstantExpression>(value));
		}
		disjuncts.push_back(std::move(in_list));
	}
	if (any_null && comparison == duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		auto is_null = duckdb::make_uniq<duckdb::BoundOperatorExpression>(duckdb::ExpressionType::OPERATOR_IS_NULL,
		                                                                  duckdb::LogicalType::BOOLEAN);
		is_null->GetChildrenMutable().push_back(expression.Copy());
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

struct RowPruning {
	duckdb::unique_ptr<duckdb::Expression> expression;
	duckdb::vector<duckdb::Value> values;
};

// Preserve the largest gaps, merging smaller gaps into conservative ranges.
duckdb::unique_ptr<duckdb::TableFilter> RowRangeFilter(const duckdb::vector<duckdb::Value> &values) {
	std::vector<int64_t> sorted;
	for (auto &value : values) {
		sorted.push_back(value.GetValue<int64_t>());
	}
	std::sort(sorted.begin(), sorted.end());
	std::vector<std::pair<uint64_t, duckdb::idx_t>> gaps;
	for (duckdb::idx_t i = 1; i < sorted.size(); i++) {
		auto gap = static_cast<uint64_t>(sorted[i]) - static_cast<uint64_t>(sorted[i - 1]);
		if (gap > 1) {
			gaps.emplace_back(gap, i);
		}
	}
	std::sort(gaps.begin(), gaps.end(), std::greater<std::pair<uint64_t, duckdb::idx_t>>());
	if (gaps.size() >= MAX_ROW_PRUNING_RANGES) {
		gaps.resize(MAX_ROW_PRUNING_RANGES - 1);
	}
	std::set<duckdb::idx_t> ends;
	for (auto &gap : gaps) {
		ends.insert(gap.second);
	}
	ends.insert(sorted.size());
	auto result = duckdb::make_uniq<duckdb::LegacyConjunctionOrFilter>();
	duckdb::idx_t start = 0;
	for (auto end : ends) {
		auto range = duckdb::make_uniq<duckdb::LegacyConjunctionAndFilter>();
		range->child_filters.push_back(duckdb::make_uniq<duckdb::LegacyConstantFilter>(
		    duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO, duckdb::Value::BIGINT(sorted[start])));
		range->child_filters.push_back(duckdb::make_uniq<duckdb::LegacyConstantFilter>(
		    duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO, duckdb::Value::BIGINT(sorted[end - 1])));
		result->child_filters.push_back(std::move(range));
		start = end;
	}
	if (result->child_filters.size() == 1) {
		return std::move(result->child_filters[0]);
	}
	return std::move(result);
}

// Attach only a statistics filter: an exact IN above the scan would repeat
// thousands of comparisons per row after the ordinary IN optimizer has run.
void PushRowPruning(duckdb::LogicalOperator &op, RowPruning &pruning) {
	auto *scan = &op;
	while (scan->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION ||
	       scan->type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
		scan = scan->children[0].get();
	}
	if (scan->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = scan->Cast<duckdb::LogicalGet>();
	duckdb::idx_t column = 0;
	if (get.function.GetDefinition()->name != "ducklake_scan" || !get.function.filter_pushdown ||
	    !ReferencedColumnBeneath(*pruning.expression, op, get, column) ||
	    pruning.expression->GetReturnType() != duckdb::LogicalType::BIGINT) {
		return;
	}
	auto projected = ProjectedColumn(get, column);
	if (!projected.IsValid()) {
		return;
	}
	// A pushed filter is an expression now, and carries no "optional" mark:
	// the ranges cover every located row id, so requiring it drops nothing.
	auto ranges = RowRangeFilter(pruning.values);
	auto expression = duckdb::ExpressionFilter::FromTableFilter(*ranges, duckdb::LogicalType::BIGINT);
	if (!expression || !expression->expr) {
		return;
	}
	get.table_filters.PushFilter(duckdb::ProjectionIndex(projected.GetIndex()), std::move(expression));
}

duckdb::LogicalGet *ScanBeneath(duckdb::LogicalOperator &op) {
	if (op.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION || op.type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
		return ScanBeneath(*op.children[0]);
	}
	return op.type == duckdb::LogicalOperatorType::LOGICAL_GET ? &op.Cast<duckdb::LogicalGet>() : nullptr;
}

bool TrySummaryJoin(duckdb::ClientContext &context, duckdb::LogicalComparisonJoin &join,
                    duckdb::idx_t index_side, duckdb::LogicalGet &index, const std::vector<MoraineRowId> &rows) {
	auto &other = *join.children[1 - index_side];
	auto scan = ScanBeneath(other);
	if (!scan) { return false; }
	bool row_id = false, file_id = false;
	for (auto &condition : join.conditions) {
		if (condition.GetComparisonType() != duckdb::ExpressionType::COMPARE_EQUAL &&
		    condition.GetComparisonType() != duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM) { continue; }
		auto &index_expression = index_side == 0 ? condition.GetLHS() : condition.GetRHS();
		auto &scan_expression = index_side == 0 ? condition.GetRHS() : condition.GetLHS();
		duckdb::idx_t index_column, scan_column;
		if (!ReferencedColumnBeneath(index_expression, *join.children[index_side], index, index_column) ||
		    !ReferencedColumnBeneath(scan_expression, other, *scan, scan_column)) { continue; }
		row_id = row_id || (index_column == INDEX_READ_ROW_ID_COLUMN && scan_column == duckdb::COLUMN_IDENTIFIER_ROW_ID);
		file_id = file_id || (index_column == INDEX_READ_DATA_FILE_ID_COLUMN &&
		    scan_column == duckdb::DuckLakeMultiFileReader::COLUMN_IDENTIFIER_DATA_FILE_ID);
	}
	return row_id && file_id && UseSummaryScan(context, *scan, index, rows);
}

void FilterJoinsAgainstIndexReads(duckdb::ClientContext &context, duckdb::Optimizer &optimizer, duckdb::unique_ptr<duckdb::LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		FilterJoinsAgainstIndexReads(context, optimizer, child);
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
	auto *rows = IndexReadRows(*index_get->function.GetDefinition(), index_get->bind_data.get());
	if (!rows || rows->empty()) {
		return;
	}
	if (TrySummaryJoin(context, join, index_side, *index_get, *rows)) { return; }

	duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> filters;
	std::vector<RowPruning> row_pruning;
	for (auto &condition : join.conditions) {
		if (condition.GetComparisonType() != duckdb::ExpressionType::COMPARE_EQUAL &&
		    condition.GetComparisonType() != duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			continue;
		}
		auto &index_expression = index_side == 0 ? condition.GetLHS() : condition.GetRHS();
		auto &other_expression = index_side == 0 ? condition.GetRHS() : condition.GetLHS();

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
		if (column == INDEX_READ_ROW_ID_COLUMN && values.size() > MAX_EXACT_ROW_ID_VALUES) {
			row_pruning.push_back({other_expression.Copy(), std::move(values)});
			continue;
		}
		if (auto filter = DerivedFilter(other_expression, condition.GetComparisonType(), values, any_null)) {
			filters.push_back(std::move(filter));
		}
	}
	auto other_side = 1 - index_side;
	if (!filters.empty()) {
		auto filter = duckdb::make_uniq<duckdb::LogicalFilter>();
		filter->expressions = std::move(filters);
		filter->children.push_back(std::move(join.children[other_side]));
		filter->ResolveOperatorTypes();
		// The built-in pushdown already ran; carry the filter down to the scan.
		duckdb::FilterPushdown pushdown(optimizer);
		join.children[other_side] = pushdown.Rewrite(std::move(filter));
	}
	for (auto &pruning : row_pruning) {
		PushRowPruning(*join.children[other_side], pruning);
	}
}

// Below this many bytes of data files a scan is cheaper than the probe it
// would save: a probe costs about 3 ms, about what scanning a megabyte does.
constexpr duckdb::idx_t MIN_PRUNABLE_TABLE_BYTES = 4ULL * 1024 * 1024;

// A column the scan reads for a filter and nothing else. Appended after the
// projected ones, with the projection pinned to what it was, so the
// operator's output does not change.
duckdb::ProjectionIndex FilterOnlyColumn(duckdb::LogicalGet &get, duckdb::column_t column_id) {
	auto existing = get.TryGetProjectionIndex(column_id);
	if (existing.IsValid()) {
		return existing;
	}
	if (get.projection_ids.empty()) {
		for (duckdb::idx_t position = 0; position < get.GetColumnIds().size(); position++) {
			get.projection_ids.push_back(duckdb::ProjectionIndex(position));
		}
	}
	return get.AddColumnId(column_id);
}

// A filter over the scan's own column, as a pushed filter names it.
duckdb::unique_ptr<duckdb::Expression> ScanColumn(duckdb::LogicalType type) {
	return duckdb::make_uniq<duckdb::BoundReferenceExpression>(std::move(type), 0);
}

// Candidate rows and exact-answer certification at the scan's snapshot.
struct ResolvedPredicate {
	duckdb::optional_ptr<duckdb::DuckLakeTableEntry> table;
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	std::vector<MoraineRowId> rows;
	// The snapshot the scan reads, which a positional change names so
	// DuckLake refuses it against a newer view.
	uint64_t snapshot_id = 0;
	bool certified = false;
};

// Resolves supported predicates through a ready index. A declined probe
// leaves the scan unchanged.
std::optional<ResolvedPredicate> ResolveIndexedPredicate(duckdb::ClientContext &context,
                                                       duckdb::Optimizer &optimizer,
                                                       duckdb::LogicalGet &get) {
	if (!get.function.GetDefinition() || get.function.GetDefinition()->name != "ducklake_scan") {
		return std::nullopt;
	}
	if (!get.function.filter_pushdown) {
		return std::nullopt;
	}
	if (!get.table_filters.HasFilters()) {
		return std::nullopt;
	}
	// DuckLake hands its scan state to the function rather than the operator.
	auto function_info = get.function.function_info ? get.function.function_info : get.bind_info;
	if (!function_info) {
		return std::nullopt;
	}
	auto *info = dynamic_cast<duckdb::DuckLakeFunctionInfo *>(function_info.get());
	if (!info || info->scan_type != duckdb::DuckLakeScanType::SCAN_TABLE) {
		return std::nullopt;
	}
	auto &table = info->table;
	auto transaction = info->GetTransaction();
	if (!transaction || table.IsTransactionLocal()) {
		return std::nullopt;
	}
	// Rows this transaction inserted carry no file id and no index entry
	// yet; a file filter would drop them.
	if (transaction->HasTransactionLocalInserts(info->table_id)) {
		return std::nullopt;
	}
	// A scan at another snapshot than the transaction's is a different cut
	// than the one the index resolves against.
	if (transaction->GetSnapshot().snapshot_id != info->snapshot.snapshot_id) {
		return std::nullopt;
	}
	auto stats = table.GetTableStats(*transaction);
	if (!stats || stats->table_size_bytes < MIN_PRUNABLE_TABLE_BYTES) {
		return std::nullopt;
	}

	auto &column_ids = get.GetColumnIds();
	auto stored_columns = table.GetColumns().PhysicalColumnCount();
	PredicateArena predicates;
	bool complete = !get.table_filters.HasMultiColumnFilters();
	for (duckdb::idx_t position = 0; position < column_ids.size(); position++) {
		auto filter = get.table_filters.TryGetFilterByColumnIndex(duckdb::ProjectionIndex(position));
		if (!filter) {
			continue;
		}
		auto column = column_ids[position].GetPrimaryIndex();
		if (column >= stored_columns || filter->filter_type != duckdb::TableFilterType::EXPRESSION_FILTER) {
			complete = false;
			continue;
		}
		auto &expression = filter->Cast<duckdb::ExpressionFilter>();
		if (!expression.column_indexes.empty()) {
			complete = false;
			continue;
		}
		auto field = table.GetFieldId(duckdb::PhysicalIndex(column)).GetFieldIndex().index;
		auto previous = predicates.predicates.size();
		try {
			complete = ExtractPredicates(*expression.expr, field, predicates) && complete;
		} catch (const std::exception &) {
			predicates.predicates.resize(previous);
			complete = false;
		}
	}
	if (predicates.predicates.empty()) {
		return std::nullopt;
	}

	bool cacheable = false;
	auto handle =
	    BindIndexReadFor(context, table.ParentCatalog().GetName().GetIdentifierName(), &optimizer.binder, cacheable);
	auto schema_name = table.ParentSchema().name.GetIdentifierName();
	auto table_name = table.name.GetIdentifierName();
	OwnedArray<MoraineRowId> rows(moraine_index_lookup_free);
	bool covered = false;
	MoraineError error {};
	bool certified = false;
	auto code = moraine_index_predicate_rows(
	    handle, schema_name.c_str(), table_name.c_str(), predicates.predicates.data(), predicates.predicates.size(),
	    rows.OutItems(), rows.OutLen(), &covered, &certified, moraine_shim_is_interrupted, &context, &error);
	if (code == MORAINE_INTERRUPTED) {
		ThrowMoraineError(error);
	}
	if (code != MORAINE_OK) {
		// The scan still answers from every file; pruning is an optimization.
		WriteMoraineLog(duckdb::DatabaseInstance::GetDatabase(context), duckdb::LogLevel::LOG_DEBUG,
		                std::string("index pushdown declined: ") +
		                    (error.message != nullptr ? error.message : "unknown error"));
		moraine_error_free(error.message);
		return std::nullopt;
	}
	if (!covered) {
		return std::nullopt;
	}

	return ResolvedPredicate {
	    &table,
	    table.ParentCatalog().GetName().GetIdentifierName(),
	    schema_name,
	    table_name,
	    std::vector<MoraineRowId>(rows.begin(), rows.end()),
	    info->snapshot.snapshot_id,
	    certified && complete,
	};
}

// Pushes `rowid` and `data_file_id` filters for the rows an index resolves a
// predicate to, as the join rule pushes them for a lookup in the query.
// Every uncertainty leaves the scan as it was: the filter still runs in full
// over whatever is read, so a pushed filter can only narrow the reading, and
// only an index that is complete for the scan's snapshot may narrow it.
void PruneScanByIndexedPredicate(duckdb::ClientContext &context, duckdb::LogicalGet &get,
                                const ResolvedPredicate &found) {
	auto &table = *found.table;
	auto &schema_name = found.schema_name;
	auto &table_name = found.table_name;
	auto &resolved = found.rows;
	if (found.rows.empty()) {
		// No row holds the value, so no row passes: a range no row id is in.
		get.table_filters.PushFilter(FilterOnlyColumn(get, duckdb::COLUMN_IDENTIFIER_ROW_ID),
		                             duckdb::ExpressionFilter::CreateComparisonFilter(
		                                 duckdb::ExpressionType::COMPARE_LESSTHAN, duckdb::Value::BIGINT(0)));
		return;
	}
	// The summary scan reads located pages and evaluates the predicate.
	// With one page per column chunk, it reads as much as row-group pruning.
	if (UseSummaryScan(context, get, table.ParentCatalog().GetName().GetIdentifierName(), schema_name, table_name,
	                   resolved)) {
		return;
	}
	duckdb::vector<duckdb::Value> file_ids;
	bool any_unplaced = false;
	duckdb::vector<duckdb::Value> row_ids;
	bool row_null = false;
	if (!ResolvedValues(resolved, INDEX_READ_DATA_FILE_ID_COLUMN, file_ids, any_unplaced) ||
	    !ResolvedValues(resolved, INDEX_READ_ROW_ID_COLUMN, row_ids, row_null)) {
		return;
	}

	// Files: the ones holding a resolved row, and — for a row the lookup
	// could not place, or one living inlined — the rows with no file at all.
	if (!file_ids.empty() || any_unplaced) {
		duckdb::unique_ptr<duckdb::Expression> file_filter;
		if (!file_ids.empty()) {
			file_filter = duckdb::ExpressionFilter::CreateInExpression(ScanColumn(duckdb::LogicalType::UBIGINT), file_ids);
		}
		if (any_unplaced) {
			auto unplaced = duckdb::ExpressionFilter::CreateNullCheckExpression(
			    ScanColumn(duckdb::LogicalType::UBIGINT), duckdb::ExpressionType::OPERATOR_IS_NULL);
			file_filter = file_filter ? duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
			                                duckdb::ExpressionType::CONJUNCTION_OR, std::move(file_filter),
			                                std::move(unplaced))
			                          : std::move(unplaced);
		}
		get.table_filters.PushFilter(
		    FilterOnlyColumn(get, duckdb::DuckLakeMultiFileReader::COLUMN_IDENTIFIER_DATA_FILE_ID),
		    duckdb::make_uniq<duckdb::ExpressionFilter>(std::move(file_filter)));
	}

	// Rows: exact while few, otherwise the ranges the join rule uses, which
	// cover every resolved id and let the reader skip row groups.
	duckdb::unique_ptr<duckdb::TableFilter> row_filter;
	if (row_ids.size() <= MAX_EXACT_ROW_ID_VALUES) {
		row_filter = duckdb::make_uniq<duckdb::ExpressionFilter>(
		    duckdb::ExpressionFilter::CreateInExpression(ScanColumn(duckdb::LogicalType::BIGINT), row_ids));
	} else {
		row_filter = duckdb::ExpressionFilter::FromTableFilter(*RowRangeFilter(row_ids), duckdb::LogicalType::BIGINT);
	}
	if (row_filter) {
		get.table_filters.PushFilter(FilterOnlyColumn(get, duckdb::COLUMN_IDENTIFIER_ROW_ID), std::move(row_filter));
	}
}

// The scan a delete reads its rows from, when nothing but projections sit
// between them. A filter, join or aggregate in that chain means a residual
// predicate the index cannot speak for, so the rewrite declines.
duckdb::optional_ptr<duckdb::LogicalGet> DeleteSourceScan(duckdb::LogicalOperator &op) {
	auto *current = &op;
	while (current->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
		if (current->children.size() != 1 ||
		    current->type != duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
			return nullptr;
		}
		current = current->children[0].get();
	}
	return &current->Cast<duckdb::LogicalGet>();
}

// The `files` argument `ducklake_delete_positions` takes: one entry per data
// file, carrying the positions to kill and the positions its current delete
// file already holds.
duckdb::Value PositionalFiles(const OwnedArray<MoraineLocatedFile> &files) {
	duckdb::child_list_t<duckdb::LogicalType> fields {
	    {"data_file_id", duckdb::LogicalType::UBIGINT},
	    {"positions", duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT)},
	    {"existing_positions", duckdb::LogicalType::BLOB}};
	duckdb::vector<duckdb::Value> entries;
	for (auto &file : files) {
		duckdb::vector<duckdb::Value> positions;
		for (size_t i = 0; i < file.positions_len; i++) {
			positions.push_back(duckdb::Value::UBIGINT(file.positions[i]));
		}
		// The positions the file's current delete file already carries,
		// packed as little-endian uint64s. NULL would make DuckLake read
		// that file itself; we already decoded it.
		std::string packed;
		packed.reserve(file.existing_positions_len * sizeof(uint64_t));
		for (size_t i = 0; i < file.existing_positions_len; i++) {
			auto position = file.existing_positions[i];
			for (size_t byte = 0; byte < sizeof(uint64_t); byte++) {
				packed.push_back(static_cast<char>((position >> (byte * 8)) & 0xFF));
			}
		}
		duckdb::child_list_t<duckdb::Value> values {
		    {"data_file_id", duckdb::Value::UBIGINT(file.data_file_id)},
		    {"positions", duckdb::Value::LIST(duckdb::LogicalType::UBIGINT, std::move(positions))},
		    {"existing_positions",
		     file.has_existing_delete ? duckdb::Value::BLOB_RAW(packed)
		                              : duckdb::Value(duckdb::LogicalType::BLOB)}};
		entries.push_back(duckdb::Value::STRUCT(std::move(values)));
	}
	return duckdb::Value::LIST(duckdb::LogicalType::STRUCT(fields), std::move(entries));
}

// Replace an exactly answered, certified delete with positional deletion.
// Residual predicates and unsupported delete formats retain the scan.
bool RewriteDeleteByIndexedPredicate(duckdb::ClientContext &context, duckdb::Optimizer &optimizer,
                                    duckdb::unique_ptr<duckdb::LogicalOperator> &plan,
                                    const ResolvedPredicate &found) {
	auto &deletion = plan->Cast<duckdb::LogicalDelete>();
	if (deletion.return_chunk) {
		return false;
	}
	// No residual predicate may survive, because none is evaluated; and the
	// index's answer stands in for the scan's, so it must be certified.
	auto &table = *found.table;
	auto &catalog = table.ParentCatalog().Cast<duckdb::DuckLakeCatalog>();
	auto &schema = table.ParentSchema().Cast<duckdb::DuckLakeSchemaEntry>();
	if (!found.certified || catalog.CatalogSnapshot() ||
	    catalog.WriteDeletionVectors(schema.GetSchemaId(), table.GetTableId())) {
		return false;
	}
	if (found.rows.empty()) {
		// Nothing to delete, and the ordinary plan already reads nothing:
		// the pushed impossible row filter prunes every file.
		return false;
	}

	// The positional writer handles committed files only.
	for (auto &row : found.rows) {
		if (!row.has_data_file_id) {
			return false;
		}
	}

	bool cacheable = false;
	auto handle = BindIndexReadFor(context, found.catalog_name, &optimizer.binder, cacheable);
	OwnedArray<MoraineLocatedFile> files(moraine_locate_row_positions_free_files);
	MoraineError error {};
	auto code = moraine_locate_row_positions(handle, found.schema_name.c_str(), found.table_name.c_str(),
	                                         found.rows.data(), found.rows.size(), files.OutItems(), files.OutLen(),
	                                         moraine_shim_is_interrupted, &context, &error);
	if (code == MORAINE_INTERRUPTED) {
		ThrowMoraineError(error);
	}
	if (code != MORAINE_OK) {
		// Positioning is exact-or-failed; an ordinary scan still answers.
		WriteMoraineLog(duckdb::DatabaseInstance::GetDatabase(context), duckdb::LogLevel::LOG_DEBUG,
		                std::string("indexed delete rewrite declined: ") +
		                    (error.message != nullptr ? error.message : "unknown error"));
		moraine_error_free(error.message);
		return false;
	}

	duckdb::DuckLakeDeletePositionsFunction function;
	duckdb::vector<duckdb::Value> inputs {duckdb::Value(found.catalog_name),
	                                      duckdb::Value(found.schema_name),
	                                      duckdb::Value(found.table_name), PositionalFiles(files)};
	duckdb::named_argument_map_t named;
	// Positions were resolved against the scan's snapshot; naming it makes
	// DuckLake refuse them if its transaction has moved past it.
	named["snapshot"] = duckdb::Value::UBIGINT(found.snapshot_id);
	duckdb::vector<duckdb::LogicalType> input_types;
	duckdb::vector<duckdb::Identifier> input_names;
	duckdb::BoundTableFunction bound_function(function);
	duckdb::TableFunctionRef reference;
	duckdb::TableFunctionBindInput bind_input(inputs, named, input_types, input_names,
	                                          function.function_info, &optimizer.binder,
	                                          bound_function, reference);
	duckdb::vector<duckdb::LogicalType> types;
	duckdb::vector<duckdb::Identifier> names;
	auto bind_data = function.bind(context, bind_input, types, names);

	auto replacement = duckdb::make_uniq<duckdb::LogicalGet>(optimizer.binder.GenerateTableIndex(),
	                                                         std::move(bound_function),
	                                                         std::move(bind_data), types, names);
	for (duckdb::idx_t column = 0; column < types.size(); column++) {
		replacement->AddColumnId(column);
	}

	// A DELETE emits one BIGINT under its own table index, so the deleted
	// count is projected there and the parent's binding still resolves.
	auto bindings = replacement->GetColumnBindings();
	duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> projected;
	projected.push_back(
	    duckdb::make_uniq<duckdb::BoundColumnRefExpression>(duckdb::LogicalType::BIGINT, bindings[0]));
	auto projection =
	    duckdb::make_uniq<duckdb::LogicalProjection>(deletion.table_index, std::move(projected));
	projection->children.push_back(std::move(replacement));
	plan = std::move(projection);
	return true;
}

void OptimizeIndexedPredicates(duckdb::ClientContext &context, duckdb::Optimizer &optimizer,
                               duckdb::unique_ptr<duckdb::LogicalOperator> &plan) {
	if (plan->type == duckdb::LogicalOperatorType::LOGICAL_DELETE && plan->children.size() == 1) {
		auto scan = DeleteSourceScan(*plan->children[0]);
		if (scan && scan->children.empty()) {
			auto found = ResolveIndexedPredicate(context, optimizer, *scan);
			if (found && !RewriteDeleteByIndexedPredicate(context, optimizer, plan, *found)) {
				PruneScanByIndexedPredicate(context, *scan, *found);
			}
			return;
		}
	}
	for (auto &child : plan->children) {
		OptimizeIndexedPredicates(context, optimizer, child);
	}
	if (plan->type == duckdb::LogicalOperatorType::LOGICAL_GET) {
		auto &get = plan->Cast<duckdb::LogicalGet>();
		auto found = ResolveIndexedPredicate(context, optimizer, get);
		if (found) {
			PruneScanByIndexedPredicate(context, get, *found);
		}
	}
}

class MoraineOptimizer : public duckdb::OptimizerExtension {
public:
	MoraineOptimizer() {
		pre_optimize_function = PreOptimize;
		optimize_function = PostOptimize;
	}

	static void PreOptimize(duckdb::OptimizerExtensionInput &input, duckdb::unique_ptr<duckdb::LogicalOperator> &plan) {
		ReplaceEmptyIndexReads(input.context, plan);
		ExposeTuplePredicates(input.context, *plan);
	}

	static void PostOptimize(duckdb::OptimizerExtensionInput &input,
	                         duckdb::unique_ptr<duckdb::LogicalOperator> &plan) {
		FilterJoinsAgainstIndexReads(input.context, input.optimizer, plan);
		OptimizeIndexedPredicates(input.context, input.optimizer, plan);
	}
};

} // namespace

void RegisterMoraineOptimizer(duckdb::DBConfig &config) {
	config.AddExtensionOption("moraine_summary_scan_threads", "Maximum concurrent located-scan read units", duckdb::LogicalType::UBIGINT,
	                          duckdb::Value::UBIGINT(4));
	duckdb::OptimizerExtension::Register(config, MoraineOptimizer());
}

} // namespace moraine_duckdb
