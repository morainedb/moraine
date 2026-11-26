#include "duckdb/planner/filter/list.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "catalog.hpp"
#include "delete_positions.hpp"
#include "index_cache.hpp"
#include "index_functions.hpp"
#include "owned_array.hpp"
#include "optimizer_predicates.hpp"
#include "summary_scan.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "common/index.hpp"
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
// join against rows the planner can already see, and the scan on the other
// side can be replaced with the located scan over exactly those rows.
//
// The pass runs after DuckDB's own optimizers, because that is when every
// qualifying join exists: DELETE and UPDATE bind their join through the WHERE
// clause as a filter over a cross product, and an EXISTS probe as a dependent
// join, both formed into comparison joins only during optimization.

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

duckdb::LogicalGet *ScanBeneath(duckdb::LogicalOperator &op) {
	if (op.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION ||
	    op.type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
		return ScanBeneath(*op.children[0]);
	}
	return op.type == duckdb::LogicalOperatorType::LOGICAL_GET ? &op.Cast<duckdb::LogicalGet>() : nullptr;
}

bool TrySummaryJoin(duckdb::ClientContext &context, duckdb::LogicalComparisonJoin &join, duckdb::idx_t index_side,
                    duckdb::LogicalGet &index, const std::vector<MoraineRowId> &rows) {
	auto &other = *join.children[1 - index_side];
	auto scan = ScanBeneath(other);
	if (!scan) {
		return false;
	}
	bool row_id = false;
	for (auto &condition : join.conditions) {
		if (condition.GetComparisonType() != duckdb::ExpressionType::COMPARE_EQUAL &&
		    condition.GetComparisonType() != duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			continue;
		}
		auto &index_expression = index_side == 0 ? condition.GetLHS() : condition.GetRHS();
		auto &scan_expression = index_side == 0 ? condition.GetRHS() : condition.GetLHS();
		duckdb::idx_t index_column, scan_column;
		if (!ReferencedColumnBeneath(index_expression, *join.children[index_side], index, index_column) ||
		    !ReferencedColumnBeneath(scan_expression, other, *scan, scan_column)) {
			continue;
		}
		row_id =
		    row_id || (index_column == INDEX_READ_ROW_ID_COLUMN && scan_column == duckdb::COLUMN_IDENTIFIER_ROW_ID);
	}
	return row_id && UseSummaryScan(context, *scan, index, rows);
}

void LocateJoinsAgainstIndexReads(duckdb::ClientContext &context, duckdb::unique_ptr<duckdb::LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		LocateJoinsAgainstIndexReads(context, child);
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
		// Two index reads leave no scan to replace.
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
	TrySummaryJoin(context, join, index_side, *index_get, *rows);
}

// Below this many bytes of data files a scan is cheaper than the probe it
// would save: a probe costs about 3 ms, about what scanning a megabyte does.
constexpr duckdb::idx_t MIN_PRUNABLE_TABLE_BYTES = 4ULL * 1024 * 1024;

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
	// Rows the transaction inserted into the table, which the index, which
	// describes committed rows, cannot know: inlined ones the located scan
	// serves from the transaction's buffer, ones in files it wrote need
	// DuckLake's scan.
	bool local_inlined = false;
	bool local_files = false;
};

// A DuckLake table scan whose rows an index may resolve: moraine serves its
// metadata, it reads the transaction's own snapshot, and the table is large
// enough for a probe to pay.
struct QualifiedScan {
	duckdb::DuckLakeTableEntry *table;
	duckdb::DuckLakeFunctionInfo *info;
	duckdb::shared_ptr<duckdb::DuckLakeTransaction> transaction;

	ResolvedPredicate Resolved(std::vector<MoraineRowId> rows, bool certified) const {
		return ResolvedPredicate {
		    table,
		    table->ParentCatalog().GetName().GetIdentifierName(),
		    table->ParentSchema().name.GetIdentifierName(),
		    table->name.GetIdentifierName(),
		    std::move(rows),
		    info->snapshot.snapshot_id,
		    certified,
		    transaction->GetTransactionLocalInlinedData(info->table_id) != nullptr,
		    !transaction->GetTransactionLocalFiles(info->table_id).empty(),
		};
	}
};

std::optional<QualifiedScan> QualifyIndexedScan(duckdb::ClientContext &context, duckdb::LogicalGet &get) {
	if (!get.function.GetDefinition() || get.function.GetDefinition()->name != "ducklake_scan" || IsSummaryScan(get)) {
		return std::nullopt;
	}
	if (!get.function.filter_pushdown) {
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
	auto &lake = table.ParentCatalog().Cast<duckdb::DuckLakeCatalog>();
	auto metadata = duckdb::Catalog::GetCatalogEntry(context, duckdb::Identifier(lake.MetadataDatabaseName()));
	if (!metadata || metadata->GetCatalogType() != "moraine") {
		return std::nullopt;
	}
	auto transaction = info->GetTransaction();
	if (!transaction || table.IsTransactionLocal()) {
		return std::nullopt;
	}
	// A scan at another snapshot than the transaction's is a different cut
	// than the one the index resolves against.
	if (transaction->GetSnapshot().snapshot_id != info->snapshot.snapshot_id) {
		return std::nullopt;
	}
	auto stats = lake.GetTableStats(*transaction, table.GetTableId());
	if (!stats || stats->table_size_bytes < MIN_PRUNABLE_TABLE_BYTES) {
		return std::nullopt;
	}
	return QualifiedScan {&table, info, std::move(transaction)};
}

// Columns whose statistics say every row holds one value, read as DuckDB's
// statistics pass reads them when it removes an equality on such a column as
// always true: no NULL possible, and numeric bounds that coincide. An
// equality on such a column is implied by every row, so it may complete a
// key without being residual. String bounds are prefixes and imply nothing.
std::vector<std::pair<duckdb::idx_t, duckdb::Value>> ImpliedConstants(duckdb::ClientContext &context,
                                                                      const QualifiedScan &qualified) {
	std::vector<std::pair<duckdb::idx_t, duckdb::Value>> result;
	auto &table = *qualified.table;
	for (duckdb::idx_t column = 0; column < table.GetColumns().PhysicalColumnCount(); column++) {
		auto stats = table.GetStatistics(context, column);
		if (!stats || stats->CanHaveNull() || stats->GetStatsType() != duckdb::StatisticsType::NUMERIC_STATS ||
		    !duckdb::NumericStats::HasMinMax(*stats)) {
			continue;
		}
		auto min = duckdb::NumericStats::Min(*stats);
		if (min.IsNull() || !duckdb::Value::NotDistinctFrom(min, duckdb::NumericStats::Max(*stats))) {
			continue;
		}
		result.emplace_back(column, std::move(min));
	}
	return result;
}

// Logs a probe the core declined and releases its message. The scan still
// answers from every file; pruning is an optimization.
void LogDeclinedProbe(duckdb::ClientContext &context, const char *what, MoraineError &error) {
	WriteMoraineLog(duckdb::DatabaseInstance::GetDatabase(context), duckdb::LogLevel::LOG_DEBUG,
	                std::string(what) + ": " + (error.message != nullptr ? error.message : "unknown error"));
	moraine_error_free(error.message);
}

// Resolves supported predicates through a ready index. A declined probe
// leaves the scan unchanged.
std::optional<ResolvedPredicate>
ResolveIndexedPredicate(duckdb::ClientContext &context, duckdb::Optimizer &optimizer, duckdb::LogicalGet &get,
                        duckdb::optional_ptr<duckdb::LogicalFilter> residual = nullptr) {
	if (!get.table_filters.HasFilters()) {
		return std::nullopt;
	}
	auto qualified = QualifyIndexedScan(context, get);
	if (!qualified) {
		return std::nullopt;
	}
	auto &table = *qualified->table;

	auto &column_ids = get.GetColumnIds();
	auto stored_columns = table.GetColumns().PhysicalColumnCount();
	PredicateArena predicates;
	std::set<duckdb::idx_t> filtered;
	bool complete = !get.table_filters.HasMultiColumnFilters();
	for (duckdb::idx_t position = 0; position < column_ids.size(); position++) {
		auto filter = get.table_filters.TryGetFilterByColumnIndex(duckdb::ProjectionIndex(position));
		if (!filter) {
			continue;
		}
		auto column = column_ids[position].GetPrimaryIndex();
		filtered.insert(column);
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
	if (residual) {
		// DuckDB can turn a tuple's dense choices into a broad prefix range.
		// Recover its discrete choices while retaining the tuple filter.
		complete = false;
		for (auto &expression : residual->expressions) {
			auto previous = predicates.predicates.size();
			try {
				ExtractTuplePredicates(context, *expression, get, table, predicates);
			} catch (const std::exception &) {
				predicates.predicates.resize(previous);
			}
		}
	}
	if (predicates.predicates.empty()) {
		return std::nullopt;
	}
	// Implied equalities complete a key whose leading column the statement
	// did not name, or named and DuckDB removed; the core never counts them
	// as residual.
	for (auto &implied : ImpliedConstants(context, *qualified)) {
		if (filtered.count(implied.first)) {
			continue;
		}
		auto previous = predicates.predicates.size();
		try {
			predicates.Add(table.GetFieldId(duckdb::PhysicalIndex(implied.first)).GetFieldIndex().index, 6,
			               {implied.second});
		} catch (const std::exception &) {
			predicates.predicates.resize(previous);
		}
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
		LogDeclinedProbe(context, "index pushdown declined", error);
		return std::nullopt;
	}
	if (!covered) {
		return std::nullopt;
	}

	return qualified->Resolved(std::vector<MoraineRowId>(rows.begin(), rows.end()), certified && complete);
}

// Resolves a delete's whole predicate, the scan's pushed filters, the filter
// DuckDB kept above it, and the IN lists it planned as MARK joins, as a set
// of complete keys through an index over exactly those columns. Every row the
// keys name is an answer and no predicate is left over, so the result is
// certified whenever the index is. Declines when any conjunct is not an
// equality list, when the columns match no index, or when the keys outnumber
// what a probe collects.
std::optional<ResolvedPredicate> ResolveKeySet(duckdb::ClientContext &context, duckdb::Optimizer &optimizer,
                                               duckdb::LogicalGet &get, duckdb::LogicalFilter &filter,
                                               const std::vector<duckdb::LogicalComparisonJoin *> &marks) {
	if (get.table_filters.HasMultiColumnFilters()) {
		return std::nullopt;
	}
	// Each mark must be what exactly one kept expression tests, bare: a mark
	// nothing tests would not constrain the rows, and a negated one is NOT IN.
	std::vector<duckdb::idx_t> tested(marks.size(), 0);
	auto mark_of = [&](const duckdb::Expression &expression) -> std::optional<size_t> {
		if (expression.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
			return std::nullopt;
		}
		auto table_index = expression.Cast<duckdb::BoundColumnRefExpression>().Binding().table_index;
		for (size_t i = 0; i < marks.size(); i++) {
			if (marks[i]->mark_index == table_index) {
				return i;
			}
		}
		return std::nullopt;
	};
	for (auto &expression : filter.expressions) {
		if (auto mark = mark_of(*expression)) {
			tested[*mark]++;
		}
	}
	if (std::any_of(tested.begin(), tested.end(), [](duckdb::idx_t count) { return count != 1; })) {
		return std::nullopt;
	}
	auto qualified = QualifyIndexedScan(context, get);
	if (!qualified) {
		return std::nullopt;
	}
	auto &table = *qualified->table;
	auto stored_columns = table.GetColumns().PhysicalColumnCount();

	KeySet keys;
	keys.keys.push_back({});
	auto conjoin = [&](const std::optional<KeySet> &part) {
		if (!part) {
			return false;
		}
		auto joined = ConjoinKeySets(keys, *part);
		if (!joined) {
			return false;
		}
		keys = std::move(*joined);
		return true;
	};
	// A pushed filter that is an equality list joins the key set. One that is
	// not, a range DuckDB folded a list into, is checked against each key's
	// constant for its column instead, once the kept filter has named the keys;
	// an optional one is a copy of what the kept filter still evaluates.
	std::vector<std::pair<duckdb::idx_t, const duckdb::ExpressionFilter *>> checks;
	auto &column_ids = get.GetColumnIds();
	for (duckdb::idx_t position = 0; position < column_ids.size(); position++) {
		auto pushed = get.table_filters.TryGetFilterByColumnIndex(duckdb::ProjectionIndex(position));
		if (!pushed) {
			continue;
		}
		if (conjoin(TableFilterKeySet(*pushed, column_ids[position].GetPrimaryIndex()))) {
			continue;
		}
		if (pushed->filter_type != duckdb::TableFilterType::EXPRESSION_FILTER) {
			return std::nullopt;
		}
		auto &expression = pushed->Cast<duckdb::ExpressionFilter>();
		if (!expression.column_indexes.empty()) {
			return std::nullopt;
		}
		if (duckdb::ExpressionFilter::GetOptionalFilterChild(*expression.expr)) {
			continue;
		}
		checks.emplace_back(column_ids[position].GetPrimaryIndex(), &expression);
	}
	for (auto mark : marks) {
		if (!conjoin(MarkJoinKeySet(context, *mark, get))) {
			return std::nullopt;
		}
	}
	for (auto &expression : filter.expressions) {
		if (mark_of(*expression)) {
			continue;
		}
		try {
			if (!conjoin(ExtractKeySet(context, *expression, get))) {
				return std::nullopt;
			}
		} catch (const std::exception &) {
			return std::nullopt;
		}
	}
	if (keys.columns.empty()) {
		return std::nullopt;
	}
	for (auto &check : checks) {
		auto found = std::find(keys.columns.begin(), keys.columns.end(), check.first);
		if (found == keys.columns.end()) {
			return std::nullopt;
		}
		auto position = static_cast<duckdb::idx_t>(found - keys.columns.begin());
		try {
			keys.keys.erase(std::remove_if(keys.keys.begin(), keys.keys.end(),
			                               [&](const duckdb::vector<duckdb::Value> &key) {
				                               return !check.second->EvaluateWithConstant(context, key[position]);
			                               }),
			                keys.keys.end());
		} catch (const std::exception &) {
			return std::nullopt;
		}
	}
	for (auto column : keys.columns) {
		if (column >= stored_columns) {
			return std::nullopt;
		}
	}

	std::vector<uint64_t> fields;
	for (auto column : keys.columns) {
		fields.push_back(table.GetFieldId(duckdb::PhysicalIndex(column)).GetFieldIndex().index);
	}
	// Implied equalities may fill the index columns the keys do not name.
	PredicateArena implied;
	for (auto &constant : ImpliedConstants(context, *qualified)) {
		if (std::find(keys.columns.begin(), keys.columns.end(), constant.first) != keys.columns.end()) {
			continue;
		}
		auto previous = implied.predicates.size();
		try {
			implied.Add(table.GetFieldId(duckdb::PhysicalIndex(constant.first)).GetFieldIndex().index, 6,
			            {constant.second});
		} catch (const std::exception &) {
			implied.predicates.resize(previous);
		}
	}
	LookupValueArena arena;
	std::vector<std::vector<MoraineLookupValue>> values;
	std::vector<MoraineLookupKey> lookup_keys;
	try {
		for (auto &key : keys.keys) {
			std::vector<MoraineLookupValue> converted;
			for (auto &value : key) {
				converted.push_back(BuildLookupValue(value, arena, "moraine index key set"));
			}
			values.push_back(std::move(converted));
			lookup_keys.push_back({values.back().data(), values.back().size()});
		}
	} catch (const std::exception &) {
		return std::nullopt;
	}

	bool cacheable = false;
	auto handle =
	    BindIndexReadFor(context, table.ParentCatalog().GetName().GetIdentifierName(), &optimizer.binder, cacheable);
	auto schema_name = table.ParentSchema().name.GetIdentifierName();
	auto table_name = table.name.GetIdentifierName();
	OwnedArray<MoraineRowId> rows(moraine_index_lookup_free);
	bool covered = false;
	bool certified = false;
	MoraineError error {};
	auto code = moraine_index_key_rows(handle, schema_name.c_str(), table_name.c_str(), fields.data(), fields.size(),
	                                   lookup_keys.data(), lookup_keys.size(), implied.predicates.data(),
	                                   implied.predicates.size(), rows.OutItems(), rows.OutLen(), &covered, &certified,
	                                   moraine_shim_is_interrupted, &context, &error);
	if (code == MORAINE_INTERRUPTED) {
		ThrowMoraineError(error);
	}
	if (code != MORAINE_OK) {
		LogDeclinedProbe(context, "index key set declined", error);
		return std::nullopt;
	}
	if (!covered) {
		return std::nullopt;
	}
	return qualified->Resolved(std::vector<MoraineRowId>(rows.begin(), rows.end()), certified);
}

// DuckLake's file list over exactly the given entries. DuckLake rebuilds a
// list it pushes filters into from the transaction's whole view, which would
// bring the committed files back, so this one declines every pushdown and
// copies as itself.
class FixedFileList final : public duckdb::DuckLakeMultiFileList {
public:
	FixedFileList(duckdb::DuckLakeFunctionInfo &info, duckdb::vector<duckdb::DuckLakeFileListEntry> entries)
	    : duckdb::DuckLakeMultiFileList(info, std::move(entries)), info(info) {
	}
	duckdb::unique_ptr<duckdb::MultiFileList>
	DynamicFilterPushdown(duckdb::MultiFileDynamicPushdownInfo &) const override {
		return nullptr;
	}
	duckdb::unique_ptr<duckdb::MultiFileList>
	ComplexFilterPushdown(duckdb::ClientContext &, const duckdb::MultiFileOptions &, duckdb::MultiFilePushdownInfo &,
	                      duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &) const override {
		return nullptr;
	}
	duckdb::unique_ptr<duckdb::MultiFileList> Copy() const override {
		return duckdb::make_uniq<FixedFileList>(info, GetFiles());
	}

private:
	duckdb::DuckLakeFunctionInfo &info;
};

// DuckLake's own scan over the Parquet files this transaction wrote for the
// table, whose rows the pinned snapshot cannot know. Null when it wrote none.
duckdb::unique_ptr<duckdb::LogicalOperator> LocalFilesScan(duckdb::Optimizer &optimizer,
                                                           const duckdb::LogicalGet &get) {
	auto &info = get.function.function_info->Cast<duckdb::DuckLakeFunctionInfo>();
	auto &files = get.bind_data->Cast<duckdb::MultiFileBindData>().file_list->Cast<duckdb::DuckLakeMultiFileList>();
	duckdb::vector<duckdb::DuckLakeFileListEntry> local;
	for (auto &entry : files.GetFiles()) {
		if (entry.data_type == duckdb::DuckLakeDataType::DATA_FILE && entry.row_id_start.IsValid() &&
		    duckdb::DuckLakeConstants::IsTransactionLocalRowId(static_cast<int64_t>(entry.row_id_start.GetIndex()))) {
			local.push_back(entry);
		}
	}
	if (local.empty()) {
		return nullptr;
	}
	auto bind_data = get.bind_data->Copy();
	bind_data->Cast<duckdb::MultiFileBindData>().file_list =
	    duckdb::make_shared_ptr<FixedFileList>(info, std::move(local));
	auto scan =
	    duckdb::make_uniq<duckdb::LogicalGet>(optimizer.binder.GenerateTableIndex(), get.function, std::move(bind_data),
	                                          get.returned_types, get.names, get.virtual_columns);
	scan->SetColumnIds(duckdb::vector<duckdb::ColumnIndex>(get.GetColumnIds()));
	scan->projection_ids = get.projection_ids;
	scan->table_filters = std::move(*get.table_filters.Copy());
	scan->bind_info = get.bind_info;
	scan->parameters = get.parameters;
	scan->named_parameters = get.named_parameters;
	scan->input_table_types = get.input_table_types;
	scan->input_table_names = get.input_table_names;
	return std::move(scan);
}

// Whether the scan feeds DuckLake's DELETE or UPDATE, which read each row's
// file and position from it.
bool ReadsFileLocation(const duckdb::LogicalGet &get) {
	for (auto &column : get.GetColumnIds()) {
		auto id = column.GetPrimaryIndex();
		if (id == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILENAME ||
		    id == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX ||
		    id == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
			return true;
		}
	}

	return false;
}

// Replaces the scan in `slot` with the located scan over the rows an index
// resolves a predicate to, as the join rule does for a lookup in the query.
// Only an index that is complete for the scan's snapshot may, and every
// uncertainty leaves the plan as written. The one exception is exact: no row
// holds the value, so the scan becomes an empty result. Inside a transaction
// that has inserted rows, the located scan serves the committed rows and the
// transaction's inlined ones, and DuckLake's scan over the files the
// transaction wrote joins it under a UNION ALL that keeps the scan's bindings.
void PruneScanByIndexedPredicate(duckdb::ClientContext &context, duckdb::Optimizer &optimizer,
                                 duckdb::unique_ptr<duckdb::LogicalOperator> &slot, const ResolvedPredicate &found) {
	auto &get = slot->Cast<duckdb::LogicalGet>();
	if (found.rows.empty() && !found.local_inlined && !found.local_files) {
		slot = duckdb::make_uniq<duckdb::LogicalEmptyResult>(std::move(slot));
		return;
	}
	auto local_files = found.local_files ? LocalFilesScan(optimizer, get) : nullptr;
	if (!UseSummaryScan(context, get, found.table->ParentCatalog().GetName().GetIdentifierName(), found.schema_name,
	                    found.table_name, found.rows) ||
	    !local_files) {
		return;
	}

	// The scan's parents name its outputs as (table index, position among its
	// column ids); the projection keeps that index and places each united
	// column at that position, with a NULL nobody reads in each gap.
	auto original_index = get.table_index;
	auto outputs = get.GetColumnBindings();
	get.table_index = optimizer.binder.GenerateTableIndex();
	auto both = duckdb::make_uniq<duckdb::LogicalSetOperation>(optimizer.binder.GenerateTableIndex(), outputs.size(),
	                                                           std::move(slot), std::move(local_files),
	                                                           duckdb::LogicalOperatorType::LOGICAL_UNION, true);
	both->ResolveOperatorTypes();
	auto united = both->GetColumnBindings();
	duckdb::idx_t width = 0;
	for (auto &output : outputs) {
		width = std::max<duckdb::idx_t>(width, output.column_index + 1);
	}
	duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> columns;
	for (duckdb::idx_t position = 0; position < width; position++) {
		columns.push_back(
		    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value(duckdb::LogicalType::SQLNULL)));
	}
	for (duckdb::idx_t i = 0; i < outputs.size(); i++) {
		columns[outputs[i].column_index] =
		    duckdb::make_uniq<duckdb::BoundColumnRefExpression>(both->types[i], united[i]);
	}
	auto projection = duckdb::make_uniq<duckdb::LogicalProjection>(original_index, std::move(columns));
	projection->children.push_back(std::move(both));
	slot = std::move(projection);
}

// The scan a delete reads its rows from, when nothing but projections, at
// most one filter, and MARK joins against constant chunks sit between them.
// The filter is the predicate DuckDB kept out of the scan, and each such join
// is an IN list of five or more constants it planned as a join. Any other
// join or an aggregate means rows the index cannot speak for, so the rewrite
// declines.
struct DeleteSource {
	duckdb::optional_ptr<duckdb::LogicalGet> scan;
	duckdb::optional_ptr<duckdb::LogicalFilter> filter;
	std::vector<duckdb::LogicalComparisonJoin *> marks;
	// Whether a projection rebinds the scan's columns below the filter.
	bool rebinds = false;
};

bool IsConstantMarkJoin(const duckdb::LogicalOperator &op) {
	if (op.type != duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.children.size() != 2) {
		return false;
	}
	return op.Cast<duckdb::LogicalComparisonJoin>().join_type == duckdb::JoinType::MARK &&
	       op.children[1]->type == duckdb::LogicalOperatorType::LOGICAL_CHUNK_GET;
}

DeleteSource DeleteSourceScan(duckdb::LogicalOperator &op) {
	DeleteSource source;
	auto *current = &op;
	while (current->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
		if (IsConstantMarkJoin(*current)) {
			source.marks.push_back(&current->Cast<duckdb::LogicalComparisonJoin>());
		} else if (current->children.size() != 1) {
			return {};
		} else if (current->type == duckdb::LogicalOperatorType::LOGICAL_FILTER && !source.filter) {
			source.filter = &current->Cast<duckdb::LogicalFilter>();
		} else if (current->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
			source.rebinds = source.rebinds || source.filter;
		} else {
			return {};
		}
		current = current->children[0].get();
	}
	source.scan = &current->Cast<duckdb::LogicalGet>();
	return source;
}

// The slot holding the scan `DeleteSourceScan` found beneath `op`, so a rule
// can replace it. Valid only once that search succeeded.
duckdb::unique_ptr<duckdb::LogicalOperator> &DeleteSourceSlot(duckdb::unique_ptr<duckdb::LogicalOperator> &op) {
	auto *current = &op;
	while ((*current)->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
		current = &(*current)->children[0];
	}
	return *current;
}

// The `files` argument `moraine_delete_positions` takes: one entry per data
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
		     file.has_existing_delete ? duckdb::Value::BLOB_RAW(packed) : duckdb::Value(duckdb::LogicalType::BLOB)}};
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

	MoraineDeletePositionsFunction function;
	duckdb::vector<duckdb::Value> inputs {duckdb::Value(found.catalog_name), duckdb::Value(found.schema_name),
	                                      duckdb::Value(found.table_name), PositionalFiles(files)};
	duckdb::named_argument_map_t named;
	// Positions were resolved against the scan's snapshot; naming it makes
	// DuckLake refuse them if its transaction has moved past it.
	named["snapshot"] = duckdb::Value::UBIGINT(found.snapshot_id);
	duckdb::vector<duckdb::LogicalType> input_types;
	duckdb::vector<duckdb::Identifier> input_names;
	duckdb::BoundTableFunction bound_function(function);
	duckdb::TableFunctionRef reference;
	duckdb::TableFunctionBindInput bind_input(inputs, named, input_types, input_names, function.function_info,
	                                          &optimizer.binder, bound_function, reference);
	duckdb::vector<duckdb::LogicalType> types;
	duckdb::vector<duckdb::Identifier> names;
	auto bind_data = function.bind(context, bind_input, types, names);

	auto replacement = duckdb::make_uniq<duckdb::LogicalGet>(
	    optimizer.binder.GenerateTableIndex(), std::move(bound_function), std::move(bind_data), types, names);
	for (duckdb::idx_t column = 0; column < types.size(); column++) {
		replacement->AddColumnId(column);
	}

	// A DELETE emits one BIGINT under its own table index, so the deleted
	// count is projected there and the parent's binding still resolves.
	auto bindings = replacement->GetColumnBindings();
	duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> projected;
	projected.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(duckdb::LogicalType::BIGINT, bindings[0]));
	auto projection = duckdb::make_uniq<duckdb::LogicalProjection>(deletion.table_index, std::move(projected));
	projection->children.push_back(std::move(replacement));
	plan = std::move(projection);
	return true;
}

void OptimizeIndexedPredicates(duckdb::ClientContext &context, duckdb::Optimizer &optimizer,
                               duckdb::unique_ptr<duckdb::LogicalOperator> &plan,
                               duckdb::optional_ptr<duckdb::LogicalFilter> residual = nullptr) {
	if (plan->type == duckdb::LogicalOperatorType::LOGICAL_DELETE && plan->children.size() == 1) {
		auto source = DeleteSourceScan(*plan->children[0]);
		auto scan = source.scan;
		if (scan && scan->children.empty()) {
			// A DELETE must remove the transaction's own rows too. The located
			// scan serves the inlined ones; rows in files the transaction wrote
			// need DuckLake's scan, and the positional delete reaches neither.
			auto filter = source.filter;
			// A filter DuckDB kept above the scan is a residual predicate,
			// unless with the pushed filters it spells complete keys: then the
			// index answers the whole predicate and the filter has nothing
			// left to decide.
			if (filter && !source.rebinds) {
				auto keyed = ResolveKeySet(context, optimizer, *scan, *filter, source.marks);
				if (keyed && !keyed->local_inlined && !keyed->local_files &&
				    RewriteDeleteByIndexedPredicate(context, optimizer, plan, *keyed)) {
					return;
				}
			}
			auto found = ResolveIndexedPredicate(context, optimizer, *scan, filter);
			if (found && !found->local_files) {
				bool positional = !filter && !found->local_inlined &&
				                  RewriteDeleteByIndexedPredicate(context, optimizer, plan, *found);
				if (!positional) {
					PruneScanByIndexedPredicate(context, optimizer, DeleteSourceSlot(plan->children[0]), *found);
				}
			}
			return;
		}
	}
	for (auto &child : plan->children) {
		OptimizeIndexedPredicates(
		    context, optimizer, child,
		    plan->type == duckdb::LogicalOperatorType::LOGICAL_FILTER ? &plan->Cast<duckdb::LogicalFilter>() : nullptr);
	}
	if (plan->type == duckdb::LogicalOperatorType::LOGICAL_GET) {
		auto &get = plan->Cast<duckdb::LogicalGet>();
		auto found = ResolveIndexedPredicate(context, optimizer, get, residual);
		if (found && !(found->local_files && ReadsFileLocation(get))) {
			PruneScanByIndexedPredicate(context, optimizer, plan, *found);
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
		LocateJoinsAgainstIndexReads(input.context, plan);
		OptimizeIndexedPredicates(input.context, input.optimizer, plan);
	}
};

} // namespace

void RegisterMoraineOptimizer(duckdb::DBConfig &config) {
	config.AddExtensionOption("moraine_summary_scan_threads", "Maximum concurrent located-scan read units",
	                          duckdb::LogicalType::UBIGINT, duckdb::Value::UBIGINT(4));
	duckdb::OptimizerExtension::Register(config, MoraineOptimizer());
}

} // namespace moraine_duckdb
