#include "metadata_tables.hpp"

#include <algorithm>
#include <atomic>
#include <optional>
#include <string>

// The bound-expression shapes `MetadataScanPushdownComplexFilter` matches,
// and the `LogicalGet` it reads the scan's projection from.
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

// Re-resolving a deferred scan's catalog at execution time.
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

#include "catalog.hpp"
#include "transaction_manager.hpp"

namespace moraine_duckdb {

namespace {

duckdb::optional_ptr<duckdb::TableCatalogEntry>
MetadataScanTableEntry(duckdb::optional_ptr<const duckdb::FunctionData> bind_data) {
	auto &data = bind_data->Cast<MetadataScanBindData>();
	return data.table_entry;
}

// The catalog a deferred scan reads through, proven still attached.
//
// Bind data outlives its bind: `PREPARE`, `DETACH`, then `EXECUTE` reaches
// here with `bind_data.catalog` pointing at a freed catalog. The pointer is
// therefore only ever compared, never followed — the reference returned is
// the one the database manager resolves now.
duckdb::Catalog &LiveBoundCatalog(duckdb::ClientContext &context, const MetadataScanBindData &bind_data) {
	auto database =
	    duckdb::DatabaseManager::Get(context).GetDatabase(context, duckdb::Identifier(bind_data.catalog_name));
	if (database && &database->GetCatalog() == bind_data.catalog) {
		return *bind_data.catalog;
	}
	throw duckdb::CatalogException("moraine: database \"%s\" was detached after this statement was bound",
	                               bind_data.catalog_name);
}

struct MetadataScanGlobalState : public duckdb::GlobalTableFunctionState {
	std::unique_ptr<MoraineMetadataCursor, decltype(&moraine_metadata_cursor_free)> cursor {
	    nullptr, moraine_metadata_cursor_free};
	MetadataPageProvider page_provider = nullptr;
	MetadataRows page;
	duckdb::idx_t page_offset = 0;
	// The next row to emit. Threads claim vectors of rows from it, so a
	// row's id (`row_id_base` plus its index) is the same whichever thread
	// emits it.
	std::atomic<duckdb::idx_t> offset {0};
	duckdb::idx_t row_count = 0;
	// The columns DuckDB asked for, by index into a materialized row, in
	// output order. Empty for a zero-column "virtual column" probe (e.g.
	// `SELECT NULL FROM ducklake_metadata LIMIT 1`), which DuckDB emits only
	// when the table function advertises `projection_pushdown = true`.
	std::vector<duckdb::column_t> column_ids;
	// Set only for a scan whose bind deferred materialization, holding what
	// this scan built.
	std::shared_ptr<const MetadataRows> rows;
	// The row id this scan's first row carries, taken from the transaction's
	// run registry. Zero for a scan emitting no row ids, and for the inline
	// tables, whose own Sinks index a re-materialization directly.
	uint64_t row_id_base = 0;

	idx_t MaxThreads() const override {
		return cursor != nullptr ? 1 : std::max<idx_t>(1, row_count / STANDARD_VECTOR_SIZE);
	}
};

bool EmitsRowIds(const std::vector<duckdb::column_t> &column_ids) {
	return std::find(column_ids.begin(), column_ids.end(), duckdb::COLUMN_IDENTIFIER_ROW_ID) != column_ids.end();
}

using OwnedMetadataCursor = std::unique_ptr<MoraineMetadataCursor, decltype(&moraine_metadata_cursor_free)>;

OwnedMetadataCursor OpenMetadataCursor(MoraineCatalogHandle *handle, const MetadataTableSpec &spec, uint64_t scope,
                                       uint64_t live_bound, duckdb::ClientContext &context) {
	MoraineMetadataCursor *cursor = nullptr;
	MoraineError err {};
	if (moraine_metadata_cursor_open(handle, spec.write_table_kind, scope, live_bound, &cursor,
	                                 moraine_shim_is_interrupted, &context, &err) != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	return OwnedMetadataCursor(cursor, moraine_metadata_cursor_free);
}

// Replays one fixed scan only when a mutation resolves its row IDs. At most one
// page is retained; an out-of-order request restarts the same immutable cursor.
struct MetadataRowResolver {
	MoraineCatalogHandle *handle;
	const MetadataTableSpec &spec;
	uint64_t scope;
	uint64_t live_bound;
	duckdb::ClientContext &context;
	OwnedMetadataCursor cursor {nullptr, moraine_metadata_cursor_free};
	uint64_t offset = 0;
	MetadataRows page;

	const std::vector<duckdb::Value> *Resolve(uint64_t index) {
		if (cursor == nullptr || index < offset) {
			cursor = OpenMetadataCursor(handle, spec, scope, live_bound, context);
			offset = 0;
			page.clear();
		}
		while (index >= offset + page.size()) {
			offset += page.size();
			page = spec.page_provider(cursor.get(), moraine_shim_is_interrupted, &context);
			if (page.empty()) {
				return nullptr;
			}
		}
		return &page[index - offset];
	}
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> MetadataScanInitGlobal(duckdb::ClientContext &context,
                                                                            duckdb::TableFunctionInitInput &input) {
	auto state = duckdb::make_uniq<MetadataScanGlobalState>();
	state->column_ids = input.column_ids;

	auto &bind_data = input.bind_data->Cast<MetadataScanBindData>();
	if (bind_data.spec == nullptr || bind_data.catalog == nullptr) {
		return state;
	}
	auto &catalog = LiveBoundCatalog(context, bind_data);
	auto catalog_transaction = catalog.GetCatalogTransaction(context);
	auto &transaction = catalog_transaction.transaction->Cast<MoraineTransaction>();
	if (bind_data.spec->page_provider != nullptr && transaction.StagedTxIfOpen() == nullptr) {
		auto *handle = moraine_snapshot_read_handle(transaction.Snapshot());
		if (handle != nullptr) {
			auto scope = bind_data.scope.IsValid() ? bind_data.scope.GetIndex() : UINT64_MAX;
			auto live_bound = bind_data.live_bound.IsValid() ? bind_data.live_bound.GetIndex() : UINT64_MAX;
			state->cursor = OpenMetadataCursor(handle, *bind_data.spec, scope, live_bound, context);
			if (EmitsRowIds(state->column_ids)) {
				auto resolver = std::make_shared<MetadataRowResolver>(
				    MetadataRowResolver {handle, *bind_data.spec, scope, live_bound, context});
				state->row_id_base = transaction.RegisterScannedCursor(
				    *bind_data.spec, [resolver](uint64_t index) { return resolver->Resolve(index); });
			}
			state->page_provider = bind_data.spec->page_provider;
			return state;
		}
	}

	if (bind_data.rows == nullptr) {
		// A scan emitting row ids narrows like any other: the staged-write
		// Sink resolves those ids against the run this scan registers
		// below, which is the narrowed list itself, so a row id names the
		// row the scan handed it out for whatever the scan left unread.
		auto live_bound = bind_data.live_bound;
		if (bind_data.scope.IsValid()) {
			state->rows =
			    ScopedMetadataRowsFor(context, catalog, *bind_data.spec, bind_data.scope.GetIndex(), live_bound);
		} else {
			state->rows = MetadataRowsFor(context, catalog, *bind_data.spec, live_bound);
		}
	}

	// Only a scan feeding an UPDATE or a DELETE asks for row ids, and only
	// its rows are ever resolved back from one, so only it is registered.
	if (EmitsRowIds(state->column_ids)) {
		auto catalog_transaction = catalog.GetCatalogTransaction(context);
		auto &transaction = catalog_transaction.transaction->Cast<MoraineTransaction>();
		state->row_id_base =
		    transaction.RegisterScannedRows(*bind_data.spec, state->rows != nullptr ? state->rows : bind_data.rows);
	}
	auto &materialized = state->rows != nullptr ? state->rows : bind_data.rows;
	state->row_count = materialized == nullptr ? 0 : materialized->size();
	return state;
}

// Where `column` sits in this scan's projection, if it is projected at
// all. A filter's column reference binds to the projection, not to the
// table's own column order.
duckdb::optional_idx ProjectedColumn(const duckdb::LogicalGet &get, duckdb::idx_t column) {
	auto &column_ids = get.GetColumnIds();
	for (duckdb::idx_t i = 0; i < column_ids.size(); i++) {
		if (!column_ids[i].IsVirtualColumn() && column_ids[i].GetPrimaryIndex() == column) {
			return i;
		}
	}
	return duckdb::optional_idx();
}

// Whether `expression` is a reference to the scan's `projected` column.
bool IsColumnRef(const duckdb::Expression &expression, const duckdb::LogicalGet &get, duckdb::idx_t projected) {
	if (expression.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &column = expression.Cast<duckdb::BoundColumnRefExpression>();
	return column.Binding().table_index == get.table_index && column.Binding().column_index == projected;
}

// The non-negative BIGINT constant `expression` holds, if it is one.
duckdb::optional_idx ConstantSnapshot(const duckdb::Expression &expression) {
	if (expression.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
		return duckdb::optional_idx();
	}
	auto &value = expression.Cast<duckdb::BoundConstantExpression>().GetValue();
	if (value.IsNull() || value.type().id() != duckdb::LogicalTypeId::BIGINT) {
		return duckdb::optional_idx();
	}
	// Read signed and refused if negative: no snapshot carries a negative
	// id, and `optional_idx` throws on the sentinel an unsigned read of -1
	// would produce.
	auto snapshot = value.GetValue<int64_t>();
	if (snapshot < 0) {
		return duckdb::optional_idx();
	}
	return static_cast<duckdb::idx_t>(snapshot);
}

// The bound a bare `end_snapshot IS NULL` carries: every snapshot a store
// can reach. `optional_idx` reserves `UINT64_MAX` for absence, so the value
// is the largest one it can carry.
constexpr uint64_t kLiveAtEverySnapshot = UINT64_MAX - 1;

// The snapshot `S` this scan's filter keeps its rows against, from either
// shape that proves the ended half unreachable: DuckLake's reads carry
// `S < end_snapshot OR end_snapshot IS NULL` alongside their
// `S >= begin_snapshot` half, and its writes carry `end_snapshot IS NULL`
// alone.
//
// An ended version's `end_snapshot` is the snapshot that ended it, never
// past the head, and is never null. So the disjunction rejects every one of
// them once `S` has reached the head — which the core, not this function,
// is what checks — and the bare `IS NULL` rejects them all wherever the
// head stands, needing no check at all.
duckdb::optional_idx LiveBoundOf(const duckdb::Expression &filter, const duckdb::LogicalGet &get,
                                 duckdb::idx_t projected) {
	if (filter.GetExpressionType() == duckdb::ExpressionType::OPERATOR_IS_NULL) {
		auto &is_null = filter.Cast<duckdb::BoundOperatorExpression>();
		if (is_null.GetChildren().size() == 1 && IsColumnRef(*is_null.GetChildren()[0], get, projected)) {
			return duckdb::optional_idx(kLiveAtEverySnapshot);
		}
		return duckdb::optional_idx();
	}
	if (filter.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONJUNCTION) {
		return duckdb::optional_idx();
	}
	auto &disjunction = filter.Cast<duckdb::BoundConjunctionExpression>();
	if (disjunction.GetExpressionType() != duckdb::ExpressionType::CONJUNCTION_OR ||
	    disjunction.GetChildren().size() != 2) {
		return duckdb::optional_idx();
	}

	duckdb::optional_idx bound;
	bool saw_is_null = false;
	for (auto &child : disjunction.GetChildren()) {
		if (child->GetExpressionType() == duckdb::ExpressionType::OPERATOR_IS_NULL) {
			auto &is_null = child->Cast<duckdb::BoundOperatorExpression>();
			if (is_null.GetChildren().size() == 1 && IsColumnRef(*is_null.GetChildren()[0], get, projected)) {
				saw_is_null = true;
			}
			continue;
		}
		auto type = child->GetExpressionType();
		if (type != duckdb::ExpressionType::COMPARE_LESSTHAN && type != duckdb::ExpressionType::COMPARE_GREATERTHAN) {
			continue;
		}
		auto &comparison = child->Cast<duckdb::BoundFunctionExpression>();
		auto &left = duckdb::BoundComparisonExpression::Left(comparison);
		auto &right = duckdb::BoundComparisonExpression::Right(comparison);
		// `S < end_snapshot` either way round; anything else, including the
		// inclusive forms, is left alone.
		auto &constant = type == duckdb::ExpressionType::COMPARE_LESSTHAN ? left : right;
		auto &column = type == duckdb::ExpressionType::COMPARE_LESSTHAN ? right : left;
		if (!IsColumnRef(column, get, projected)) {
			continue;
		}
		if (auto snapshot = ConstantSnapshot(constant); snapshot.IsValid()) {
			bound = snapshot;
		}
	}

	return saw_is_null ? bound : duckdb::optional_idx();
}

// Records the snapshot a lifecycle filter keeps this scan's rows against,
// so `InitGlobal` can leave the ended half unread.
//
// **Consumes nothing**, as with the scope pushdown below: the filter stays
// in `filters` and DuckLake keeps applying it, so a bound recognized too
// generously costs a narrower materialization, never a wrong row — the core
// still refuses to narrow a bound that has fallen behind its read point.
void MetadataScanPushdownLiveBound(duckdb::LogicalGet &get, MetadataScanBindData &bind_data,
                                   const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &filters) {
	// `end_snapshot_column` only names a column on a kind that declares an
	// end key, so a kind marked narrowable without one narrows nothing
	// rather than reading column zero as a lifecycle.
	if (!bind_data.spec->live_narrowable || bind_data.spec->end_key_columns.empty() || bind_data.live_bound.IsValid()) {
		return;
	}
	auto projected = ProjectedColumn(get, bind_data.spec->end_snapshot_column);
	if (!projected.IsValid()) {
		return;
	}
	for (auto &filter : filters) {
		if (auto bound = LiveBoundOf(*filter, get, projected.GetIndex()); bound.IsValid()) {
			bind_data.live_bound = bound;
			return;
		}
	}
}

// Records the `table_id` an equality filter pins this scan to, so
// `InitGlobal` can materialize just that table's run.
//
// **Consumes nothing.** Every filter stays in `filters`, so DuckDB keeps
// applying all of them and this scan never owes a predicate it did not
// implement — a shape not matched here costs a wider materialization, never
// a wrong row. That is also why `filter_pushdown` stays off: setting it
// would delete the filter operator and make the scan responsible for the
// whole of `TableFilterType`.
void MetadataScanPushdownComplexFilter(duckdb::ClientContext &, duckdb::LogicalGet &get,
                                       duckdb::FunctionData *bind_data_p,
                                       duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &filters) {
	if (bind_data_p == nullptr) {
		return;
	}
	auto &bind_data = bind_data_p->Cast<MetadataScanBindData>();
	if (bind_data.spec == nullptr) {
		return;
	}
	MetadataScanPushdownLiveBound(get, bind_data, filters);
	if (bind_data.spec->scope_column < 0 || bind_data.scope.IsValid()) {
		return;
	}

	auto projected = ProjectedColumn(get, static_cast<duckdb::idx_t>(bind_data.spec->scope_column));
	if (!projected.IsValid()) {
		return;
	}

	for (auto &filter : filters) {
		if (filter->GetExpressionType() != duckdb::ExpressionType::COMPARE_EQUAL) {
			continue;
		}
		auto &comparison = filter->Cast<duckdb::BoundFunctionExpression>();
		auto &left = duckdb::BoundComparisonExpression::Left(comparison);
		auto &right = duckdb::BoundComparisonExpression::Right(comparison);
		// Either side may hold the column reference.
		const duckdb::Expression *orderings[2][2] = {
		    {&left, &right},
		    {&right, &left},
		};
		for (auto &ordering : orderings) {
			auto *reference = ordering[0];
			auto *constant = ordering[1];
			if (reference->GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF ||
			    constant->GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
				continue;
			}
			auto &column = reference->Cast<duckdb::BoundColumnRefExpression>();
			if (column.Binding().table_index != get.table_index ||
			    column.Binding().column_index != projected.GetIndex()) {
				continue;
			}
			auto &value = constant->Cast<duckdb::BoundConstantExpression>().GetValue();
			if (value.IsNull() || value.type().id() != duckdb::LogicalTypeId::BIGINT) {
				continue;
			}
			// Read signed and refused if negative: no table carries a
			// negative id, and `optional_idx` throws on the sentinel an
			// unsigned read of -1 would produce.
			auto table_id = value.GetValue<int64_t>();
			if (table_id < 0) {
				continue;
			}
			bind_data.scope = static_cast<duckdb::idx_t>(table_id);
			return;
		}
	}
}

// Writes `count` rows of column `col_id`, from `start`, into `target` as
// typed flat data. A virtual column other than the row id, or an id past
// the row's width, is NULL: the former has no synthesized value and the
// latter would be a DuckDB/shim mismatch, never a read out of bounds. A
// column of a type without a typed path below goes through `SetValue`.
void EmitMetadataColumn(const MetadataRows &rows, duckdb::idx_t start, duckdb::idx_t count, duckdb::column_t col_id,
                        duckdb::Vector &target) {
	auto &validity = duckdb::FlatVector::ValidityMutable(target);
	auto cell = [&](duckdb::idx_t out_row) -> const duckdb::Value * {
		auto &row = rows[start + out_row];
		if (duckdb::IsVirtualColumn(col_id) || col_id >= row.size() || row[col_id].IsNull()) {
			validity.SetInvalid(out_row);
			return nullptr;
		}
		return &row[col_id];
	};
	switch (target.GetType().id()) {
	case duckdb::LogicalTypeId::BIGINT: {
		auto data = duckdb::FlatVector::GetDataMutable<int64_t>(target);
		for (duckdb::idx_t out_row = 0; out_row < count; out_row++) {
			if (auto *value = cell(out_row)) {
				data[out_row] = value->GetValue<int64_t>();
			}
		}
		return;
	}
	case duckdb::LogicalTypeId::BOOLEAN: {
		auto data = duckdb::FlatVector::GetDataMutable<bool>(target);
		for (duckdb::idx_t out_row = 0; out_row < count; out_row++) {
			if (auto *value = cell(out_row)) {
				data[out_row] = value->GetValue<bool>();
			}
		}
		return;
	}
	case duckdb::LogicalTypeId::VARCHAR: {
		auto data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(target);
		for (duckdb::idx_t out_row = 0; out_row < count; out_row++) {
			if (auto *value = cell(out_row)) {
				data[out_row] = duckdb::StringVector::AddString(target, duckdb::StringValue::Get(*value));
			}
		}
		return;
	}
	default:
		for (duckdb::idx_t out_row = 0; out_row < count; out_row++) {
			if (auto *value = cell(out_row)) {
				target.SetValue(out_row, *value);
			}
		}
		return;
	}
}

void MetadataScanFunctionImpl(duckdb::ClientContext &context, duckdb::TableFunctionInput &data,
                              duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<MetadataScanBindData>();
	auto &state = data.global_state->Cast<MetadataScanGlobalState>();
	if (state.cursor != nullptr) {
		if (state.page_offset == state.page.size()) {
			state.page = state.page_provider(state.cursor.get(), moraine_shim_is_interrupted, &context);
			state.page_offset = 0;
		}
		auto count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, state.page.size() - state.page_offset);
		auto start = state.offset.fetch_add(count);
		if (start + count > MoraineTransaction::kScannedRunCapacity) {
			throw duckdb::InvalidInputException("moraine: metadata scan exhausted its row identities");
		}
		for (duckdb::idx_t column = 0; column < state.column_ids.size(); column++) {
			if (state.column_ids[column] == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
				auto ids = duckdb::FlatVector::GetDataMutable<int64_t>(output.data[column]);
				for (duckdb::idx_t row = 0; row < count; row++) {
					ids[row] = static_cast<int64_t>(state.row_id_base + start + row);
				}
			} else {
				EmitMetadataColumn(state.page, state.page_offset, count, state.column_ids[column], output.data[column]);
			}
		}
		state.page_offset += count;
		output.SetChildCardinality(count);
		return;
	}

	// A scan that deferred materialization holds its rows on the state;
	// every other one holds them on the bind data.
	auto &materialized = state.rows != nullptr ? state.rows : bind_data.rows;
	if (materialized == nullptr) {
		throw duckdb::InternalException("moraine: metadata scan bound without a materialized row set");
	}
	auto &rows = *materialized;
	auto start = state.offset.fetch_add(STANDARD_VECTOR_SIZE);
	if (start >= rows.size()) {
		output.SetChildCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, rows.size() - start);
	for (duckdb::idx_t out_col = 0; out_col < state.column_ids.size(); out_col++) {
		auto col_id = state.column_ids[out_col];
		auto &target = output.data[out_col];
		if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
			// An id in the transaction's run registry, which the
			// staged-write Sink (staged_write.cpp) resolves back to this
			// very row — never to the same position in a second
			// materialization, which a narrowed scan and a commit
			// landing under it can each make a different list.
			auto ids = duckdb::FlatVector::GetDataMutable<int64_t>(target);
			for (duckdb::idx_t out_row = 0; out_row < count; out_row++) {
				ids[out_row] = static_cast<int64_t>(state.row_id_base + start + out_row);
			}
			continue;
		}
		EmitMetadataColumn(rows, start, count, col_id, target);
	}
	output.SetChildCardinality(count);
}

} // namespace

duckdb::unique_ptr<duckdb::FunctionData> MetadataScanBindData::Copy() const {
	auto result = duckdb::make_uniq<MetadataScanBindData>();
	result->rows = rows;
	result->spec = spec;
	result->catalog = catalog;
	result->catalog_name = catalog_name;
	result->scope = scope;
	result->live_bound = live_bound;
	result->table_entry = table_entry;
	return result;
}

bool MetadataScanBindData::Equals(const duckdb::FunctionData &other_p) const {
	auto &other = other_p.Cast<MetadataScanBindData>();
	// `spec` and `scope` carry the identity `rows` does elsewhere: a
	// deferred scan has no row set at bind to compare, and two scans of one
	// kind narrowed to different tables are not interchangeable.
	auto same_scope =
	    scope.IsValid() == other.scope.IsValid() && (!scope.IsValid() || scope.GetIndex() == other.scope.GetIndex());
	auto same_live_bound = live_bound.IsValid() == other.live_bound.IsValid() &&
	                       (!live_bound.IsValid() || live_bound.GetIndex() == other.live_bound.GetIndex());
	return rows == other.rows && spec == other.spec && same_scope && same_live_bound &&
	       table_entry.get() == other.table_entry.get();
}

duckdb::TableFunction MetadataScanTableFunction() {
	// No `bind` callback (as in `MoraineScanFunction`, scan.cpp): the caller
	// already produces complete bind data itself.
	duckdb::TableFunction function("moraine_metadata_scan", {}, MetadataScanFunctionImpl, nullptr,
	                               MetadataScanInitGlobal, nullptr);
	// Required for the zero-real-column "virtual column" scan shape the
	// exists-probe query uses (see `MetadataScanGlobalState::column_ids`);
	// real projection pushdown falls out of the same mechanism.
	function.projection_pushdown = true;
	// Reads the equality a narrowable kind scopes its materialization by,
	// without consuming it (`MetadataScanPushdownComplexFilter`).
	// `filter_pushdown` deliberately stays off: it would delete the filter
	// operator and make this scan answerable for every `TableFilterType`.
	function.pushdown_complex_filter = MetadataScanPushdownComplexFilter;
	// Resolves `LogicalGet::GetTable()` so UPDATE/DELETE statements bind
	// against these tables.
	function.get_table_entry = MetadataScanTableEntry;
	return function;
}

MoraineMetadataTableEntry::MoraineMetadataTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
                                                     duckdb::CreateTableInfo &info, const MetadataTableSpec &spec,
                                                     MoraineCatalogHandle *handle)
    : duckdb::TableCatalogEntry(catalog, schema, info), spec_(spec), handle_(handle),
      columns_(std::move(info.columns)) {
}

duckdb::unique_ptr<duckdb::BaseStatistics> MoraineMetadataTableEntry::GetStatistics(duckdb::ClientContext &,
                                                                                    duckdb::column_t) {
	throw duckdb::NotImplementedException("moraine: column statistics are not supported yet");
}

std::shared_ptr<const MetadataRows> MetadataRowsFor(duckdb::ClientContext &context, duckdb::Catalog &catalog,
                                                    const MetadataTableSpec &spec, duckdb::optional_idx live_bound) {
	auto catalog_transaction = catalog.GetCatalogTransaction(context);
	auto &transaction = catalog_transaction.transaction->Cast<MoraineTransaction>();
	auto *handle = moraine_snapshot_read_handle(transaction.Snapshot());
	if (handle == nullptr) {
		handle = catalog.Cast<MoraineCatalog>().Handle();
	}
	if (auto *staged = transaction.StagedTxIfOpen()) {
		if (live_bound.IsValid() && spec.live_narrowable && spec.tx.live_provider) {
			return std::make_shared<const MetadataRows>(spec.tx.live_provider(staged, live_bound.GetIndex()));
		}
		if (spec.tx.provider) {
			return std::make_shared<const MetadataRows>(
			    spec.tx.provider(staged, handle, moraine_shim_is_interrupted, &context));
		}
	}
	return std::make_shared<const MetadataRows>(spec.provider(handle, moraine_shim_is_interrupted, &context));
}

std::shared_ptr<const MetadataRows> ScopedMetadataRowsFor(duckdb::ClientContext &context, duckdb::Catalog &catalog,
                                                          const MetadataTableSpec &spec, uint64_t table_id,
                                                          duckdb::optional_idx live_bound) {
	auto catalog_transaction = catalog.GetCatalogTransaction(context);
	auto &transaction = catalog_transaction.transaction->Cast<MoraineTransaction>();
	if (auto *staged = transaction.StagedTxIfOpen()) {
		if (!spec.tx.scoped_provider) {
			return MetadataRowsFor(context, catalog, spec, live_bound);
		}
		auto filter = live_bound.IsValid() && spec.live_narrowable ? live_bound.GetIndex() : kEveryVersion;
		return std::make_shared<const MetadataRows>(spec.tx.scoped_provider(staged, table_id, filter));
	}
	if (spec.scoped_provider == nullptr) {
		return MetadataRowsFor(context, catalog, spec, live_bound);
	}
	auto *handle = moraine_snapshot_read_handle(transaction.Snapshot());
	if (handle == nullptr) {
		handle = catalog.Cast<MoraineCatalog>().Handle();
	}
	return std::make_shared<const MetadataRows>(
	    spec.scoped_provider(handle, table_id, moraine_shim_is_interrupted, &context));
}

duckdb::TableFunction MoraineMetadataTableEntry::GetScanFunction(duckdb::ClientContext &context,
                                                                 duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	auto scan_bind_data = duckdb::make_uniq<MetadataScanBindData>();
	scan_bind_data->spec = &spec_;
	scan_bind_data->catalog = &ParentCatalog();
	scan_bind_data->catalog_name = ParentCatalog().GetName().GetIdentifierName();
	// A narrowable kind is left unmaterialized for `InitGlobal`: the filter
	// deciding how much of it to build has not been pushed down yet.
	if (spec_.page_provider == nullptr && spec_.scope_column < 0 && !spec_.live_narrowable) {
		scan_bind_data->rows = MetadataRowsFor(context, ParentCatalog(), spec_);
	}
	scan_bind_data->table_entry = this;
	bind_data = std::move(scan_bind_data);
	return MetadataScanTableFunction();
}

duckdb::TableStorageInfo MoraineMetadataTableEntry::GetStorageInfo(duckdb::ClientContext &) {
	return duckdb::TableStorageInfo();
}

void PopulateMetadataTables(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, MoraineCatalogHandle *handle,
                            duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CatalogEntry>> &tables) {
	// The attach pinned the shape; the entries registered below carry that
	// one choice for its life.
	for (auto &spec : MoraineMetadataTableSpecs(catalog.Cast<MoraineCatalog>().ServesExtendedCatalog())) {
		duckdb::CreateTableInfo info(schema, spec.name);
		duckdb::idx_t column_index = 0;
		for (auto &col : spec.columns) {
			info.columns.AddColumn(duckdb::ColumnDefinition(col.name, MapColumnType(col.ducklake_type)));
			if (col.not_null) {
				info.constraints.push_back(duckdb::make_uniq_base<duckdb::Constraint, duckdb::NotNullConstraint>(
				    duckdb::LogicalIndex(column_index)));
			}
			column_index++;
		}
		tables.emplace(spec.name, duckdb::make_uniq<MoraineMetadataTableEntry>(catalog, schema, info, spec, handle));
	}
}

} // namespace moraine_duckdb
