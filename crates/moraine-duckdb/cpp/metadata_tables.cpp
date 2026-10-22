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

// The store state a dump's rows stand at.
struct MoraineHeadStamp {
	uint64_t snapshot_id;
	uint64_t batch_seq;
};

// Reads the head stamp, reporting whether one exists. A store with no
// head yet (mid-bootstrap) and a failed read both report false, which
// costs a caller only the caching it would otherwise have done — never
// correctness, so the error is swallowed rather than raised.
bool ReadHeadStamp(MoraineCatalogHandle *handle, duckdb::ClientContext &context, MoraineHeadStamp &out) {
	MoraineError err {};
	bool present = false;
	if (moraine_head_stamp(handle, &out.snapshot_id, &out.batch_seq, &present, moraine_shim_is_interrupted, &context,
	                       &err) != MORAINE_OK) {
		if (err.message != nullptr) {
			moraine_error_free(err.message);
		}
		return false;
	}
	return present;
}

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
		return std::max<idx_t>(1, row_count / STANDARD_VECTOR_SIZE);
	}
};

bool EmitsRowIds(const std::vector<duckdb::column_t> &column_ids) {
	return std::find(column_ids.begin(), column_ids.end(), duckdb::COLUMN_IDENTIFIER_ROW_ID) != column_ids.end();
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> MetadataScanInitGlobal(duckdb::ClientContext &context,
                                                                            duckdb::TableFunctionInitInput &input) {
	auto state = duckdb::make_uniq<MetadataScanGlobalState>();
	state->column_ids = input.column_ids;

	auto &bind_data = input.bind_data->Cast<MetadataScanBindData>();
	if (bind_data.spec == nullptr || bind_data.catalog == nullptr) {
		return state;
	}
	auto &catalog = LiveBoundCatalog(context, bind_data);
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

void MetadataScanFunctionImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<MetadataScanBindData>();
	auto &state = data.global_state->Cast<MetadataScanGlobalState>();
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
	auto *handle = catalog.Cast<MoraineCatalog>().Handle();
	auto *staged_tx = transaction.StagedTxIfOpen();

	if (staged_tx != nullptr) {
		// A writing transaction reads through its staged tx, which pins one
		// read point for its whole life and overlays the rows staged so far.
		// That is the pinning this function otherwise supplies.
		//
		// The read point cannot move under a materialization, so what makes
		// one stale is this transaction staging into the same table —
		// `StagedTxFor` drops exactly that entry as it hands out the tx.
		// A commit attempt takes the staged tx, so the retry DuckLake drives
		// between attempts opens a fresh one and re-reads through it.
		// The staged tx pins one read point for its whole life, so a
		// narrowed and an unnarrowed read of one table cannot stand at two
		// heads — which is what confines the narrowing to this branch.
		const bool live_only = live_bound.IsValid() && spec.live_narrowable;
		if (auto cached = transaction.GetMetadataRows(spec, live_only)) {
			return cached;
		}
		// Taken before the materialization it stamps: a row staged into
		// this table while the set is built invalidates it, and the Put
		// drops it rather than serving it.
		auto epoch = transaction.MetadataRowsEpoch();
		// A table with no staged reader serves committed state, which is
		// right for one no transaction can write.
		std::shared_ptr<const MetadataRows> rows;
		if (live_only && spec.tx.live_provider) {
			rows = std::make_shared<const MetadataRows>(
			    spec.tx.live_provider(staged_tx, static_cast<uint64_t>(live_bound.GetIndex())));
		} else if (spec.tx.provider) {
			rows = std::make_shared<const MetadataRows>(
			    spec.tx.provider(staged_tx, handle, moraine_shim_is_interrupted, &context));
		} else {
			rows = std::make_shared<const MetadataRows>(spec.provider(handle, moraine_shim_is_interrupted, &context));
		}
		transaction.PutMetadataRows(spec, rows, live_only, epoch, std::nullopt);
		return rows;
	}

	if (auto cached = transaction.GetMetadataRows(spec)) {
		return cached;
	}
	// Taken before the materialization it stamps, as in the staged branch:
	// a staged tx opened while the set is built invalidates it.
	auto epoch = transaction.MetadataRowsEpoch();

	// The rows this attach dumped last time are byte-identical to a fresh
	// dump whenever no batch landed since, so compare against the stamp
	// the transaction's snapshot stands at before paying the ABI crossing
	// again. A store with no head yet simply dumps.
	auto &moraine_catalog = catalog.Cast<MoraineCatalog>();
	MoraineHeadStamp before;
	const bool stamped = transaction.SnapshotStamp(before.snapshot_id, before.batch_seq);
	if (stamped) {
		if (auto held = moraine_catalog.HeldMetadataRows(spec, before.snapshot_id, before.batch_seq)) {
			transaction.PutMetadataRows(spec, held, false, epoch, std::nullopt);
			return held;
		}
	}

	auto rows = std::make_shared<const MetadataRows>(spec.provider(handle, moraine_shim_is_interrupted, &context));
	transaction.PutMetadataRows(spec, rows, false, epoch, std::nullopt);

	// Hold them for the next transaction only if the store did not move
	// under the dump: rows that straddled a commit stand at no single
	// stamp, and holding them under the earlier one would serve a
	// concurrent reader at that stamp a row set from beyond it.
	MoraineHeadStamp after;
	if (stamped && ReadHeadStamp(handle, context, after) && after.snapshot_id == before.snapshot_id &&
	    after.batch_seq == before.batch_seq) {
		moraine_catalog.HoldMetadataRows(spec, before.snapshot_id, before.batch_seq, rows);
	}
	return rows;
}

std::shared_ptr<const MetadataRows> ScopedMetadataRowsFor(duckdb::ClientContext &context, duckdb::Catalog &catalog,
                                                          const MetadataTableSpec &spec, uint64_t table_id,
                                                          duckdb::optional_idx live_bound) {
	auto catalog_transaction = catalog.GetCatalogTransaction(context);
	auto &transaction = catalog_transaction.transaction->Cast<MoraineTransaction>();
	if (spec.scope_column < 0 || spec.scoped_provider == nullptr) {
		return MetadataRowsFor(context, catalog, spec, live_bound);
	}
	const std::optional<uint64_t> scope = table_id;

	// Mid-write a read owes the staged tx's overlay, so the narrowed dump
	// is the staged one, which carries it — and takes the live bound too,
	// so a read naming a table and a snapshot pays for neither the other
	// tables nor the ended versions.
	if (auto *staged_tx = transaction.StagedTxIfOpen()) {
		if (!spec.tx.scoped_provider) {
			return MetadataRowsFor(context, catalog, spec, live_bound);
		}
		const bool live_only = live_bound.IsValid() && spec.live_narrowable;
		if (auto cached = transaction.GetMetadataRows(spec, live_only, scope)) {
			return cached;
		}
		// Taken before the dump it stamps: a row staged into this table
		// while it is built invalidates it.
		auto epoch = transaction.MetadataRowsEpoch();
		const uint64_t filter_snapshot = live_only ? static_cast<uint64_t>(live_bound.GetIndex()) : kEveryVersion;
		auto rows = std::make_shared<const MetadataRows>(spec.tx.scoped_provider(staged_tx, table_id, filter_snapshot));
		// Held for this transaction only: the rows carry its uncommitted
		// overlay, so the attach must never serve them to another.
		transaction.PutMetadataRows(spec, rows, live_only, epoch, scope);
		return rows;
	}

	if (auto cached = transaction.GetMetadataRows(spec, false, scope)) {
		return cached;
	}
	// Taken before the materialization it stamps, as the unscoped path
	// does: a staged tx opened while the set is built invalidates it.
	auto epoch = transaction.MetadataRowsEpoch();

	// A transaction that already materialized this table narrows what it
	// holds. Reading the store again here would serve one scan of the table
	// rows from beyond the point every other scan of it stands at.
	if (auto held = transaction.GetMetadataRows(spec)) {
		auto column = static_cast<duckdb::idx_t>(spec.scope_column);
		MetadataRows narrowed;
		for (auto &row : *held) {
			if (column < row.size() && !row[column].IsNull() &&
			    row[column].GetValue<int64_t>() == static_cast<int64_t>(table_id)) {
				narrowed.push_back(row);
			}
		}
		auto rows = std::make_shared<const MetadataRows>(std::move(narrowed));
		transaction.PutMetadataRows(spec, rows, false, epoch, scope);
		return rows;
	}

	// As the unscoped path: the rows this attach last narrowed to this
	// table are byte-identical to a fresh dump whenever no batch landed
	// since, and are held only if the store did not move under the dump.
	auto &moraine_catalog = catalog.Cast<MoraineCatalog>();
	auto *handle = moraine_catalog.Handle();
	MoraineHeadStamp before;
	const bool stamped = transaction.SnapshotStamp(before.snapshot_id, before.batch_seq);
	if (stamped) {
		if (auto held = moraine_catalog.HeldMetadataRows(spec, before.snapshot_id, before.batch_seq, scope)) {
			transaction.PutMetadataRows(spec, held, false, epoch, scope);
			return held;
		}
	}

	auto rows = std::make_shared<const MetadataRows>(
	    spec.scoped_provider(handle, table_id, moraine_shim_is_interrupted, &context));
	transaction.PutMetadataRows(spec, rows, false, epoch, scope);

	MoraineHeadStamp after;
	if (stamped && ReadHeadStamp(handle, context, after) && after.snapshot_id == before.snapshot_id &&
	    after.batch_seq == before.batch_seq) {
		moraine_catalog.HoldMetadataRows(spec, before.snapshot_id, before.batch_seq, rows, scope);
	}
	return rows;
}

duckdb::TableFunction MoraineMetadataTableEntry::GetScanFunction(duckdb::ClientContext &context,
                                                                 duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	auto scan_bind_data = duckdb::make_uniq<MetadataScanBindData>();
	scan_bind_data->spec = &spec_;
	scan_bind_data->catalog = &ParentCatalog();
	scan_bind_data->catalog_name = ParentCatalog().GetName().GetIdentifierName();
	// A narrowable kind is left unmaterialized for `InitGlobal`: the filter
	// deciding how much of it to build has not been pushed down yet.
	if (spec_.scope_column < 0 && !spec_.live_narrowable) {
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
