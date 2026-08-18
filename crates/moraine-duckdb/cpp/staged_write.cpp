#include "staged_write.hpp"

#include "catalog.hpp"
#include "metadata_tables.hpp"
#include "transaction_manager.hpp"

#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include <algorithm>
#include <deque>

namespace moraine_duckdb {

namespace {

// Converts one scanned cell to a `MoraineCell`, appending any decoded
// string into `string_storage` — a deque because the `c_str()` pointers
// taken here are borrowed by `moraine_tx_stage` and must survive later
// appends.
MoraineCell CellFromValue(const duckdb::Value &value, const duckdb::LogicalType &type,
                          std::deque<std::string> &string_storage) {
	MoraineCell cell {};
	if (value.IsNull()) {
		cell.kind = 0;
		return cell;
	}
	switch (type.id()) {
	case duckdb::LogicalTypeId::BIGINT:
		cell.kind = 1;
		cell.u64_value = static_cast<uint64_t>(value.GetValue<int64_t>());
		return cell;
	case duckdb::LogicalTypeId::UBIGINT:
		cell.kind = 1;
		cell.u64_value = value.GetValue<uint64_t>();
		return cell;
	case duckdb::LogicalTypeId::TIMESTAMP_TZ:
		cell.kind = 2;
		cell.i64_value = value.GetValue<duckdb::timestamp_tz_t>().value;
		return cell;
	case duckdb::LogicalTypeId::BOOLEAN:
		cell.kind = 3;
		cell.bool_value = value.GetValue<bool>();
		return cell;
	case duckdb::LogicalTypeId::VARCHAR:
	case duckdb::LogicalTypeId::UUID:
		// `Value::ToString()` renders a VARCHAR verbatim and a UUID in its
		// canonical text form — the string form the store carries for both,
		// so one code path serves both column kinds.
		cell.kind = 4;
		string_storage.push_back(value.ToString());
		cell.str_value = string_storage.back().c_str();
		return cell;
	default:
		// Every `ducklake_*` column declared (metadata_tables.cpp) uses one
		// of the types above; any other type here is a spec/Sink mismatch.
		throw duckdb::NotImplementedException(
		    "moraine: staged write hit an unsupported column type (%s) — spec/Sink type mismatch", type.ToString());
	}
}

// Shared between the Sink and Source halves of every staged-write operator
// (the same dual-role shape DuckDB's own `PhysicalInsert` uses): the row
// count staged so far and whether the one-row `Count` result has been
// emitted.
struct MetadataDmlState : public duckdb::GlobalSinkState {
	duckdb::idx_t affected_count = 0;
	bool emitted = false;
};

// Base for the three staged-write operators: owns the spec/catalog
// references, the shared state type, and the one-row `Count` Source half.
class MoraineMetadataDml : public duckdb::PhysicalOperator {
public:
	static constexpr const duckdb::PhysicalOperatorType TYPE = duckdb::PhysicalOperatorType::EXTENSION;

	MoraineMetadataDml(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                   const MetadataTableSpec &spec, duckdb::Catalog &catalog, duckdb::idx_t estimated_cardinality)
	    : duckdb::PhysicalOperator(physical_plan, TYPE, std::move(types), estimated_cardinality), spec_(spec),
	      catalog_(catalog) {
	}

	const MetadataTableSpec &spec_;
	duckdb::Catalog &catalog_;

public:
	duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(duckdb::ClientContext &) const override {
		return duckdb::make_uniq<MetadataDmlState>();
	}

	bool IsSink() const override {
		return true;
	}

protected:
	// The staged tx every row this operator translates lands in: the
	// DuckDB transaction's one lazily-opened staged transaction.
	MoraineTxHandle *StagedTx(duckdb::ClientContext &client) const {
		auto catalog_transaction = catalog_.GetCatalogTransaction(client);
		auto &moraine_tx = catalog_transaction.transaction->Cast<MoraineTransaction>();
		// Named, because every row this operator stages lands in `spec_`'s
		// table: no other table's materialization goes stale for it.
		return moraine_tx.StagedTxFor(spec_);
	}

	// Resolves a rowid the metadata scan emitted back to the row it stands
	// for, through the run that scan registered with this transaction.
	//
	// The registry holds the very rows the scan handed out, so no second
	// materialization is taken and none has to agree with the first: a
	// commit landing under this statement, or a narrowed scan reading a
	// different list from the one a re-dump would build, cannot move what a
	// rowid names.
	const std::vector<duckdb::Value> &ResolveRow(const duckdb::Value &row_id,
	                                             duckdb::ClientContext &client) const {
		if (row_id.IsNull()) {
			throw duckdb::InternalException("moraine: staged write received a NULL rowid");
		}
		auto catalog_transaction = catalog_.GetCatalogTransaction(client);
		auto &transaction = catalog_transaction.transaction->Cast<MoraineTransaction>();
		auto *row = transaction.ScannedRow(spec_, static_cast<uint64_t>(row_id.GetValue<int64_t>()));
		if (row == nullptr) {
			throw duckdb::InternalException(
			    "moraine: staged write received a rowid on \"%s\" that no scan of this transaction emitted",
			    spec_.name);
		}
		return *row;
	}

public:
	duckdb::SourceResultType GetDataInternal(duckdb::ExecutionContext &, duckdb::DataChunk &chunk,
	                                         duckdb::OperatorSourceInput &) const override {
		// `sink_state` is the same `MetadataDmlState` `Sink` populated: the
		// base `PhysicalOperator` carries it from the sink phase into this
		// later source phase.
		auto &state = sink_state->Cast<MetadataDmlState>();
		if (state.emitted) {
			chunk.SetCardinality(0);
			return duckdb::SourceResultType::FINISHED;
		}
		chunk.SetValue(0, 0, duckdb::Value::BIGINT(static_cast<int64_t>(state.affected_count)));
		chunk.SetCardinality(1);
		state.emitted = true;
		return duckdb::SourceResultType::FINISHED;
	}

	bool IsSource() const override {
		return true;
	}
};

// Translates every row of its input into one `moraine_tx_stage` INSERT
// call each. Nothing lands in the store until the DuckDB transaction
// commits (`MoraineTransactionManager::CommitTransaction`).
class MoraineMetadataInsert : public MoraineMetadataDml {
public:
	using MoraineMetadataDml::MoraineMetadataDml;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<MetadataDmlState>();
		auto *tx = StagedTx(context.client);

		for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
			std::deque<std::string> string_storage;
			std::vector<MoraineCell> cells;
			cells.reserve(chunk.ColumnCount());
			for (duckdb::idx_t col = 0; col < chunk.ColumnCount(); col++) {
				auto value = chunk.GetValue(col, row);
				auto type = MapColumnType(spec_.columns[col].ducklake_type);
				cells.push_back(CellFromValue(value, type, string_storage));
			}
			MoraineError err {};
			auto code = moraine_tx_stage(tx, spec_.write_table_kind, /* insert */ 0, cells.data(), cells.size(), &err);
			if (code != MORAINE_OK) {
				ThrowMoraineError(err);
			}
			state.affected_count++;
		}
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}
};

// `spec.write_table_kind == kVoidInsertable`: accepts every row (counts it
// for the `Count` result DuckLake's own commit path expects) but stages
// nothing — see `kVoidInsertable`'s doc (metadata_tables.hpp) for why the
// row is redundant here, not unsupported.
class MoraineMetadataVoidInsert : public MoraineMetadataDml {
public:
	using MoraineMetadataDml::MoraineMetadataDml;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<MetadataDmlState>();
		state.affected_count += chunk.size();
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}
};

// Translates an UPDATE's rows. Two modes, decided at plan time (see
// staged_write.hpp): ending a versioned row's lifecycle
// (`SET end_snapshot`), or overlaying an unversioned statistics row in
// place.
class MoraineMetadataUpdate : public MoraineMetadataDml {
public:
	MoraineMetadataUpdate(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                      const MetadataTableSpec &spec, duckdb::Catalog &catalog, duckdb::idx_t estimated_cardinality,
	                      int32_t lifecycle_op, std::vector<duckdb::idx_t> set_columns,
	                      std::vector<duckdb::idx_t> set_refs)
	    : MoraineMetadataDml(physical_plan, std::move(types), spec, catalog, estimated_cardinality),
	      lifecycle_op_(lifecycle_op), set_columns_(std::move(set_columns)), set_refs_(std::move(set_refs)) {
	}

	// The staged operation_kind for a lifecycle update — 2 (SET
	// end_snapshot) or 3 (SET begin_snapshot) — or -1 for the statistics
	// overlay staged as a full-row insert (the in-place overwrite an
	// insert means for unversioned kinds).
	int32_t lifecycle_op_;
	// Declared column index of each SET target, and the chunk column its
	// new value arrives in, index-aligned.
	std::vector<duckdb::idx_t> set_columns_;
	std::vector<duckdb::idx_t> set_refs_;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<MetadataDmlState>();
		auto *tx = StagedTx(context.client);

		// Pinned layout: the row-id column is the last column of the sink
		// chunk (`Binder::BindRowIdColumns` appends it after the SET and
		// constraint columns; DuckDB's own `PhysicalUpdate` reads the same
		// position).
		auto row_id_col = chunk.ColumnCount() - 1;

		for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
			auto &old_row = ResolveRow(chunk.GetValue(row_id_col, row), context.client);
			std::deque<std::string> string_storage;
			std::vector<MoraineCell> cells;

			if (lifecycle_op_ >= 0) {
				// [entity key cells in decoder order, new snapshot value].
				cells.reserve(spec_.end_key_columns.size() + 1);
				for (auto key_col : spec_.end_key_columns) {
					auto type = MapColumnType(spec_.columns[key_col].ducklake_type);
					cells.push_back(CellFromValue(old_row[key_col], type, string_storage));
				}
				auto value_type = MapColumnType(spec_.columns[set_columns_[0]].ducklake_type);
				cells.push_back(CellFromValue(chunk.GetValue(set_refs_[0], row), value_type, string_storage));
				MoraineError err {};
				auto code =
				    moraine_tx_stage(tx, spec_.write_table_kind, lifecycle_op_, cells.data(), cells.size(), &err);
				if (code != MORAINE_OK) {
					ThrowMoraineError(err);
				}
			} else {
				// The full updated row: old values overlaid with the SET
				// values.
				std::vector<duckdb::Value> new_row = old_row;
				for (duckdb::idx_t j = 0; j < set_columns_.size(); j++) {
					new_row[set_columns_[j]] = chunk.GetValue(set_refs_[j], row);
				}
				cells.reserve(new_row.size());
				for (duckdb::idx_t col = 0; col < new_row.size(); col++) {
					auto type = MapColumnType(spec_.columns[col].ducklake_type);
					cells.push_back(CellFromValue(new_row[col], type, string_storage));
				}
				MoraineError err {};
				auto code = moraine_tx_stage(tx, spec_.write_table_kind, /* insert (overwrite) */ 0, cells.data(),
				                             cells.size(), &err);
				if (code != MORAINE_OK) {
					ThrowMoraineError(err);
				}
			}
			state.affected_count++;
		}
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}
};

// Translates a DELETE's rows: the removed row's key cells, staged as the
// raw delete only the three unversioned statistics kinds define.
class MoraineMetadataDelete : public MoraineMetadataDml {
public:
	MoraineMetadataDelete(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                      const MetadataTableSpec &spec, duckdb::Catalog &catalog, duckdb::idx_t estimated_cardinality,
	                      duckdb::idx_t row_id_chunk_index)
	    : MoraineMetadataDml(physical_plan, std::move(types), spec, catalog, estimated_cardinality),
	      row_id_chunk_index_(row_id_chunk_index) {
	}

	// Where the row-id column sits in the sink chunk (from
	// `LogicalDelete::expressions[0]`'s bound reference — pinned layout).
	duckdb::idx_t row_id_chunk_index_;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<MetadataDmlState>();
		auto *tx = StagedTx(context.client);

		for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
			auto &old_row = ResolveRow(chunk.GetValue(row_id_chunk_index_, row), context.client);
			std::deque<std::string> string_storage;
			std::vector<MoraineCell> cells;
			cells.reserve(spec_.delete_key_columns.size());
			for (auto key_col : spec_.delete_key_columns) {
				auto type = MapColumnType(spec_.columns[key_col].ducklake_type);
				cells.push_back(CellFromValue(old_row[key_col], type, string_storage));
			}
			MoraineError err {};
			auto code = moraine_tx_stage(tx, spec_.write_table_kind, /* delete */ 1, cells.data(), cells.size(), &err);
			if (code != MORAINE_OK) {
				ThrowMoraineError(err);
			}
			state.affected_count++;
		}
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}
};

// DuckLake's DROP/RENAME batch issues `UPDATE {table} SET end_snapshot ...`
// against every metadata table a dropped/renamed object could reference,
// including always-empty stand-ins (`ducklake_column_tag`,
// `ducklake_tag`). Since those tables can never have a live row, that
// UPDATE's WHERE clause matches nothing and the child scan produces zero
// rows, so accepting it as a no-op is sound. The Sink still throws if a
// row ever does arrive.
class MoraineMetadataVoidUpdate : public MoraineMetadataDml {
public:
	using MoraineMetadataDml::MoraineMetadataDml;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &) const override {
		if (chunk.size() != 0) {
			throw duckdb::InternalException(
			    "moraine: UPDATE on \"%s\" unexpectedly matched %llu row(s) — this table has no entity model on "
			    "the staged-row path and was assumed to always be empty",
			    spec_.name, static_cast<unsigned long long>(chunk.size()));
		}
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}
};

// The expiry cascade's dead-table cleanup deletes each dead table's
// `ducklake_inlined_data_tables` registration — DuckLake defers
// inline-table cleanup to expiry, not DROP TABLE, so the row is real.
// Each matched row stages the registration's removal (the paired dynamic
// `DROP TABLE IF EXISTS ducklake_inlined_data_<t>_<v>` in the same
// cascade removes the chunks through the ordinary drop path).
class MoraineInlineRegistryDelete : public MoraineMetadataDml {
public:
	MoraineInlineRegistryDelete(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                            const MetadataTableSpec &spec, duckdb::Catalog &catalog,
	                            duckdb::idx_t estimated_cardinality, duckdb::idx_t row_id_chunk_index)
	    : MoraineMetadataDml(physical_plan, std::move(types), spec, catalog, estimated_cardinality),
	      row_id_chunk_index_(row_id_chunk_index) {
	}

	duckdb::idx_t row_id_chunk_index_;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<MetadataDmlState>();
		auto *tx = StagedTx(context.client);

		for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
			auto &old_row = ResolveRow(chunk.GetValue(row_id_chunk_index_, row), context.client);
			// Column order: table_id, table_name, schema_version.
			auto table_id = old_row[0].GetValue<uint64_t>();
			auto schema_version = old_row[2].GetValue<uint64_t>();
			MoraineError err {};
			auto code = moraine_tx_stage_inline_schema_drop(tx, table_id, schema_version, &err);
			if (code != MORAINE_OK) {
				ThrowMoraineError(err);
			}
			state.affected_count++;
		}
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}
};

// Raw DELETEs against tables with no delete translation (always-empty
// stand-ins, `ducklake_metadata`, the void-insertable inline registry)
// plan as a no-op that still binds — the expiry cascade issues them
// unconditionally — but throws if a row ever actually matches, exactly
// like MoraineMetadataVoidUpdate: silence would lose a real deletion.
class MoraineMetadataVoidDelete : public MoraineMetadataDml {
public:
	using MoraineMetadataDml::MoraineMetadataDml;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &) const override {
		if (chunk.size() != 0) {
			throw duckdb::InternalException(
			    "moraine: DELETE on \"%s\" unexpectedly matched %llu row(s) — this table has no delete "
			    "translation on the staged-row path and was assumed to always be empty",
			    spec_.name, static_cast<unsigned long long>(chunk.size()));
		}
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}
};

// Extracts the chunk index each SET expression's value arrives in. After
// column-binding resolution every SET expression is a plain bound
// reference (or a BOUND_DEFAULT for `SET col = DEFAULT`, which DuckLake
// never issues against its metadata catalog and this path does not
// translate).
std::vector<duckdb::idx_t> ExtractSetRefs(duckdb::LogicalUpdate &op) {
	std::vector<duckdb::idx_t> refs;
	refs.reserve(op.expressions.size());
	for (auto &expr : op.expressions) {
		if (expr->GetExpressionClass() != duckdb::ExpressionClass::BOUND_REF) {
			throw duckdb::NotImplementedException(
			    "moraine: UPDATE with a non-column SET expression (e.g. SET ... = DEFAULT) is not supported on "
			    "\"%s\"",
			    op.table.name);
		}
		refs.push_back(expr->Cast<duckdb::BoundReferenceExpression>().index);
	}
	return refs;
}

} // namespace

duckdb::PhysicalOperator &PlanMetadataInsert(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalInsert &op,
                                             const MetadataTableSpec &spec) {
	if (spec.write_table_kind == kVoidInsertable) {
		return planner.Make<MoraineMetadataVoidInsert>(op.types, spec, op.table.catalog, op.estimated_cardinality);
	}
	return planner.Make<MoraineMetadataInsert>(op.types, spec, op.table.catalog, op.estimated_cardinality);
}

duckdb::PhysicalOperator &PlanMetadataUpdate(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalUpdate &op,
                                             const MetadataTableSpec &spec) {
	if (op.return_chunk) {
		throw duckdb::NotImplementedException("moraine: UPDATE ... RETURNING is not supported on \"%s\"",
		                                      op.table.name);
	}
	std::vector<duckdb::idx_t> set_columns;
	set_columns.reserve(op.columns.size());
	for (auto &col : op.columns) {
		set_columns.push_back(col.index);
	}
	auto set_refs = ExtractSetRefs(op);

	if (!spec.end_key_columns.empty()) {
		// A versioned kind: the translatable UPDATEs are the lifecycle
		// conventions — SET end_snapshot alone (ends the version) or, for
		// the delete-rewrite's replacement file, SET begin_snapshot alone
		// (rebases the visibility window).
		if (set_columns.size() == 1 && set_columns[0] == spec.end_snapshot_column) {
			return planner.Make<MoraineMetadataUpdate>(op.types, spec, op.table.catalog, op.estimated_cardinality,
			                                           /* update_set_end */ 2, std::move(set_columns),
			                                           std::move(set_refs));
		}
		if (set_columns.size() == 1 && std::string(spec.columns[set_columns[0]].name) == "begin_snapshot") {
			return planner.Make<MoraineMetadataUpdate>(op.types, spec, op.table.catalog, op.estimated_cardinality,
			                                           /* update_set_begin */ 3, std::move(set_columns),
			                                           std::move(set_refs));
		}
		throw duckdb::NotImplementedException(
		    "moraine: the only UPDATEs supported on \"%s\" are SET end_snapshot / SET begin_snapshot (the "
		    "staged-row lifecycle conventions)",
		    spec.name);
	}
	if (spec.overlay_updatable) {
		// An unversioned kind: any SET subset overlays the row in place.
		// Its key columns are the exception — an overlay stages the whole
		// row as an insert, so moving a row's key would add a row rather
		// than rename one, and the original would survive unreferenced.
		for (auto set_column : set_columns) {
			auto &key_columns = spec.delete_key_columns;
			if (std::find(key_columns.begin(), key_columns.end(), set_column) != key_columns.end()) {
				throw duckdb::NotImplementedException("moraine: UPDATE on \"%s\" cannot SET the key column \"%s\"",
				                                      spec.name, spec.columns[set_column].name);
			}
		}
		return planner.Make<MoraineMetadataUpdate>(op.types, spec, op.table.catalog, op.estimated_cardinality,
		                                           /* overlay */ -1, std::move(set_columns), std::move(set_refs));
	}
	// `kNotWritable`: DuckLake's DROP/RENAME batch still issues `SET
	// end_snapshot` against unmodeled tables (see MoraineMetadataVoidUpdate),
	// translatable as a no-op since such a table can never have a live row.
	// Anything else against an unwritable table stays rejected.
	if (set_columns.size() == 1 && std::string(spec.columns[set_columns[0]].name) == "end_snapshot") {
		return planner.Make<MoraineMetadataVoidUpdate>(op.types, spec, op.table.catalog, op.estimated_cardinality);
	}
	throw duckdb::NotImplementedException("moraine: UPDATE is not supported on \"%s\"", spec.name);
}

duckdb::PhysicalOperator &PlanMetadataDelete(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalDelete &op,
                                             const MetadataTableSpec &spec) {
	if (op.return_chunk) {
		throw duckdb::NotImplementedException("moraine: DELETE ... RETURNING is not supported on \"%s\"",
		                                      op.table.name);
	}
	if (spec.delete_key_columns.empty()) {
		if (spec.write_table_kind == kVoidInsertable) {
			// `ducklake_inlined_data_tables`: the one registry-backed
			// table, deleted for real by the expiry cascade.
			if (op.expressions.size() != 1) {
				throw duckdb::InternalException("moraine: expected exactly one row-id expression for DELETE on \"%s\"",
				                                spec.name);
			}
			auto &bound_ref = op.expressions[0]->Cast<duckdb::BoundReferenceExpression>();
			return planner.Make<MoraineInlineRegistryDelete>(op.types, spec, op.table.catalog, op.estimated_cardinality,
			                                                 bound_ref.index);
		}
		return planner.Make<MoraineMetadataVoidDelete>(op.types, spec, op.table.catalog, op.estimated_cardinality);
	}
	// Pinned layout: expressions[0] is the bound reference locating the
	// row-id column in the child chunk (a single rowid — the base
	// TableCatalogEntry row identity this catalog inherits).
	if (op.expressions.size() != 1) {
		throw duckdb::InternalException("moraine: expected exactly one row-id expression for DELETE on \"%s\"",
		                                spec.name);
	}
	auto &bound_ref = op.expressions[0]->Cast<duckdb::BoundReferenceExpression>();
	return planner.Make<MoraineMetadataDelete>(op.types, spec, op.table.catalog, op.estimated_cardinality,
	                                           bound_ref.index);
}

} // namespace moraine_duckdb
