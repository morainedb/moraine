// moraine_rows_at(catalog, schema, table, rows): reads already-located
// `rows` — a LIST of STRUCT(row_id BIGINT, data_file_id UBIGINT), as an
// index lookup reports them — back whole at the current transaction's
// snapshot, with no scan. File rows come from exactly their pages; inlined
// rows decode from their chunk. The output is the table's current columns
// followed by `row_id` and `data_file_id`.
#include <unordered_map>

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "catalog.hpp"
#include "moraine_abi.h"
#include "owned_array.hpp"
#include "rows_at.hpp"
#include "transaction_manager.hpp"

namespace moraine_duckdb {

PinnedSnapshot PinTransactionSnapshot(duckdb::ClientContext &context, const std::string &catalog_name,
                                      const std::string &schema_name, const std::string &table_name) {
	// DuckLake loads its transaction's snapshot lazily, on its own metadata
	// connection, the first time the lake is touched. Touching the table
	// first pins that snapshot before the metadata catalog's own view is
	// taken below, so the view can only be the same or newer; DuckLake
	// refuses positions resolved against a newer one.
	auto lake = duckdb::Catalog::GetCatalogEntry(context, catalog_name);
	if (lake && lake->GetCatalogType() != "moraine") {
		lake->GetEntry(context, duckdb::CatalogType::TABLE_ENTRY, schema_name, table_name,
		               duckdb::OnEntryNotFound::THROW_EXCEPTION);
	}

	auto &catalog = ResolveMoraineCatalog(context, catalog_name);
	auto transaction = catalog.GetCatalogTransaction(context);
	if (!transaction.transaction) {
		throw duckdb::InternalException("moraine: no active transaction on the metadata catalog");
	}
	PinnedSnapshot pinned;
	pinned.catalog = &catalog;
	pinned.snapshot = transaction.transaction->Cast<MoraineTransaction>().Snapshot();
	MoraineError error {};
	if (moraine_snapshot_id(pinned.snapshot, &pinned.snapshot_id, &error) != MORAINE_OK) {
		ThrowMoraineError(error);
	}
	return pinned;
}

std::vector<MorainePositionPair> ParseLocatedPairs(const duckdb::Value &rows, const char *caller) {
	std::vector<MorainePositionPair> pairs;
	if (rows.IsNull()) {
		throw duckdb::InvalidInputException("%s: `rows` cannot be NULL", caller);
	}

	// Field names are resolved by name, not position: a caller naming
	// `data_file_id` before `row_id` must still resolve to the right row.
	auto element_type = duckdb::ListType::GetChildType(rows.type());
	if (element_type.id() != duckdb::LogicalTypeId::STRUCT) {
		throw duckdb::InvalidInputException("%s: each `rows` entry must be STRUCT(row_id BIGINT, data_file_id UBIGINT)",
		                                    caller);
	}
	auto &children = duckdb::StructType::GetChildTypes(element_type);
	duckdb::optional_idx row_id_index;
	duckdb::optional_idx data_file_id_index;
	for (duckdb::idx_t i = 0; i < children.size(); i++) {
		auto name = duckdb::StringUtil::Lower(children[i].first);
		if (name == "row_id") {
			row_id_index = i;
		} else if (name == "data_file_id") {
			data_file_id_index = i;
		}
	}
	if (children.size() != 2 || !row_id_index.IsValid() || !data_file_id_index.IsValid()) {
		throw duckdb::InvalidInputException(
		    "%s: each `rows` entry must be exactly STRUCT(row_id BIGINT, data_file_id UBIGINT)", caller);
	}

	for (auto &row : duckdb::ListValue::GetChildren(rows)) {
		if (row.IsNull()) {
			throw duckdb::InvalidInputException("%s: a `rows` entry cannot be NULL", caller);
		}
		auto &fields = duckdb::StructValue::GetChildren(row);
		auto &row_id_value = fields[row_id_index.GetIndex()];
		auto &data_file_id_value = fields[data_file_id_index.GetIndex()];
		if (row_id_value.IsNull()) {
			throw duckdb::InvalidInputException("%s: `row_id` cannot be NULL", caller);
		}
		MorainePositionPair pair {};
		pair.row_id = row_id_value.DefaultCastAs(duckdb::LogicalType::UBIGINT).GetValue<uint64_t>();
		if (!data_file_id_value.IsNull()) {
			pair.has_data_file_id = true;
			pair.data_file_id = data_file_id_value.DefaultCastAs(duckdb::LogicalType::UBIGINT).GetValue<uint64_t>();
		}
		pairs.push_back(pair);
	}

	return pairs;
}

LocatedArguments ResolveLocatedArguments(duckdb::ClientContext &context, const std::string &catalog_name,
                                         const std::string &schema_name, const std::string &table_name,
                                         const duckdb::Value &rows, const char *caller) {
	auto pairs = ParseLocatedPairs(rows, caller);
	auto pinned = PinTransactionSnapshot(context, catalog_name, schema_name, table_name);
	OwnedArray<MoraineLocatedFile> files(moraine_locate_row_positions_free_files);
	OwnedArray<uint64_t> inlined(moraine_locate_row_positions_free_inlined);
	char *raw_write_directory = nullptr;
	MoraineError error {};
	auto code = moraine_locate_row_positions(pinned.catalog->Handle(), pinned.snapshot, schema_name.c_str(),
	                                         table_name.c_str(), pairs.data(), pairs.size(), files.OutItems(),
	                                         files.OutLen(), inlined.OutItems(), inlined.OutLen(),
	                                         &raw_write_directory, moraine_shim_is_interrupted, &context, &error);
	if (code != MORAINE_OK) {
		ThrowMoraineError(error);
	}
	// DuckLake composes the delete file's directory itself.
	moraine_string_free(raw_write_directory);

	using duckdb::LogicalType;
	using duckdb::Value;
	duckdb::child_list_t<LogicalType> file_fields {{"data_file_id", LogicalType::UBIGINT},
	                                               {"positions", LogicalType::LIST(LogicalType::UBIGINT)}};
	auto file_type = LogicalType::STRUCT(file_fields);
	duckdb::vector<Value> file_values;
	file_values.reserve(files.size());
	for (auto &file : files) {
		duckdb::vector<Value> positions;
		positions.reserve(file.positions_len);
		for (size_t i = 0; i < file.positions_len; i++) {
			positions.push_back(Value::UBIGINT(file.positions[i]));
		}
		duckdb::child_list_t<Value> fields {
		    {"data_file_id", Value::UBIGINT(file.data_file_id)},
		    {"positions", Value::LIST(LogicalType::UBIGINT, std::move(positions))}};
		file_values.push_back(Value::STRUCT(std::move(fields)));
	}
	duckdb::vector<Value> inlined_values;
	inlined_values.reserve(inlined.size());
	for (auto row_id : inlined) {
		inlined_values.push_back(Value::UBIGINT(row_id));
	}

	LocatedArguments arguments;
	arguments.files = Value::LIST(file_type, std::move(file_values));
	arguments.inlined_rows = Value::LIST(LogicalType::UBIGINT, std::move(inlined_values));
	arguments.snapshot_id = pinned.snapshot_id;
	return arguments;
}

void TableColumns(MoraineSnapshotHandle *snapshot, const std::string &schema_name, const std::string &table_name,
                  duckdb::vector<duckdb::LogicalType> &types, duckdb::vector<std::string> &names) {
	uint64_t table_id = 0;
	MoraineError error {};
	if (moraine_snapshot_resolve_table(snapshot, schema_name.c_str(), table_name.c_str(), &table_id, &error) !=
	    MORAINE_OK) {
		ThrowMoraineError(error);
	}
	OwnedArray<MoraineColumnDesc> columns(moraine_snapshot_columns_of_free);
	if (moraine_snapshot_columns_of(snapshot, table_id, columns.OutItems(), columns.OutLen(), &error) != MORAINE_OK) {
		ThrowMoraineError(error);
	}
	std::unordered_map<uint64_t, const MoraineColumnDesc *> by_id;
	std::unordered_map<uint64_t, std::vector<uint64_t>> children_of;
	for (auto &column : columns) {
		by_id.emplace(column.id, &column);
		if (column.has_parent_column) {
			children_of[column.parent_column].push_back(column.id);
		}
	}
	for (auto &column : columns) {
		if (column.has_parent_column) {
			continue;
		}
		types.push_back(BuildColumnType(column, by_id, children_of));
		names.push_back(column.name);
	}
}

namespace {

struct RowsAtBindData : public duckdb::FunctionData {
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	std::string rows_repr;
	duckdb::vector<duckdb::LogicalType> types;
	// Each batch is one Arrow IPC stream, decoded during execution.
	std::vector<std::vector<uint8_t>> batches;

	// Resolved rows belong to one execution.
	bool SupportStatementCache() const override {
		return false;
	}
	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<RowsAtBindData>();
		*result = *this;
		return result;
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<RowsAtBindData>();
		return catalog_name == other.catalog_name && schema_name == other.schema_name &&
		       table_name == other.table_name && rows_repr == other.rows_repr;
	}
};

// Resolves and reads the rows at bind, as the index lookups do: the rows
// belong to the binding transaction's snapshot, and a prepared call binds
// again for each execution.
duckdb::unique_ptr<duckdb::FunctionData> RowsAtBind(duckdb::ClientContext &context,
                                                    duckdb::TableFunctionBindInput &input,
                                                    duckdb::vector<duckdb::LogicalType> &return_types,
                                                    duckdb::vector<std::string> &names) {
	auto bind_data = duckdb::make_uniq<RowsAtBindData>();
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	bind_data->rows_repr = input.inputs[3].ToString();
	auto pairs = ParseLocatedPairs(input.inputs[3], "moraine_rows_at");

	auto pinned =
	    PinTransactionSnapshot(context, bind_data->catalog_name, bind_data->schema_name, bind_data->table_name);
	auto snapshot = pinned.snapshot;
	TableColumns(snapshot, bind_data->schema_name, bind_data->table_name, return_types, names);
	return_types.push_back(duckdb::LogicalType::UBIGINT);
	names.push_back("row_id");
	return_types.push_back(duckdb::LogicalType::UBIGINT);
	names.push_back("data_file_id");
	bind_data->types = return_types;

	OwnedArray<MoraineRowBatch> batches(moraine_rows_at_free);
	MoraineError error {};
	auto code = moraine_rows_at(pinned.catalog->Handle(), snapshot, bind_data->schema_name.c_str(),
	                            bind_data->table_name.c_str(), pairs.data(), pairs.size(), batches.OutItems(),
	                            batches.OutLen(), moraine_shim_is_interrupted, &context, &error);
	if (code != MORAINE_OK) {
		ThrowMoraineError(error);
	}
	for (auto &batch : batches) {
		bind_data->batches.emplace_back(batch.data, batch.data + batch.len);
	}
	input.binder->SetAlwaysRequireRebind();
	return bind_data;
}

struct RowsAtGlobalState : public duckdb::GlobalTableFunctionState {
	size_t next_batch = 0;
	std::vector<duckdb::unique_ptr<duckdb::DataChunk>> pending;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> RowsAtInit(duckdb::ClientContext &,
                                                                duckdb::TableFunctionInitInput &) {
	return duckdb::make_uniq<RowsAtGlobalState>();
}

// Decodes one IPC stream into chunks typed as its own schema says. Each
// batch is self-describing because a file written under an older schema
// may carry a narrower type than the table does now; the caller casts.
std::vector<duckdb::unique_ptr<duckdb::DataChunk>> DecodeBatch(duckdb::ClientContext &context,
                                                               const std::vector<uint8_t> &ipc) {
	ArrowSchema c_schema;
	ArrowArray c_array;
	MoraineError error {};
	if (moraine_arrow_decode_stream(ipc.data(), ipc.size(), &c_schema, &c_array, &error) != MORAINE_OK) {
		ThrowMoraineError(error);
	}

	duckdb::ArrowTableSchema arrow_table;
	duckdb::ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, c_schema);
	auto &columns = arrow_table.GetColumns();
	duckdb::vector<duckdb::LogicalType> types;
	for (duckdb::idx_t i = 0; i < columns.size(); i++) {
		types.push_back(columns.at(i)->GetDuckType());
	}

	auto chunk_wrapper = duckdb::make_uniq<duckdb::ArrowArrayWrapper>();
	chunk_wrapper->arrow_array = c_array;
	auto total = static_cast<duckdb::idx_t>(chunk_wrapper->arrow_array.length);
	duckdb::ArrowScanLocalState scan_state(std::move(chunk_wrapper), context);
	for (duckdb::idx_t i = 0; i < types.size(); i++) {
		scan_state.column_ids.push_back(i);
	}

	std::vector<duckdb::unique_ptr<duckdb::DataChunk>> pieces;
	while (scan_state.chunk_offset < total) {
		auto size = std::min<duckdb::idx_t>(total - scan_state.chunk_offset, STANDARD_VECTOR_SIZE);
		auto out = duckdb::make_uniq<duckdb::DataChunk>();
		out->Initialize(context, types);
		out->SetCardinality(size);
		duckdb::ArrowTableFunction::ArrowToDuckDB(scan_state, columns, *out, /* arrow_scan_is_projected */ false);
		pieces.push_back(std::move(out));
		scan_state.chunk_offset += size;
	}
	if (c_schema.release) {
		c_schema.release(&c_schema);
	}
	return pieces;
}

void RowsAtImpl(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RowsAtBindData>();
	auto &state = data.global_state->Cast<RowsAtGlobalState>();
	while (state.pending.empty()) {
		if (state.next_batch >= bind_data.batches.size()) {
			output.SetCardinality(0);
			return;
		}
		auto pieces = DecodeBatch(context, bind_data.batches[state.next_batch++]);
		// Emitted in order: the last decoded piece goes out last.
		for (auto piece = pieces.rbegin(); piece != pieces.rend(); ++piece) {
			state.pending.push_back(std::move(*piece));
		}
	}

	auto piece = std::move(state.pending.back());
	state.pending.pop_back();
	if (piece->ColumnCount() != output.ColumnCount()) {
		throw duckdb::InternalException("moraine_rows_at: batch has %llu columns, expected %llu",
		                                static_cast<unsigned long long>(piece->ColumnCount()),
		                                static_cast<unsigned long long>(output.ColumnCount()));
	}
	for (duckdb::idx_t i = 0; i < output.ColumnCount(); i++) {
		if (piece->data[i].GetType() == bind_data.types[i]) {
			output.data[i].Reference(piece->data[i]);
		} else {
			duckdb::VectorOperations::DefaultCast(piece->data[i], output.data[i], piece->size());
		}
	}
	output.SetCardinality(piece->size());
}

} // namespace

void RegisterMoraineRowsAtFunction(duckdb::ExtensionLoader &loader) {
	using duckdb::LogicalType;

	// (catalog, schema, table, rows), where `rows` is a LIST of
	// STRUCT(row_id BIGINT, data_file_id UBIGINT), resolved by field name.
	duckdb::TableFunction rows_at(
	    "moraine_rows_at",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	    RowsAtImpl, RowsAtBind, RowsAtInit);
	loader.RegisterFunction(rows_at);
}

} // namespace moraine_duckdb
