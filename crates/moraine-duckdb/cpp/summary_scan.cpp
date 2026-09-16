#include "summary_scan.hpp"

#include "catalog.hpp"
#include "index_functions.hpp"
#include "owned_array.hpp"
#include "rows_at.hpp"
#include "transaction_manager.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_delete_filter.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include <chrono>
#include <map>
#include <set>

namespace moraine_duckdb {
namespace {

constexpr auto FILE_ID = duckdb::DuckLakeMultiFileReader::COLUMN_IDENTIFIER_DATA_FILE_ID;
using ScanClock = std::chrono::steady_clock;
double Milliseconds(ScanClock::time_point start) {
	return std::chrono::duration<double, std::milli>(ScanClock::now() - start).count();
}

bool SupportedType(const duckdb::LogicalType &type) {
	using duckdb::LogicalTypeId;
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return true;
	default:
		return false;
	}
}

struct SummaryScanBindData : duckdb::FunctionData {
	std::string lake, schema, table;
	uint64_t snapshot_id;
	uint64_t table_id = 0;
	double costing_ms = 0;
	std::vector<MorainePositionPair> pairs;
	// DuckLake's path for every data file the pairs name, keyed by file id,
	// and the inlined tables the scan covers: the keys the transaction's own
	// deletions are recorded under.
	std::map<uint64_t, std::string> file_paths;
	std::vector<std::string> inlined_tables;
	duckdb::vector<std::string> names;
	duckdb::vector<duckdb::LogicalType> types;
	duckdb::virtual_column_map_t virtual_columns;

	bool SupportStatementCache() const override {
		return false;
	}
	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		return duckdb::make_uniq<SummaryScanBindData>(*this);
	}
	bool Equals(const duckdb::FunctionData &) const override {
		return false;
	}
};

struct SummaryScanState : duckdb::GlobalTableFunctionState {
	MoraineRowScan *scan = nullptr;
	duckdb::vector<duckdb::idx_t> decoded_columns, projection;
	duckdb::vector<duckdb::LogicalType> types;
	duckdb::unique_ptr<duckdb::Expression> predicate;
	duckdb::unique_ptr<duckdb::ExpressionExecutor> executor;
	std::vector<duckdb::unique_ptr<duckdb::DataChunk>> pending;
	double positioning_ms = 0, batch_ms = 0, conversion_ms = 0;
	~SummaryScanState() override {
		moraine_row_scan_free(scan);
	}
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

// The transaction's own deletions over what the pairs name, which the pinned
// snapshot cannot know: positions per file (its pending delete file and
// inlined file deletions), and the pairs left once deleted inlined rows are
// dropped. Gathered per execution, so later deletes in the transaction count.
struct LocalExclusions {
	std::vector<std::pair<uint64_t, std::vector<uint64_t>>> positions;
	std::vector<MoraineExcludedPositions> entries;
	std::vector<MorainePositionPair> pairs;
};

LocalExclusions CollectLocalExclusions(duckdb::ClientContext &context, duckdb::DuckLakeTransaction &transaction,
                                       const SummaryScanBindData &bound) {
	LocalExclusions result;
	if (!transaction.ChangesMade()) {
		result.pairs = bound.pairs;
		return result;
	}
	duckdb::TableIndex table_id(bound.table_id);
	std::set<uint64_t> file_ids;
	for (auto &pair : bound.pairs) {
		if (pair.has_data_file_id) {
			file_ids.insert(pair.data_file_id);
		}
	}
	for (auto file_id : file_ids) {
		std::set<duckdb::idx_t> deleted;
		auto path = bound.file_paths.find(file_id);
		if (path != bound.file_paths.end() && transaction.HasLocalDeleteForFile(table_id, path->second)) {
			duckdb::DuckLakeFileData pending;
			transaction.GetLocalDeleteForFile(table_id, path->second, pending);
			auto scanned = duckdb::DuckLakeDeleteFilter::ScanDeleteFile(context, pending);
			deleted.insert(scanned.deleted_rows.begin(), scanned.deleted_rows.end());
		}
		transaction.GetLocalInlinedFileDeletesForFile(table_id, file_id, deleted);
		if (!deleted.empty()) {
			result.positions.emplace_back(file_id, std::vector<uint64_t>(deleted.begin(), deleted.end()));
		}
	}
	for (auto &entry : result.positions) {
		result.entries.push_back({entry.first, entry.second.data(), entry.second.size()});
	}
	std::set<duckdb::idx_t> deleted_inlined;
	for (auto &name : bound.inlined_tables) {
		auto deletes = transaction.GetInlinedDeletes(table_id, name);
		if (deletes) {
			deleted_inlined.insert(deletes->rows.begin(), deletes->rows.end());
		}
	}
	for (auto &pair : bound.pairs) {
		if (!pair.has_data_file_id && deleted_inlined.count(pair.row_id)) {
			continue;
		}
		result.pairs.push_back(pair);
	}
	return result;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitSummaryScan(duckdb::ClientContext &context,
                                                                     duckdb::TableFunctionInitInput &input) {
	auto &bound = input.bind_data->Cast<SummaryScanBindData>();
	auto started = ScanClock::now();
	auto state = duckdb::make_uniq<SummaryScanState>();
	duckdb::vector<const char *> requested;
	for (auto column : input.column_ids) {
		if (column == duckdb::COLUMN_IDENTIFIER_ROW_ID || column == FILE_ID) {
			state->decoded_columns.push_back(0);
			state->types.push_back(column == FILE_ID ? duckdb::LogicalType::UBIGINT : duckdb::LogicalType::BIGINT);
		} else {
			state->decoded_columns.push_back(requested.size());
			requested.push_back(bound.names[column].c_str());
			state->types.push_back(bound.types[column]);
		}
	}
	for (duckdb::idx_t i = 0; i < input.column_ids.size(); i++) {
		if (input.column_ids[i] == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
			state->decoded_columns[i] = requested.size();
		}
		if (input.column_ids[i] == FILE_ID) {
			state->decoded_columns[i] = requested.size() + 1;
		}
	}
	state->projection = input.projection_ids;
	if (state->projection.empty()) {
		for (duckdb::idx_t i = 0; i < input.column_ids.size(); i++) {
			state->projection.push_back(i);
		}
	}
	if (input.filters && !input.filters->filters.empty()) {
		auto conjunction =
		    duckdb::make_uniq<duckdb::BoundConjunctionExpression>(duckdb::ExpressionType::CONJUNCTION_AND);
		for (auto &filter : input.filters->filters) {
			duckdb::BoundReferenceExpression column(state->types[filter.first], filter.first);
			conjunction->children.push_back(filter.second->ToExpression(column));
		}
		state->predicate = std::move(conjunction);
		state->executor = duckdb::make_uniq<duckdb::ExpressionExecutor>(context, *state->predicate);
	}
	auto &lake = duckdb::Catalog::GetCatalog(context, bound.lake).Cast<duckdb::DuckLakeCatalog>();
	auto &transaction = duckdb::DuckLakeTransaction::Get(context, lake);
	auto &catalog = ResolveMoraineCatalog(context, lake.MetadataDatabaseName());
	auto metadata = catalog.GetCatalogTransaction(*transaction.GetConnection().context);
	auto snapshot = metadata.transaction->Cast<MoraineTransaction>().Snapshot();
	auto handle = moraine_snapshot_read_handle(snapshot);
	uint64_t snapshot_id = 0;
	MoraineError error {};
	if (moraine_snapshot_id(snapshot, &snapshot_id, &error) != MORAINE_OK) {
		ThrowMoraineError(error);
	}
	if (!handle || snapshot_id != bound.snapshot_id) {
		throw duckdb::InvalidInputException("moraine: selective scan snapshot changed after binding");
	}
	auto exclusions = CollectLocalExclusions(context, transaction, bound);
	auto code = moraine_row_scan_open(handle, snapshot, bound.schema.c_str(), bound.table.c_str(),
	                                  exclusions.pairs.data(), exclusions.pairs.size(), requested.data(),
	                                  requested.size(), exclusions.entries.data(), exclusions.entries.size(),
	                                  &state->scan, moraine_shim_is_interrupted, &context, &error);
	if (code != MORAINE_OK) {
		ThrowMoraineError(error);
	}
	duckdb::Value parallelism;
	context.TryGetCurrentSetting("moraine_summary_scan_threads", parallelism);
	auto maximum = std::min<uint64_t>(parallelism.GetValue<uint64_t>(),
	                                  duckdb::DatabaseInstance::GetDatabase(context).NumberOfThreads());
	if (moraine_row_scan_parallelism(state->scan, maximum, &error) != MORAINE_OK) {
		ThrowMoraineError(error);
	}
	WriteMoraineLog(duckdb::DatabaseInstance::GetDatabase(context), duckdb::LogLevel::LOG_DEBUG,
	                "summary scan opened pairs=" + std::to_string(bound.pairs.size()) +
	                    " columns=" + std::to_string(requested.size()));
	state->positioning_ms = Milliseconds(started);
	return std::move(state);
}

void ScanSummary(duckdb::ClientContext &context, duckdb::TableFunctionInput &input, duckdb::DataChunk &output) {
	auto &state = input.global_state->Cast<SummaryScanState>();
	while (true) {
		if (state.pending.empty()) {
			auto started = ScanClock::now();
			duckdb::ArrowSchemaWrapper schema;
			duckdb::ArrowArrayWrapper array;
			bool has_batch = false;
			MoraineError error {};
			if (moraine_row_scan_next_arrow(state.scan, &schema.arrow_schema, &array.arrow_array, &has_batch,
			                                moraine_shim_is_interrupted, &context, &error) != MORAINE_OK) {
				ThrowMoraineError(error);
			}
			state.batch_ms += Milliseconds(started);
			if (!has_batch) {
				output.SetCardinality(0);
				return;
			}
			started = ScanClock::now();
			auto pieces = ImportLocatedBatch(context, schema.arrow_schema, array.arrow_array);
			state.conversion_ms += Milliseconds(started);
			for (auto piece = pieces.rbegin(); piece != pieces.rend(); ++piece) {
				state.pending.push_back(std::move(*piece));
			}
			if (state.pending.empty()) {
				continue;
			}
		}
		auto piece = std::move(state.pending.back());
		state.pending.pop_back();
		duckdb::DataChunk projected;
		projected.Initialize(context, state.types);
		projected.SetCardinality(piece->size());
		for (duckdb::idx_t i = 0; i < state.types.size(); i++) {
			auto &source = piece->data[state.decoded_columns[i]];
			if (source.GetType() == state.types[i]) {
				projected.data[i].Reference(source);
			} else {
				duckdb::VectorOperations::DefaultCast(source, projected.data[i], piece->size());
			}
		}
		if (state.executor) {
			duckdb::SelectionVector selection(STANDARD_VECTOR_SIZE);
			auto count = state.executor->SelectExpression(projected, selection);
			projected.Slice(selection, count);
		}
		if (!projected.size()) {
			continue;
		}
		for (duckdb::idx_t i = 0; i < state.projection.size(); i++) {
			output.data[i].Reference(projected.data[state.projection[i]]);
		}
		output.SetCardinality(projected.size());
		return;
	}
}

} // namespace

bool UseSummaryScan(duckdb::ClientContext &context, duckdb::LogicalGet &scan, const duckdb::LogicalGet &index,
                    const std::vector<MoraineRowId> &rows) {
	if (scan.function.name != "ducklake_scan" || !scan.function.function_info || scan.row_group_order_options ||
	    scan.extra_info.sample_options) {
		return false;
	}
	auto &info = scan.function.function_info->Cast<duckdb::DuckLakeFunctionInfo>();
	auto &lake = info.table.catalog.Cast<duckdb::DuckLakeCatalog>();
	auto transaction = info.GetTransaction();
	auto &files = scan.bind_data->Cast<duckdb::MultiFileBindData>().file_list->Cast<duckdb::DuckLakeMultiFileList>();
	auto statistics = info.table.GetTableStats(context);
	if (statistics && rows.size() > 4096 && rows.size() > statistics->record_count / 4) {
		return false;
	}
	if (info.scan_type != duckdb::DuckLakeScanType::SCAN_TABLE || lake.CatalogSnapshot() ||
	    info.snapshot.snapshot_id != transaction->GetSnapshot().snapshot_id) {
		return false;
	}
	std::string catalog_name, schema, table;
	if (!IndexReadTable(index.function, index.bind_data.get(), catalog_name, schema, table) ||
	    schema != info.table.schema.name || table != info.table.name) {
		return false;
	}
	auto catalog = duckdb::Catalog::GetCatalogEntry(context, catalog_name);
	if (!catalog || (catalog->GetName() != lake.GetName() && catalog->GetName() != lake.MetadataDatabaseName())) {
		return false;
	}
	auto &metadata = ResolveMoraineCatalog(context, lake.MetadataDatabaseName());
	auto metadata_tx = metadata.GetCatalogTransaction(*transaction->GetConnection().context);
	if (!moraine_snapshot_read_handle(metadata_tx.transaction->Cast<MoraineTransaction>().Snapshot())) {
		return false;
	}
	for (auto &column : scan.GetColumnIds()) {
		auto id = column.GetPrimaryIndex();
		if (column.HasChildren() ||
		    (id >= scan.returned_types.size() && id != duckdb::COLUMN_IDENTIFIER_ROW_ID && id != FILE_ID)) {
			return false;
		}
		if (id < scan.returned_types.size() && !SupportedType(scan.returned_types[id])) {
			return false;
		}
	}
	auto bound = duckdb::make_uniq<SummaryScanBindData>();
	bound->lake = lake.GetName();
	bound->schema = schema;
	bound->table = table;
	bound->snapshot_id = info.snapshot.snapshot_id;
	bound->table_id = info.table.GetTableId().index;
	bound->names = scan.names;
	bound->types = scan.returned_types;
	bound->virtual_columns = scan.virtual_columns;
	for (auto &row : rows) {
		bound->pairs.push_back({row.value, row.data_file_id, row.has_data_file_id});
	}
	// Transaction-local rows are never index hits, so the scan may skip the
	// files and inlined data the transaction added; its deletions are
	// subtracted at execution, which needs every named file's DuckLake path.
	for (auto &entry : files.GetFiles()) {
		if (entry.data_type == duckdb::DuckLakeDataType::INLINED_DATA) {
			bound->inlined_tables.push_back(entry.file.path);
		} else if (entry.data_type == duckdb::DuckLakeDataType::DATA_FILE && entry.file_id.IsValid()) {
			bound->file_paths.emplace(entry.file_id.index, entry.file.path);
		}
	}
	for (auto &pair : bound->pairs) {
		if (pair.has_data_file_id && !bound->file_paths.count(pair.data_file_id)) {
			return false;
		}
	}
	std::vector<MorainePositionPair> file_pairs;
	for (auto &pair : bound->pairs) {
		if (pair.has_data_file_id) {
			file_pairs.push_back(pair);
		}
	}
	if (file_pairs.size() > 16) {
		auto started = ScanClock::now();
		SummaryScanState estimate;
		duckdb::vector<const char *> columns;
		for (auto &column : scan.GetColumnIds()) {
			auto id = column.GetPrimaryIndex();
			if (id < scan.names.size()) {
				columns.push_back(scan.names[id].c_str());
			}
		}
		auto snapshot = metadata_tx.transaction->Cast<MoraineTransaction>().Snapshot();
		MoraineError error {};
		// Coverage is estimated over the committed positions alone: the
		// transaction's own deletions only remove rows, so they are gathered
		// once, at execution.
		auto code = moraine_row_scan_open(moraine_snapshot_read_handle(snapshot), snapshot, schema.c_str(),
		                                  table.c_str(), file_pairs.data(), file_pairs.size(), columns.data(),
		                                  columns.size(), nullptr, 0, &estimate.scan, moraine_shim_is_interrupted,
		                                  &context, &error);
		if (code != MORAINE_OK) {
			ThrowMoraineError(error);
		}
		bool selective = false;
		code = moraine_row_scan_is_selective(estimate.scan, &selective, moraine_shim_is_interrupted, &context, &error);
		if (code != MORAINE_OK) {
			ThrowMoraineError(error);
		}
		bound->costing_ms = Milliseconds(started);
		WriteMoraineLog(duckdb::DatabaseInstance::GetDatabase(context), duckdb::LogLevel::LOG_DEBUG,
		                "summary scan costing_ms=" + std::to_string(bound->costing_ms) +
		                    " selected=" + std::to_string(selective));
		if (!selective) {
			return false;
		}
	}
	duckdb::TableFunction function("moraine_summary_scan", {}, ScanSummary, nullptr, InitSummaryScan);
	function.projection_pushdown = true;
	function.filter_pushdown = true;
	function.filter_prune = true;
	function.verify_serialization = false;
	function.dynamic_to_string = [](duckdb::TableFunctionDynamicToStringInput &input) {
		duckdb::InsertionOrderPreservingMap<std::string> result;
		if (input.bind_data) {
			result["Coverage costing ms"] = std::to_string(input.bind_data->Cast<SummaryScanBindData>().costing_ms);
		}
		if (input.global_state) {
			auto &state = input.global_state->Cast<SummaryScanState>();
			auto metrics = moraine_row_scan_metrics(state.scan);
			result["Total Files Read"] = std::to_string(moraine_row_scan_files_read(state.scan));
			result["Positioning ms"] = std::to_string(state.positioning_ms);
			result["Read and decode ms"] = std::to_string(state.batch_ms);
			result["Arrow import ms"] = std::to_string(state.conversion_ms);
			result["Data bytes fetched"] = std::to_string(metrics.bytes_read);
			result["Data ranges fetched"] = std::to_string(metrics.ranges_read);
			result["Peak read workers"] = std::to_string(metrics.peak_workers);
			result["Decode active ms"] = std::to_string(metrics.decode_seconds * 1000);
			result["Data fetch ms"] = std::to_string(metrics.fetch_seconds * 1000);
		}
		return result;
	};
	function.get_virtual_columns = [](duckdb::ClientContext &, duckdb::optional_ptr<duckdb::FunctionData> data) {
		return data->Cast<SummaryScanBindData>().virtual_columns;
	};
	scan.bind_data = std::move(bound);
	scan.function = std::move(function);
	scan.extra_info.file_filters.clear();
	return true;
}

} // namespace moraine_duckdb
