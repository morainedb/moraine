#include <atomic>
#include <memory>
#include "summary_scan.hpp"

#include "duckdb/main/profiler/profiling_node.hpp"
#include "catalog.hpp"
#include "index_functions.hpp"
#include "metadata_manager.hpp"
#include "owned_array.hpp"
#include "transaction_manager.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_delete_filter.hpp"
#include "storage/ducklake_inlined_data.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/index.hpp"
#include <algorithm>
#include <chrono>
#include <map>
#include <set>

namespace moraine_duckdb {

namespace {

// The name DuckLake's file list gives the transaction's inlined rows, which
// keys their deletes.
constexpr const char *LOCAL_INLINED_ENTRY = "__ducklake_inlined_transaction_local_data";

// Imports an Arrow batch the row scan exported as DuckDB chunks. Consumes
// both structs, including when the import throws.
std::vector<duckdb::unique_ptr<duckdb::DataChunk>> ImportLocatedBatch(duckdb::ClientContext &context,
                                                                      ArrowSchema &c_schema, ArrowArray &c_array) {
	duckdb::ArrowSchemaWrapper schema;
	schema.arrow_schema = c_schema;
	c_schema.release = nullptr;
	auto chunk_wrapper = duckdb::make_uniq<duckdb::ArrowArrayWrapper>();
	chunk_wrapper->arrow_array = c_array;
	c_array.release = nullptr;

	duckdb::ArrowTableSchema arrow_table;
	duckdb::ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, schema.arrow_schema);
	auto &columns = arrow_table.GetColumns();
	duckdb::vector<duckdb::LogicalType> types;
	for (duckdb::idx_t i = 0; i < columns.size(); i++) {
		types.push_back(columns.at(i)->GetDuckType());
	}
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
	return pieces;
}

} // namespace

namespace {

using ScanClock = std::chrono::steady_clock;
double Milliseconds(ScanClock::time_point start) {
	return std::chrono::duration<double, std::milli>(ScanClock::now() - start).count();
}

// Optional filters are pruning hints; their exact predicates remain above
// the scan. Re-evaluating a wide IN hint here repeats the hash join's work.
duckdb::unique_ptr<duckdb::Expression> RequiredPredicate(const duckdb::Expression &expression) {
	if (duckdb::ExpressionFilter::IsRootOptionalExpression(expression)) {
		return nullptr;
	}
	if (expression.GetExpressionType() != duckdb::ExpressionType::CONJUNCTION_AND) {
		return expression.Copy();
	}
	auto result = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(duckdb::ExpressionType::CONJUNCTION_AND);
	for (auto &child : expression.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
		auto required = RequiredPredicate(*child);
		if (required) {
			result->GetChildrenMutable().push_back(std::move(required));
		}
	}
	if (result->GetChildren().empty()) {
		return nullptr;
	}
	return std::move(result);
}

// Whether `type` is one the located reader cannot decode as DuckLake's scan
// does, or nests one. parquet-rs types a Parquet INTERVAL as a day-time
// interval and drops its months; a VARIANT and a GEOMETRY come back as the
// shredded struct and the WKB the file holds rather than the value.
bool HoldsUndecodableType(const duckdb::LogicalType &type) {
	switch (type.id()) {
	case duckdb::LogicalTypeId::INTERVAL:
	case duckdb::LogicalTypeId::VARIANT:
	case duckdb::LogicalTypeId::GEOMETRY:
		return true;
	case duckdb::LogicalTypeId::LIST:
	case duckdb::LogicalTypeId::ARRAY:
		return HoldsUndecodableType(duckdb::ListType::GetChildType(type));
	case duckdb::LogicalTypeId::MAP:
		return HoldsUndecodableType(duckdb::MapType::KeyType(type)) ||
		       HoldsUndecodableType(duckdb::MapType::ValueType(type));
	case duckdb::LogicalTypeId::STRUCT:
	case duckdb::LogicalTypeId::UNION:
		for (auto &child : duckdb::StructType::GetChildTypes(type)) {
			if (HoldsUndecodableType(child.second)) {
				return true;
			}
		}
		return false;
	default:
		return false;
	}
}

// Derives from DuckLake's own scan bind data, carrying the scan's file list
// and reader: DuckLake's DELETE and UPDATE find the scan beneath them by its
// `file_row_number` column and read their file map from that bind data.
struct SummaryScanBindData : duckdb::MultiFileBindData {
	std::string lake, schema, table;
	uint64_t snapshot_id;
	uint64_t table_id = 0;
	double costing_ms = 0;
	std::vector<MoraineRowId> pairs;
	// DuckLake's path for every data file the pairs name, keyed by file id,
	// and the inlined tables the scan covers: the keys the transaction's own
	// deletions are recorded under.
	std::map<uint64_t, std::string> file_paths;
	std::vector<std::string> inlined_tables;
	// The transaction's own inlined rows for the table, under the name
	// DuckLake records their deletions under, and the row id of the first;
	// empty when it inserted none inline.
	std::string local_inlined_path;
	uint64_t local_inlined_row_id_start = 0;
	duckdb::vector<std::string> column_names;
	duckdb::vector<duckdb::LogicalType> column_types;
	// The replaced scan's function info, which its file list and reader
	// refer to and nothing else keeps alive.
	duckdb::shared_ptr<duckdb::TableFunctionInfo> scan_info;

	// Takes over the scan's file list, reader, and columns, leaving `scan`
	// hollow.
	void AdoptScan(duckdb::MultiFileBindData &scan) {
		bind_data = std::move(scan.bind_data);
		file_list = scan.file_list;
		multi_file_reader = std::move(scan.multi_file_reader);
		interface = std::move(scan.interface);
		columns = scan.columns;
		reader_bind = scan.reader_bind;
		file_options = scan.file_options;
		types = scan.types;
		names = scan.names;
		virtual_columns = scan.virtual_columns;
		table_columns = scan.table_columns;
		initial_reader = scan.initial_reader;
		union_readers = scan.union_readers;
	}

	bool SupportStatementCache() const override {
		return false;
	}
	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<SummaryScanBindData>();
		auto base = duckdb::MultiFileBindData::Copy();
		result->AdoptScan(base->Cast<duckdb::MultiFileBindData>());
		result->lake = lake;
		result->schema = schema;
		result->table = table;
		result->snapshot_id = snapshot_id;
		result->table_id = table_id;
		result->costing_ms = costing_ms;
		result->pairs = pairs;
		result->file_paths = file_paths;
		result->inlined_tables = inlined_tables;
		result->local_inlined_path = local_inlined_path;
		result->local_inlined_row_id_start = local_inlined_row_id_start;
		result->column_names = column_names;
		result->column_types = column_types;
		result->scan_info = scan_info;
		return std::move(result);
	}
	bool Equals(const duckdb::FunctionData &) const override {
		return false;
	}
};

// The virtual columns the located scan serves: DuckDB's row id and the three
// DuckLake's DELETE and UPDATE read their deletion info from.
bool IsLocatedVirtualColumn(duckdb::column_t column) {
	return column == duckdb::COLUMN_IDENTIFIER_ROW_ID ||
	       column == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILENAME ||
	       column == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX ||
	       column == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER;
}

// Whether `column` names a row's file or position, which an inlined row lacks.
bool IsFileLocationColumn(duckdb::column_t column) {
	return column == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILENAME ||
	       column == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX ||
	       column == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER;
}

duckdb::LogicalType LocatedVirtualColumnType(duckdb::column_t column) {
	if (column == duckdb::COLUMN_IDENTIFIER_ROW_ID ||
	    column == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
		return duckdb::LogicalType::BIGINT;
	}
	if (column == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILENAME) {
		return duckdb::LogicalType::VARCHAR;
	}
	return duckdb::LogicalType::UBIGINT;
}

struct SummaryScanState : duckdb::GlobalTableFunctionState {
	MoraineRowScan *scan = nullptr;
	duckdb::vector<duckdb::idx_t> decoded_columns, projection;
	// The table columns the located read decodes, in the order it emits them.
	duckdb::vector<duckdb::column_t> requested_columns;
	bool local_inlined_emitted = false;
	// Output columns that carry the file's path, derived from its id.
	duckdb::vector<duckdb::idx_t> filename_columns;
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
	std::vector<MoraineRowId> pairs;
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
			duckdb::optional_idx pending_begin_snapshot;
			transaction.GetLocalDeleteForFile(table_id, path->second, pending, pending_begin_snapshot);
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
		if (!pair.has_data_file_id && deleted_inlined.count(pair.value)) {
			continue;
		}
		result.pairs.push_back(pair);
	}
	return result;
}

// A DuckLake delete planned over this scan learns each file's committed
// deletions from the scan's reader, which never runs here; serve them for
// the files the located rows touch, as the reader would.
void PrimeDeleteMap(duckdb::ClientContext &context, const SummaryScanBindData &bound) {
	auto &reader = bound.multi_file_reader->Cast<duckdb::DuckLakeMultiFileReader>();
	if (!reader.delete_map) {
		return;
	}
	std::set<uint64_t> touched;
	for (auto &pair : bound.pairs) {
		if (pair.has_data_file_id) {
			touched.insert(pair.data_file_id);
		}
	}
	for (auto &entry : bound.file_list->Cast<duckdb::DuckLakeMultiFileList>().GetFiles()) {
		if (entry.data_type != duckdb::DuckLakeDataType::DATA_FILE || !entry.file_id.IsValid() ||
		    !touched.count(entry.file_id.index) || entry.delete_file.path.empty()) {
			continue;
		}
		duckdb::DuckLakeDeleteFilter filter;
		filter.Initialize(context, entry.delete_file);
		reader.delete_map->AddDeleteData(entry.file.path,
		                                 duckdb::make_shared_ptr<duckdb::DuckLakeDeleteData>(*filter.delete_data));
	}
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitSummaryScan(duckdb::ClientContext &context,
                                                                     duckdb::TableFunctionInitInput &input) {
	auto &bound = input.bind_data->Cast<SummaryScanBindData>();
	PrimeDeleteMap(context, bound);
	auto started = ScanClock::now();
	auto state = duckdb::make_uniq<SummaryScanState>();
	duckdb::vector<const char *> requested;
	for (auto column : input.column_ids) {
		if (IsLocatedVirtualColumn(column)) {
			state->decoded_columns.push_back(0);
			state->types.push_back(LocatedVirtualColumnType(column));
		} else {
			state->decoded_columns.push_back(requested.size());
			state->requested_columns.push_back(column);
			requested.push_back(bound.column_names[column].c_str());
			state->types.push_back(bound.column_types[column]);
		}
	}
	// Every located batch ends with the row id, the file id, and the row's
	// position in its file; the deletion-info columns derive from the last two.
	const auto row_id_at = requested.size();
	const auto file_id_at = requested.size() + 1;
	const auto position_at = requested.size() + 2;
	for (duckdb::idx_t i = 0; i < input.column_ids.size(); i++) {
		auto column = input.column_ids[i];
		if (column == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
			state->decoded_columns[i] = row_id_at;
		} else if (column == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX) {
			state->decoded_columns[i] = file_id_at;
		} else if (column == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILENAME) {
			state->decoded_columns[i] = file_id_at;
			state->filename_columns.push_back(i);
		} else if (column == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
			state->decoded_columns[i] = position_at;
		}
	}
	state->projection = input.projection_ids;
	if (state->projection.empty()) {
		for (duckdb::idx_t i = 0; i < input.column_ids.size(); i++) {
			state->projection.push_back(i);
		}
	}
	if (input.filters && input.filters->HasFilters()) {
		auto conjunction =
		    duckdb::make_uniq<duckdb::BoundConjunctionExpression>(duckdb::ExpressionType::CONJUNCTION_AND);
		for (auto filter : *input.filters) {
			auto column_index = filter.GetIndex().GetIndex();
			duckdb::BoundReferenceExpression column(state->types[column_index], column_index);
			auto expression = filter.Filter().ToExpression(column);
			auto required = RequiredPredicate(*expression);
			if (required) {
				conjunction->GetChildrenMutable().push_back(std::move(required));
			}
		}
		if (!conjunction->GetChildren().empty()) {
			state->predicate = std::move(conjunction);
			state->executor = duckdb::make_uniq<duckdb::ExpressionExecutor>(context, *state->predicate);
		}
	}
	auto &lake = duckdb::Catalog::GetCatalog(context, duckdb::Identifier(bound.lake)).Cast<duckdb::DuckLakeCatalog>();
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
	auto code =
	    moraine_row_scan_open(handle, snapshot, bound.schema.c_str(), bound.table.c_str(), exclusions.pairs.data(),
	                          exclusions.pairs.size(), requested.data(), requested.size(), exclusions.entries.data(),
	                          exclusions.entries.size(), &state->scan, moraine_shim_is_interrupted, &context, &error);
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

// A scan reports a batch and this loop imports no rows from it: legal once,
// since a batch may carry only rows the scan has already accounted for, but
// an unbroken run of them means the scan is producing without progressing.
// The loop would then spin on a thread that never sleeps and never yields,
// which is unreadable from the outside, so it is bounded here instead.
constexpr duckdb::idx_t MAX_BARREN_BATCHES = 1024;

// Writes each row's data file path, as DuckLake names the file, from the
// file id the located batch carries. A row without a file never reaches a
// scan that asked for one.
void FillFilenames(const SummaryScanBindData &bound, duckdb::Vector &file_ids, duckdb::Vector &filenames,
                   duckdb::idx_t count) {
	duckdb::UnifiedVectorFormat ids;
	file_ids.ToUnifiedFormat(count, ids);
	auto values = duckdb::UnifiedVectorFormat::GetData<uint64_t>(ids);
	auto out = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(filenames);
	for (duckdb::idx_t row = 0; row < count; row++) {
		auto index = ids.sel->get_index(row);
		if (!ids.validity.RowIsValid(index)) {
			throw duckdb::InternalException(
			    "moraine summary scan: a row without a data file reached a filename column");
		}
		auto path = bound.file_paths.find(values[index]);
		if (path == bound.file_paths.end()) {
			throw duckdb::InternalException("moraine summary scan: data file %llu has no recorded path",
			                                static_cast<unsigned long long>(values[index]));
		}
		out[row] = duckdb::StringVector::AddString(filenames, path->second.c_str(), path->second.size());
	}
}

// The rows this transaction inserted inline, which the pinned snapshot
// cannot know and no data file holds, in the located batch layout, so the
// predicate applies to them as to every located row. Their row ids follow
// DuckLake's numbering of the same rows.
void AppendLocalInlinedRows(duckdb::ClientContext &context, const SummaryScanBindData &bound, SummaryScanState &state) {
	if (bound.local_inlined_path.empty()) {
		return;
	}
	auto &lake = duckdb::Catalog::GetCatalog(context, duckdb::Identifier(bound.lake)).Cast<duckdb::DuckLakeCatalog>();
	auto &transaction = duckdb::DuckLakeTransaction::Get(context, lake);
	duckdb::TableIndex table_id(bound.table_id);
	auto inlined = transaction.GetTransactionLocalInlinedData(table_id);
	if (!inlined || !inlined->data) {
		return;
	}
	std::set<duckdb::idx_t> deleted;
	if (auto deletes = transaction.GetInlinedDeletes(table_id, bound.local_inlined_path)) {
		deleted.insert(deletes->rows.begin(), deletes->rows.end());
	}
	duckdb::vector<duckdb::LogicalType> types;
	for (auto column : state.requested_columns) {
		types.push_back(bound.column_types[column]);
	}
	const auto row_id_at = types.size();
	for (int trailing = 0; trailing < 3; trailing++) {
		types.push_back(duckdb::LogicalType::UBIGINT);
	}
	duckdb::idx_t position = 0;
	for (auto &chunk : inlined->data->Chunks()) {
		duckdb::SelectionVector kept(chunk.size());
		duckdb::idx_t count = 0;
		for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
			if (!deleted.count(position + row)) {
				kept.set_index(count++, row);
			}
		}
		if (count > 0) {
			auto piece = duckdb::make_uniq<duckdb::DataChunk>();
			piece->Initialize(context, types);
			for (duckdb::idx_t i = 0; i < state.requested_columns.size(); i++) {
				duckdb::VectorOperations::Copy(chunk.data[state.requested_columns[i]], piece->data[i], kept, count, 0,
				                               0);
			}
			auto row_ids = duckdb::FlatVector::GetDataMutable<uint64_t>(piece->data[row_id_at]);
			for (duckdb::idx_t i = 0; i < count; i++) {
				auto at = position + kept.get_index(i);
				row_ids[i] = inlined->HasPreservedRowIds() ? static_cast<uint64_t>(inlined->row_ids[at])
				                                           : bound.local_inlined_row_id_start + at;
			}
			for (auto absent : {row_id_at + 1, row_id_at + 2}) {
				piece->data[absent].SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
				duckdb::ConstantVector::SetNull(piece->data[absent], true);
			}
			piece->SetChildCardinality(count);
			state.pending.push_back(std::move(piece));
		}
		position += chunk.size();
	}
}

void ScanSummary(duckdb::ClientContext &context, duckdb::TableFunctionInput &input, duckdb::DataChunk &output) {
	auto &state = input.global_state->Cast<SummaryScanState>();
	duckdb::idx_t barren_batches = 0;
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
				if (!state.local_inlined_emitted) {
					state.local_inlined_emitted = true;
					AppendLocalInlinedRows(context, input.bind_data->Cast<SummaryScanBindData>(), state);
					if (!state.pending.empty()) {
						continue;
					}
				}
				output.SetChildCardinality(0);
				return;
			}
			started = ScanClock::now();
			auto pieces = ImportLocatedBatch(context, schema.arrow_schema, array.arrow_array);
			state.conversion_ms += Milliseconds(started);
			for (auto piece = pieces.rbegin(); piece != pieces.rend(); ++piece) {
				state.pending.push_back(std::move(*piece));
			}
			if (state.pending.empty()) {
				if (++barren_batches > MAX_BARREN_BATCHES) {
					throw duckdb::InternalException(
					    "moraine summary scan read %llu consecutive batches without producing a row; "
					    "refusing to spin",
					    static_cast<unsigned long long>(barren_batches));
				}
				continue;
			}
			barren_batches = 0;
		}
		auto piece = std::move(state.pending.back());
		state.pending.pop_back();
		duckdb::DataChunk projected;
		projected.Initialize(context, state.types);
		projected.SetChildCardinality(piece->size());
		for (duckdb::idx_t i = 0; i < state.types.size(); i++) {
			auto &source = piece->data[state.decoded_columns[i]];
			if (std::find(state.filename_columns.begin(), state.filename_columns.end(), i) !=
			    state.filename_columns.end()) {
				FillFilenames(input.bind_data->Cast<SummaryScanBindData>(), source, projected.data[i], piece->size());
			} else if (source.GetType() == state.types[i]) {
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
		output.SetChildCardinality(projected.size());
		return;
	}
}

} // namespace

bool UseSummaryScan(duckdb::ClientContext &context, duckdb::LogicalGet &scan, const duckdb::LogicalGet &index,
                    const std::vector<MoraineRowId> &rows) {
	std::string catalog_name, schema, table;
	if (!IndexReadTable(*index.function.GetDefinition(), index.bind_data.get(), catalog_name, schema, table)) {
		return false;
	}
	return UseSummaryScan(context, scan, catalog_name, schema, table, rows);
}

bool UseSummaryScan(duckdb::ClientContext &context, duckdb::LogicalGet &scan, const std::string &catalog_name,
                    const std::string &schema, const std::string &table, const std::vector<MoraineRowId> &rows) {
	if (scan.function.GetDefinition()->name != "ducklake_scan" || !scan.function.function_info ||
	    scan.row_group_order_options || scan.table_filters.HasMultiColumnFilters() || scan.extra_info.sample_options) {
		return false;
	}
	auto &info = scan.function.function_info->Cast<duckdb::DuckLakeFunctionInfo>();
	auto &lake = info.table.catalog.Cast<duckdb::DuckLakeCatalog>();
	auto transaction = info.GetTransaction();
	auto statistics = lake.GetTableStats(*transaction, info.table.GetTableId());
	if (statistics && rows.size() > 4096 && rows.size() > statistics->record_count / 4) {
		return false;
	}
	if (info.scan_type != duckdb::DuckLakeScanType::SCAN_TABLE || lake.CatalogSnapshot() ||
	    info.snapshot.snapshot_id != transaction->GetSnapshot().snapshot_id) {
		return false;
	}
	if (schema != info.table.schema.name || table != info.table.name) {
		return false;
	}
	auto catalog = duckdb::Catalog::GetCatalogEntry(context, duckdb::Identifier(catalog_name));
	if (!catalog || (catalog->GetName() != lake.GetName() && catalog->GetName() != lake.MetadataDatabaseName())) {
		return false;
	}
	auto &metadata = ResolveMoraineCatalog(context, lake.MetadataDatabaseName());
	auto metadata_tx = metadata.GetCatalogTransaction(*transaction->GetConnection().context);
	if (!moraine_snapshot_read_handle(metadata_tx.transaction->Cast<MoraineTransaction>().Snapshot())) {
		return false;
	}
	std::set<uint64_t> file_ids;
	bool includes_inline = false;
	for (auto &row : rows) {
		if (row.has_data_file_id) {
			file_ids.insert(row.data_file_id);
		} else {
			includes_inline = true;
		}
	}
	// The rows the transaction inserted inline, numbered after the rows of
	// the files it wrote, as DuckLake's own file list places them.
	std::string local_inlined_path;
	uint64_t local_inlined_row_id_start = 0;
	if (info.include_local_changes && transaction->GetTransactionLocalInlinedData(info.table_id)) {
		local_inlined_path = LOCAL_INLINED_ENTRY;
		local_inlined_row_id_start = duckdb::DuckLakeConstants::TRANSACTION_LOCAL_ROW_ID_START;
		for (auto &file : transaction->GetTransactionLocalFiles(info.table_id)) {
			local_inlined_row_id_start += file.row_count;
		}
		includes_inline = true;
	}
	// Whole columns of any type the table declares: the located reader decodes
	// what DuckLake wrote and the import casts to the declared type. A struct
	// field projection asks for part of a column, which it does not serve,
	// and a column of a type it cannot decode as the scan does keeps the scan.
	// A row's file and position exist only for a row in a data file.
	for (auto &column : scan.GetColumnIds()) {
		auto id = column.GetPrimaryIndex();
		if (column.HasChildren() || (id >= scan.returned_types.size() && !IsLocatedVirtualColumn(id))) {
			return false;
		}
		if (id < scan.returned_types.size() && HoldsUndecodableType(scan.returned_types[id])) {
			return false;
		}
		if (includes_inline && IsFileLocationColumn(id)) {
			return false;
		}
	}
	auto bound = duckdb::make_uniq<SummaryScanBindData>();
	bound->local_inlined_path = local_inlined_path;
	bound->local_inlined_row_id_start = local_inlined_row_id_start;
	bound->lake = lake.GetName().GetIdentifierName();
	bound->schema = schema;
	bound->table = table;
	bound->snapshot_id = info.snapshot.snapshot_id;
	bound->table_id = info.table.GetTableId().index;
	for (auto &name : scan.names) {
		bound->column_names.push_back(name.GetIdentifierName());
	}
	bound->column_types = scan.returned_types;
	bound->pairs = rows;
	// The located files' paths come from the catalog by id, not from the
	// scan's file list.
	auto *manager = dynamic_cast<MoraineMetadataManager *>(&transaction->GetMetadataManager());
	if (!manager) {
		return false;
	}
	if (!file_ids.empty()) {
		std::vector<uint64_t> requested(file_ids.begin(), file_ids.end());
		OwnedArray<MoraineDataFilePath> paths(moraine_data_file_paths_free);
		MoraineError error {};
		auto code = moraine_data_file_paths(
		    moraine_snapshot_read_handle(metadata_tx.transaction->Cast<MoraineTransaction>().Snapshot()),
		    schema.c_str(), table.c_str(), requested.data(), requested.size(), paths.OutItems(), paths.OutLen(),
		    moraine_shim_is_interrupted, &context, &error);
		if (code != MORAINE_OK) {
			ThrowMoraineError(error);
		}
		for (auto &file : paths) {
			bound->file_paths.emplace(
			    file.data_file_id,
			    manager->DataFilePath(info.table, duckdb::DuckLakePath {file.path, file.path_is_relative}));
		}
	}
	for (auto &inlined : info.table.GetInlinedDataTables(*transaction, info.snapshot, info.include_local_changes)) {
		bound->inlined_tables.push_back(inlined.table_name);
	}
	std::vector<MoraineRowId> file_pairs;
	std::vector<MoraineRowId> readable;
	for (auto &pair : bound->pairs) {
		if (pair.has_data_file_id) {
			if (!bound->file_paths.count(pair.data_file_id)) {
				continue;
			}
			file_pairs.push_back(pair);
		}
		readable.push_back(pair);
	}
	bound->pairs = std::move(readable);
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
		auto code =
		    moraine_row_scan_open(moraine_snapshot_read_handle(snapshot), snapshot, schema.c_str(), table.c_str(),
		                          file_pairs.data(), file_pairs.size(), columns.data(), columns.size(), nullptr, 0,
		                          &estimate.scan, moraine_shim_is_interrupted, &context, &error);
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
	// Runtime counters go through the metrics hook, which is handed the
	// scan's state after it has run; `to_string` renders before it reads.
	function.get_metrics = [](duckdb::TableFunctionGetMetricsInput &input) {
		if (!input.global_state) {
			return;
		}
		auto &state = input.global_state->Cast<SummaryScanState>();
		if (!state.scan) {
			return;
		}
		auto metrics = moraine_row_scan_metrics(state.scan);
		input.operator_metrics.AddExtraInfo("Total Files Read",
		                                    std::to_string(moraine_row_scan_files_read(state.scan)));
		input.operator_metrics.AddExtraInfo("Positioning ms", std::to_string(state.positioning_ms));
		input.operator_metrics.AddExtraInfo("Read and decode ms", std::to_string(state.batch_ms));
		input.operator_metrics.AddExtraInfo("Arrow import ms", std::to_string(state.conversion_ms));
		input.operator_metrics.AddExtraInfo("Data bytes fetched", std::to_string(metrics.bytes_read));
		input.operator_metrics.AddExtraInfo("Data ranges fetched", std::to_string(metrics.ranges_read));
		input.operator_metrics.AddExtraInfo("Peak read workers", std::to_string(metrics.peak_workers));
		input.operator_metrics.AddExtraInfo("Decode active ms", std::to_string(metrics.decode_seconds * 1000));
		input.operator_metrics.AddExtraInfo("Data fetch ms", std::to_string(metrics.fetch_seconds * 1000));
	};
	function.to_string = [](duckdb::TableFunctionToStringInput &input) {
		duckdb::InsertionOrderPreservingMap<std::string> result;
		if (input.bind_data) {
			result["Coverage costing ms"] = std::to_string(input.bind_data->Cast<SummaryScanBindData>().costing_ms);
		}
		return result;
	};
	function.get_virtual_columns = [](duckdb::ClientContext &, duckdb::optional_ptr<duckdb::FunctionData> data) {
		return data->Cast<SummaryScanBindData>().virtual_columns;
	};
	// Moved rather than copied, and only once the scan is certainly replaced:
	// the scan's file list and reader refer to the function info its bound
	// function holds, and a moved pointer keeps its address.
	bound->AdoptScan(scan.bind_data->Cast<duckdb::MultiFileBindData>());
	bound->scan_info = scan.function.function_info;
	scan.bind_data = std::move(bound);
	scan.function = duckdb::BoundTableFunction(function);
	scan.extra_info.file_filters.clear();
	return true;
}

} // namespace moraine_duckdb
