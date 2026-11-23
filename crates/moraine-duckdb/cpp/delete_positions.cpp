// Positional deletes: the located-row write path the optimizer selects when a
// certified index answers a DELETE's predicate in full.
#include "delete_positions.hpp"

#include "common/index.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_delete_filter.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"

#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/planner/binder.hpp"

#include <algorithm>
#include <cstring>

namespace moraine_duckdb {

namespace {

using duckdb::idx_t;
using duckdb::map;
using duckdb::optional_idx;
using duckdb::set;
using duckdb::Value;
using duckdb::vector;

constexpr const char *FUNCTION = "moraine_delete_positions";

struct PositionalDeletes {
	map<idx_t, set<idx_t>> file_positions;
	optional_idx resolved_snapshot;
	map<idx_t, vector<idx_t>> existing_positions;
};

struct StagedDeletes {
	idx_t file_rows = 0;
	idx_t files_written = 0;
};

void ParseFilePositions(const Value &files, map<idx_t, set<idx_t>> &result, map<idx_t, vector<idx_t>> &existing) {
	using duckdb::InvalidInputException;
	using duckdb::ListType;
	using duckdb::ListValue;
	using duckdb::LogicalType;
	using duckdb::LogicalTypeId;
	using duckdb::StringValue;
	using duckdb::StructType;
	using duckdb::StructValue;

	if (files.IsNull()) {
		throw InvalidInputException("%s: `files` cannot be NULL", FUNCTION);
	}
	if (ListValue::GetChildren(files).empty()) {
		return;
	}
	auto &element_type = ListType::GetChildType(files.type());
	if (element_type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("%s: each `files` entry must be STRUCT(data_file_id UBIGINT, positions UBIGINT[])",
		                            FUNCTION);
	}
	optional_idx file_index;
	optional_idx positions_index;
	optional_idx existing_index;
	auto &children = StructType::GetChildTypes(element_type);
	for (idx_t i = 0; i < children.size(); i++) {
		auto &name = children[i].first;
		if (name == "data_file_id") {
			file_index = i;
		} else if (name == "positions") {
			positions_index = i;
		} else if (name == "existing_positions") {
			existing_index = i;
		}
	}
	if (children.size() != 2 + (existing_index.IsValid() ? 1 : 0) || !file_index.IsValid() ||
	    !positions_index.IsValid()) {
		throw InvalidInputException("%s: each `files` entry must be STRUCT(data_file_id UBIGINT, positions UBIGINT[])",
		                            FUNCTION);
	}
	for (auto &entry : ListValue::GetChildren(files)) {
		if (entry.IsNull()) {
			throw InvalidInputException("%s: a `files` entry cannot be NULL", FUNCTION);
		}
		auto &fields = StructValue::GetChildren(entry);
		auto &file_value = fields[file_index.GetIndex()];
		auto &positions_value = fields[positions_index.GetIndex()];
		if (file_value.IsNull() || positions_value.IsNull()) {
			throw InvalidInputException("%s: `data_file_id` and `positions` cannot be NULL", FUNCTION);
		}
		if (positions_value.type().id() != LogicalTypeId::LIST) {
			throw InvalidInputException("%s: `positions` must be a list", FUNCTION);
		}
		auto file_id = file_value.DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>();
		auto &positions = result[file_id];
		for (auto &position : ListValue::GetChildren(positions_value)) {
			if (position.IsNull()) {
				throw InvalidInputException("%s: a position cannot be NULL", FUNCTION);
			}
			positions.insert(position.DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>());
		}
		// A NULL `existing_positions` means the caller does not know them and
		// the committed delete file is read here instead. Non-NULL is a BLOB
		// of little-endian uint64 positions.
		if (existing_index.IsValid() && !fields[existing_index.GetIndex()].IsNull()) {
			auto &existing_value = fields[existing_index.GetIndex()];
			if (existing_value.type().id() != LogicalTypeId::BLOB) {
				throw InvalidInputException("%s: `existing_positions` must be a BLOB", FUNCTION);
			}
			auto &packed = StringValue::Get(existing_value);
			if (packed.size() % sizeof(uint64_t) != 0) {
				throw InvalidInputException("%s: `existing_positions` must hold whole uint64 positions", FUNCTION);
			}
			auto &known = existing[file_id];
			known.reserve(packed.size() / sizeof(uint64_t));
			for (idx_t offset = 0; offset + sizeof(uint64_t) <= packed.size(); offset += sizeof(uint64_t)) {
				uint64_t position;
				memcpy(&position, packed.data() + offset, sizeof(uint64_t));
				known.push_back(position);
			}
			std::sort(known.begin(), known.end());
		}
	}
}

// Parses committed file positions and the snapshot they were resolved at.
PositionalDeletes ParsePositionalDeletes(duckdb::TableFunctionBindInput &input) {
	PositionalDeletes result;
	ParseFilePositions(input.inputs[3], result.file_positions, result.existing_positions);
	auto resolved = input.named_parameters.find("snapshot");
	if (resolved != input.named_parameters.end() && !resolved->second.IsNull()) {
		result.resolved_snapshot = resolved->second.DefaultCastAs(duckdb::LogicalType::UBIGINT).GetValue<idx_t>();
	}
	return result;
}

duckdb::DuckLakeTableEntry &LookupTable(duckdb::ClientContext &context, duckdb::DuckLakeCatalog &catalog,
                                        const Value &schema, const Value &table) {
	duckdb::EntryLookupInfo lookup(duckdb::CatalogType::TABLE_ENTRY,
	                               duckdb::Identifier(table.GetValue<duckdb::string>()), nullptr,
	                               duckdb::QueryErrorContext());
	return catalog
	    .GetEntry(context, duckdb::Identifier(schema.GetValue<duckdb::string>()), lookup,
	              duckdb::OnEntryNotFound::THROW_EXCEPTION)
	    ->Cast<duckdb::DuckLakeTableEntry>();
}

// Positions of `file_id` deleted through inlined file deletions, committed or
// pending in this transaction.
void AddInlinedFileDeletes(duckdb::DuckLakeTransaction &transaction, duckdb::DuckLakeSnapshot snapshot,
                           duckdb::TableIndex table, idx_t file_id, set<idx_t> &positions) {
	transaction.GetLocalInlinedFileDeletesForFile(table, file_id, positions);
	auto name = transaction.GetMetadataManager().GetInlinedDeletionTableName(table, snapshot);
	if (name.empty()) {
		return;
	}
	auto result = transaction.GetMetadataManager().Query(
	    snapshot, duckdb::StringUtil::Format(
	                  "SELECT row_id FROM {METADATA_CATALOG}.%s WHERE file_id = %d AND begin_snapshot <= {SNAPSHOT_ID}",
	                  name, file_id));
	if (result->HasError()) {
		result->GetErrorObject().Throw();
	}
	for (auto &row : *result) {
		positions.insert(row.GetValue<idx_t>(0));
	}
}

// Stages file deletions in the executing transaction, skipping positions
// already deleted. A failure after staging begins aborts the transaction.
StagedDeletes StagePositionalDeletes(duckdb::ClientContext &context, duckdb::DuckLakeCatalog &catalog,
                                     duckdb::DuckLakeTableEntry &table, const PositionalDeletes &request) {
	using duckdb::DuckLakeDeleteFilter;
	using duckdb::InvalidInputException;
	using duckdb::TransactionException;

	auto &transaction = duckdb::DuckLakeTransaction::Get(context, catalog);
	auto snapshot = transaction.GetSnapshot();
	auto table_id = table.GetTableId();
	auto &schema = table.ParentSchema().Cast<duckdb::DuckLakeSchemaEntry>();
	if (catalog.WriteDeletionVectors(schema.GetSchemaId(), table_id)) {
		throw InvalidInputException("%s: table \"%s\" is configured to write deletion vectors, which this function "
		                            "does not support; use DELETE instead",
		                            FUNCTION, table.name);
	}
	if (duckdb::IsTransactionLocal(table_id)) {
		throw InvalidInputException("%s: table \"%s\" was created in this transaction; only committed tables have "
		                            "addressable files",
		                            FUNCTION, table.name);
	}
	// Positions resolved against an older snapshot may name rows this
	// transaction already sees deleted; a newer one only stages what this
	// snapshot still holds, and a deletion in between surfaces as a commit
	// conflict.
	if (request.resolved_snapshot.IsValid() && request.resolved_snapshot.GetIndex() < snapshot.snapshot_id) {
		throw TransactionException("%s: positions were resolved against snapshot %d, older than snapshot %d this "
		                           "transaction reads; retry the statement",
		                           FUNCTION, request.resolved_snapshot.GetIndex(), snapshot.snapshot_id);
	}

	map<idx_t, duckdb::DuckLakeFileListExtendedEntry> files;
	if (!request.file_positions.empty()) {
		for (auto &entry : transaction.GetMetadataManager().GetExtendedFilesForTable(table, snapshot)) {
			if (entry.file_id.IsValid()) {
				files.emplace(entry.file_id.index, std::move(entry));
			}
		}
	}
	idx_t threshold = catalog.DataInliningRowLimit(context, schema.GetSchemaId(), table_id);

	StagedDeletes staged;
	auto &fs = duckdb::FileSystem::GetFileSystem(context);
	try {
		for (auto &requested : request.file_positions) {
			if (context.IsInterrupted()) {
				throw duckdb::InterruptException();
			}
			auto file_entry = files.find(requested.first);
			if (file_entry == files.end()) {
				throw InvalidInputException("%s: data file %d is not part of table \"%s\" at this snapshot", FUNCTION,
				                            requested.first, table.name);
			}
			auto &file = file_entry->second;
			if (transaction.FileIsDropped(file.file.path)) {
				continue;
			}
			for (auto position : requested.second) {
				if (position >= file.row_count) {
					throw InvalidInputException("%s: position %d is beyond the %d rows of data file %d", FUNCTION,
					                            position, file.row_count, requested.first);
				}
			}

			// Positions the file already has deleted: its pending delete file
			// if this transaction wrote one, else the committed delete file,
			// plus inlined file deletions from both.
			set<idx_t> positions;
			auto known = request.existing_positions.find(requested.first);
			if (transaction.HasLocalDeleteForFile(table_id, file.file.path)) {
				duckdb::DuckLakeFileData pending;
				optional_idx pending_begin_snapshot;
				transaction.GetLocalDeleteForFile(table_id, file.file.path, pending, pending_begin_snapshot);
				auto scanned = DuckLakeDeleteFilter::ScanDeleteFile(context, pending);
				positions.insert(scanned.deleted_rows.begin(), scanned.deleted_rows.end());
			} else if (known != request.existing_positions.end()) {
				// The caller resolved its positions at this snapshot or a newer
				// one, so its decode of the committed delete file holds every
				// position deleted here.
				positions.insert(known->second.begin(), known->second.end());
			} else if (!file.delete_file.path.empty()) {
				// A replaced delete file embeds per-position snapshots and keeps
				// the earliest begin; only the positions deleted at this snapshot
				// count as already gone.
				auto scanned = DuckLakeDeleteFilter::ScanDeleteFile(context, file.delete_file, optional_idx(),
				                                                    snapshot.snapshot_id);
				positions.insert(scanned.deleted_rows.begin(), scanned.deleted_rows.end());
			}
			set<idx_t> inlined_positions;
			AddInlinedFileDeletes(transaction, snapshot, table_id, requested.first, inlined_positions);

			set<idx_t> new_positions;
			for (auto position : requested.second) {
				if (!inlined_positions.count(position) && !positions.count(position)) {
					new_positions.insert(position);
				}
			}
			if (new_positions.empty()) {
				continue;
			}
			staged.file_rows += new_positions.size();
			if (threshold > 0 && new_positions.size() <= threshold) {
				transaction.AddNewInlinedFileDeletes(table_id, requested.first, std::move(new_positions));
				continue;
			}

			positions.insert(new_positions.begin(), new_positions.end());
			fs.CreateDirectoriesRecursive(table.DataPath());
			duckdb::WriteDeleteFileInput write {context,
			                                    transaction,
			                                    fs,
			                                    table.DataPath(),
			                                    catalog.GenerateEncryptionKey(context),
			                                    file.file.path,
			                                    std::move(positions),
			                                    duckdb::DeleteFileSource::REGULAR};
			auto written = duckdb::DuckLakeDeleteFileWriter::WriteDeleteFile(context, write);
			auto written_path = written.file_name;
			try {
				written.data_file_id = file.file_id;
				if (file.delete_file_id.IsValid()) {
					written.overwrites_existing_delete = true;
					written.overwritten_delete_file.delete_file_id = file.delete_file_id;
					written.overwritten_delete_file.path = file.delete_file.path;
				}
				vector<duckdb::DuckLakeDeleteFile> delete_files;
				delete_files.push_back(std::move(written));
				transaction.AddDeletes(table_id, std::move(delete_files));
			} catch (...) {
				fs.TryRemoveFile(written_path);
				throw;
			}
			staged.files_written++;
		}
	} catch (std::exception &error) {
		// A partially staged call must abort the surrounding transaction.
		throw TransactionException("%s: %s", FUNCTION, error.what());
	}
	return staged;
}

struct DeletePositionsBindData : public duckdb::TableFunctionData {
	DeletePositionsBindData(duckdb::DuckLakeCatalog &catalog_p, duckdb::DuckLakeTableEntry &table_p,
	                        PositionalDeletes request_p)
	    : catalog(catalog_p), table(table_p), request(std::move(request_p)) {
	}
	duckdb::DuckLakeCatalog &catalog;
	duckdb::DuckLakeTableEntry &table;
	PositionalDeletes request;
};

struct DeletePositionsState : public duckdb::GlobalTableFunctionState {
	bool finished = false;
	idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> DeletePositionsBind(duckdb::ClientContext &context,
                                                             duckdb::TableFunctionBindInput &input,
                                                             vector<duckdb::LogicalType> &types,
                                                             vector<duckdb::Identifier> &names) {
	auto &catalog = duckdb::DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	auto &table = LookupTable(context, catalog, input.inputs[1], input.inputs[2]);
	auto result = duckdb::make_uniq<DeletePositionsBindData>(catalog, table, ParsePositionalDeletes(input));
	// The staged deletes belong to the executing transaction, so a prepared
	// call binds again for each execution.
	input.binder->SetAlwaysRequireRebind();
	input.binder->GetStatementProperties().RegisterDBModify(catalog, context,
	                                                        duckdb::DatabaseModificationType::DELETE_DATA);
	types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT};
	names = {"file_rows_deleted", "delete_files_written"};
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> DeletePositionsInit(duckdb::ClientContext &,
                                                                         duckdb::TableFunctionInitInput &) {
	return duckdb::make_uniq<DeletePositionsState>();
}

void DeletePositionsExecute(duckdb::ClientContext &context, duckdb::TableFunctionInput &input,
                            duckdb::DataChunk &output) {
	auto &state = input.global_state->Cast<DeletePositionsState>();
	if (state.finished) {
		return;
	}
	auto &data = input.bind_data->Cast<DeletePositionsBindData>();
	auto staged = StagePositionalDeletes(context, data.catalog, data.table, data.request);
	output.SetValue(0, 0, Value::BIGINT(duckdb::NumericCast<int64_t>(staged.file_rows)));
	output.SetValue(1, 0, Value::BIGINT(duckdb::NumericCast<int64_t>(staged.files_written)));
	output.SetCardinality(1);
	state.finished = true;
}

} // namespace

MoraineDeletePositionsFunction::MoraineDeletePositionsFunction()
    : duckdb::TableFunction(FUNCTION,
                            {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
                             duckdb::LogicalType::LIST(duckdb::LogicalType::ANY)},
                            DeletePositionsExecute, DeletePositionsBind, DeletePositionsInit) {
	GetSignature().WithTypedKwargs(
	    "options", [](duckdb::TypedKwargs &options) { options.Add("snapshot", duckdb::LogicalType::UBIGINT); });
}

void RegisterMoraineDeletePositionsFunction(duckdb::ExtensionLoader &loader) {
	MoraineDeletePositionsFunction function;
	loader.RegisterFunction(function);
}

} // namespace moraine_duckdb
