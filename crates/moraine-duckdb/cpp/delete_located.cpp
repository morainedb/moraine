// moraine_delete_located(catalog, schema, table, rows): resolves
// already-located `rows` — a LIST of STRUCT(row_id BIGINT, data_file_id
// UBIGINT), as an index lookup reports them — to exact file positions and
// lands their deletion through a shim-written delete-file Parquet plus the
// register/expire/inline-delete verbs, with no scan.
#include "duckdb.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "catalog.hpp"
#include "delete_located.hpp"
#include "moraine_abi.h"
#include "owned_array.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"

#include <algorithm>
#include <optional>

namespace moraine_duckdb {

namespace {

// The delete-file field ids DuckLake's own DELETE writer uses (verified
// against its `WriteDeleteFileInternal`, which writes `file_path` and
// `pos` under these ids regardless of the `written_columns` map a related,
// unused struct in the same file declares) — numerically DuckDB's own
// `FILENAME_FIELD_ID`/`ORDINAL_FIELD_ID`
// (`duckdb/common/multi_file/multi_file_reader.hpp`), not the
// similarly-named-but-different-valued `DELETE_FILE_PATH_FIELD_ID`/
// `DELETE_POS_FIELD_ID` constants in the same DuckDB header.
constexpr int32_t WRITTEN_FILE_PATH_FIELD_ID = 2147483646;
constexpr int32_t WRITTEN_POS_FIELD_ID = 2147483645;

// A table's catalog ids, resolved by name for the `write_deletion_vectors`
// scope check. Path composition for the file this call writes is the
// core's job (`moraine_locate_row_positions`'s `write_directory` output),
// not reimplemented here.
struct TableIds {
	uint64_t schema_id;
	uint64_t table_id;
};

TableIds ResolveTableIds(MoraineCatalogHandle *handle, const std::string &schema_name, const std::string &table_name,
                         MoraineInterruptProbe probe, void *probe_ctx) {
	OwnedArray<MoraineSchemaRow> schemas(moraine_dump_schemas_free);
	MoraineError schema_err {};
	if (moraine_dump_schemas(handle, schemas.OutItems(), schemas.OutLen(), probe, probe_ctx, &schema_err) !=
	    MORAINE_OK) {
		ThrowMoraineError(schema_err);
	}
	const MoraineSchemaRow *schema_row = nullptr;
	for (auto &row : schemas) {
		if (!row.has_end_snapshot && schema_name == row.schema_name) {
			schema_row = &row;
			break;
		}
	}
	if (schema_row == nullptr) {
		throw duckdb::InvalidInputException("moraine_delete_located: unknown schema \"%s\"", schema_name);
	}
	uint64_t schema_id = schema_row->schema_id;

	OwnedArray<MoraineTableRow> tables(moraine_dump_tables_free);
	MoraineError table_err {};
	if (moraine_dump_tables(handle, tables.OutItems(), tables.OutLen(), probe, probe_ctx, &table_err) != MORAINE_OK) {
		ThrowMoraineError(table_err);
	}
	const MoraineTableRow *table_row = nullptr;
	for (auto &row : tables) {
		if (!row.has_end_snapshot && row.schema_id == schema_id && table_name == row.table_name) {
			table_row = &row;
			break;
		}
	}
	if (table_row == nullptr) {
		throw duckdb::InvalidInputException("moraine_delete_located: unknown table \"%s\"", table_name);
	}

	return TableIds {schema_id, table_row->table_id};
}

// DuckLake accepts `write_deletion_vectors` as an attach-only option that
// lands solely in the attaching connection's in-memory catalog options, so
// a durable read (`moraine_dump_options`) can miss it; DuckDB retains every
// attach's raw option map on its `AttachedDatabase` regardless of storage
// extension, which is what this reads instead.
std::optional<bool> AttachTimeWritesDeletionVectors(duckdb::ClientContext &context, const std::string &catalog_name) {
	auto catalog = duckdb::Catalog::GetCatalogEntry(context, catalog_name);
	if (!catalog) {
		return std::nullopt;
	}
	for (auto &option : catalog->GetAttached().GetAttachOptions()) {
		if (duckdb::StringUtil::Lower(option.first) == "write_deletion_vectors") {
			return option.second.GetValue<bool>();
		}
	}
	return std::nullopt;
}

// Whether `table_id`/`schema_id` resolve `write_deletion_vectors` to
// `true`, at table scope, else schema scope, else the lake's global
// scope — the same precedence DuckLake's own option resolution applies.
// `attach_time_global` is consulted only when no scope, including global,
// was durably `SET` (see `AttachTimeWritesDeletionVectors`).
bool WritesDeletionVectors(MoraineCatalogHandle *handle, uint64_t schema_id, uint64_t table_id,
                           std::optional<bool> attach_time_global, MoraineInterruptProbe probe, void *probe_ctx) {
	OwnedArray<MoraineOptionRow> options(moraine_dump_options_free);
	MoraineError err {};
	if (moraine_dump_options(handle, options.OutItems(), options.OutLen(), probe, probe_ctx, &err) != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::optional<bool> table_value;
	std::optional<bool> schema_value;
	std::optional<bool> global_value;
	for (auto &row : options) {
		if (std::string(row.key) != "write_deletion_vectors") {
			continue;
		}
		bool value = std::string(row.value) == "true";
		if (row.scope != nullptr && row.has_scope_id && std::string(row.scope) == "table" &&
		    row.scope_id == table_id) {
			table_value = value;
		} else if (row.scope != nullptr && row.has_scope_id && std::string(row.scope) == "schema" &&
		          row.scope_id == schema_id) {
			schema_value = value;
		} else if (row.scope == nullptr) {
			global_value = value;
		}
	}
	if (table_value.has_value()) {
		return *table_value;
	}
	if (schema_value.has_value()) {
		return *schema_value;
	}
	if (global_value.has_value()) {
		return *global_value;
	}
	return attach_time_global.value_or(false);
}

// Writes `positions` (already sorted and duplicate-free) as one DuckLake
// delete file at `dest_path`, `file_path` written for format compatibility
// with DuckLake's own writer alone — its reader applies deletes by position
// against the catalog-named data file, never reading this column back.
void WriteDeleteParquet(duckdb::Connection &connection, const std::string &file_path,
                        const std::vector<uint64_t> &positions, const std::string &dest_path) {
	if (positions.empty()) {
		throw duckdb::InternalException("moraine_delete_located: refusing to write a delete file with no positions");
	}

	auto create = connection.Query(
	    "CREATE OR REPLACE TEMP TABLE __moraine_delete_positions(file_path VARCHAR, pos BIGINT);");
	if (create->HasError()) {
		throw duckdb::IOException("moraine_delete_located: preparing the delete-file staging table failed: %s",
		                         create->GetError());
	}
	{
		duckdb::Appender appender(connection, "__moraine_delete_positions");
		for (auto position : positions) {
			appender.BeginRow();
			appender.Append<const char *>(file_path.c_str());
			appender.Append<int64_t>(static_cast<int64_t>(position));
			appender.EndRow();
		}
		appender.Close();
	}

	std::string sql = "COPY (SELECT file_path, pos FROM __moraine_delete_positions) TO " +
	                  duckdb::KeywordHelper::WriteQuoted(dest_path, '\'') + " (FORMAT parquet, FIELD_IDS {file_path: " +
	                  std::to_string(WRITTEN_FILE_PATH_FIELD_ID) + ", pos: " + std::to_string(WRITTEN_POS_FIELD_ID) +
	                  "});";
	auto result = connection.Query(sql);
	if (result->HasError()) {
		throw duckdb::IOException("moraine_delete_located: writing delete file \"%s\" failed: %s", dest_path,
		                         result->GetError());
	}
	connection.Query("DROP TABLE __moraine_delete_positions;");
}

// The Parquet footer length, per the spec's fixed trailer: the last 4
// bytes are the magic `PAR1`, and the 4 bytes before that are the
// thrift-encoded footer length as a little-endian `u32` — the same value
// DuckLake registers as `footer_size` when it authors a file.
uint64_t ReadParquetFooterSize(duckdb::FileSystem &fs, duckdb::FileHandle &handle, uint64_t file_size) {
	if (file_size < 8) {
		throw duckdb::IOException("moraine_delete_located: written delete file is smaller than a Parquet trailer");
	}
	uint8_t trailer[8];
	fs.Read(handle, trailer, 8, static_cast<duckdb::idx_t>(file_size - 8));
	return static_cast<uint64_t>(trailer[0]) | (static_cast<uint64_t>(trailer[1]) << 8) |
	      (static_cast<uint64_t>(trailer[2]) << 16) | (static_cast<uint64_t>(trailer[3]) << 24);
}

struct DeleteLocatedBindData : public duckdb::FunctionData {
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	std::vector<MorainePositionPair> pairs;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<DeleteLocatedBindData>();
		*result = *this;
		return result;
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<DeleteLocatedBindData>();
		if (catalog_name != other.catalog_name || schema_name != other.schema_name ||
		    table_name != other.table_name || pairs.size() != other.pairs.size()) {
			return false;
		}
		for (size_t i = 0; i < pairs.size(); i++) {
			if (pairs[i].row_id != other.pairs[i].row_id ||
			    pairs[i].has_data_file_id != other.pairs[i].has_data_file_id ||
			    pairs[i].data_file_id != other.pairs[i].data_file_id) {
				return false;
			}
		}
		return true;
	}
};

// One data file's deletion work: the positions this call newly marks dead
// (already the located positions minus whatever the existing delete file
// carried), the union to write, and the delete file it replaces, if any.
struct FileWork {
	uint64_t data_file_id = 0;
	std::string file_path;
	std::vector<uint64_t> new_positions;
	std::vector<uint64_t> union_positions;
	bool has_expires = false;
	uint64_t expires = 0;
};

duckdb::unique_ptr<duckdb::FunctionData> DeleteLocatedBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                           duckdb::vector<duckdb::LogicalType> &return_types,
                                                           duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<DeleteLocatedBindData>();
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();

	if (input.inputs[3].IsNull()) {
		throw duckdb::InvalidInputException("moraine_delete_located: `rows` cannot be NULL");
	}

	// The element struct's field names are validated once here, by type,
	// rather than assumed positional: a caller naming `data_file_id` before
	// `row_id` must still resolve to the right row.
	auto element_type = duckdb::ListType::GetChildType(input.inputs[3].type());
	if (element_type.id() != duckdb::LogicalTypeId::STRUCT) {
		throw duckdb::InvalidInputException(
		    "moraine_delete_located: each `rows` entry must be STRUCT(row_id BIGINT, data_file_id UBIGINT)");
	}
	auto field_count = duckdb::StructType::GetChildCount(element_type);
	int row_id_field = -1;
	int data_file_id_field = -1;
	for (idx_t i = 0; i < field_count; i++) {
		auto &name = duckdb::StructType::GetChildName(element_type, i);
		if (name == "row_id") {
			row_id_field = static_cast<int>(i);
		} else if (name == "data_file_id") {
			data_file_id_field = static_cast<int>(i);
		} else {
			throw duckdb::InvalidInputException(
			    "moraine_delete_located: unexpected field \"%s\"; each `rows` entry must be exactly "
			    "STRUCT(row_id BIGINT, data_file_id UBIGINT)",
			    name);
		}
	}
	if (field_count != 2 || row_id_field < 0 || data_file_id_field < 0) {
		throw duckdb::InvalidInputException(
		    "moraine_delete_located: each `rows` entry must be exactly STRUCT(row_id BIGINT, data_file_id UBIGINT)");
	}

	for (auto &row : duckdb::ListValue::GetChildren(input.inputs[3])) {
		if (row.IsNull()) {
			throw duckdb::InvalidInputException("moraine_delete_located: a `rows` entry cannot be NULL");
		}
		auto &fields = duckdb::StructValue::GetChildren(row);
		MorainePositionPair pair {};
		if (fields[static_cast<size_t>(row_id_field)].IsNull()) {
			throw duckdb::InvalidInputException("moraine_delete_located: a `rows` entry's row_id cannot be NULL");
		}
		pair.row_id = static_cast<uint64_t>(fields[static_cast<size_t>(row_id_field)].GetValue<int64_t>());
		auto &data_file_id_value = fields[static_cast<size_t>(data_file_id_field)];
		if (data_file_id_value.IsNull()) {
			pair.has_data_file_id = false;
		} else {
			pair.has_data_file_id = true;
			pair.data_file_id = data_file_id_value.GetValue<uint64_t>();
		}
		bind_data->pairs.push_back(pair);
	}

	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT};
	names = {"file_rows_deleted", "inline_rows_deleted", "delete_files_written"};
	return bind_data;
}

struct DeleteLocatedGlobalState : public duckdb::GlobalTableFunctionState {
	bool emitted = false;
	int64_t file_rows_deleted = 0;
	int64_t inline_rows_deleted = 0;
	int64_t delete_files_written = 0;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

// Applied once, at execution start — the DDL functions' pattern, since
// this call's effect (writing delete files and committing) is a one-shot
// side effect rather than something to redo per output chunk.
duckdb::unique_ptr<duckdb::GlobalTableFunctionState> DeleteLocatedInitGlobal(duckdb::ClientContext &context,
                                                                             duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DeleteLocatedBindData>();
	auto handle = ResolveMoraineCatalog(context, bind_data.catalog_name).Handle();
	MoraineInterruptProbe probe = moraine_shim_is_interrupted;
	void *probe_ctx = &context;

	auto ids = ResolveTableIds(handle, bind_data.schema_name, bind_data.table_name, probe, probe_ctx);
	auto attach_time_global = AttachTimeWritesDeletionVectors(context, bind_data.catalog_name);
	if (WritesDeletionVectors(handle, ids.schema_id, ids.table_id, attach_time_global, probe, probe_ctx)) {
		throw duckdb::InvalidInputException(
		    "moraine_delete_located: table \"%s\".\"%s\" is configured to write deletion vectors, which this "
		    "function does not support; delete these rows with DELETE ... USING instead",
		    bind_data.schema_name, bind_data.table_name);
	}

	OwnedArray<MoraineLocatedFile> files(moraine_locate_row_positions_free_files);
	OwnedArray<uint64_t> inlined(moraine_locate_row_positions_free_inlined);
	char *raw_write_directory = nullptr;
	MoraineError locate_err {};
	auto locate_code = moraine_locate_row_positions(
	    handle, bind_data.schema_name.c_str(), bind_data.table_name.c_str(), bind_data.pairs.data(),
	    bind_data.pairs.size(), files.OutItems(), files.OutLen(), inlined.OutItems(), inlined.OutLen(),
	    &raw_write_directory, probe, probe_ctx, &locate_err);
	if (locate_code != MORAINE_OK) {
		ThrowMoraineError(locate_err);
	}
	// Owned by the ABI's string-ownership convention: copied out and freed
	// immediately, before anything else here can throw.
	std::string write_directory;
	if (raw_write_directory != nullptr) {
		write_directory = raw_write_directory;
		moraine_string_free(raw_write_directory);
	}
	if (files.size() > 0 && write_directory.empty()) {
		// `moraine_locate_row_positions` only omits this when nothing was
		// positioned, which is not the case here.
		throw duckdb::InternalException(
		    "moraine_delete_located: the locate call resolved positions but named no write directory");
	}

	// New positions are the located ones minus whatever the file's existing
	// delete file already carries (both sides arrive sorted, duplicate-free);
	// a file left with nothing newly dead is dropped rather than rewritten.
	std::vector<FileWork> works;
	works.reserve(files.size());
	int64_t new_position_total = 0;
	for (auto &file : files) {
		std::vector<uint64_t> new_positions(file.positions, file.positions + file.positions_len);
		std::vector<uint64_t> existing_positions;
		if (file.has_existing_delete) {
			existing_positions.assign(file.existing_positions, file.existing_positions + file.existing_positions_len);
		}

		std::vector<uint64_t> truly_new;
		std::set_difference(new_positions.begin(), new_positions.end(), existing_positions.begin(),
		                    existing_positions.end(), std::back_inserter(truly_new));
		if (truly_new.empty()) {
			continue;
		}

		FileWork work;
		work.data_file_id = file.data_file_id;
		work.file_path = file.file_path;
		work.has_expires = file.has_existing_delete;
		work.expires = file.existing_delete_file_id;
		std::set_union(new_positions.begin(), new_positions.end(), existing_positions.begin(),
		              existing_positions.end(), std::back_inserter(work.union_positions));
		new_position_total += static_cast<int64_t>(truly_new.size());
		work.new_positions = std::move(truly_new);
		works.push_back(std::move(work));
	}

	if (works.empty() && inlined.size() == 0) {
		// Nothing newly dead and no inlined rows: mint no snapshot.
		auto empty_result = duckdb::make_uniq<DeleteLocatedGlobalState>();
		return empty_result;
	}

	duckdb::Connection connection(*context.db);
	auto &fs = duckdb::FileSystem::GetFileSystem(*context.db);
	if (!works.empty()) {
		fs.CreateDirectoriesRecursive(write_directory);
	}

	// Reserved to their final size up front, so a borrowed `.c_str()`/`.data()`
	// pointer taken below stays valid for every later `push_back`.
	std::vector<std::string> file_names;
	file_names.reserve(works.size());
	std::vector<MorainePositionedDeleteFile> registrations;
	registrations.reserve(works.size());
	std::vector<std::string> written_paths;
	written_paths.reserve(works.size());

	try {
		for (auto &work : works) {
			auto &file_name = file_names.emplace_back(
			    "ducklake-" + duckdb::UUID::ToString(duckdb::UUID::GenerateRandomUUID()) + "-delete.parquet");
			std::string write_path = write_directory + file_name;

			WriteDeleteParquet(connection, work.file_path, work.union_positions, write_path);
			written_paths.push_back(write_path);

			auto file_handle = fs.OpenFile(write_path, duckdb::FileOpenFlags::FILE_FLAGS_READ);
			auto file_size = static_cast<uint64_t>(fs.GetFileSize(*file_handle));
			auto footer_size = ReadParquetFooterSize(fs, *file_handle, file_size);

			MorainePositionedDeleteFile registration {};
			registration.data_file_id = work.data_file_id;
			registration.path = file_name.c_str();
			registration.file_size = file_size;
			registration.footer_size = footer_size;
			registration.delete_count = work.union_positions.size();
			registration.has_expires = work.has_expires;
			registration.expires = work.expires;
			registration.new_positions = work.new_positions.data();
			registration.new_positions_len = work.new_positions.size();
			registrations.push_back(registration);
		}

		uint64_t snapshot_id = 0;
		MoraineError commit_err {};
		auto commit_code = moraine_commit_located_deletion(
		    handle, bind_data.schema_name.c_str(), bind_data.table_name.c_str(),
		    registrations.empty() ? nullptr : registrations.data(), registrations.size(),
		    inlined.size() == 0 ? nullptr : inlined.begin(), inlined.size(), &snapshot_id, probe, probe_ctx,
		    &commit_err);
		if (commit_code != MORAINE_OK) {
			ThrowMoraineError(commit_err);
		}
	} catch (...) {
		for (auto &path : written_paths) {
			fs.TryRemoveFile(path);
		}
		throw;
	}

	auto result = duckdb::make_uniq<DeleteLocatedGlobalState>();
	result->file_rows_deleted = new_position_total;
	result->inline_rows_deleted = static_cast<int64_t>(inlined.size());
	result->delete_files_written = static_cast<int64_t>(registrations.size());
	return result;
}

void DeleteLocatedImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<DeleteLocatedGlobalState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	output.SetValue(0, 0, duckdb::Value::BIGINT(state.file_rows_deleted));
	output.SetValue(1, 0, duckdb::Value::BIGINT(state.inline_rows_deleted));
	output.SetValue(2, 0, duckdb::Value::BIGINT(state.delete_files_written));
	output.SetCardinality(1);
	state.emitted = true;
}

} // namespace

void RegisterMoraineDeleteLocatedFunction(duckdb::ExtensionLoader &loader) {
	using duckdb::LogicalType;

	// (catalog, schema, table, rows), where `rows` is a LIST of
	// STRUCT(row_id BIGINT, data_file_id UBIGINT); DuckDB's ANY element
	// type accepts the struct shape without pinning field names or order —
	// the bind above resolves each field by name.
	duckdb::TableFunction delete_located(
	    "moraine_delete_located",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	    DeleteLocatedImpl, DeleteLocatedBind, DeleteLocatedInitGlobal);
	loader.RegisterFunction(delete_located);
}

} // namespace moraine_duckdb
