#include "metadata_manager.hpp"

#include "common/ducklake_util.hpp"
#include "common/index.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_inlined_data.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/appender.hpp"

#include <mutex>
#include <set>

namespace moraine_duckdb {

MoraineMetadataManager::MoraineMetadataManager(duckdb::DuckLakeTransaction &transaction)
    : duckdb::DuckLakeMetadataManager(transaction) {
}

bool MoraineMetadataManager::TryAppendInlinedData(duckdb::DuckLakeSnapshot &commit_snapshot,
                                                 const duckdb::string &inlined_table_name,
                                                 const duckdb::DuckLakeInlinedDataInfo &entry,
                                                 bool has_preserved_row_ids) {
	using duckdb::Allocator;
	using duckdb::Appender;
	using duckdb::ConstantVector;
	using duckdb::DataChunk;
	using duckdb::DuckLakeConstants;
	using duckdb::FlatVector;
	using duckdb::LogicalType;
	using duckdb::VectorType;

	if (!SupportsAppender() || !entry.data || !entry.data->data || entry.data->data->Count() == 0) {
		return false;
	}
	auto &catalog = transaction.GetCatalog();
	auto &connection = transaction.GetConnection();
	auto schema_name = catalog.MetadataSchemaName();
	if (schema_name.empty()) {
		schema_name = "main";
	}

	auto &collection = *entry.data->data;
	duckdb::vector<LogicalType> append_types {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
	for (auto &type : collection.Types()) {
		append_types.push_back(type);
	}

	Appender appender(connection, catalog.MetadataDatabaseName(), schema_name, inlined_table_name);
	DataChunk append_chunk;
	append_chunk.Initialize(Allocator::DefaultAllocator(), append_types);

	auto row_id = entry.row_id_start;
	auto snapshot_id = static_cast<int64_t>(commit_snapshot.snapshot_id);
	duckdb::idx_t position = 0;
	for (auto &chunk : collection.Chunks()) {
		auto count = chunk.size();
		append_chunk.Reset();
		auto row_ids = FlatVector::GetData<int64_t>(append_chunk.data[0]);
		auto snapshots = FlatVector::GetData<int64_t>(append_chunk.data[1]);
		for (duckdb::idx_t r = 0; r < count; r++) {
			int64_t emit_rid;
			if (has_preserved_row_ids) {
				int64_t staged_rid = entry.data->row_ids[position];
				emit_rid = DuckLakeConstants::IsTransactionLocalRowId(staged_rid) ? static_cast<int64_t>(row_id++)
				                                                                 : staged_rid;
			} else {
				emit_rid = static_cast<int64_t>(row_id++);
			}
			row_ids[r] = emit_rid;
			snapshots[r] = snapshot_id;
			position++;
		}
		append_chunk.data[2].SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(append_chunk.data[2], true);
		for (duckdb::idx_t c = 0; c < chunk.ColumnCount(); c++) {
			append_chunk.data[3 + c].Reference(chunk.data[c]);
		}
		append_chunk.SetCardinality(count);
		appender.AppendDataChunk(append_chunk);
	}
	appender.Close();
	return true;
}

duckdb::string MoraineMetadataManager::WriteNewInlinedData(
    duckdb::DuckLakeSnapshot &commit_snapshot, const duckdb::vector<duckdb::DuckLakeInlinedDataInfo> &new_data,
    const duckdb::vector<duckdb::DuckLakeTableInfo> &new_tables,
    const duckdb::vector<duckdb::DuckLakeTableInfo> &new_inlined_data_tables_result) {
	using duckdb::DuckLakeTableEntry;
	using duckdb::DuckLakeTableInfo;
	using duckdb::DuckLakeUtil;
	using duckdb::InternalException;

	duckdb::string batch_query;
	if (new_data.empty()) {
		return batch_query;
	}

	auto context_ptr = transaction.context.lock();
	auto &context = *context_ptr;
	// Appends run now; the batch this builds runs later. A table the batch
	// still has to CREATE is therefore not appendable yet, and those rows keep
	// taking the SQL path.
	std::set<duckdb::string> created_in_batch;
	for (auto &entry : new_data) {
		duckdb::string inlined_table_name;
		for (auto &inlined_table : new_inlined_data_tables_result) {
			if (inlined_table.id == entry.table_id) {
				inlined_table_name = InlinedTableNameFor(inlined_table.id.index, commit_snapshot.schema_version);
				created_in_batch.insert(inlined_table_name);
			}
		}
		if (inlined_table_name.empty()) {
			// get the latest table to insert into
			auto it = insert_inlined_table_name_cache.find(entry.table_id.index);
			if (it != insert_inlined_table_name_cache.end()) {
				inlined_table_name = it->second;
			}
		}
		if (inlined_table_name.empty()) {
			auto query = LatestInlinedTableQuery(entry.table_id.index) + ";";
			auto result = transaction.Query(commit_snapshot, query);
			for (auto &row : *result) {
				inlined_table_name = row.GetValue<duckdb::string>(0);
				insert_inlined_table_name_cache[entry.table_id.index] = inlined_table_name;
			}
		}

		DuckLakeTableInfo table_info;
		if (inlined_table_name.empty()) {
			// no inlined table yet - create a new one
			// first fetch the table info
			auto current_snapshot = transaction.GetSnapshot();
			auto table_entry = transaction.GetCatalog().GetEntryById(transaction, current_snapshot, entry.table_id);
			if (table_entry) {
				auto &table = table_entry->Cast<DuckLakeTableEntry>();
				table_info = table.GetTableInfo();
				table_info.columns = table.GetTableColumns();
			} else {
				// We try from our added tables
				bool found = false;
				for (auto &new_table : new_tables) {
					if (new_table.id == entry.table_id) {
						table_info = new_table;
						found = true;
					}
				}
				if (!found) {
					throw InternalException("Writing inlined data for a table that cannot be found in the catalog");
				}
			}
			// write the new inlined table
			duckdb::string inlined_tables;
			duckdb::string inlined_table_queries;
			commit_snapshot.schema_version++;
			inlined_table_name =
			    GetInlinedTableQueries(commit_snapshot, table_info, inlined_tables, inlined_table_queries);
			batch_query += "INSERT INTO {METADATA_CATALOG}.ducklake_inlined_data_tables VALUES " + inlined_tables + ";";
			batch_query += inlined_table_queries;
			created_in_batch.insert(inlined_table_name);
		}

		const bool has_preserved_row_ids = entry.data->HasPreservedRowIds();
		if (!created_in_batch.count(inlined_table_name) &&
		    TryAppendInlinedData(commit_snapshot, inlined_table_name, entry, has_preserved_row_ids)) {
			continue;
		}

		// Build one cell list per row, then defer formatting to the shared helper.
		duckdb::vector<duckdb::string> cells_per_row;
		for (auto &chunk : entry.data->data->Chunks()) {
			for (duckdb::idx_t r = 0; r < chunk.size(); r++) {
				cells_per_row.push_back(DuckLakeUtil::ChunkRowToSQL(*this, context, chunk, r));
			}
		}
		batch_query += FormatInlinedDataInsert(inlined_table_name, entry.row_id_start, has_preserved_row_ids,
		                                       has_preserved_row_ids ? &entry.data->row_ids : nullptr, cells_per_row);
	}
	return batch_query;
}

namespace {

duckdb::unique_ptr<duckdb::DuckLakeMetadataManager>
CreateMoraineMetadataManager(duckdb::DuckLakeTransaction &transaction) {
	return duckdb::make_uniq<MoraineMetadataManager>(transaction);
}

} // namespace

void RegisterMoraineMetadataManager() {
	// One registry for the process, one extension load per database
	// instance, and a duplicate name throws.
	static std::once_flag registered;
	std::call_once(registered,
	               [] { duckdb::DuckLakeMetadataManager::Register("moraine", CreateMoraineMetadataManager); });
}

} // namespace moraine_duckdb
