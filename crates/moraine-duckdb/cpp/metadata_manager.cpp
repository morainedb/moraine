#include "metadata_manager.hpp"

#include "catalog.hpp"
#include "transaction_manager.hpp"

#include "common/index.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_inlined_data.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include <algorithm>
#include <array>
#include <mutex>

namespace moraine_duckdb {

const MetadataTableSpec &MoraineMetadataManager::MetadataSpec(const char *name) {
	auto &context = *transaction.GetConnection().context;
	auto &catalog = ResolveMoraineCatalog(context, transaction.GetCatalog().MetadataDatabaseName());
	for (const auto &spec : MoraineMetadataTableSpecs(catalog.ServesExtendedCatalog())) {
		if (std::string(spec.name) == name) {
			return spec;
		}
	}
	throw duckdb::InternalException("moraine: missing metadata table %s", name);
}

MoraineMetadataManager::MoraineMetadataManager(duckdb::DuckLakeTransaction &transaction)
    : duckdb::DuckLakeMetadataManager(transaction) {
	// Every commit finishes through FlushChangesServerSide below, which
	// is where the id the catalog landed can reach DuckLake.
	transaction.GetCatalog().SetRetrialsServerSide(true);
}

void MoraineMetadataManager::FlushChangesServerSide(duckdb::DuckLakeTransaction &flush_transaction,
                                                    duckdb::DuckLakeSnapshot transaction_snapshot,
                                                    const duckdb::TransactionChangeInformation &transaction_changes,
                                                    const duckdb::DuckLakeRetryConfig &retry_config) {
	// Resolved while the metadata connection's transaction is still open:
	// the loop commits it, and a catalog lookup needs one.
	auto &lake = transaction.GetCatalog();
	auto &catalog = ResolveMoraineCatalog(*transaction.GetConnection().context, lake.MetadataDatabaseName());
	flush_transaction.RunCommitLoop(transaction_snapshot, transaction_changes, retry_config);
	// The loop recorded the id it stamped; the catalog's is authoritative.
	// Monotone: the value is the catalog's last commit, which another
	// connection may already have moved past this one.
	auto landed = catalog.LastCommittedSnapshot();
	auto recorded = lake.GetLastCommittedSnapshotId();
	auto current = recorded.IsNull() ? 0 : recorded.GetValue<uint64_t>();
	if (landed > current) {
		lake.SetCommittedSnapshotId(static_cast<duckdb::idx_t>(landed));
	}
}

duckdb::unique_ptr<duckdb::DuckLakeSnapshot> MoraineMetadataManager::GetSnapshot() {
	RecordDrivingContext();
	auto &context = *transaction.GetConnection().context;
	auto &catalog = ResolveMoraineCatalog(context, transaction.GetCatalog().MetadataDatabaseName());
	auto &moraine_transaction = catalog.GetCatalogTransaction(context).transaction->Cast<MoraineTransaction>();
	uint64_t snapshot_id = 0;
	uint64_t batch_seq = 0;
	if (moraine_transaction.StagedTxIfOpen() || !moraine_transaction.SnapshotStamp(snapshot_id, batch_seq)) {
		return duckdb::DuckLakeMetadataManager::GetSnapshot();
	}

	const auto &snapshot_spec = MetadataSpec("ducklake_snapshot");
	MoraineMetadataCursor *cursor = nullptr;
	MoraineError error {};
	if (moraine_metadata_cursor_open(moraine_transaction.ReadHandle(), snapshot_spec.write_table_kind, kEveryVersion,
	                                 snapshot_id, &cursor, moraine_shim_is_interrupted, &context,
	                                 &error) != MORAINE_OK) {
		ThrowMoraineError(error);
	}
	std::unique_ptr<MoraineMetadataCursor, decltype(&moraine_metadata_cursor_free)> owned_cursor(
	    cursor, moraine_metadata_cursor_free);
	auto snapshots = snapshot_spec.page_provider(cursor, moraine_shim_is_interrupted, &context);
	if (snapshots.size() != 1 || snapshots[0][0].GetValue<uint64_t>() != snapshot_id) {
		throw duckdb::InvalidInputException("Corrupt DuckLake - pinned head snapshot is missing or ambiguous");
	}

	const auto &metadata_spec = MetadataSpec("ducklake_metadata");
	auto metadata = MetadataRowsFor(context, catalog, metadata_spec);
	duckdb::string catalog_version;
	for (const auto &row : *metadata) {
		if (!row[0].IsNull() && row[0].GetValue<duckdb::string>() == "version" && !row[1].IsNull()) {
			catalog_version = std::max(catalog_version, row[1].GetValue<duckdb::string>());
		}
	}
	auto attached_version = duckdb::DuckLakeVersionToString(transaction.GetCatalog().GetDuckLakeVersion());
	if (!catalog_version.empty() && catalog_version != attached_version) {
		throw duckdb::InvalidInputException(
		    "The catalog was migrated by another connection from version %s to %s, reattach to pick up the new version",
		    attached_version, catalog_version);
	}
	const auto &row = snapshots[0];
	return duckdb::make_uniq<duckdb::DuckLakeSnapshot>(
	    row[0].GetValue<duckdb::idx_t>(), row[2].GetValue<duckdb::idx_t>(), row[3].GetValue<duckdb::idx_t>(),
	    row[4].GetValue<duckdb::idx_t>());
}

duckdb::string MoraineMetadataManager::GetLatestSnapshotQuery() const {
	// The metadata connection's transaction is open here, so its catalog
	// transaction either exists or starts on the same pinned head the query
	// would read.
	auto &context = *transaction.GetConnection().context;
	auto &catalog = ResolveMoraineCatalog(context, transaction.GetCatalog().MetadataDatabaseName());
	auto &moraine_transaction = catalog.GetCatalogTransaction(context).transaction->Cast<MoraineTransaction>();
	uint64_t snapshot_id = 0;
	uint64_t batch_seq = 0;
	if (!moraine_transaction.SnapshotStamp(snapshot_id, batch_seq)) {
		return duckdb::DuckLakeMetadataManager::GetLatestSnapshotQuery();
	}
	return duckdb::StringUtil::Format(R"(SELECT snapshot_id, schema_version, next_catalog_id, next_file_id,
(SELECT MAX(value) FROM {METADATA_CATALOG}.ducklake_metadata WHERE key = 'version')
FROM {METADATA_CATALOG}.ducklake_snapshot
WHERE snapshot_id = %llu;)",
	                                  static_cast<unsigned long long>(snapshot_id));
}

void MoraineMetadataManager::RecordDrivingContext() {
	auto &registered = *transaction.GetConnection().context->registered_state;
	if (registered.Get<ParentContextState>(ParentContextState::KEY)) {
		return;
	}
	registered.Insert(ParentContextState::KEY, duckdb::make_shared_ptr<ParentContextState>(transaction.context));
}

duckdb::unique_ptr<duckdb::QueryResult> MoraineMetadataManager::Query(duckdb::string &query) {
	RecordDrivingContext();
	return duckdb::DuckLakeMetadataManager::Query(query);
}

duckdb::unique_ptr<duckdb::QueryResult> MoraineMetadataManager::AttachMetadata(const duckdb::string &attach_query) {
	RecordDrivingContext();
	return duckdb::DuckLakeMetadataManager::AttachMetadata(attach_query);
}

bool MoraineMetadataManager::TryWriteSnapshot(duckdb::DuckLakeSnapshot snapshot) {
	auto &context = *transaction.GetConnection().context;
	auto &catalog = ResolveMoraineCatalog(context, transaction.GetCatalog().MetadataDatabaseName());
	auto &moraine_transaction = catalog.GetCatalogTransaction(context).transaction->Cast<MoraineTransaction>();
	std::array<MoraineCell, 5> cells {};
	for (auto &cell : cells) {
		cell.kind = 1;
	}
	cells[0].u64_value = snapshot.snapshot_id;
	cells[1].kind = 2;
	cells[1].i64_value = duckdb::MetaTransaction::Get(context).GetCurrentTransactionStartTimestamp().value;
	cells[2].u64_value = snapshot.schema_version;
	cells[3].u64_value = snapshot.next_catalog_id;
	cells[4].u64_value = snapshot.next_file_id;
	MoraineError error {};
	if (moraine_tx_stage(moraine_transaction.StagedTx(), /* snapshot */ 0, /* insert */ 0, cells.data(), cells.size(),
	                     &error) != MORAINE_OK) {
		ThrowMoraineError(error);
	}
	return true;
}

void MoraineMetadataManager::ClearInlinedAppends() {
	pending_inline_appends.clear();
}

void MoraineMetadataManager::FlushInlinedAppends(duckdb::DuckLakeSnapshot snapshot) {
	auto appends = std::move(pending_inline_appends);
	pending_inline_appends.clear();
	for (auto &append : appends) {
		if (!TryAppendInlinedData(snapshot, append.table_name, append.entry, append.entry.data->HasPreservedRowIds())) {
			throw duckdb::InternalException("moraine: queued inline data could not be appended");
		}
	}
}

duckdb::string MoraineMetadataManager::LatestInlineTableName(duckdb::idx_t table_id) {
	auto &context = *transaction.GetConnection().context;
	auto &catalog = ResolveMoraineCatalog(context, transaction.GetCatalog().MetadataDatabaseName());
	auto rows = MetadataRowsFor(context, catalog, MetadataSpec("ducklake_inlined_data_tables"));
	duckdb::optional_idx latest_version;
	duckdb::string table_name;
	for (const auto &row : *rows) {
		if (row[0].GetValue<duckdb::idx_t>() != table_id) {
			continue;
		}
		auto version = row[2].GetValue<duckdb::idx_t>();
		if (!latest_version.IsValid() || version > latest_version.GetIndex()) {
			latest_version = version;
			table_name = row[1].GetValue<duckdb::string>();
		}
	}
	return table_name;
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

	Appender appender(connection, duckdb::Identifier(catalog.MetadataDatabaseName()), duckdb::Identifier(schema_name),
	                  duckdb::Identifier(inlined_table_name));
	DataChunk append_chunk;
	append_chunk.Initialize(Allocator::DefaultAllocator(), append_types);

	auto row_id = entry.row_id_start;
	auto snapshot_id = static_cast<int64_t>(commit_snapshot.snapshot_id);
	duckdb::idx_t position = 0;
	for (auto &chunk : collection.Chunks()) {
		auto count = chunk.size();
		append_chunk.Reset();
		auto row_ids = FlatVector::GetDataMutable<int64_t>(append_chunk.data[0]);
		auto snapshots = FlatVector::GetDataMutable<int64_t>(append_chunk.data[1]);
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
		append_chunk.SetChildCardinality(count);
		appender.AppendDataChunk(append_chunk);
	}
	appender.Close();
	return true;
}

duckdb::string MoraineMetadataManager::WriteNewInlinedData(
    duckdb::DuckLakeSnapshot &commit_snapshot, const duckdb::vector<duckdb::DuckLakeInlinedDataInfo> &new_data,
    const duckdb::vector<duckdb::DuckLakeTableInfo> &new_tables,
    const duckdb::vector<duckdb::DuckLakeTableInfo> &new_inlined_data_tables_result,
    duckdb::vector<duckdb::unique_ptr<duckdb::SQLStatement>> &inlined_inserts) {
	using duckdb::DuckLakeTableEntry;
	using duckdb::DuckLakeTableInfo;
	using duckdb::InternalException;

	duckdb::string batch_query;
	if (new_data.empty()) {
		return batch_query;
	}

	for (auto &entry : new_data) {
		duckdb::string inlined_table_name;
		for (auto &inlined_table : new_inlined_data_tables_result) {
			if (inlined_table.id == entry.table_id) {
				inlined_table_name = InlinedTableNameFor(inlined_table.id.index, commit_snapshot.schema_version);
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
			inlined_table_name = LatestInlineTableName(entry.table_id.index);
			if (!inlined_table_name.empty()) {
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
			duckdb::vector<duckdb::string> inlined_tables;
			duckdb::string inlined_table_queries;
			commit_snapshot.schema_version++;
			inlined_table_name =
			    GetInlinedTableQueries(commit_snapshot, table_info, inlined_tables, inlined_table_queries);
			batch_query += InsertValuesSql("ducklake_inlined_data_tables", inlined_tables);
			batch_query += inlined_table_queries;
		}

		if (entry.data && entry.data->data && entry.data->data->Count() > 0) {
			pending_inline_appends.push_back({inlined_table_name, entry});
		}
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
