#pragma once

#include "storage/ducklake_metadata_manager.hpp"

#include "duckdb/main/client_context_state.hpp"

namespace moraine_duckdb {

//! Names the context whose statement is driving the metadata connection
//! this is registered on. DuckLake creates that connection and never hands
//! it out, so its own interrupt flag is one nobody can set; work blocking
//! beneath it reads the driver's flag through this instead.
class ParentContextState : public duckdb::ClientContextState {
public:
	static constexpr const char *KEY = "moraine_parent_context";

	explicit ParentContextState(duckdb::weak_ptr<duckdb::ClientContext> parent_p) : parent(std::move(parent_p)) {
	}

	//! The context whose statement reached this catalog.
	duckdb::weak_ptr<duckdb::ClientContext> parent;
};

//! The metadata manager DuckLake creates for a lake whose metadata path
//! carries the `moraine:` prefix.
class MoraineMetadataManager : public duckdb::DuckLakeMetadataManager {
public:
	explicit MoraineMetadataManager(duckdb::DuckLakeTransaction &transaction);

	using duckdb::DuckLakeMetadataManager::Query;

	//! Records the driving context before the base runs anything on the
	//! metadata connection. Every metadata read reaches `Query`, including
	//! the commit batch; the nested attach is the one that does not.
	duckdb::unique_ptr<duckdb::QueryResult> Query(duckdb::string &query) override;
	duckdb::unique_ptr<duckdb::QueryResult> AttachMetadata(const duckdb::string &attach_query) override;

	//! Writes each table's inlined rows through the Appender API, falling
	//! back to the base `INSERT ... VALUES` batch for a table the same
	//! commit still has to create.
	duckdb::string
	WriteNewInlinedData(duckdb::DuckLakeSnapshot &commit_snapshot,
	                    const duckdb::vector<duckdb::DuckLakeInlinedDataInfo> &new_data,
	                    const duckdb::vector<duckdb::DuckLakeTableInfo> &new_tables,
	                    const duckdb::vector<duckdb::DuckLakeTableInfo> &new_inlined_data_tables_result) override;

private:
	//! One table's rows through the Appender. False means the caller must
	//! emit the SQL instead: no appender, or no rows.
	bool TryAppendInlinedData(duckdb::DuckLakeSnapshot &commit_snapshot, const duckdb::string &inlined_table_name,
	                          const duckdb::DuckLakeInlinedDataInfo &entry, bool has_preserved_row_ids);

	//! Registers [`ParentContextState`] on the metadata connection, once.
	void RecordDrivingContext();
};

//! Registers the manager under the prefix DuckLake extracts from a metadata
//! path, so `ducklake:moraine:<store>` resolves to it rather than to the
//! base one. Safe to call per database instance: the registry is
//! process-wide and refuses a name it already holds.
void RegisterMoraineMetadataManager();

} // namespace moraine_duckdb
