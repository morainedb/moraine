#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace moraine_duckdb {

//! The metadata manager DuckLake creates for a lake whose metadata path
//! carries the `moraine:` prefix.
class MoraineMetadataManager : public duckdb::DuckLakeMetadataManager {
public:
	explicit MoraineMetadataManager(duckdb::DuckLakeTransaction &transaction);

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
};

//! Registers the manager under the prefix DuckLake extracts from a metadata
//! path, so `ducklake:moraine:<store>` resolves to it rather than to the
//! base one. Safe to call per database instance: the registry is
//! process-wide and refuses a name it already holds.
void RegisterMoraineMetadataManager();

} // namespace moraine_duckdb
