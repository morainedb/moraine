#pragma once

#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction_changes.hpp"

#include "duckdb/main/client_context_state.hpp"

namespace moraine_duckdb {

struct MetadataTableSpec;

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

	using duckdb::DuckLakeMetadataManager::GetSnapshot;

	//! Reads the pinned head and catalog version without planning metadata SQL.
	duckdb::unique_ptr<duckdb::DuckLakeSnapshot> GetSnapshot() override;

	//! The head snapshot by the id the metadata transaction pinned when it
	//! began, which is what DuckLake's `MAX(snapshot_id)` over every retained
	//! snapshot resolves to at that read point.
	duckdb::string GetLatestSnapshotQuery() const override;

	//! DuckLake's server-side commit route, which the constructor opts
	//! into: the commit's rows are generated and driven as on the
	//! client-side route, under DuckLake's own retry budget — which a
	//! data-only commit never spends, since the catalog's batch re-drives
	//! its benign races itself — and the snapshot id the catalog landed is
	//! then reported back, since the batch may have moved it above the id
	//! DuckLake stamped.
	void FlushChangesServerSide(duckdb::DuckLakeTransaction &flush_transaction,
	                            duckdb::DuckLakeSnapshot transaction_snapshot,
	                            const duckdb::TransactionChangeInformation &transaction_changes,
	                            const duckdb::DuckLakeRetryConfig &retry_config) override;

	bool TryGetTableStats(duckdb::DuckLakeSnapshot snapshot, duckdb::TableIndex table_id,
	                      duckdb::vector<duckdb::DuckLakeGlobalStatsInfo> &stats) override;
	bool TryWriteAppendStats(const duckdb::DuckLakeGlobalStatsInfo &stats) override;
	void FlushAppendStats() override;
	void ClearAppendStats() override;
	bool TryWriteSnapshotChanges(duckdb::DuckLakeSnapshot snapshot, const duckdb::SnapshotChangeInfo &changes,
	                             const duckdb::DuckLakeSnapshotCommit &commit_info) override;

	//! Stages the snapshot using the same row contract as metadata INSERT.
	bool TryWriteSnapshot(duckdb::DuckLakeSnapshot snapshot) override;
	//! Appends inline chunks after their table definitions have been staged.
	void FlushInlinedAppends(duckdb::DuckLakeSnapshot snapshot) override;
	void ClearInlinedAppends() override;

	//! Queues inline chunks and returns any table-creation SQL they require.
	duckdb::string
	WriteNewInlinedData(duckdb::DuckLakeSnapshot &commit_snapshot,
	                    const duckdb::vector<duckdb::DuckLakeInlinedDataInfo> &new_data,
	                    const duckdb::vector<duckdb::DuckLakeTableInfo> &new_tables,
	                    const duckdb::vector<duckdb::DuckLakeTableInfo> &new_inlined_data_tables_result,
	                    duckdb::vector<duckdb::unique_ptr<duckdb::SQLStatement>> &inlined_inserts) override;

private:
	const MetadataTableSpec &MetadataSpec(const char *name);
	std::vector<std::vector<duckdb::Value>> TableStatisticsRows(const char *name, duckdb::TableIndex table_id);
	duckdb::vector<duckdb::DuckLakeGlobalStatsInfo> pending_statistics;

	struct PendingInlineAppend {
		duckdb::string table_name;
		// The transaction owns the data until its commit attempt completes.
		duckdb::DuckLakeInlinedDataInfo entry;
	};
	duckdb::vector<PendingInlineAppend> pending_inline_appends;

	//! Returns the latest committed inline registration at the pinned read point.
	duckdb::string LatestInlineTableName(duckdb::idx_t table_id);

	//! Appends one table's rows; false if there is no Appender support or no rows.
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
