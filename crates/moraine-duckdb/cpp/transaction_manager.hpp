// One pinned Moraine read scope and staged write set per DuckDB transaction.
#pragma once

#include "duckdb.hpp"

#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include "metadata_tables.hpp"
#include "moraine_abi.h"

#include <mutex>
#include <map>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace moraine_duckdb {

class MoraineCatalog;

// Schema catalog entries must outlive every reference returned within the transaction.
class MoraineTransaction : public duckdb::Transaction {
public:
	MoraineTransaction(duckdb::TransactionManager &manager, duckdb::ClientContext &context,
	                   MoraineSnapshotHandle *snapshot, MoraineCatalogHandle *catalog_handle);
	~MoraineTransaction() override;

	MoraineSnapshotHandle *Snapshot() const {
		return snapshot_;
	}

	MoraineCatalogHandle *ReadHandle() const {
		auto scoped = moraine_snapshot_read_handle(snapshot_);
		return scoped && !StagedTxIfOpen() ? scoped : catalog_handle_;
	}

	// Index entries have no staged overlay; probes always use the original read point.
	MoraineCatalogHandle *IndexReadHandle() const {
		auto scoped = moraine_snapshot_read_handle(snapshot_);
		return scoped ? scoped : catalog_handle_;
	}

	bool SchemasLoaded() const {
		return schemas_loaded_;
	}
	void SetSchemasLoaded() {
		schemas_loaded_ = true;
	}
	duckdb::optional_ptr<duckdb::SchemaCatalogEntry> GetCachedSchema(uint64_t schema_id) const;
	void PutSchema(uint64_t schema_id, duckdb::unique_ptr<duckdb::SchemaCatalogEntry> entry);
	void ForEachSchema(const std::function<void(duckdb::SchemaCatalogEntry &)> &callback) const;

	// Releases the snapshot once, including during destructor cleanup.
	void ReleaseSnapshot();

	// Resolves the pinned store stamp once; false if absent or released.
	bool SnapshotStamp(uint64_t &snapshot_id, uint64_t &batch_seq);

	// Lazily opens the shared staged transaction for all writes.
	MoraineTxHandle *StagedTx();

	// Observes staged writes without opening a write transaction.
	MoraineTxHandle *StagedTxIfOpen() const;

	// Transfers ownership to the caller; null if no write transaction was opened.
	MoraineTxHandle *TakeStagedTx();

	// Assigns stable row IDs that remain addressable after later scans and writes.
	uint64_t RegisterScannedRows(const MetadataTableSpec &spec, std::shared_ptr<const MetadataRows> rows);
	using ScannedRowResolver = std::function<const std::vector<duckdb::Value> *(uint64_t)>;
	static constexpr uint64_t kScannedRunCapacity = uint64_t(1) << 40;
	uint64_t RegisterScannedCursor(const MetadataTableSpec &spec, ScannedRowResolver resolver);

	// The row a scan emitted `row_id` for, or null if no run of this
	// transaction covers it. A streamed row stays valid until the next lookup.
	const std::vector<duckdb::Value> *ScannedRow(const MetadataTableSpec &spec, uint64_t row_id) const;

private:
	// One scan's rows, at the id its first row was handed out at.
	struct ScannedRun {
		const MetadataTableSpec *spec;
		uint64_t base;
		std::shared_ptr<const MetadataRows> rows;
		ScannedRowResolver resolver;
	};

	MoraineSnapshotHandle *snapshot_;
	MoraineCatalogHandle *catalog_handle_;
	bool schemas_loaded_ = false;
	bool stamp_resolved_ = false;
	bool stamp_present_ = false;
	uint64_t stamp_snapshot_id_ = 0;
	uint64_t stamp_batch_seq_ = 0;
	std::unordered_map<uint64_t, duckdb::unique_ptr<duckdb::SchemaCatalogEntry>> schema_cache_;
	MoraineTxHandle *staged_tx_ = nullptr;
	// Serializes opening and taking the staged transaction.
	mutable std::mutex staged_state_lock_;
	// Ascending runs; executor scans and write sinks may access these concurrently.
	std::vector<ScannedRun> scanned_runs_;
	uint64_t next_scanned_row_id_ = 0;
	mutable std::mutex scanned_runs_lock_;

	// Caller holds staged_state_lock_.
	MoraineTxHandle *OpenStagedTx();
};

class MoraineTransactionManager : public duckdb::TransactionManager {
public:
	MoraineTransactionManager(duckdb::AttachedDatabase &db, MoraineCatalog &catalog);

	// The create_transaction_manager_t the storage extension registers.
	static duckdb::unique_ptr<duckdb::TransactionManager>
	Create(duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info, duckdb::AttachedDatabase &db,
	       duckdb::Catalog &catalog);

	duckdb::Transaction &StartTransaction(duckdb::ClientContext &context) override;
	duckdb::ErrorData CommitTransaction(duckdb::ClientContext &context, duckdb::Transaction &transaction) override;
	void RollbackTransaction(duckdb::Transaction &transaction) override;
	void Checkpoint(duckdb::ClientContext &context, bool force = false) override;

private:
	MoraineCatalog &catalog_;
	std::mutex lock_;
	std::vector<duckdb::unique_ptr<duckdb::Transaction>> active_transactions_;
};

} // namespace moraine_duckdb
