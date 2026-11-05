// A duckdb::TransactionManager backed by moraine: snapshot-per-transaction.
// StartTransaction pins one moraine read scope and hands out a
// MoraineTransaction that owns it; CommitTransaction commits the staged tx
// (if one was opened) and RollbackTransaction discards it, both releasing
// the snapshot.
#pragma once

#include "duckdb.hpp"

// Defines duckdb::StorageExtensionInfo, named by
// MoraineTransactionManager::Create's signature.
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

// One DuckDB transaction's view of a moraine catalog: the snapshot
// pinned at StartTransaction, plus the schema-entry cache built
// lazily against it. Cached CatalogEntry objects must outlive every
// reference to them returned within the transaction.
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

	// The handle index probes read through: the revision this transaction
	// started on, whether or not it has staged writes since. Index entries
	// carry no staged overlay, so the pin stays the one view consistent with
	// the DuckLake snapshot the transaction scans at.
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

	// Frees the snapshot and marks it released, so the destructor's
	// defensive free becomes a no-op.
	void ReleaseSnapshot();

	// The store stamp this transaction's snapshot stands at, resolved once
	// from the snapshot rather than read from the store per table. False
	// when the snapshot is gone or reports no stamp.
	bool SnapshotStamp(uint64_t &snapshot_id, uint64_t &batch_seq);

	// Lazily opens (on the first call) the one staged-row transaction this
	// DuckDB transaction stages every write into, and returns it. Every
	// subsequent INSERT/UPDATE/DELETE within the same DuckDB transaction
	// reuses it: one moraine staged tx per DuckDB transaction.
	//
	// For a caller staging only `inline/*` rows. No metadata table's
	// overlay reads those, so every held materialization stands.
	MoraineTxHandle *StagedTxForInline();

	// The same, for a caller staging against exactly `spec`. Only that
	// table's overlay can change, so only its materialization is dropped.
	MoraineTxHandle *StagedTxFor(const MetadataTableSpec &spec);

	// The staged tx if one has been opened, else null — a peek for read
	// paths that must observe the transaction's own staged writes without
	// ever opening a write transaction themselves.
	MoraineTxHandle *StagedTxIfOpen() const;

	// Hands ownership of the staged tx (if one was opened) to the caller,
	// clearing this transaction's reference so the destructor's defensive
	// rollback becomes a no-op. Returns null if no write ever opened one.
	MoraineTxHandle *TakeStagedTx();

	// Takes the rows one metadata scan emits row ids for, returning the id
	// its first row carries. Ids come from a counter this transaction never
	// rewinds, so a run stays addressable however its table is scanned or
	// staged into afterwards — the write that resolves those ids reaches
	// the very rows the scan handed out, not a second materialization of
	// them.
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
	// Registered runs, by ascending base, and the next id to hand out.
	// Guarded, because scans of one transaction init on the executor's
	// threads while another statement's Sink resolves against them.
	std::vector<ScannedRun> scanned_runs_;
	uint64_t next_scanned_row_id_ = 0;
	mutable std::mutex scanned_runs_lock_;

	// Opens the staged tx if this transaction has none, leaving what the
	// two public accessors hold to them. Caller holds `staged_state_lock_`.
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
