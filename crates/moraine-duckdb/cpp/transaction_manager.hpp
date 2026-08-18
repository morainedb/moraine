// A duckdb::TransactionManager backed by moraine: snapshot-per-transaction.
// StartTransaction materializes one moraine_snapshot and hands out a
// MoraineTransaction that owns it; CommitTransaction commits the staged tx
// (if one was opened) and RollbackTransaction discards it, both releasing
// the snapshot.
#pragma once

#include "duckdb.hpp"

// Defines duckdb::StorageExtensionInfo, named by
// MoraineTransactionManager::Create's signature.
#include "duckdb/storage/storage_extension.hpp"

#include "metadata_tables.hpp"
#include "moraine_abi.h"

#include <mutex>
#include <map>
#include <unordered_map>
#include <vector>

namespace moraine_duckdb {

class MoraineCatalog;

// One DuckDB transaction's view of a moraine catalog: the snapshot
// materialized at StartTransaction, plus the schema-entry cache built
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

	// Lazily opens (on the first call) the one staged-row transaction this
	// DuckDB transaction stages every write into, and returns it. Every
	// subsequent INSERT/UPDATE/DELETE within the same DuckDB transaction
	// reuses it: one moraine staged tx per DuckDB transaction.
	//
	// Drops every held materialization, because a caller reaching for the
	// staged tx without naming a table may stage against any of them.
	// `StagedTxFor` is the narrower form.
	MoraineTxHandle *StagedTx();

	// `StagedTx` for a caller staging against exactly `spec`. Only that
	// table's overlay can change, so only its materialization is dropped.
	MoraineTxHandle *StagedTxFor(const MetadataTableSpec &spec);

	// The staged tx if one has been opened, else null — a peek for read
	// paths that must observe the transaction's own staged writes without
	// ever opening a write transaction themselves.
	MoraineTxHandle *StagedTxIfOpen() const {
		return staged_tx_;
	}

	// Hands ownership of the staged tx (if one was opened) to the caller,
	// clearing this transaction's reference so the destructor's defensive
	// rollback becomes a no-op. Returns null if no write ever opened one.
	MoraineTxHandle *TakeStagedTx();

	// The one materialization of a synthesized `ducklake_*` table this
	// transaction serves every reader of it from, or null before its first
	// scan.
	//
	// Held across both read points a transaction can have, because neither
	// moves under a materialization: before any write, the transaction's
	// snapshot; after, the staged tx's own pinned read point. Opening a
	// staged tx crosses between them and drops everything held; staging a
	// row drops the table it lands in, whose overlay just changed.
	// `MetadataRowsFor` (metadata_tables.hpp) is the only caller of this
	// pair and states what rests on the sharing.
	//
	// A live-narrowed materialization is a different row set from the full
	// one, so `live_only` keys them apart rather than letting either stand
	// in for the other.
	std::shared_ptr<const MetadataRows> GetMetadataRows(const MetadataTableSpec &spec, bool live_only = false) const;
	void PutMetadataRows(const MetadataTableSpec &spec, std::shared_ptr<const MetadataRows> rows,
	                     bool live_only = false);

	// Takes the rows one metadata scan emits row ids for, returning the id
	// its first row carries. Ids come from a counter this transaction never
	// rewinds, so a run stays addressable however its table is scanned or
	// staged into afterwards — the write that resolves those ids reaches
	// the very rows the scan handed out, not a second materialization of
	// them.
	uint64_t RegisterScannedRows(const MetadataTableSpec &spec, std::shared_ptr<const MetadataRows> rows);

	// The row a scan emitted `row_id` for, or null if no run of this
	// transaction covers it.
	const std::vector<duckdb::Value> *ScannedRow(const MetadataTableSpec &spec, uint64_t row_id) const;

private:
	// One scan's rows, at the id its first row was handed out at.
	struct ScannedRun {
		const MetadataTableSpec *spec;
		uint64_t base;
		std::shared_ptr<const MetadataRows> rows;
	};

	MoraineSnapshotHandle *snapshot_;
	MoraineCatalogHandle *catalog_handle_;
	bool schemas_loaded_ = false;
	std::unordered_map<uint64_t, duckdb::unique_ptr<duckdb::SchemaCatalogEntry>> schema_cache_;
	MoraineTxHandle *staged_tx_ = nullptr;
	std::map<std::pair<const MetadataTableSpec *, bool>, std::shared_ptr<const MetadataRows>> metadata_rows_;
	// Registered runs, by ascending base, and the next id to hand out.
	// Guarded, because scans of one transaction init on the executor's
	// threads while another statement's Sink resolves against them.
	std::vector<ScannedRun> scanned_runs_;
	uint64_t next_scanned_row_id_ = 0;
	mutable std::mutex scanned_runs_lock_;

	// Opens the staged tx if this transaction has none, leaving what the
	// two public accessors hold to them.
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
