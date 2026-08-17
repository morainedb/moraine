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
	std::shared_ptr<const MetadataRows> GetMetadataRows(const MetadataTableSpec &spec) const;
	void PutMetadataRows(const MetadataTableSpec &spec, std::shared_ptr<const MetadataRows> rows);

private:
	MoraineSnapshotHandle *snapshot_;
	MoraineCatalogHandle *catalog_handle_;
	bool schemas_loaded_ = false;
	std::unordered_map<uint64_t, duckdb::unique_ptr<duckdb::SchemaCatalogEntry>> schema_cache_;
	MoraineTxHandle *staged_tx_ = nullptr;
	std::unordered_map<const MetadataTableSpec *, std::shared_ptr<const MetadataRows>> metadata_rows_;

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
