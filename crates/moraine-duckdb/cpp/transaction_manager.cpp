#include "transaction_manager.hpp"

#include "duckdb/common/allocator.hpp"

#include "catalog.hpp"

#include <mutex>
#include <unordered_map>
#include <vector>

namespace moraine_duckdb {

MoraineTransaction::MoraineTransaction(duckdb::TransactionManager &manager, duckdb::ClientContext &context,
                                       MoraineSnapshotHandle *snapshot, MoraineCatalogHandle *catalog_handle)
    : duckdb::Transaction(manager, context), snapshot_(snapshot), catalog_handle_(catalog_handle) {
}

bool MoraineTransaction::SnapshotStamp(uint64_t &snapshot_id, uint64_t &batch_seq) {
	if (!stamp_resolved_) {
		stamp_resolved_ = true;
		if (snapshot_ != nullptr) {
			MoraineError err {};
			stamp_present_ =
			    moraine_snapshot_stamp(snapshot_, &stamp_snapshot_id_, &stamp_batch_seq_, &err) == MORAINE_OK;
			if (!stamp_present_ && err.message != nullptr) {
				moraine_error_free(err.message);
			}
		}
	}
	snapshot_id = stamp_snapshot_id_;
	batch_seq = stamp_batch_seq_;
	return stamp_present_;
}

MoraineTransaction::~MoraineTransaction() {
	// Release resources if normal transaction teardown did not.
	if (snapshot_ != nullptr) {
		moraine_snapshot_free(snapshot_);
		snapshot_ = nullptr;
	}
	if (staged_tx_ != nullptr) {
		moraine_tx_rollback(staged_tx_);
		staged_tx_ = nullptr;
	}
}

MoraineTxHandle *MoraineTransaction::OpenStagedTx() {
	if (staged_tx_ == nullptr) {
		MoraineTxHandle *tx = nullptr;
		MoraineError err {};
		// Cancellation requires a live client context.
		auto client = context.lock();
		auto code = moraine_tx_begin(catalog_handle_, snapshot_, &tx, client ? moraine_shim_is_interrupted : nullptr,
		                             client.get(), &err);
		if (code != MORAINE_OK) {
			ThrowMoraineError(err);
		}
		staged_tx_ = tx;
	}
	return staged_tx_;
}

MoraineTxHandle *MoraineTransaction::StagedTx() {
	std::lock_guard<std::mutex> guard(staged_state_lock_);
	return OpenStagedTx();
}

MoraineTxHandle *MoraineTransaction::StagedTxIfOpen() const {
	std::lock_guard<std::mutex> guard(staged_state_lock_);
	return staged_tx_;
}

MoraineTxHandle *MoraineTransaction::TakeStagedTx() {
	MoraineTxHandle *result = nullptr;
	{
		std::lock_guard<std::mutex> guard(staged_state_lock_);
		result = staged_tx_;
		staged_tx_ = nullptr;
	}
	// Retried statements register new runs.
	{
		std::lock_guard<std::mutex> guard(scanned_runs_lock_);
		scanned_runs_.clear();
	}
	return result;
}

uint64_t MoraineTransaction::RegisterScannedRows(const MetadataTableSpec &spec,
                                                 std::shared_ptr<const MetadataRows> rows) {
	std::lock_guard<std::mutex> guard(scanned_runs_lock_);
	auto base = next_scanned_row_id_;
	next_scanned_row_id_ += rows == nullptr ? 0 : rows->size();
	scanned_runs_.push_back(ScannedRun {&spec, base, std::move(rows), {}});
	return base;
}

uint64_t MoraineTransaction::RegisterScannedCursor(const MetadataTableSpec &spec, ScannedRowResolver resolver) {
	std::lock_guard<std::mutex> guard(scanned_runs_lock_);
	if (next_scanned_row_id_ > uint64_t(INT64_MAX) - kScannedRunCapacity) {
		throw duckdb::InvalidInputException("moraine: transaction exhausted metadata scan identities");
	}
	auto base = next_scanned_row_id_;
	next_scanned_row_id_ += kScannedRunCapacity;
	scanned_runs_.push_back(ScannedRun {&spec, base, nullptr, std::move(resolver)});
	return base;
}

const std::vector<duckdb::Value> *MoraineTransaction::ScannedRow(const MetadataTableSpec &spec, uint64_t row_id) const {
	std::lock_guard<std::mutex> guard(scanned_runs_lock_);
	// Newest first: a statement resolves against the scan it just ran.
	for (auto run = scanned_runs_.rbegin(); run != scanned_runs_.rend(); ++run) {
		if (run->spec != &spec || row_id < run->base) {
			continue;
		}
		auto index = row_id - run->base;
		if (run->resolver) {
			if (index < kScannedRunCapacity) {
				return run->resolver(index);
			}
			continue;
		}
		if (run->rows != nullptr && index < run->rows->size()) {
			return &(*run->rows)[index];
		}
	}
	return nullptr;
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> MoraineTransaction::GetCachedSchema(uint64_t schema_id) const {
	auto it = schema_cache_.find(schema_id);
	if (it == schema_cache_.end()) {
		return nullptr;
	}
	return it->second.get();
}

void MoraineTransaction::PutSchema(uint64_t schema_id, duckdb::unique_ptr<duckdb::SchemaCatalogEntry> entry) {
	schema_cache_[schema_id] = std::move(entry);
}

void MoraineTransaction::ForEachSchema(const std::function<void(duckdb::SchemaCatalogEntry &)> &callback) const {
	for (auto &entry : schema_cache_) {
		callback(*entry.second);
	}
}

void MoraineTransaction::ReleaseSnapshot() {
	if (snapshot_ != nullptr) {
		moraine_snapshot_free(snapshot_);
		snapshot_ = nullptr;
	}
}

MoraineTransactionManager::MoraineTransactionManager(duckdb::AttachedDatabase &db, MoraineCatalog &catalog)
    : duckdb::TransactionManager(db), catalog_(catalog) {
}

duckdb::unique_ptr<duckdb::TransactionManager>
MoraineTransactionManager::Create(duckdb::optional_ptr<duckdb::StorageExtensionInfo>, duckdb::AttachedDatabase &db,
                                  duckdb::Catalog &catalog) {
	return duckdb::make_uniq<MoraineTransactionManager>(db, catalog.Cast<MoraineCatalog>());
}

duckdb::Transaction &MoraineTransactionManager::StartTransaction(duckdb::ClientContext &context) {
	MoraineSnapshotHandle *snapshot = nullptr;
	MoraineError err {};
	auto code = moraine_snapshot_scoped(catalog_.Handle(), &snapshot, moraine_shim_is_interrupted, &context, &err);
	// Drain diagnostics for read-only workloads too.
	DrainMoraineLogs(context);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	auto transaction = duckdb::make_uniq<MoraineTransaction>(*this, context, snapshot, catalog_.Handle());
	auto &transaction_ref = *transaction;
	std::lock_guard<std::mutex> guard(lock_);
	active_transactions_.push_back(std::move(transaction));
	return transaction_ref;
}

duckdb::ErrorData MoraineTransactionManager::CommitTransaction(duckdb::ClientContext &context,
                                                               duckdb::Transaction &transaction) {
	// Drain diagnostics even when the commit fails.
	auto drain = [&context]() noexcept {
		DrainMoraineLogs(context);
	};
	// Release the catalog lock before blocking on commit I/O.
	MoraineTxHandle *staged = nullptr;
	{
		std::lock_guard<std::mutex> guard(lock_);
		for (auto it = active_transactions_.begin(); it != active_transactions_.end(); ++it) {
			if (it->get() == &transaction) {
				auto &moraine_tx = it->get()->Cast<MoraineTransaction>();
				staged = moraine_tx.TakeStagedTx();
				moraine_tx.ReleaseSnapshot();
				active_transactions_.erase(it);
				break;
			}
		}
	}
	if (staged == nullptr) {
		drain();
		return duckdb::ErrorData();
	}
	uint64_t new_snapshot_id = 0;
	MoraineError err {};
	// Cancellation before polling submits nothing. Once the call starts,
	// cancellation reports COMMIT_OUTCOME_UNKNOWN and the write may still land.
	auto code = moraine_tx_commit(staged, &new_snapshot_id, moraine_shim_is_interrupted, &context, &err);
	if (code == MORAINE_OK) {
		catalog_.RecordCommittedSnapshot(new_snapshot_id);
	}
	MoraineMemoryTally memory {};
	auto measured = code == MORAINE_OK && moraine_catalog_memory_tally(catalog_.Handle(), &memory) == MORAINE_OK;
	constexpr uint64_t ALLOCATOR_FLUSH_INDEX_ENTRIES = 100'000;
	constexpr uint64_t ALLOCATOR_FLUSH_STAGED_BYTES = 64ULL * 1024ULL * 1024ULL;
	if (code != MORAINE_OK || (measured && (memory.last_commit_index_entries >= ALLOCATOR_FLUSH_INDEX_ENTRIES ||
	                                        memory.last_commit_staged_bytes >= ALLOCATOR_FLUSH_STAGED_BYTES))) {
		duckdb::Allocator::FlushAll();
	}
	drain();
	if (code != MORAINE_OK) {
		try {
			ThrowMoraineError(err);
		} catch (std::exception &ex) {
			return duckdb::ErrorData(ex);
		}
	}
	return duckdb::ErrorData();
}

void MoraineTransactionManager::RollbackTransaction(duckdb::Transaction &transaction) {
	MoraineTxHandle *staged = nullptr;
	{
		std::lock_guard<std::mutex> guard(lock_);
		for (auto it = active_transactions_.begin(); it != active_transactions_.end(); ++it) {
			if (it->get() == &transaction) {
				auto &moraine_tx = it->get()->Cast<MoraineTransaction>();
				staged = moraine_tx.TakeStagedTx();
				moraine_tx.ReleaseSnapshot();
				active_transactions_.erase(it);
				break;
			}
		}
	}
	if (staged != nullptr) {
		moraine_tx_rollback(staged);
		duckdb::Allocator::FlushAll();
	}
}

void MoraineTransactionManager::Checkpoint(duckdb::ClientContext &, bool) {
	// Commits publish durable checkpoints through the core.
}

} // namespace moraine_duckdb
