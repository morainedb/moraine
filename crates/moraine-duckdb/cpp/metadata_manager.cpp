#include "metadata_manager.hpp"

#include "storage/ducklake_transaction.hpp"

#include <mutex>

namespace moraine_duckdb {

MoraineMetadataManager::MoraineMetadataManager(duckdb::DuckLakeTransaction &transaction)
    : duckdb::DuckLakeMetadataManager(transaction) {
}

namespace {

duckdb::unique_ptr<duckdb::DuckLakeMetadataManager> CreateMoraineMetadataManager(
    duckdb::DuckLakeTransaction &transaction) {
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
