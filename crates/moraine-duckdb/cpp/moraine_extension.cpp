// The DuckDB extension entry point. Registers moraine's StorageExtension
// (attach type `moraine`) on the loading database. The extension toolchain
// exports only this file's `moraine_duckdb_cpp_init` symbol; the C++ shim
// reaches the moraine core through the C ABI in moraine_abi.h, and DuckDB is
// statically linked into the loadable by the toolchain.
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace moraine_duckdb {
// Defined in storage_extension.cpp.
void RegisterMoraineStorageExtension(duckdb::DBConfig &config);
// Defined in census.cpp.
void RegisterMoraineCensusFunctions(duckdb::ExtensionLoader &loader);
// Defined in index_functions.cpp.
void RegisterMoraineIndexFunctions(duckdb::ExtensionLoader &loader);
// Defined in optimizer.cpp.
void RegisterMoraineOptimizer(duckdb::DBConfig &config);
void RegisterMoraineMaintenanceFunctions(duckdb::ExtensionLoader &loader);
void RegisterMoraineMigrateFunction(duckdb::ExtensionLoader &loader);
// Defined in checkpoints.cpp.
void RegisterMoraineCheckpointFunctions(duckdb::ExtensionLoader &loader);
} // namespace moraine_duckdb

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("moraine: a SlateDB-backed DuckLake catalog");
	moraine_duckdb::RegisterMoraineStorageExtension(loader.GetDatabaseInstance().config);
	moraine_duckdb::RegisterMoraineOptimizer(loader.GetDatabaseInstance().config);
	moraine_duckdb::RegisterMoraineCensusFunctions(loader);
	moraine_duckdb::RegisterMoraineIndexFunctions(loader);
	moraine_duckdb::RegisterMoraineMaintenanceFunctions(loader);
	moraine_duckdb::RegisterMoraineMigrateFunction(loader);
	moraine_duckdb::RegisterMoraineCheckpointFunctions(loader);
}

class MoraineExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override {
		LoadInternal(loader);
	}
	std::string Name() override {
		return "moraine";
	}
	std::string Version() const override {
#ifdef EXT_VERSION_MORAINE
		return EXT_VERSION_MORAINE;
#else
		return "";
#endif
	}
};

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(moraine, loader) {
	duckdb::LoadInternal(loader);
}
}
