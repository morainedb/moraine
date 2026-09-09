// The DuckDB extension entry point. Registers the bundled patched DuckLake
// and then moraine's StorageExtension (attach type `moraine`) on the loading
// database, so one `LOAD moraine` serves `ducklake:moraine:` attaches. The
// extension toolchain exports only this file's `moraine_duckdb_cpp_init`
// symbol; the C++ shim reaches the moraine core through the C ABI in
// moraine_abi.h, and DuckDB is statically linked into the loadable by the
// toolchain.
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_install_info.hpp"
#include "duckdb/main/extension_manager.hpp"
#include "ducklake_extension.hpp"

namespace moraine_duckdb {
// Defined in storage_extension.cpp.
void RegisterMoraineStorageExtension(duckdb::DBConfig &config);
// Defined in census.cpp.
void RegisterMoraineCensusFunctions(duckdb::ExtensionLoader &loader);
// Defined in index_functions.cpp.
void RegisterMoraineIndexFunctions(duckdb::ExtensionLoader &loader);
// Defined in delete_located.cpp.
void RegisterMoraineDeleteLocatedFunction(duckdb::ExtensionLoader &loader);
void RegisterMoraineRowsAtFunction(duckdb::ExtensionLoader &loader);
void RegisterMoraineUpdateFunction(duckdb::ExtensionLoader &loader);
// Defined in optimizer.cpp.
void RegisterMoraineOptimizer(duckdb::DBConfig &config);
void RegisterMoraineMaintenanceFunctions(duckdb::ExtensionLoader &loader);
void RegisterMoraineMigrateFunction(duckdb::ExtensionLoader &loader);
// Defined in checkpoints.cpp.
void RegisterMoraineCheckpointFunctions(duckdb::ExtensionLoader &loader);
} // namespace moraine_duckdb

namespace duckdb {

// Registers the bundled DuckLake and records it as the loaded `ducklake`
// extension, so a later `LOAD ducklake` is a no-op rather than a second
// registration. A standalone DuckLake loaded earlier lacks the patches and
// collides on its log type, so it is refused.
static void LoadBundledDuckLake(ExtensionLoader &loader) {
	auto load = ExtensionManager::Get(loader.GetDatabaseInstance()).BeginLoad("ducklake");
	if (!load) {
		throw InvalidInputException("moraine bundles its own DuckLake; LOAD moraine without loading ducklake");
	}
	try {
		DucklakeExtension ducklake;
		ducklake.Load(loader);
		ExtensionInstallInfo install_info;
		install_info.mode = ExtensionInstallMode::STATICALLY_LINKED;
		install_info.version = ducklake.Version();
		load->FinishLoad(install_info);
	} catch (std::exception &error) {
		load->LoadFail(ErrorData(error));
		throw;
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	// DuckLake first: moraine's attach type resolves the storage extension
	// it registers.
	LoadBundledDuckLake(loader);
	loader.SetDescription("moraine: a SlateDB-backed DuckLake catalog, DuckLake bundled");
	moraine_duckdb::RegisterMoraineStorageExtension(loader.GetDatabaseInstance().config);
	moraine_duckdb::RegisterMoraineOptimizer(loader.GetDatabaseInstance().config);
	moraine_duckdb::RegisterMoraineCensusFunctions(loader);
	moraine_duckdb::RegisterMoraineIndexFunctions(loader);
	moraine_duckdb::RegisterMoraineDeleteLocatedFunction(loader);
	moraine_duckdb::RegisterMoraineRowsAtFunction(loader);
	moraine_duckdb::RegisterMoraineUpdateFunction(loader);
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
