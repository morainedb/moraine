#include "scan.hpp"

namespace moraine_duckdb {

namespace {

// No per-scan state: init_global always throws before any state would be
// used.
struct MoraineScanGlobalState : public duckdb::GlobalTableFunctionState {
	idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> MoraineScanInitGlobal(duckdb::ClientContext &context,
                                                                           duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MoraineScanBindData>();
	throw duckdb::InvalidInputException(
	    "moraine: table \"%s\" data is served only through DuckLake, not the standalone attach — "
	    "attach the lake with ATTACH 'ducklake:moraine:%s' AS lake (DATA_PATH '<data-path>') "
	    "and query it as lake.%s",
	    bind_data.qualified_table_name, bind_data.store_path, bind_data.qualified_table_name);
}

// Never reached: MoraineScanInitGlobal always throws before the executor
// calls this. Present only because TableFunction's constructor requires a
// non-null function pointer.
void MoraineScanFunctionImpl(duckdb::ClientContext &context, duckdb::TableFunctionInput &data,
                             duckdb::DataChunk &output) {
	output.SetCardinality(0);
}

} // namespace

duckdb::unique_ptr<duckdb::FunctionData> MoraineScanBindData::Copy() const {
	auto result = duckdb::make_uniq<MoraineScanBindData>();
	result->qualified_table_name = qualified_table_name;
	result->store_path = store_path;
	result->table_entry = table_entry;
	return result;
}

bool MoraineScanBindData::Equals(const duckdb::FunctionData &other_p) const {
	auto &other = other_p.Cast<MoraineScanBindData>();
	return qualified_table_name == other.qualified_table_name && store_path == other.store_path &&
	       table_entry == other.table_entry;
}

duckdb::TableFunction MoraineScanFunction() {
	// No `bind`/`bind_replace` callback: the sole caller
	// (MoraineTableEntry::GetScanFunction) already produces complete bind data.
	auto function =
	    duckdb::TableFunction("moraine_scan", {}, MoraineScanFunctionImpl, nullptr, MoraineScanInitGlobal, nullptr);
	// Lets `LogicalGet::GetTable()` find the catalog entry behind this scan,
	// so DESCRIBE/SHOW can read its NOT NULL constraints (otherwise every
	// column looks nullable).
	function.get_bind_info = [](const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
		auto &data = bind_data->Cast<MoraineScanBindData>();
		if (data.table_entry != nullptr) {
			return duckdb::BindInfo(*data.table_entry);
		}
		return duckdb::BindInfo(duckdb::ScanType::EXTERNAL);
	};
	return function;
}

} // namespace moraine_duckdb
