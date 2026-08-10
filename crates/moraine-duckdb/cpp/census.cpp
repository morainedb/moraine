// moraine_store_census: what a store weighs, subspace by subspace.
//
// Measurement rather than maintenance, so it is a function of its own
// rather than a step: an operator runs it against a store whose size is
// unexplained, reads which subspace holds the bulk, and only then decides
// whether the answer is a store merge, a DuckLake cleanup, or neither. It
// serves a read-only attach, which is the attach shape an operator
// investigating a production store has.
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "catalog.hpp"
#include "moraine_abi.h"
#include "owned_array.hpp"

namespace moraine_duckdb {

namespace {

struct CensusBindData : public duckdb::FunctionData {
	std::string catalog_name;
	// Adds the scanning leg, which costs a full read of the store. The
	// default reads the manifest alone.
	bool count_live_entries = false;
	uint64_t manifest_id = 0;
	// Store-wide, repeated on every row: the object store holds bytes no
	// subspace accounts for, and a slow attach that no merge will fix is
	// exactly the case where they dominate.
	MoraineStoreObjects objects {};

	struct Row {
		std::string subspace;
		uint64_t bytes;
		uint32_t l0_ssts;
		uint32_t sorted_runs;
		uint32_t sorted_run_ssts;
		bool has_live;
		uint64_t live_keys;
		uint64_t live_key_bytes;
		uint64_t live_value_bytes;
		uint64_t scheduled_files;
	};
	std::vector<Row> rows;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CensusBindData>();
		*result = *this;
		return result;
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<CensusBindData>();
		return catalog_name == other.catalog_name && count_live_entries == other.count_live_entries;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CensusBind(duckdb::ClientContext &context,
                                                    duckdb::TableFunctionBindInput &input,
                                                    duckdb::vector<duckdb::LogicalType> &return_types,
                                                    duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<CensusBindData>();
	if (input.inputs[0].IsNull()) {
		throw duckdb::BinderException("moraine_store_census: the lake name must not be NULL");
	}
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();

	auto live = input.named_parameters.find("live");
	if (live != input.named_parameters.end() && !live->second.IsNull()) {
		bind_data->count_live_entries = live->second.GetValue<bool>();
	}

	auto handle = ResolveMoraineCatalog(context, bind_data->catalog_name).Handle();
	OwnedArray<MoraineSubspaceCensus> measured(moraine_store_census_free);
	MoraineError err {};
	auto code = moraine_store_census(handle, bind_data->count_live_entries, measured.OutItems(), measured.OutLen(),
	                                 &bind_data->manifest_id, &bind_data->objects, moraine_shim_is_interrupted,
	                                 &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	for (auto &row : measured) {
		bind_data->rows.push_back({std::string(row.subspace), row.bytes, row.l0_ssts, row.sorted_runs,
		                           row.sorted_run_ssts, row.has_live, row.live_keys, row.live_key_bytes,
		                           row.live_value_bytes, row.scheduled_files});
	}

	using duckdb::LogicalType;
	return_types = {LogicalType::UBIGINT,  LogicalType::VARCHAR,  LogicalType::UBIGINT, LogicalType::UINTEGER,
	                LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UBIGINT, LogicalType::UBIGINT,
	                LogicalType::UBIGINT,  LogicalType::UBIGINT,  LogicalType::UBIGINT, LogicalType::UBIGINT,
	                LogicalType::UBIGINT,  LogicalType::UBIGINT};
	names = {"manifest_id",   "subspace",         "bytes",           "l0_ssts",
	         "sorted_runs",   "sorted_run_ssts",  "live_keys",       "live_key_bytes",
	         "live_value_bytes", "scheduled_files", "store_total_bytes", "store_wal_bytes",
	         "store_manifest_bytes", "store_sst_bytes"};
	return bind_data;
}

struct CensusGlobalState : public duckdb::GlobalTableFunctionState {
	duckdb::idx_t offset = 0;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CensusInitGlobal(duckdb::ClientContext &,
                                                                      duckdb::TableFunctionInitInput &) {
	return duckdb::make_uniq<CensusGlobalState>();
}

void CensusImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<CensusBindData>();
	auto &state = data.global_state->Cast<CensusGlobalState>();
	if (state.offset >= bind_data.rows.size()) {
		output.SetCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - state.offset);
	for (duckdb::idx_t i = 0; i < count; i++) {
		auto &row = bind_data.rows[state.offset + i];
		output.SetValue(0, i, duckdb::Value::UBIGINT(bind_data.manifest_id));
		output.SetValue(1, i, duckdb::Value(row.subspace));
		output.SetValue(2, i, duckdb::Value::UBIGINT(row.bytes));
		output.SetValue(3, i, duckdb::Value::UINTEGER(row.l0_ssts));
		output.SetValue(4, i, duckdb::Value::UINTEGER(row.sorted_runs));
		output.SetValue(5, i, duckdb::Value::UINTEGER(row.sorted_run_ssts));
		// The live columns are NULL rather than zero when the census did
		// not scan: a subspace with no live keys and a subspace nobody
		// counted are different answers.
		auto live = [&](uint64_t value) {
			return row.has_live ? duckdb::Value::UBIGINT(value) : duckdb::Value(duckdb::LogicalType::UBIGINT);
		};
		output.SetValue(6, i, live(row.live_keys));
		output.SetValue(7, i, live(row.live_key_bytes));
		output.SetValue(8, i, live(row.live_value_bytes));
		output.SetValue(9, i, live(row.scheduled_files));

		// Store-wide totals, repeated per row. NULL when the store could
		// not be listed — read-only credentials commonly grant GetObject
		// without ListBucket, and zero would read as "no WAL" rather than
		// "not measured".
		auto &objects = bind_data.objects;
		auto store = [&](uint64_t value) {
			return objects.listed ? duckdb::Value::UBIGINT(value)
			                      : duckdb::Value(duckdb::LogicalType::UBIGINT);
		};
		output.SetValue(10, i, store(objects.total_bytes));
		output.SetValue(11, i, store(objects.wal_bytes));
		output.SetValue(12, i, store(objects.manifest_bytes));
		output.SetValue(13, i, store(objects.sst_bytes));
	}
	state.offset += count;
	output.SetCardinality(count);
}

// moraine_cache_tally: what the block cache has served.
//
// Two forms over one cache. Without arguments the numbers are the host's:
// a process keeps one cache and every attached store reads through it, so
// that is the scope its budget is set at. Given a lake name they are that
// attach's alone, which is the question a host with several catalogs on
// one cache has — which of them is spending the budget. One row either
// way, so it composes into a monitoring query without an aggregate.
struct TallyBindData : public duckdb::FunctionData {
	// Empty for the process-wide form.
	std::string catalog_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<TallyBindData>();
		*result = *this;
		return result;
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		return catalog_name == other_p.Cast<TallyBindData>().catalog_name;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> TallyBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                   duckdb::vector<duckdb::LogicalType> &return_types,
                                                   duckdb::vector<duckdb::string> &names) {
	names = {"metadata_hits",  "metadata_misses",   "metadata_hit_rate", "block_hits",
	         "block_misses",   "block_hit_rate",    "errors"};
	return_types = {duckdb::LogicalType::UBIGINT, duckdb::LogicalType::UBIGINT, duckdb::LogicalType::DOUBLE,
	                duckdb::LogicalType::UBIGINT, duckdb::LogicalType::UBIGINT, duckdb::LogicalType::DOUBLE,
	                duckdb::LogicalType::UBIGINT};

	auto bind_data = duckdb::make_uniq<TallyBindData>();
	if (!input.inputs.empty()) {
		if (input.inputs[0].IsNull()) {
			throw duckdb::BinderException("moraine_cache_tally: the lake name must not be NULL");
		}
		bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	}
	return bind_data;
}

struct TallyGlobalState : public duckdb::GlobalTableFunctionState {
	bool emitted = false;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> TallyInitGlobal(duckdb::ClientContext &,
                                                                     duckdb::TableFunctionInitInput &) {
	return duckdb::make_uniq<TallyGlobalState>();
}

void TallyImpl(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<TallyBindData>();
	auto &state = data.global_state->Cast<TallyGlobalState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	state.emitted = true;

	uint64_t metadata_hits = 0;
	uint64_t metadata_misses = 0;
	uint64_t block_hits = 0;
	uint64_t block_misses = 0;
	uint64_t errors = 0;
	auto code = bind_data.catalog_name.empty()
	                ? moraine_cache_tally(&metadata_hits, &metadata_misses, &block_hits, &block_misses, &errors)
	                : moraine_catalog_cache_tally(ResolveMoraineCatalog(context, bind_data.catalog_name).Handle(),
	                                              &metadata_hits, &metadata_misses, &block_hits, &block_misses,
	                                              &errors);
	if (code != MORAINE_OK) {
		throw duckdb::InternalException("moraine_cache_tally: could not read the cache counters");
	}

	// NULL rather than zero before anything has been looked up: a rate
	// over no lookups is not zero, it is absent, and a monitoring query
	// that averaged the difference would be reading a lie.
	auto rate = [](uint64_t hits, uint64_t misses) {
		auto total = hits + misses;
		return total == 0 ? duckdb::Value(duckdb::LogicalType::DOUBLE)
		                  : duckdb::Value::DOUBLE(static_cast<double>(hits) / static_cast<double>(total));
	};

	output.SetValue(0, 0, duckdb::Value::UBIGINT(metadata_hits));
	output.SetValue(1, 0, duckdb::Value::UBIGINT(metadata_misses));
	output.SetValue(2, 0, rate(metadata_hits, metadata_misses));
	output.SetValue(3, 0, duckdb::Value::UBIGINT(block_hits));
	output.SetValue(4, 0, duckdb::Value::UBIGINT(block_misses));
	output.SetValue(5, 0, rate(block_hits, block_misses));
	output.SetValue(6, 0, duckdb::Value::UBIGINT(errors));
	output.SetCardinality(1);
}

} // namespace

void RegisterMoraineCensusFunctions(duckdb::ExtensionLoader &loader) {
	duckdb::TableFunction census("moraine_store_census", {duckdb::LogicalType::VARCHAR}, CensusImpl, CensusBind,
	                             CensusInitGlobal);
	census.named_parameters["live"] = duckdb::LogicalType::BOOLEAN;
	loader.RegisterFunction(census);

	duckdb::TableFunctionSet tally("moraine_cache_tally");
	tally.AddFunction(duckdb::TableFunction({}, TallyImpl, TallyBind, TallyInitGlobal));
	tally.AddFunction(
	    duckdb::TableFunction({duckdb::LogicalType::VARCHAR}, TallyImpl, TallyBind, TallyInitGlobal));
	loader.RegisterFunction(tally);
}

} // namespace moraine_duckdb
