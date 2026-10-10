// `moraine_raise_catalog_version`: the operator's SQL surface for moving a
// store to the newest DuckLake catalog version this build serves.
//
// A separate axis from `moraine_migrate`, which raises the store format —
// how moraine writes bytes. This raises the shape moraine serves DuckLake,
// and the two move on their own schedules. It takes an attached catalog
// name rather than a path: unlike a format below the floor, a catalog
// version never refuses ATTACH, so the stores this moves are exactly the
// ones already attached.
//
// One-way in practice: a DuckLake that requires the older version refuses
// the store outright at its own catalog-version check, before it reads a
// table. DuckLake advertises no required version to gate on — it compares
// against a literal — so a real raise takes `confirm`, and the operator
// weighs it against the DuckLake they intend to run. A dry run needs no
// acknowledgement: it records nothing, and reading the version a store
// serves is what the pre-flight is for.

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "catalog.hpp"
#include "moraine_abi.h"

#include <string>

namespace moraine_duckdb {

namespace {

struct RaiseBindData : public duckdb::FunctionData {
	std::string catalog_name;
	// Reports the move without making it: the pre-flight for a one-way
	// door, and the only way to read the version a store records.
	bool dry_run = false;
	// Acknowledges that door. Required for a raise that records.
	bool confirm = false;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<RaiseBindData>();
		*result = *this;
		return result;
	}

	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<RaiseBindData>();
		return catalog_name == other.catalog_name && dry_run == other.dry_run && confirm == other.confirm;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> RaiseBind(duckdb::ClientContext &,
                                                   duckdb::TableFunctionBindInput &input,
                                                   duckdb::vector<duckdb::LogicalType> &return_types,
                                                   duckdb::vector<duckdb::string> &names) {
	names = {"from_version", "to_version"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};

	if (input.inputs[0].IsNull()) {
		throw duckdb::BinderException("moraine_raise_catalog_version: the lake name must not be NULL");
	}
	auto bind_data = duckdb::make_uniq<RaiseBindData>();
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	if (bind_data->catalog_name.empty()) {
		throw duckdb::BinderException("moraine_raise_catalog_version: the lake name must not be empty");
	}
	for (auto &option : input.named_parameters) {
		if (duckdb::StringUtil::CIEquals(option.first, "dry_run")) {
			if (option.second.IsNull()) {
				throw duckdb::BinderException("moraine_raise_catalog_version: dry_run must not be NULL");
			}
			bind_data->dry_run = option.second.GetValue<bool>();
		} else if (duckdb::StringUtil::CIEquals(option.first, "confirm")) {
			if (option.second.IsNull()) {
				throw duckdb::BinderException("moraine_raise_catalog_version: confirm must not be NULL");
			}
			bind_data->confirm = option.second.GetValue<bool>();
		}
	}

	if (!bind_data->dry_run && !bind_data->confirm) {
		throw duckdb::BinderException(
		    "moraine_raise_catalog_version: raising is one way and a DuckLake that requires the older catalog "
		    "version refuses the store outright; run it with dry_run => true to see the move, then confirm => "
		    "true to make it");
	}
	return bind_data;
}

struct RaiseGlobalState : public duckdb::GlobalTableFunctionState {
	bool emitted = false;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> RaiseInitGlobal(duckdb::ClientContext &,
                                                                     duckdb::TableFunctionInitInput &) {
	return duckdb::make_uniq<RaiseGlobalState>();
}

void RaiseImpl(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RaiseBindData>();
	auto &state = data.global_state->Cast<RaiseGlobalState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	state.emitted = true;

	auto &catalog = ResolveMoraineCatalog(context, bind_data.catalog_name);
	char *from = nullptr;
	char *to = nullptr;
	MoraineError err {};
	auto code = moraine_raise_catalog_version(catalog.Handle(), bind_data.dry_run, moraine_shim_is_interrupted,
	                                          &context, &from, &to, &err);
	DrainMoraineLogs(context);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	std::string from_version(from == nullptr ? "" : from);
	std::string to_version(to == nullptr ? "" : to);
	moraine_string_free(from);
	moraine_string_free(to);

	output.SetValue(0, 0, duckdb::Value(from_version));
	output.SetValue(1, 0, duckdb::Value(to_version));
	output.SetCardinality(1);
}

} // namespace

void RegisterMoraineRaiseCatalogVersionFunction(duckdb::ExtensionLoader &loader) {
	duckdb::TableFunction raise("moraine_raise_catalog_version", {duckdb::LogicalType::VARCHAR}, RaiseImpl, RaiseBind,
	                            RaiseInitGlobal);
	raise.named_parameters["dry_run"] = duckdb::LogicalType::BOOLEAN;
	raise.named_parameters["confirm"] = duckdb::LogicalType::BOOLEAN;
	loader.RegisterFunction(raise);
}

} // namespace moraine_duckdb
