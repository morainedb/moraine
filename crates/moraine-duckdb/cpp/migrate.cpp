// `moraine_migrate`: the operator's SQL surface for a structural format
// migration.
//
// It takes a path rather than an attached catalog name, and that is the
// whole point. A store whose format sits below this binary's floor, or one
// left carrying a migration marker by an interrupted run, is refused by
// ATTACH — that refusal is what keeps readers off a keyspace in motion. So
// the stores this function exists to repair are exactly the ones that
// cannot be attached, and resolving an attached catalog would make it
// reachable only for stores that never needed it.

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "catalog.hpp"
#include "moraine_abi.h"
#include "s3_secret.hpp"

#include <string>

namespace moraine_duckdb {

namespace {

struct MigrateBindData : public duckdb::FunctionData {
	std::string path;
	bool checkpoint = false;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto copy = duckdb::make_uniq<MigrateBindData>();
		copy->path = path;
		copy->checkpoint = checkpoint;
		return copy;
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		auto &that = other.Cast<MigrateBindData>();
		return path == that.path && checkpoint == that.checkpoint;
	}
};

// The migration runs once, at execution start, and its report is one row.
struct MigrateGlobalState : public duckdb::GlobalTableFunctionState {
	uint64_t from_format = 0;
	uint64_t to_format = 0;
	bool resumed = false;
	std::string units_run;
	bool emitted = false;

	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> MigrateBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                     duckdb::vector<duckdb::LogicalType> &return_types,
                                                     duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<MigrateBindData>();
	if (input.inputs[0].IsNull()) {
		throw duckdb::BinderException("moraine_migrate: the store path must not be NULL");
	}
	bind_data->path = input.inputs[0].GetValue<std::string>();
	if (bind_data->path.empty()) {
		throw duckdb::BinderException("moraine_migrate: the store path must not be empty");
	}
	for (auto &option : input.named_parameters) {
		if (duckdb::StringUtil::CIEquals(option.first, "checkpoint")) {
			bind_data->checkpoint = duckdb::BooleanValue::Get(option.second);
		}
	}

	return_types = {duckdb::LogicalType::UBIGINT, duckdb::LogicalType::UBIGINT, duckdb::LogicalType::BOOLEAN,
	                duckdb::LogicalType::VARCHAR};
	names = {"from_format", "to_format", "resumed", "units_run"};
	return bind_data;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> MigrateInitGlobal(duckdb::ClientContext &context,
                                                                       duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MigrateBindData>();

	// A migration takes the store's single writer for its duration and
	// commits through its own connection, so running inside an explicit
	// transaction invites a self-deadlock — the same reason maintenance
	// refuses one.
	if (!context.transaction.IsAutoCommit()) {
		throw duckdb::TransactionException(
		    "moraine_migrate cannot run inside an explicit transaction; COMMIT first");
	}

	MoraineS3Config s3 {};
	S3SecretStrings s3_strings;
	bool is_s3 = ResolveS3Config(context, bind_data.path, s3, s3_strings);

	MoraineMigrationReport report {};
	MoraineError err {};
	auto code = moraine_migrate(bind_data.path.c_str(), is_s3 ? &s3 : nullptr, 0, nullptr, 0, 0, false, bind_data.checkpoint,
	                            &report, &err);
	// Drained on both exits: a failed migration's events would otherwise sit
	// buffered behind a commit that never comes.
	DrainMoraineLogs(context);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	auto state = duckdb::make_uniq<MigrateGlobalState>();
	state->from_format = report.from_format;
	state->to_format = report.to_format;
	state->resumed = report.resumed;
	if (report.units_run != nullptr) {
		state->units_run = report.units_run;
		moraine_string_free(report.units_run);
	}
	return state;
}

void MigrateImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<MigrateGlobalState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	output.SetValue(0, 0, duckdb::Value::UBIGINT(state.from_format));
	output.SetValue(1, 0, duckdb::Value::UBIGINT(state.to_format));
	output.SetValue(2, 0, duckdb::Value::BOOLEAN(state.resumed));
	output.SetValue(3, 0, duckdb::Value(state.units_run));
	state.emitted = true;
	output.SetCardinality(1);
}

} // namespace

void RegisterMoraineMigrateFunction(duckdb::ExtensionLoader &loader) {
	duckdb::TableFunction migrate("moraine_migrate", {duckdb::LogicalType::VARCHAR}, MigrateImpl, MigrateBind,
	                              MigrateInitGlobal);
	migrate.named_parameters["checkpoint"] = duckdb::LogicalType::BOOLEAN;
	loader.RegisterFunction(migrate);
}

} // namespace moraine_duckdb
