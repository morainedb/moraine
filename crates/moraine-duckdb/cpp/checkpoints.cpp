// The checkpoint lifecycle as SQL: `moraine_create_checkpoint`,
// `moraine_checkpoints`, and `moraine_delete_checkpoint`.
//
// A read-only attach normally records a checkpoint in the manifest and
// refreshes it, so its credentials need write access. Pinning it to a
// checkpoint minted ahead of time removes that last write — the attach then
// issues object-store reads and nothing else. These three functions are how
// an operator mints, inspects, and releases those checkpoints.
//
// Minting names an attached catalog, because the core mints through the
// writer that attach already opened and a second read-write open would
// fence it. Listing and releasing name a store path instead, like
// `moraine_migrate`: they touch only the manifest, and the processes that
// attach against the id hold no write credentials, so those calls happen
// wherever the credentials are.

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "catalog.hpp"
#include "moraine_abi.h"
#include "owned_array.hpp"
#include "s3_secret.hpp"

#include <string>
#include <vector>

namespace moraine_duckdb {

namespace {

struct CheckpointBindData : public duckdb::FunctionData {
	// A store path for the list/delete functions, an attached catalog name
	// for create.
	std::string target;
	std::string checkpoint_id;
	uint64_t lifetime_ms = 0;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto copy = duckdb::make_uniq<CheckpointBindData>();
		copy->target = target;
		copy->checkpoint_id = checkpoint_id;
		copy->lifetime_ms = lifetime_ms;
		return copy;
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		auto &that = other.Cast<CheckpointBindData>();
		return target == that.target && checkpoint_id == that.checkpoint_id && lifetime_ms == that.lifetime_ms;
	}
};

// One row per checkpoint, materialized at init and emitted in one chunk.
// Every one of these calls touches a single manifest object, so there is
// never more than a handful.
struct CheckpointRow {
	std::string id;
};

struct CheckpointGlobalState : public duckdb::GlobalTableFunctionState {
	std::vector<CheckpointRow> rows;
	bool emitted = false;

	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

std::string RequireArgument(const std::string &function, const std::string &what,
                            duckdb::TableFunctionBindInput &input, duckdb::idx_t index) {
	if (input.inputs[index].IsNull()) {
		throw duckdb::BinderException("%s: the %s must not be NULL", function, what);
	}
	auto value = input.inputs[index].GetValue<std::string>();
	if (value.empty()) {
		throw duckdb::BinderException("%s: the %s must not be empty", function, what);
	}
	return value;
}

void CheckpointReturnTypes(duckdb::vector<duckdb::LogicalType> &return_types, duckdb::vector<duckdb::string> &names) {
	return_types = {duckdb::LogicalType::VARCHAR};
	names = {"checkpoint_id"};
}

duckdb::unique_ptr<duckdb::FunctionData> CreateBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                    duckdb::vector<duckdb::LogicalType> &return_types,
                                                    duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<CheckpointBindData>();
	bind_data->target = RequireArgument("moraine_create_checkpoint", "catalog name", input, 0);
	for (auto &option : input.named_parameters) {
		if (duckdb::StringUtil::CIEquals(option.first, "lifetime")) {
			// An interval, not a timestamp: the checkpoint's expiry is
			// relative to when it is minted, and SlateDB stamps it from its
			// own clock.
			auto interval = duckdb::IntervalValue::Get(option.second);
			auto micros = duckdb::Interval::GetMicro(interval);
			if (micros <= 0) {
				throw duckdb::BinderException("moraine_create_checkpoint: lifetime must be positive");
			}
			bind_data->lifetime_ms = static_cast<uint64_t>(micros / duckdb::Interval::MICROS_PER_MSEC);
		}
	}
	CheckpointReturnTypes(return_types, names);
	return bind_data;
}

duckdb::unique_ptr<duckdb::FunctionData> ListBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                  duckdb::vector<duckdb::LogicalType> &return_types,
                                                  duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<CheckpointBindData>();
	bind_data->target = RequireArgument("moraine_checkpoints", "store path", input, 0);
	CheckpointReturnTypes(return_types, names);
	return bind_data;
}

duckdb::unique_ptr<duckdb::FunctionData> DeleteBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                    duckdb::vector<duckdb::LogicalType> &return_types,
                                                    duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<CheckpointBindData>();
	bind_data->target = RequireArgument("moraine_delete_checkpoint", "store path", input, 0);
	if (input.inputs[1].IsNull()) {
		throw duckdb::BinderException("moraine_delete_checkpoint: the checkpoint id must not be NULL");
	}
	bind_data->checkpoint_id = input.inputs[1].GetValue<std::string>();
	return_types = {duckdb::LogicalType::VARCHAR};
	names = {"released"};
	return bind_data;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CreateInitGlobal(duckdb::ClientContext &context,
                                                                      duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<CheckpointBindData>();
	auto &catalog = ResolveMoraineCatalog(context, bind_data.target);

	char *id = nullptr;
	MoraineError err {};
	auto code = moraine_create_checkpoint(catalog.Handle(), bind_data.lifetime_ms, &id, &err);
	DrainMoraineLogs(context);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	auto state = duckdb::make_uniq<CheckpointGlobalState>();
	CheckpointRow row;
	if (id != nullptr) {
		row.id = id;
		moraine_string_free(id);
	}
	state->rows.push_back(std::move(row));
	return state;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ListInitGlobal(duckdb::ClientContext &context,
                                                                    duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<CheckpointBindData>();

	MoraineS3Config s3 {};
	S3SecretStrings s3_strings;
	bool is_s3 = ResolveS3Config(context, bind_data.target, s3, s3_strings);

	OwnedArray<MoraineCheckpoint> checkpoints(moraine_checkpoints_free);
	MoraineError err {};
	auto code = moraine_checkpoints(bind_data.target.c_str(), is_s3 ? &s3 : nullptr, checkpoints.OutItems(),
	                                checkpoints.OutLen(), &err);
	DrainMoraineLogs(context);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	auto state = duckdb::make_uniq<CheckpointGlobalState>();
	for (auto &checkpoint : checkpoints) {
		CheckpointRow row;
		if (checkpoint.id != nullptr) {
			row.id = checkpoint.id;
		}
		state->rows.push_back(std::move(row));
	}
	return state;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> DeleteInitGlobal(duckdb::ClientContext &context,
                                                                      duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<CheckpointBindData>();

	MoraineS3Config s3 {};
	S3SecretStrings s3_strings;
	bool is_s3 = ResolveS3Config(context, bind_data.target, s3, s3_strings);

	MoraineError err {};
	auto code = moraine_delete_checkpoint(bind_data.target.c_str(), is_s3 ? &s3 : nullptr,
	                                      bind_data.checkpoint_id.c_str(), &err);
	DrainMoraineLogs(context);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	auto state = duckdb::make_uniq<CheckpointGlobalState>();
	CheckpointRow row;
	row.id = bind_data.checkpoint_id;
	state->rows.push_back(std::move(row));
	return state;
}

void CheckpointImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<CheckpointGlobalState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	duckdb::idx_t row_index = 0;
	for (auto &row : state.rows) {
		output.SetValue(0, row_index, duckdb::Value(row.id));
		row_index++;
	}
	state.emitted = true;
	output.SetCardinality(row_index);
}

// The delete function returns only the released id, so it projects one
// column out of the same row shape.
void DeleteImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<CheckpointGlobalState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	output.SetValue(0, 0, duckdb::Value(state.rows.front().id));
	state.emitted = true;
	output.SetCardinality(1);
}

} // namespace

void RegisterMoraineCheckpointFunctions(duckdb::ExtensionLoader &loader) {
	duckdb::TableFunction create("moraine_create_checkpoint", {duckdb::LogicalType::VARCHAR}, CheckpointImpl,
	                             CreateBind, CreateInitGlobal);
	create.named_parameters["lifetime"] = duckdb::LogicalType::INTERVAL;
	loader.RegisterFunction(create);

	loader.RegisterFunction(duckdb::TableFunction("moraine_checkpoints", {duckdb::LogicalType::VARCHAR},
	                                              CheckpointImpl, ListBind, ListInitGlobal));

	loader.RegisterFunction(duckdb::TableFunction(
	    "moraine_delete_checkpoint", {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR}, DeleteImpl,
	    DeleteBind, DeleteInitGlobal));
}

} // namespace moraine_duckdb
