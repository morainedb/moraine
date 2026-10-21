// A session-owned fault seam for exercising cancellation of blocked reads.

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "fault.hpp"
#include "duckdb/main/client_context_state.hpp"
#include <atomic>

namespace moraine_duckdb {

namespace {

struct ReadStallState : public duckdb::ClientContextState {
	static constexpr const char *KEY = "moraine_read_stall";
	std::atomic<uint64_t> milliseconds {0};
};

struct StallBindData : public duckdb::FunctionData {
	uint64_t milliseconds = 0;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto copy = duckdb::make_uniq<StallBindData>();
		copy->milliseconds = milliseconds;
		return copy;
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return milliseconds == other.Cast<StallBindData>().milliseconds;
	}
};

struct StallGlobalState : public duckdb::GlobalTableFunctionState {
	bool emitted = false;

	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> StallBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                   duckdb::vector<duckdb::LogicalType> &return_types,
                                                   duckdb::vector<duckdb::Identifier> &names) {
	if (input.inputs[0].IsNull()) {
		throw duckdb::BinderException("moraine_fault_stall_next_read: the stall must not be NULL");
	}
	auto bind_data = duckdb::make_uniq<StallBindData>();
	bind_data->milliseconds = input.inputs[0].GetValue<uint64_t>();

	return_types = {duckdb::LogicalType::UBIGINT};
	names = {"milliseconds"};
	return bind_data;
}

// Armed at execution rather than at bind: a prepared or re-bound statement
// would otherwise arm the seam without running it.
duckdb::unique_ptr<duckdb::GlobalTableFunctionState> StallInitGlobal(duckdb::ClientContext &context,
                                                                     duckdb::TableFunctionInitInput &input) {
	auto state = context.registered_state->Get<ReadStallState>(ReadStallState::KEY);
	if (!state) {
		state = duckdb::make_shared_ptr<ReadStallState>();
		context.registered_state->Insert(ReadStallState::KEY, state);
	}
	state->milliseconds.store(input.bind_data->Cast<StallBindData>().milliseconds);
	return duckdb::make_uniq<StallGlobalState>();
}

void StallImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<StallBindData>();
	auto &state = data.global_state->Cast<StallGlobalState>();
	if (state.emitted) {
		output.SetChildCardinality(0);
		return;
	}
	output.SetValue(0, 0, duckdb::Value::UBIGINT(bind_data.milliseconds));
	state.emitted = true;
	output.SetChildCardinality(1);
}

} // namespace

uint64_t TakeMoraineReadStall(duckdb::ClientContext &context) {
	auto state = context.registered_state->Get<ReadStallState>(ReadStallState::KEY);
	return state ? state->milliseconds.exchange(0) : 0;
}

void RegisterMoraineFaultFunctions(duckdb::ExtensionLoader &loader) {
	duckdb::TableFunction stall("moraine_fault_stall_next_read", {duckdb::LogicalType::UBIGINT}, StallImpl, StallBind,
	                            StallInitGlobal);
	loader.RegisterFunction(stall);
}

} // namespace moraine_duckdb
