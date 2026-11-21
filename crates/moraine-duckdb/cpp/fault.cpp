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

struct CommitAckState : public duckdb::ClientContextState {
	static constexpr const char *KEY = "moraine_commit_ack";
	std::atomic<bool> drop_next {false};
};

struct DropAckGlobalState : public duckdb::GlobalTableFunctionState {
	bool emitted = false;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> DropAckBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &,
                                                     duckdb::vector<duckdb::LogicalType> &return_types,
                                                     duckdb::vector<duckdb::Identifier> &names) {
	return_types = {duckdb::LogicalType::BOOLEAN};
	names = {"armed"};
	return nullptr;
}

// Armed at execution, as the stall is.
duckdb::unique_ptr<duckdb::GlobalTableFunctionState> DropAckInitGlobal(duckdb::ClientContext &context,
                                                                       duckdb::TableFunctionInitInput &) {
	auto state = context.registered_state->Get<CommitAckState>(CommitAckState::KEY);
	if (!state) {
		state = duckdb::make_shared_ptr<CommitAckState>();
		context.registered_state->Insert(CommitAckState::KEY, state);
	}
	state->drop_next.store(true);
	return duckdb::make_uniq<DropAckGlobalState>();
}

void DropAckImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<DropAckGlobalState>();
	if (state.emitted) {
		output.SetChildCardinality(0);
		return;
	}
	output.SetValue(0, 0, duckdb::Value::BOOLEAN(true));
	state.emitted = true;
	output.SetChildCardinality(1);
}

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

bool TakeMoraineDroppedCommitAck(duckdb::ClientContext &context) {
	auto state = context.registered_state->Get<CommitAckState>(CommitAckState::KEY);
	return state ? state->drop_next.exchange(false) : false;
}

void RegisterMoraineFaultFunctions(duckdb::ExtensionLoader &loader) {
	duckdb::TableFunction stall("moraine_fault_stall_next_read", {duckdb::LogicalType::UBIGINT}, StallImpl, StallBind,
	                            StallInitGlobal);
	loader.RegisterFunction(stall);
	duckdb::TableFunction drop_ack("moraine_fault_drop_next_commit_ack", {}, DropAckImpl, DropAckBind,
	                               DropAckInitGlobal);
	loader.RegisterFunction(drop_ack);
}

} // namespace moraine_duckdb
