// `moraine_move_wal`: the operator's SQL surface for moving a catalog's
// write-ahead log to another object store — a standard bucket to an S3
// Express One Zone one, or back.
//
// It takes a store path rather than an attached catalog name, and for the
// same reason `moraine_migrate` does: the move takes the store's writer
// twice and must run with nothing attached. It also takes the log store as
// it stands, because draining that log is what makes the move lossless; a
// wrong one is refused by a message naming the store the catalog records.

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "catalog.hpp"
#include "moraine_abi.h"
#include "s3_secret.hpp"

#include <string>

namespace moraine_duckdb {

namespace {

struct MoveWalBindData : public duckdb::FunctionData {
	std::string path;
	// Empty means the catalog store itself, on either side.
	std::string from_wal_path;
	std::string to_wal_path;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto copy = duckdb::make_uniq<MoveWalBindData>();
		copy->path = path;
		copy->from_wal_path = from_wal_path;
		copy->to_wal_path = to_wal_path;
		return copy;
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		auto &that = other.Cast<MoveWalBindData>();
		return path == that.path && from_wal_path == that.from_wal_path && to_wal_path == that.to_wal_path;
	}
};

// The move runs once, at execution start, and its report is one row.
struct MoveWalGlobalState : public duckdb::GlobalTableFunctionState {
	std::string from_wal_path;
	std::string to_wal_path;
	bool moved = false;
	bool emitted = false;

	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> MoveWalBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                     duckdb::vector<duckdb::LogicalType> &return_types,
                                                     duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<MoveWalBindData>();
	if (input.inputs[0].IsNull()) {
		throw duckdb::BinderException("moraine_move_wal: the store path must not be NULL");
	}
	bind_data->path = input.inputs[0].GetValue<std::string>();
	if (bind_data->path.empty()) {
		throw duckdb::BinderException("moraine_move_wal: the store path must not be empty");
	}
	bool to_given = false;
	for (auto &option : input.named_parameters) {
		if (duckdb::StringUtil::CIEquals(option.first, "wal_path")) {
			to_given = true;
			if (!option.second.IsNull()) {
				bind_data->to_wal_path = option.second.GetValue<std::string>();
			}
		} else if (duckdb::StringUtil::CIEquals(option.first, "from_wal_path")) {
			if (!option.second.IsNull()) {
				bind_data->from_wal_path = option.second.GetValue<std::string>();
			}
		}
	}
	// NULL moves the log back into the catalog store, which is a different
	// request from not saying where it should go at all.
	if (!to_given) {
		throw duckdb::BinderException(
		    "moraine_move_wal: name the store the log moves to with wal_path => '<uri>' (or NULL "
		    "to move it into the catalog store)");
	}

	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN};
	names = {"from_wal_path", "to_wal_path", "moved"};
	return bind_data;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> MoveWalInitGlobal(duckdb::ClientContext &context,
                                                                       duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MoveWalBindData>();

	// The move commits through its own connection, as a migration does, so
	// an explicit transaction would wait on a writer it is itself holding.
	if (!context.transaction.IsAutoCommit()) {
		throw duckdb::TransactionException("moraine_move_wal cannot run inside an explicit transaction; COMMIT first");
	}

	// Each store resolves its own secret: they are different buckets, and on
	// S3 Express a different endpoint and region.
	MoraineS3Config s3 {};
	S3SecretStrings s3_strings;
	bool is_s3 = ResolveS3Config(context, bind_data.path, s3, s3_strings);
	MoraineS3Config from_s3 {};
	S3SecretStrings from_s3_strings;
	bool from_is_s3 =
	    !bind_data.from_wal_path.empty() && ResolveS3Config(context, bind_data.from_wal_path, from_s3, from_s3_strings);
	MoraineS3Config to_s3 {};
	S3SecretStrings to_s3_strings;
	bool to_is_s3 =
	    !bind_data.to_wal_path.empty() && ResolveS3Config(context, bind_data.to_wal_path, to_s3, to_s3_strings);

	MoraineWalStore from {};
	from.path = bind_data.from_wal_path.empty() ? nullptr : bind_data.from_wal_path.c_str();
	from.s3 = from_is_s3 ? &from_s3 : nullptr;
	MoraineWalStore to {};
	to.path = bind_data.to_wal_path.empty() ? nullptr : bind_data.to_wal_path.c_str();
	to.s3 = to_is_s3 ? &to_s3 : nullptr;

	bool moved = false;
	MoraineError err {};
	auto code = moraine_move_wal_store(bind_data.path.c_str(), is_s3 ? &s3 : nullptr,
	                                   bind_data.from_wal_path.empty() ? nullptr : &from,
	                                   bind_data.to_wal_path.empty() ? nullptr : &to, &moved, &err);
	// Drained on both exits: a failed move's events would otherwise sit
	// buffered behind a commit that never comes.
	DrainMoraineLogs(context);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	auto state = duckdb::make_uniq<MoveWalGlobalState>();
	state->from_wal_path = bind_data.from_wal_path;
	state->to_wal_path = bind_data.to_wal_path;
	state->moved = moved;
	return state;
}

void MoveWalImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<MoveWalGlobalState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	auto store = [](const std::string &path) {
		return path.empty() ? duckdb::Value(duckdb::LogicalType::VARCHAR) : duckdb::Value(path);
	};
	output.SetValue(0, 0, store(state.from_wal_path));
	output.SetValue(1, 0, store(state.to_wal_path));
	output.SetValue(2, 0, duckdb::Value::BOOLEAN(state.moved));
	state.emitted = true;
	output.SetCardinality(1);
}

} // namespace

void RegisterMoraineMoveWalFunction(duckdb::ExtensionLoader &loader) {
	duckdb::TableFunction move_wal("moraine_move_wal", {duckdb::LogicalType::VARCHAR}, MoveWalImpl, MoveWalBind,
	                               MoveWalInitGlobal);
	move_wal.named_parameters["wal_path"] = duckdb::LogicalType::VARCHAR;
	move_wal.named_parameters["from_wal_path"] = duckdb::LogicalType::VARCHAR;
	loader.RegisterFunction(move_wal);
}

} // namespace moraine_duckdb
