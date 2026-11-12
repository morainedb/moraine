#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "catalog.hpp"
#include "moraine_abi.h"
#include "s3_secret.hpp"

namespace moraine_duckdb {
namespace {
struct RewriteDataPathBindData : public duckdb::FunctionData {
	std::string path;
	std::string old_root;
	std::string new_root;
	std::string wal_path;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto copy = duckdb::make_uniq<RewriteDataPathBindData>();
		copy->path = path;
		copy->old_root = old_root;
		copy->new_root = new_root;
		copy->wal_path = wal_path;
		return copy;
	}
	bool Equals(const duckdb::FunctionData &other) const override {
		auto &that = other.Cast<RewriteDataPathBindData>();
		return path == that.path && old_root == that.old_root && new_root == that.new_root && wal_path == that.wal_path;
	}
};

struct RewriteDataPathState : public duckdb::GlobalTableFunctionState {
	bool emitted = false;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> RewriteDataPathBind(duckdb::ClientContext &,
                                                             duckdb::TableFunctionBindInput &input,
                                                             duckdb::vector<duckdb::LogicalType> &types,
                                                             duckdb::vector<duckdb::Identifier> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull() || value.GetValue<std::string>().empty()) {
			throw duckdb::BinderException("moraine_rewrite_data_path: paths must not be NULL or empty");
		}
	}
	auto result = duckdb::make_uniq<RewriteDataPathBindData>();
	result->path = input.inputs[0].GetValue<std::string>();
	result->old_root = input.inputs[1].GetValue<std::string>();
	result->new_root = input.inputs[2].GetValue<std::string>();
	for (auto &option : input.named_parameters) {
		if (option.first == "wal_path" && !option.second.IsNull()) {
			result->wal_path = option.second.GetValue<std::string>();
		}
	}
	types = {duckdb::LogicalType::BOOLEAN};
	names = {"success"};
	return result;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> RewriteDataPathInit(duckdb::ClientContext &context,
                                                                         duckdb::TableFunctionInitInput &input) {
	if (!context.transaction.IsAutoCommit()) {
		throw duckdb::TransactionException(
		    "moraine_rewrite_data_path cannot run inside an explicit transaction; COMMIT first");
	}
	auto &bind = input.bind_data->Cast<RewriteDataPathBindData>();
	MoraineS3Config s3 {};
	S3SecretStrings strings;
	bool is_s3 = ResolveS3Config(context, bind.path, s3, strings);
	MoraineS3Config wal_s3 {};
	S3SecretStrings wal_strings;
	bool wal_is_s3 = !bind.wal_path.empty() && ResolveS3Config(context, bind.wal_path, wal_s3, wal_strings);
	MoraineWalStore wal {};
	wal.path = bind.wal_path.empty() ? nullptr : bind.wal_path.c_str();
	wal.s3 = wal_is_s3 ? &wal_s3 : nullptr;
	MoraineError error {};
	auto code = moraine_rewrite_data_path_store(bind.path.c_str(), bind.old_root.c_str(), bind.new_root.c_str(),
	                                            is_s3 ? &s3 : nullptr, bind.wal_path.empty() ? nullptr : &wal, &error);
	DrainMoraineLogs(context);
	if (code != MORAINE_OK) {
		ThrowMoraineError(error);
	}
	return duckdb::make_uniq<RewriteDataPathState>();
}

void RewriteDataPathExecute(duckdb::ClientContext &, duckdb::TableFunctionInput &input, duckdb::DataChunk &output) {
	auto &state = input.global_state->Cast<RewriteDataPathState>();
	if (state.emitted) {
		output.SetChildCardinality(0);
		return;
	}
	output.SetValue(0, 0, duckdb::Value::BOOLEAN(true));
	output.SetChildCardinality(1);
	state.emitted = true;
}
} // namespace

void RegisterMoraineRewriteDataPathFunction(duckdb::ExtensionLoader &loader) {
	duckdb::TableFunction function(
	    "moraine_rewrite_data_path",
	    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
	    RewriteDataPathExecute, RewriteDataPathBind, RewriteDataPathInit);
	function.GetSignature().WithTypedKwargs(
	    "options", [](duckdb::TypedKwargs &options) { options.Add("wal_path", duckdb::LogicalType::VARCHAR); });
	loader.RegisterFunction(function);
}
} // namespace moraine_duckdb
