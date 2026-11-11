#include "settings.hpp"

#include "duckdb/main/config.hpp"

namespace moraine_duckdb {

namespace {

struct ByteSetting {
	const char *name;
	const char *description;
};

// Each is read by the first attach in the process; later attaches share
// what it sized.
const ByteSetting kByteSettings[] = {
    {"moraine_cache_size", "Byte cap on the block cache's disk tier, under moraine_cache_dir"},
    {"moraine_cache_memory", "Process-wide byte cap on the in-memory block cache"},
    {"moraine_working_memory", "Process-wide allowance for live working allocations"},
    {"moraine_staging_memory", "Rows one transaction may stage"},
    {"moraine_commit_memory", "One commit's batch"},
    {"moraine_store_memory", "SlateDB's unflushed writes per writer"},
    {"moraine_summary_memory", "One file's row-summary build"},
    {"moraine_host_memory", "The host's memory, checked against the budgets before attach"},
    {"moraine_host_headroom", "Memory left unclaimed on the host; a quarter of moraine_host_memory"},
};

} // namespace

void RegisterMoraineSettings(duckdb::DBConfig &config) {
	config.AddExtensionOption("moraine_cache_dir", "Local directory for the block cache's disk tier",
	                          duckdb::LogicalType::VARCHAR, duckdb::Value(""));
	for (auto &setting : kByteSettings) {
		config.AddExtensionOption(setting.name, setting.description, duckdb::LogicalType::VARCHAR, duckdb::Value(""));
	}
	config.AddExtensionOption("moraine_cache_auxiliary_percent",
	                          "Share of moraine_cache_memory for parsed Parquet metadata, capped at 50",
	                          duckdb::LogicalType::UBIGINT, duckdb::Value::UBIGINT(0));
}

std::string SettingString(duckdb::ClientContext &context, const char *name) {
	duckdb::Value value;
	if (!context.TryGetCurrentSetting(name, value) || value.IsNull()) {
		return std::string();
	}
	return value.GetValue<std::string>();
}

std::optional<uint64_t> SettingBytes(duckdb::ClientContext &context, const char *name) {
	auto text = SettingString(context, name);
	if (text.empty()) {
		return std::nullopt;
	}
	auto bytes = duckdb::DBConfig::ParseMemoryLimit(text);
	if (!bytes.IsValid() || bytes.GetIndex() == 0) {
		throw duckdb::InvalidInputException("moraine: %s must be a memory size such as '512MB'; got \"%s\"", name,
		                                    text);
	}
	return bytes.GetIndex();
}

uint64_t SettingUnsigned(duckdb::ClientContext &context, const char *name) {
	duckdb::Value value;
	if (!context.TryGetCurrentSetting(name, value) || value.IsNull()) {
		return 0;
	}
	return value.GetValue<uint64_t>();
}

MoraineMemoryBudgets MemoryBudgetSettings(duckdb::ClientContext &context) {
	auto bytes = [&](const char *name) {
		return SettingBytes(context, name).value_or(0);
	};
	MoraineMemoryBudgets budgets {};
	budgets.working = bytes("moraine_working_memory");
	budgets.staging = bytes("moraine_staging_memory");
	budgets.commit = bytes("moraine_commit_memory");
	budgets.store = bytes("moraine_store_memory");
	budgets.summary = bytes("moraine_summary_memory");
	budgets.host = bytes("moraine_host_memory");
	budgets.host_headroom = bytes("moraine_host_headroom");
	return budgets;
}

} // namespace moraine_duckdb
