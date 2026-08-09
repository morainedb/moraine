#include "maintenance.hpp"

#include "duckdb/main/client_context_state.hpp"

#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/extension_callback.hpp"

#include "catalog.hpp"
#include "moraine_abi.h"
#include "owned_array.hpp"

#include <algorithm>

namespace moraine_duckdb {

namespace {

// The DuckLake steps a pass runs, in order, with the parameters each
// accepts. Names are DuckLake's own minus the `ducklake_` prefix; an
// attach option is `MAINTENANCE_<step>` to enable a step with DuckLake's
// defaults, or `MAINTENANCE_<step>_<parameter>` to pass one through.
//
// Expiry leads because it is the only step that shrinks the catalog
// rather than adding to it, and every later step is then served a smaller
// snapshot projection. Flush precedes merge so its small files are merge
// input; merge and rewrite precede cleanup because merge schedules its
// superseded bytes directly and cleanup drains that schedule in the same
// pass; cleanup precedes orphan detection so the schedule is drained
// first. The sweep runs last, on what the earlier steps left behind.
struct StepSpec {
	const char *name;
	std::vector<const char *> parameters;
};

const std::vector<StepSpec> &StepSpecs() {
	static const std::vector<StepSpec> specs = {
	    {"expire_snapshots", {"older_than", "versions"}},
	    {"flush_inlined_data", {}},
	    {"merge_adjacent_files", {}},
	    {"rewrite_data_files", {"delete_threshold"}},
	    {"cleanup_old_files", {"older_than", "cleanup_all"}},
	    {"delete_orphaned_files", {"older_than", "cleanup_all"}},
	};
	return specs;
}

// A step's accumulating state while options are parsed in arrival order.
struct PendingStep {
	bool enabled = false;
	bool explicitly_disabled = false;
	std::vector<std::string> arguments;
};

std::string KnownStepList() {
	std::string known;
	for (auto &spec : StepSpecs()) {
		if (!known.empty()) {
			known += ", ";
		}
		known += spec.name;
	}
	return known;
}

} // namespace

MaintenanceConfig ParseMaintenanceOptions(const std::vector<std::pair<std::string, duckdb::Value>> &options) {
	MaintenanceConfig config;
	std::vector<PendingStep> pending(StepSpecs().size());
	bool compact_store_parameter_given = false;
	bool compact_store_explicitly_disabled = false;

	for (auto &option : options) {
		auto name = duckdb::StringUtil::Lower(option.first);
		if (!duckdb::StringUtil::StartsWith(name, "maintenance_")) {
			continue;
		}
		auto rest = name.substr(std::string("maintenance_").size());

		if (rest == "interval") {
			// Accept a plain count of seconds or an INTERVAL value; both
			// read naturally at an attach.
			if (option.second.type().id() == duckdb::LogicalTypeId::INTERVAL) {
				auto interval = option.second.GetValue<duckdb::interval_t>();
				config.interval_ms = static_cast<uint64_t>(duckdb::Interval::GetMicro(interval) / 1000);
			} else {
				config.interval_ms = option.second.GetValue<uint64_t>() * 1000;
			}
			if (config.interval_ms == 0) {
				throw duckdb::BinderException("MAINTENANCE_INTERVAL must be a positive duration");
			}
			continue;
		}
		if (rest == "batch_size") {
			config.batch_size = option.second.GetValue<uint64_t>();
			if (config.batch_size == 0) {
				throw duckdb::BinderException("MAINTENANCE_BATCH_SIZE must be positive");
			}
			continue;
		}
		if (rest == "sweep_indexes") {
			config.sweep_indexes = option.second.GetValue<bool>();
			continue;
		}
		if (rest == "compact_store") {
			config.compact_store = option.second.GetValue<bool>();
			compact_store_explicitly_disabled = !config.compact_store;
			continue;
		}
		if (rest == "compact_store_subspace") {
			config.compact_store_subspace = option.second.GetValue<std::string>();
			// Checked here rather than when a pass runs: a name validated
			// only at pass time would let a typo attach cleanly and then
			// fail every scheduled pass, unattended, for as long as it
			// stood. The vocabulary stays the core's — this asks it.
			if (!moraine_subspace_is_known(config.compact_store_subspace.c_str())) {
				auto known = moraine_subspace_names();
				std::string list = known != nullptr ? std::string(known) : "";
				if (known != nullptr) {
					moraine_error_free(known);
				}
				throw duckdb::BinderException(
				    "MAINTENANCE_COMPACT_STORE_SUBSPACE names no subspace \"%s\"; known subspaces are: %s",
				    config.compact_store_subspace, list);
			}
			// Supplying a parameter enables its step.
			compact_store_parameter_given = true;
			continue;
		}
		if (rest == "compact_store_timeout") {
			if (option.second.type().id() == duckdb::LogicalTypeId::INTERVAL) {
				auto interval = option.second.GetValue<duckdb::interval_t>();
				config.compact_store_timeout_ms =
				    static_cast<uint64_t>(duckdb::Interval::GetMicro(interval) / 1000);
			} else {
				config.compact_store_timeout_ms = option.second.GetValue<uint64_t>() * 1000;
			}
			if (config.compact_store_timeout_ms == 0) {
				throw duckdb::BinderException("MAINTENANCE_COMPACT_STORE_TIMEOUT must be a positive duration");
			}
			compact_store_parameter_given = true;
			continue;
		}

		// Longest-match the step name, then treat any remainder as one of
		// that step's own parameters. Both contain underscores, so the
		// split cannot be positional.
		bool matched = false;
		for (duckdb::idx_t i = 0; i < StepSpecs().size(); i++) {
			auto &spec = StepSpecs()[i];
			std::string step = spec.name;
			if (rest == step) {
				matched = true;
				if (option.second.GetValue<bool>()) {
					pending[i].enabled = true;
				} else {
					pending[i].explicitly_disabled = true;
				}
				break;
			}
			if (!duckdb::StringUtil::StartsWith(rest, step + "_")) {
				continue;
			}
			auto parameter = rest.substr(step.size() + 1);
			auto &accepted = spec.parameters;
			if (std::find_if(accepted.begin(), accepted.end(), [&](const char *candidate) {
				    return parameter == candidate;
			    }) == accepted.end()) {
				throw duckdb::BinderException(
				    "ducklake_%s takes no parameter named \"%s\"; MAINTENANCE_%s_<parameter> passes "
				    "DuckLake's own parameters through unchanged",
				    step, parameter, duckdb::StringUtil::Upper(step));
			}
			matched = true;
			// Supplying a parameter enables its step.
			pending[i].enabled = true;
			// Attach options are evaluated once, at attach. A timestamp
			// written as `now()` therefore freezes into a literal, and a
			// schedule would keep expiring against its attach-time
			// instant forever — retention silently stopping as the lake
			// moves on. An interval expresses the rolling window that
			// was meant, rendered so DuckLake evaluates it each pass.
			if (parameter == "older_than" && option.second.type().id() == duckdb::LogicalTypeId::INTERVAL) {
				// `now()` is TIMESTAMPTZ, and subtracting an interval from
				// one needs the `icu` extension. Casting to TIMESTAMP uses
				// an operator that is always available, and DuckLake
				// accepts either for this parameter.
				pending[i].arguments.push_back(parameter + " => now()::TIMESTAMP - " +
				                               option.second.ToSQLString());
			} else {
				pending[i].arguments.push_back(parameter + " => " + option.second.ToSQLString());
			}
			break;
		}
		if (!matched) {
			throw duckdb::BinderException("unknown maintenance option \"MAINTENANCE_%s\"; known steps are: %s",
			                              duckdb::StringUtil::Upper(rest), KnownStepList());
		}
	}

	// Supplying a parameter enables its step, as it does for every
	// DuckLake step — but disabling a step while parameterizing it says
	// two contradictory things, so it is refused rather than resolved.
	if (compact_store_parameter_given) {
		if (compact_store_explicitly_disabled) {
			throw duckdb::BinderException(
			    "MAINTENANCE_COMPACT_STORE is false but one of its parameters was supplied; drop one or "
			    "the other");
		}
		config.compact_store = true;
	}

	for (duckdb::idx_t i = 0; i < pending.size(); i++) {
		auto &step = pending[i];
		if (step.explicitly_disabled && !step.arguments.empty()) {
			throw duckdb::BinderException(
			    "MAINTENANCE_%s is false but one of its parameters was supplied; drop one or the other",
			    duckdb::StringUtil::Upper(StepSpecs()[i].name));
		}
		if (step.enabled && !step.explicitly_disabled) {
			config.ducklake_steps.push_back(DuckLakeStep {StepSpecs()[i].name, step.arguments});
		}
	}
	return config;
}

namespace {

// The name DuckLake gives its metadata catalog by default. An attach
// that passes `METADATA_CATALOG` uses its own name instead, which is why
// this prefix is only ever a fallback.
constexpr const char *METADATA_PREFIX = "__ducklake_metadata_";
constexpr const char *MAINTENANCE_CLOSE_STATE = "moraine_maintenance_close";
constexpr const char *MAINTENANCE_CONNECTION_STATE = "moraine_maintenance_connection";

class MaintenanceConnectionState : public duckdb::ClientContextState {
};

class MaintenanceLifecycle;

class MaintenanceCloseState : public duckdb::ClientContextState {
public:
	MaintenanceCloseState(MaintenanceLifecycle &lifecycle, uint64_t host_epoch)
	    : lifecycle_(lifecycle), host_epoch_(host_epoch) {
	}
	~MaintenanceCloseState() override;

private:
	MaintenanceLifecycle &lifecycle_;
	uint64_t host_epoch_;
};

thread_local bool opening_maintenance_connection = false;

class MaintenanceConnectionScope {
public:
	MaintenanceConnectionScope() : previous_(opening_maintenance_connection) {
		opening_maintenance_connection = true;
	}
	~MaintenanceConnectionScope() {
		opening_maintenance_connection = previous_;
	}

private:
	bool previous_;
};

class MaintenanceLifecycle : public duckdb::ExtensionCallback {
public:
	void Add(const duckdb::shared_ptr<MaintenanceScheduler> &scheduler) {
		std::lock_guard<std::mutex> guard(lock_);
		Prune();
		schedulers_.push_back(scheduler);
	}

	void OnConnectionOpened(duckdb::ClientContext &context) override {
		if (opening_maintenance_connection) {
			context.registered_state->GetOrCreate<MaintenanceConnectionState>(MAINTENANCE_CONNECTION_STATE);
			return;
		}

		std::lock_guard<std::mutex> guard(lock_);
		host_epoch_++;
	}

	void OnConnectionClosed(duckdb::ClientContext &context) override {
		if (context.registered_state->Get<MaintenanceConnectionState>(MAINTENANCE_CONNECTION_STATE)) {
			return;
		}

		auto &connections = duckdb::ConnectionManager::Get(context).GetConnectionListReference();
		for (auto &entry : connections) {
			auto &other = entry.first.get();
			if (&other == &context || entry.second.expired()) {
				continue;
			}
			if (!other.registered_state->Get<MaintenanceConnectionState>(MAINTENANCE_CONNECTION_STATE)) {
				return;
			}
		}

		uint64_t host_epoch;
		{
			std::lock_guard<std::mutex> guard(lock_);
			host_epoch = host_epoch_;
		}
		context.registered_state->GetOrCreate<MaintenanceCloseState>(MAINTENANCE_CLOSE_STATE, *this, host_epoch);
	}

	void StopIfNoHostOpened(uint64_t host_epoch) {
		std::vector<duckdb::shared_ptr<MaintenanceScheduler>> schedulers;
		{
			std::lock_guard<std::mutex> guard(lock_);
			if (host_epoch_ != host_epoch) {
				return;
			}
			Prune();
			for (auto &scheduler : schedulers_) {
				auto live = scheduler.lock();
				if (live) {
					schedulers.push_back(std::move(live));
				}
			}
		}
		for (auto &scheduler : schedulers) {
			scheduler->Stop();
		}
	}

private:
	void Prune() {
		schedulers_.erase(std::remove_if(schedulers_.begin(), schedulers_.end(),
		                                 [](const auto &scheduler) { return scheduler.expired(); }),
		                  schedulers_.end());
	}

	std::mutex lock_;
	std::vector<duckdb::weak_ptr<MaintenanceScheduler>> schedulers_;
	uint64_t host_epoch_ = 0;
};

MaintenanceCloseState::~MaintenanceCloseState() {
	lifecycle_.StopIfNoHostOpened(host_epoch_);
}

} // namespace

MaintenanceScheduler::MaintenanceScheduler(duckdb::DatabaseInstance &db, std::string attached_name,
                                           std::string store_path, MoraineCatalogHandle *handle,
                                           MaintenanceConfig config)
    : db_(db), attached_name_(std::move(attached_name)), store_path_(std::move(store_path)),
      handle_(handle), config_(std::move(config)) {
}

std::string MaintenanceScheduler::ResolveLakeName(duckdb::Connection &connection) {
	// DuckLake's maintenance functions take the *lake* name, while this
	// catalog is attached under its metadata name. The two are related by
	// path — the lake attaches `moraine:<store path>` and this catalog
	// reports `<store path>` — so match on that rather than assuming the
	// default `__ducklake_metadata_<lake>` naming, which an attach can
	// override with `METADATA_CATALOG`.
	auto result = connection.Query("SELECT database_name, path FROM duckdb_databases() "
	                               "WHERE type = 'ducklake'");
	if (!result->HasError()) {
		for (auto &row : *result) {
			auto name = row.GetValue<std::string>(0);
			auto path = row.GetValue<std::string>(1);
			if (duckdb::StringUtil::EndsWith(path, store_path_)) {
				return name;
			}
		}
	}
	// No DuckLake above this catalog (a standalone `moraine:` attach), or
	// the listing failed. Fall back to the default naming so a lake that
	// follows it still works.
	if (duckdb::StringUtil::StartsWith(attached_name_, METADATA_PREFIX)) {
		return attached_name_.substr(std::string(METADATA_PREFIX).size());
	}
	return attached_name_;
}

MaintenanceScheduler::~MaintenanceScheduler() {
	Stop();
}

void MaintenanceScheduler::Start() {
	std::lock_guard<std::mutex> stop_guard(stop_lock_);
	if (config_.interval_ms == 0 || thread_.joinable()) {
		return;
	}
	{
		std::lock_guard<std::mutex> guard(wake_lock_);
		stopping_ = false;
	}
	thread_ = std::thread([this]() { Loop(); });
}

void MaintenanceScheduler::Stop() {
	std::lock_guard<std::mutex> stop_guard(stop_lock_);
	{
		std::lock_guard<std::mutex> guard(wake_lock_);
		stopping_ = true;
	}
	wake_.notify_all();
	if (thread_.joinable()) {
		thread_.join();
	}
}

void BindMaintenanceScheduler(duckdb::ClientContext &context,
                              const duckdb::shared_ptr<MaintenanceScheduler> &scheduler) {
	for (auto &callback : duckdb::ExtensionCallback::Iterate(context)) {
		auto lifecycle = dynamic_cast<MaintenanceLifecycle *>(callback.get());
		if (lifecycle != nullptr) {
			lifecycle->Add(scheduler);
			return;
		}
	}
	throw duckdb::InternalException("moraine: maintenance lifecycle callback is not registered");
}

void MaintenanceScheduler::Loop() {
	auto interval = std::chrono::milliseconds(config_.interval_ms);
	for (;;) {
		{
			std::unique_lock<std::mutex> guard(wake_lock_);
			// Waiting on the stop flag means detach does not have to
			// outlast a whole interval to shut the thread down.
			if (wake_.wait_for(guard, interval, [this]() { return stopping_; })) {
				return;
			}
		}
		// A tick arriving while a pass is still running skips rather than
		// queueing: two concurrent passes are safe but collide on
		// DuckLake's own conflict rules, and a scheduler that
		// manufactures its own conflicts is indefensible.
		RunPass(true, "scheduled");
	}
}

std::vector<MaintenanceStep> MaintenanceScheduler::RunNow() {
	return RunPass(false, "manual");
}

std::vector<MaintenanceStep> MaintenanceScheduler::RunPass(bool skip_if_busy, const char *trigger) {
	std::unique_lock<std::mutex> pass(pass_lock_, std::defer_lock);
	if (skip_if_busy) {
		if (!pass.try_lock()) {
			return {};
		}
	} else {
		pass.lock();
	}

	auto started_at = duckdb::Timestamp::GetCurrentTimestamp();
	std::vector<MaintenanceStep> report;
	// The pass runs on a connection of its own. `ClientContext::Query`
	// takes a non-recursive lock, so a query issued from inside a running
	// operator on the caller's own context would deadlock; a separate
	// connection on the same instance is the supported way in.
	MaintenanceConnectionScope maintenance_connection;
	duckdb::Connection connection(db_);
	auto lake = ResolveLakeName(connection);

	// A failed step abandons the rest of the *DuckLake* sequence, whose
	// steps depend on each other — cleanup drains what merge scheduled,
	// so running it after a failed merge does partial work on a premise
	// that did not hold. The abandoned steps are still reported, so every
	// pass emits the same rows and two passes can be compared.
	std::string aborted_by;
	for (auto &spec : StepSpecs()) {
		if (!aborted_by.empty()) {
			report.push_back(
			    MaintenanceStep {spec.name, "skipped", "not attempted: " + aborted_by + " failed"});
			continue;
		}
		auto configured = std::find_if(config_.ducklake_steps.begin(), config_.ducklake_steps.end(),
		                               [&](const DuckLakeStep &step) { return step.name == spec.name; });
		if (configured == config_.ducklake_steps.end()) {
			report.push_back(MaintenanceStep {spec.name, "skipped", "not configured at attach"});
			continue;
		}
		auto outcome = RunDuckLakeStep(connection, lake, *configured);
		if (outcome.status == "failed") {
			aborted_by = spec.name;
		}
		report.push_back(std::move(outcome));
	}

	// The sweep runs regardless. It depends on no DuckLake step — it
	// reclaims moraine's own orphaned index ranges — so letting a failed
	// expiry suppress it would stop the reclamation this pass exists for
	// on every future pass too, for as long as the misconfiguration
	// stands, while the leak it fixes kept growing.
	report.push_back(config_.sweep_indexes
	                     ? RunSweep()
	                     : MaintenanceStep {"sweep_indexes", "skipped", "disabled at attach"});

	// The store merge runs last, on what every step above it left behind:
	// expiry tombstones rows and the sweep deletes index ranges, so
	// merging earlier would leave exactly the tombstones this pass just
	// created for the next pass to reclaim.
	report.push_back(config_.compact_store
	                     ? RunStoreMerge()
	                     : MaintenanceStep {"compact_store", "skipped", "not configured at attach"});

	RecordPass(started_at, trigger, report);

	// The pass ran with no ClientContext (its own connection, its own
	// thread), so the core's events from the sweep and DuckLake steps sit
	// buffered; the database-scoped drain is the only one that can reach
	// them here. The outcome line makes a scheduled pass visible without
	// querying the report table: `warn` names each failed step, `info`
	// closes a clean pass.
	DrainMoraineLogs(db_);
	std::string failed;
	for (auto &step : report) {
		if (step.status == "failed") {
			failed += (failed.empty() ? "" : ", ") + step.step + ": " + step.detail;
		}
	}
	if (!failed.empty()) {
		WriteMoraineLog(db_, duckdb::LogLevel::LOG_WARNING,
		                std::string("maintenance pass (") + trigger + ") had failures — " + failed);
	} else {
		WriteMoraineLog(db_, duckdb::LogLevel::LOG_INFO,
		                std::string("maintenance pass (") + trigger + ") completed");
	}
	return report;
}

void MaintenanceScheduler::RecordPass(duckdb::timestamp_t started_at, const char *trigger,
                                      const std::vector<MaintenanceStep> &report) noexcept {
	try {
		std::vector<MoraineMaintenanceStatusStepInput> steps;
		steps.reserve(report.size());
		for (auto &step : report) {
			steps.push_back(MoraineMaintenanceStatusStepInput {step.step.c_str(), step.status.c_str(),
			                                                   step.detail.c_str()});
		}

		MoraineError err {};
		auto code = moraine_maintenance_status_record(handle_, started_at.value, trigger, steps.data(), steps.size(), &err);
		if (code == MORAINE_OK) {
			return;
		}
		std::string message = err.message != nullptr ? std::string(err.message) : "unknown error";
		if (err.message != nullptr) {
			moraine_error_free(err.message);
		}
		WriteMoraineLog(db_, duckdb::LogLevel::LOG_WARNING,
		                "maintenance pass completed but its status could not be persisted: " + message);
	} catch (const std::exception &error) {
		WriteMoraineLog(db_, duckdb::LogLevel::LOG_WARNING,
		                "maintenance pass completed but its status could not be persisted: " + std::string(error.what()));
	} catch (...) {
		WriteMoraineLog(db_, duckdb::LogLevel::LOG_WARNING,
		                "maintenance pass completed but its status could not be persisted: unknown error");
	}
}

MaintenanceStep MaintenanceScheduler::RunDuckLakeStep(duckdb::Connection &connection, const std::string &lake,
                                                     const DuckLakeStep &step) {
	std::string sql = "CALL ducklake_" + step.name + "(" + duckdb::KeywordHelper::WriteQuoted(lake, '\'');
	for (auto &argument : step.arguments) {
		sql += ", " + argument;
	}
	sql += ")";

	auto result = connection.Query(sql);
	if (result->HasError()) {
		return MaintenanceStep {step.name, "failed", result->GetError()};
	}
	return MaintenanceStep {step.name, "ran", sql};
}

MaintenanceStep MaintenanceScheduler::RunSweep() {
	uint64_t indexes = 0;
	uint64_t entries = 0;
	MoraineError err {};
	// No interrupt probe: the sweep runs on the scheduler's own thread,
	// which stops through the stop flag rather than a query interrupt.
	auto code = moraine_maintain(handle_, config_.batch_size, &indexes, &entries, nullptr, nullptr, &err);
	if (code != MORAINE_OK) {
		std::string message = err.message != nullptr ? std::string(err.message) : "unknown error";
		if (err.message != nullptr) {
			moraine_error_free(err.message);
		}
		return MaintenanceStep {"sweep_indexes", "failed", message};
	}
	return MaintenanceStep {"sweep_indexes", "ran",
	                        "reclaimed " + std::to_string(entries) + (entries == 1 ? " entry" : " entries") +
	                            " from " + std::to_string(indexes) +
	                            (indexes == 1 ? " dropped index" : " dropped indexes")};
}

MaintenanceStep MaintenanceScheduler::RunStoreMerge() {
	OwnedArray<MoraineSubspaceMerge> merges(moraine_compact_store_free);
	MoraineError err {};
	// No interrupt probe: the pass runs on the scheduler's own thread,
	// which stops through the stop flag rather than a query interrupt.
	auto code = moraine_compact_store(handle_,
	                                  config_.compact_store_subspace.empty()
	                                      ? nullptr
	                                      : config_.compact_store_subspace.c_str(),
	                                  config_.compact_store_timeout_ms, merges.OutItems(), merges.OutLen(),
	                                  nullptr, nullptr, &err);
	if (code != MORAINE_OK) {
		std::string message = err.message != nullptr ? std::string(err.message) : "unknown error";
		if (err.message != nullptr) {
			moraine_error_free(err.message);
		}
		return MaintenanceStep {"compact_store", "failed", message};
	}

	// One clause per subspace, so a pass that merged some and skipped
	// others says which was which rather than reporting a total.
	std::string detail;
	uint64_t reclaimed = 0;
	for (auto &merge : merges) {
		if (!detail.empty()) {
			detail += ", ";
		}
		detail += std::string(merge.subspace) + ": " + merge.outcome;
		if (merge.detail != nullptr && *merge.detail != '\0') {
			detail += " (" + std::string(merge.detail) + ")";
		}
		if (merge.has_bytes_after && merge.bytes_after < merge.bytes_before) {
			reclaimed += merge.bytes_before - merge.bytes_after;
		}
	}
	if (reclaimed > 0) {
		detail += "; reclaimed " + std::to_string(reclaimed) + " bytes";
	}
	return MaintenanceStep {"compact_store", "ran", detail};
}

namespace {

// Resolves the `MoraineCatalog` behind a lake name, accepting either the
// DuckLake name or moraine's metadata catalog directly — the same pair
// `moraine_index_*` accepts.
struct MaintenanceBindData : public duckdb::FunctionData {
	std::string catalog_name;
	// True for the status function, which reports without running.
	bool status_only = false;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<MaintenanceBindData>();
		*result = *this;
		return std::move(result);
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<MaintenanceBindData>();
		return catalog_name == other.catalog_name && status_only == other.status_only;
	}
};

// One emitted row. The trigger reports a single pass and omits the pass
// columns; the status function reports the retained passes and carries
// them, so a failure stays attributable to when it happened and to what
// drove it.
struct ReportRow {
	duckdb::timestamp_t started_at;
	std::string trigger;
	MaintenanceStep step;
};

struct MaintenanceGlobalState : public duckdb::GlobalTableFunctionState {
	std::vector<ReportRow> rows;
	bool with_pass_columns = false;
	duckdb::idx_t emitted = 0;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> MaintenanceBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                         duckdb::vector<duckdb::LogicalType> &return_types,
                                                         duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<MaintenanceBindData>();
	if (input.inputs[0].IsNull()) {
		throw duckdb::BinderException("moraine_maintenance: the lake name must not be NULL");
	}
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
	names = {"step", "status", "detail"};
	return std::move(bind_data);
}

duckdb::unique_ptr<duckdb::FunctionData> StatusBind(duckdb::ClientContext &context,
                                                    duckdb::TableFunctionBindInput &input,
                                                    duckdb::vector<duckdb::LogicalType> &return_types,
                                                    duckdb::vector<duckdb::string> &names) {
	auto bind_data = MaintenanceBind(context, input, return_types, names);
	bind_data->Cast<MaintenanceBindData>().status_only = true;
	// Retained passes carry when they ran and what drove them.
	return_types.insert(return_types.begin(),
	                    {duckdb::LogicalType::TIMESTAMP, duckdb::LogicalType::VARCHAR});
	names.insert(names.begin(), {"started_at", "trigger"});
	return bind_data;
}

// The pass runs once, at execution start.
duckdb::unique_ptr<duckdb::GlobalTableFunctionState> MaintenanceInitGlobal(duckdb::ClientContext &context,
                                                                           duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MaintenanceBindData>();
	auto &catalog = ResolveMoraineCatalog(context, bind_data.catalog_name);
	auto state = duckdb::make_uniq<MaintenanceGlobalState>();

	if (bind_data.status_only) {
		state->with_pass_columns = true;
		OwnedArray<MoraineMaintenanceStatusRow> rows(moraine_maintenance_status_free);
		MoraineError err {};
		auto code = moraine_maintenance_status_rows(catalog.Handle(), rows.OutItems(), rows.OutLen(), &err);
		if (code != MORAINE_OK) {
			ThrowMoraineError(err);
		}
		for (auto &row : rows) {
			state->rows.push_back(ReportRow {
			    duckdb::timestamp_t(row.started_at_micros), std::string(row.trigger),
			    MaintenanceStep {std::string(row.step), std::string(row.status), std::string(row.detail)}});
		}
		return std::move(state);
	}

	if (catalog.GetAttached().IsReadOnly()) {
		throw duckdb::CatalogException("moraine_maintenance: \"%s\" is attached read-only; maintenance mutates",
		                               bind_data.catalog_name);
	}
	// The caller blocks while a separate connection writes the catalog,
	// so running inside an explicit transaction invites a self-deadlock.
	if (!context.transaction.IsAutoCommit()) {
		throw duckdb::TransactionException(
		    "moraine_maintenance cannot run inside an explicit transaction; COMMIT first");
	}
	for (auto &step : catalog.Scheduler().RunNow()) {
		state->rows.push_back(ReportRow {duckdb::timestamp_t(0), "manual", step});
	}
	return std::move(state);
}

void MaintenanceImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<MaintenanceGlobalState>();
	duckdb::idx_t count = 0;
	while (state.emitted < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.emitted];
		duckdb::idx_t column = 0;
		if (state.with_pass_columns) {
			output.SetValue(column++, count, duckdb::Value::TIMESTAMP(row.started_at));
			output.SetValue(column++, count, duckdb::Value(row.trigger));
		}
		output.SetValue(column++, count, duckdb::Value(row.step.step));
		output.SetValue(column++, count, duckdb::Value(row.step.status));
		output.SetValue(column, count, duckdb::Value(row.step.detail));
		state.emitted++;
		count++;
	}
	output.SetCardinality(count);
}

// moraine_compact_store: step 8 on its own, for a store that needs
// merging once rather than on a cadence. The pass is the scheduled form;
// this is the form an operator reaches for after a census says where the
// weight is, without re-attaching a live application's catalog.

struct CompactBindData : public duckdb::FunctionData {
	std::string catalog_name;
	// Empty merges every subspace holding sorted runs.
	std::string subspace;
	// Zero returns as soon as the merges are submitted.
	uint64_t timeout_ms = 0;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CompactBindData>();
		*result = *this;
		return std::move(result);
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<CompactBindData>();
		return catalog_name == other.catalog_name && subspace == other.subspace &&
		       timeout_ms == other.timeout_ms;
	}
};

// One merged subspace. Held on the global state rather than the bind
// data: these are what this run did, and bind data is shared and const.
struct CompactRow {
	std::string subspace;
	std::string outcome;
	std::string detail;
	uint64_t bytes_before;
	bool has_bytes_after;
	uint64_t bytes_after;
};

duckdb::unique_ptr<duckdb::FunctionData> CompactBind(duckdb::ClientContext &,
                                                     duckdb::TableFunctionBindInput &input,
                                                     duckdb::vector<duckdb::LogicalType> &return_types,
                                                     duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<CompactBindData>();
	if (input.inputs[0].IsNull()) {
		throw duckdb::BinderException("moraine_compact_store: the lake name must not be NULL");
	}
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();

	auto subspace = input.named_parameters.find("subspace");
	if (subspace != input.named_parameters.end() && !subspace->second.IsNull()) {
		bind_data->subspace = subspace->second.GetValue<std::string>();
		// Checked here for the same reason the attach option is: a name
		// this build does not know names no tree, so there is nothing to
		// merge and nothing sensible to report.
		if (!moraine_subspace_is_known(bind_data->subspace.c_str())) {
			auto known = moraine_subspace_names();
			std::string list = known != nullptr ? std::string(known) : "";
			if (known != nullptr) {
				moraine_error_free(known);
			}
			throw duckdb::BinderException("moraine_compact_store: no subspace named \"%s\"; known subspaces "
			                              "are: %s",
			                              bind_data->subspace, list);
		}
	}

	auto timeout = input.named_parameters.find("timeout");
	if (timeout != input.named_parameters.end() && !timeout->second.IsNull()) {
		if (timeout->second.type().id() == duckdb::LogicalTypeId::INTERVAL) {
			auto interval = timeout->second.GetValue<duckdb::interval_t>();
			bind_data->timeout_ms = static_cast<uint64_t>(duckdb::Interval::GetMicro(interval) / 1000);
		} else {
			bind_data->timeout_ms = timeout->second.GetValue<uint64_t>() * 1000;
		}
		if (bind_data->timeout_ms == 0) {
			throw duckdb::BinderException("moraine_compact_store: timeout must be a positive duration");
		}
	}

	using duckdb::LogicalType;
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT,
	                LogicalType::UBIGINT};
	names = {"subspace", "outcome", "detail", "bytes_before", "bytes_after"};
	return std::move(bind_data);
}

struct CompactGlobalState : public duckdb::GlobalTableFunctionState {
	std::vector<CompactRow> rows;
	duckdb::idx_t offset = 0;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

// The merge is submitted once, at execution start — not at bind, which
// planning may repeat.
duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CompactInitGlobal(duckdb::ClientContext &context,
                                                                       duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<CompactBindData>();
	auto &catalog = ResolveMoraineCatalog(context, bind_data.catalog_name);
	if (catalog.GetAttached().IsReadOnly()) {
		// Submitting needs no writer, but executing does: the compactor
		// that promotes a submission runs inside the writer, so a reader
		// would queue work nothing would run and then wait it out.
		throw duckdb::CatalogException(
		    "moraine_compact_store: \"%s\" is attached read-only; the merge runs inside the writer",
		    bind_data.catalog_name);
	}

	OwnedArray<MoraineSubspaceMerge> merges(moraine_compact_store_free);
	MoraineError err {};
	auto code = moraine_compact_store(catalog.Handle(), bind_data.subspace.empty() ? nullptr
	                                                                               : bind_data.subspace.c_str(),
	                                  bind_data.timeout_ms, merges.OutItems(), merges.OutLen(),
	                                  moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	auto state = duckdb::make_uniq<CompactGlobalState>();
	for (auto &merge : merges) {
		state->rows.push_back({std::string(merge.subspace), std::string(merge.outcome),
		                       merge.detail != nullptr ? std::string(merge.detail) : std::string(),
		                       merge.bytes_before, merge.has_bytes_after, merge.bytes_after});
	}
	return std::move(state);
}

void CompactImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<CompactGlobalState>();
	if (state.offset >= state.rows.size()) {
		output.SetCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.offset);
	for (duckdb::idx_t i = 0; i < count; i++) {
		auto &row = state.rows[state.offset + i];
		output.SetValue(0, i, duckdb::Value(row.subspace));
		output.SetValue(1, i, duckdb::Value(row.outcome));
		output.SetValue(2, i, duckdb::Value(row.detail));
		output.SetValue(3, i, duckdb::Value::UBIGINT(row.bytes_before));
		// NULL rather than zero unless the merge committed: a subspace
		// that reclaimed nothing and one that never ran differ.
		output.SetValue(4, i,
		                row.has_bytes_after ? duckdb::Value::UBIGINT(row.bytes_after)
		                                    : duckdb::Value(duckdb::LogicalType::UBIGINT));
	}
	state.offset += count;
	output.SetCardinality(count);
}

} // namespace

void RegisterMoraineMaintenanceFunctions(duckdb::ExtensionLoader &loader) {
	duckdb::ExtensionCallback::Register(loader.GetDatabaseInstance().config,
	                                    duckdb::make_shared_ptr<MaintenanceLifecycle>());
	duckdb::TableFunction maintenance("moraine_maintenance", {duckdb::LogicalType::VARCHAR}, MaintenanceImpl,
	                                  MaintenanceBind, MaintenanceInitGlobal);
	loader.RegisterFunction(maintenance);

	duckdb::TableFunction status("moraine_maintenance_status", {duckdb::LogicalType::VARCHAR}, MaintenanceImpl,
	                             StatusBind, MaintenanceInitGlobal);
	loader.RegisterFunction(status);

	duckdb::TableFunction compact("moraine_compact_store", {duckdb::LogicalType::VARCHAR}, CompactImpl,
	                              CompactBind, CompactInitGlobal);
	compact.named_parameters["subspace"] = duckdb::LogicalType::VARCHAR;
	compact.named_parameters["timeout"] = duckdb::LogicalType::ANY;
	loader.RegisterFunction(compact);
}

} // namespace moraine_duckdb
