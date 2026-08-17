// The maintenance pass: DuckLake's own maintenance SQL, in a fixed order,
// followed by moraine's orphaned-index-entry sweep. A pass runs on a
// connection of its own — never the caller's context, whose lock a
// re-entrant query would deadlock on.
#pragma once

#include "duckdb.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

struct MoraineCatalogHandle;

namespace moraine_duckdb {

// One step's outcome. `status` is "ran", "skipped" (the attach did not
// configure it), or "failed".
struct MaintenanceStep {
	std::string step;
	std::string status;
	std::string detail;
};

// A DuckLake step the attach configured, with the arguments to pass.
struct DuckLakeStep {
	// The function minus its `ducklake_` prefix, e.g. `expire_snapshots`.
	std::string name;
	// Rendered `name => literal` arguments, in the order supplied.
	std::vector<std::string> arguments;
};

// What the attach configured. Steps absent here are reported "skipped":
// every step that mutates the lake is opt-in, so an attach that
// configures nothing reclaims only what no query can observe.
struct MaintenanceConfig {
	// Zero starts no thread; the trigger still runs passes on demand.
	uint64_t interval_ms = 0;
	// Zero takes the core's own batch default.
	uint64_t batch_size = 0;
	bool sweep_indexes = true;
	// Off by default: the merge rewrites whatever the store holds and
	// pays for every byte in object-store traffic. It destroys nothing a
	// query can observe, so the default is about cost, not safety.
	bool compact_store = false;
	// Empty merges every subspace holding sorted runs.
	std::string compact_store_subspace;
	// How long a pass waits for each submitted merge to commit. Zero
	// returns as soon as they are submitted; a merge that outlives the
	// wait keeps running and is reported pending.
	uint64_t compact_store_timeout_ms = 0;
	std::vector<DuckLakeStep> ducklake_steps;
};

// Parses the `MAINTENANCE_*` attach options, in whatever order they
// arrive. Throws `BinderException` on an unknown step, an unknown
// parameter, or a step disabled while one of its parameters is supplied.
MaintenanceConfig ParseMaintenanceOptions(
    const std::vector<std::pair<std::string, duckdb::Value>> &options);

// Drives maintenance for one attached lake: a timer thread when the
// attach configured an interval, plus the on-demand trigger. Both run the
// same pass, and never two at once.
class MaintenanceScheduler {
public:
	MaintenanceScheduler(duckdb::DatabaseInstance &db, std::string attached_name, std::string store_path,
	                     MoraineCatalogHandle *handle, MaintenanceConfig config);
	~MaintenanceScheduler();

	MaintenanceScheduler(const MaintenanceScheduler &) = delete;
	MaintenanceScheduler &operator=(const MaintenanceScheduler &) = delete;

	// Starts the timer thread if an interval is configured. A read-only
	// attach must never call this — maintenance mutates.
	void Start();

	// Signals the thread to stop and joins it. Idempotent, and safe to
	// call from the detach hook and again from the destructor.
	void Stop();

	// Runs one pass now, blocking until it finishes, and returns its
	// report. If a pass is already running, waits for it and runs after.
	std::vector<MaintenanceStep> RunNow();

private:
	void Loop();
	// Runs the configured steps in order. `skip_if_busy` makes a tick
	// yield to a pass already in flight rather than queueing behind it.
	std::vector<MaintenanceStep> RunPass(bool skip_if_busy, const char *trigger);
	// Persists one completed pass. A status-write failure is logged but
	// cannot change maintenance work that has already completed.
	void RecordPass(duckdb::timestamp_t started_at, const char *trigger,
	                const std::vector<MaintenanceStep> &report) noexcept;
	// Issues one `CALL ducklake_<step>(...)` on `connection`.
	MaintenanceStep RunDuckLakeStep(duckdb::Connection &connection, const std::string &lake,
	                                const DuckLakeStep &step);
	MaintenanceStep RunSweep();
	// Reports what `RunSweep`'s single pass reclaimed from the file column
	// statistics of data files no snapshot can still resolve. Runs after
	// it and reads the counters it left.
	MaintenanceStep RunFileStatsSweep();
	MaintenanceStep RunStoreMerge();
	// The DuckLake catalog sitting above this metadata catalog, found by
	// matching attached databases on path. DuckLake's own maintenance
	// functions take that name, not this catalog's.
	std::string ResolveLakeName(duckdb::Connection &connection);

	duckdb::DatabaseInstance &db_;
	// This catalog's own attached name and store path.
	std::string attached_name_;
	std::string store_path_;
	MoraineCatalogHandle *handle_;
	MaintenanceConfig config_;

	// What the last `RunSweep` reclaimed from file column statistics, and
	// whether it got far enough to have reclaimed anything. Written by it,
	// read by `RunFileStatsSweep`, which always follows it in one pass.
	uint64_t file_stats_reclaimed_ = 0;
	bool file_stats_swept_ = false;

	// Held for the duration of a pass, so the timer and the trigger can
	// never overlap.
	std::mutex pass_lock_;

	std::mutex wake_lock_;
	std::condition_variable wake_;
	bool stopping_ = false;
	// Serializes callers from explicit detach, catalog destruction, and
	// last-host-context destruction around the one joinable thread.
	std::mutex stop_lock_;
	std::thread thread_;
};

// Registers a scheduler with its database's connection lifecycle. Destruction
// of the last non-maintenance context stops it before that context releases
// its DatabaseInstance reference.
void BindMaintenanceScheduler(duckdb::ClientContext &context,
                              const duckdb::shared_ptr<MaintenanceScheduler> &scheduler);

void RegisterMoraineMaintenanceFunctions(duckdb::ExtensionLoader &loader);

} // namespace moraine_duckdb
