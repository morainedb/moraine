#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <map>
#include <stdexcept>

using namespace duckdb;

static void Require(bool condition, const std::string &message) {
	if (!condition) {
		throw std::runtime_error(message);
	}
}

static unique_ptr<QueryResult> Query(Connection &connection, const std::string &sql) {
	auto result = connection.Query(sql);
	Require(!result->HasError(), sql + ": " + (result->HasError() ? result->GetError() : ""));
	return result;
}

struct RaceState {
	DuckDB &database;
	unsigned conflicts;
	bool permanent_error;
	bool unknown_commit;
	std::map<std::string, unsigned> calls;
};

static RaceState *race_state;

// The real flush has prepared its files but has not committed when this
// projection commits a competing update on another connection.
static void Race(DataChunk &input, ExpressionState &, Vector &result) {
	for (idx_t row = 0; row < input.size(); ++row) {
		auto table = input.GetValue(0, row).GetValue<std::string>();
		auto call = ++race_state->calls[table];
		if (table == "a_busy" && race_state->unknown_commit) {
			throw TransactionException("commit outcome unknown: Transaction conflict - cannot establish the outcome");
		}
		if (table == "a_busy" && race_state->permanent_error) {
			throw InvalidInputException("Transaction conflict is text here, not a retryable transaction error");
		}
		if (table == "a_busy" && call <= race_state->conflicts) {
			Connection writer(race_state->database);
			Query(writer, "UPDATE lake.a_busy SET value = value + 1");
		}
		result.SetValue(row, Value::UBIGINT(call));
	}
}

static void CheckPass(const std::string &extension, const std::filesystem::path &directory, unsigned conflicts,
                      bool permanent_error, bool table_batches, bool unknown_commit = false) {
	DBConfig config;
	config.SetOptionByName("allow_unsigned_extensions", Value(true));
	DuckDB database(nullptr, &config);
	RaceState state {database, conflicts, permanent_error, unknown_commit, {}};
	race_state = &state;
	Connection connection(database);
	connection.BeginTransaction();
	ScalarFunction race("maintenance_race", {LogicalType::VARCHAR}, LogicalType::UBIGINT, Race);
	race.SetVolatile();
	race.SetFallible();
	CreateScalarFunctionInfo info(race);
	Catalog::GetSystemCatalog(*connection.context).CreateFunction(*connection.context, info);
	connection.Commit();
	Query(connection, "SET threads=1; LOAD " + SQLString::ToString(extension) + "; ATTACH " +
	                      SQLString::ToString("ducklake:moraine:" + (directory / "store").string()) +
	                      " AS lake (DATA_PATH " + SQLString::ToString((directory / "data").string()) +
	                      ", META_MAINTENANCE_TABLE_BATCHES " + (table_batches ? "true" : "false") +
	                      ", META_MAINTENANCE_PROGRESS true, META_MAINTENANCE_FLUSH_INLINED_DATA true, "
	                      "META_MAINTENANCE_MERGE_ADJACENT_FILES true, META_MAINTENANCE_CLEANUP_OLD_FILES true)");
	Query(connection, "CREATE TABLE lake.a_busy(value BIGINT); INSERT INTO lake.a_busy VALUES (1); "
	                  "CREATE TABLE lake.z_healthy(value BIGINT); INSERT INTO lake.z_healthy VALUES (99)");
	// The wrapper executes the real DuckLake function, then races its commit.
	Query(connection,
	      "CREATE MACRO ducklake_flush_inlined_data(lake_name, table_name := NULL, schema_name := NULL) AS TABLE "
	      "SELECT *, maintenance_race(table_name) AS raced "
	      "FROM system.main.ducklake_flush_inlined_data(lake_name, table_name => table_name, schema_name => "
	      "schema_name)");
	auto report = Query(connection, "SELECT step, status, detail FROM moraine_maintenance('lake')");
	std::map<std::string, std::pair<std::string, std::string>> steps;
	for (auto &row : *report) {
		steps[row.GetValue<std::string>(0)] = {row.GetValue<std::string>(1), row.GetValue<std::string>(2)};
	}
	const auto &flush = steps.at("flush_inlined_data");
	if (!permanent_error && !unknown_commit && conflicts <= 1) {
		Require(flush.first == "ran", "a transient conflict was not retried: " + flush.second);
		Require(state.calls["a_busy"] == conflicts + 1, "the flush used an unexpected number of attempts");
		Require(steps.at("cleanup_old_files").first == "ran", "successful retry blocked later maintenance");
	} else if (permanent_error || unknown_commit) {
		Require(flush.first == "failed", "a permanent error was hidden");
		Require(state.calls["a_busy"] == 1, "a non-retryable error was retried");
		Require(state.calls["z_healthy"] == 0, "a permanent error did not stop the step");
		Require(steps.at("merge_adjacent_files").first == "skipped", "a permanent error did not stop later steps");
	} else {
		Require(flush.first == "failed", "exhausted conflicts were hidden");
		Require(state.calls["a_busy"] == 3, "a busy table did not stop after three attempts");
		Require(state.calls["z_healthy"] == 1, "a busy table prevented flushing a healthy table");
		Require(flush.second.find("z_healthy") == std::string::npos, "a healthy table was reported as failed");
		Require(flush.second.find("3 attempts") != std::string::npos, "the report omitted the retry count");
		Require(steps.at("merge_adjacent_files").first == "ran", "a deferred flush blocked healthy table compaction");
		Require(steps.at("merge_adjacent_files").second.find("deferred 1") != std::string::npos,
		        "compaction did not report the deferred table");
		Require(steps.at("cleanup_old_files").first == "skipped", "cleanup ran after an incomplete flush");
	}
	auto pending = Query(connection, "SELECT work_pending FROM moraine_maintenance_status('lake') "
	                                 "ORDER BY started_at DESC LIMIT 1");
	Require(pending->Fetch()->GetValue(0, 0).GetValue<bool>() == (permanent_error || unknown_commit || conflicts > 1),
	        "the durable pass status lost pending work");
	auto rows =
	    Query(connection, "SELECT value FROM lake.a_busy UNION ALL SELECT value FROM lake.z_healthy ORDER BY value");
	auto chunk = rows->Fetch();
	Require(chunk->size() == 2 && chunk->GetValue(0, 1).GetValue<int64_t>() == 99, "maintenance lost user rows");
	Require(chunk->GetValue(0, 0).GetValue<int64_t>() == 1 + (permanent_error ? 0 : std::min(conflicts, 3U)),
	        "the retry did not preserve the competing committed updates");
	if (!permanent_error && conflicts > 1) {
		state.conflicts = 0;
		Query(connection, "SELECT * FROM moraine_maintenance('lake')");
		auto resumed = Query(connection, "SELECT work_pending FROM moraine_maintenance_status('lake') "
		                                 "ORDER BY started_at DESC LIMIT 1");
		Require(!resumed->Fetch()->GetValue(0, 0).GetValue<bool>(), "a later pass did not resume the deferred table");
	}
}

int main(int argc, char **argv) {
	auto directory =
	    std::filesystem::temp_directory_path() /
	    ("moraine-maintenance-retry-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
	try {
		Require(argc == 2, "usage: maintenance-retry-test <moraine extension>");
		CheckPass(argv[1], directory / "transient", 1, false, true);
		CheckPass(argv[1], directory / "persistent", 10, false, true);
		CheckPass(argv[1], directory / "permanent", 0, true, true);
		CheckPass(argv[1], directory / "default", 1, false, false);
		CheckPass(argv[1], directory / "default-persistent", 10, false, false);
		CheckPass(argv[1], directory / "quiet", 0, false, true);
		CheckPass(argv[1], directory / "unknown", 0, false, true, true);
		std::filesystem::remove_all(directory);
		std::cout << "Maintenance conflict retry, isolation, and recovery checks passed\n";
		return 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		std::error_code ignored;
		std::filesystem::remove_all(directory, ignored);
		return 1;
	}
}
