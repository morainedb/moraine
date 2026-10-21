#include "s3_secret.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <atomic>
#include <iostream>
#include <stdexcept>
#include <chrono>
#include <thread>

using namespace duckdb;
using namespace moraine_duckdb;

static std::atomic<int> generation {0};
static std::atomic<bool> fail {false};

static unique_ptr<BaseSecret> Refresh(ClientContext &, CreateSecretInput &input) {
	if (fail) {
		throw InvalidInputException("provider failure must not cross the ABI");
	}
	if (input.options.at("marker").GetValue<int>() != 42) {
		throw InvalidInputException("refresh options were lost");
	}
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);
	secret->secret_map["key_id"] = Value("key-" + std::to_string(++generation));
	secret->secret_map["secret"] = Value("secret");
	secret->secret_map["session_token"] = Value("token");
	secret->secret_map["expiration_epoch_ms"] = Value::BIGINT(Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp()) + 60000);
	return secret;
}

struct Received {
	std::string key;
	int64_t expiry = 0;
};

static void Receive(void *output, const char *key, const char *, const char *, int64_t expiry) {
	auto &result = *static_cast<Received *>(output);
	result.key = key;
	result.expiry = expiry;
}

static void Require(bool condition, const char *message) {
	if (!condition) {
		throw std::runtime_error(message);
	}
}

int main() {
	try {
		MoraineS3CredentialSource source {};
		weak_ptr<DatabaseInstance> database;
		{
			DuckDB db(nullptr);
			database = db.instance;
			{
				Connection connection(db);
				connection.BeginTransaction();
				auto &manager = SecretManager::Get(*connection.context);
				SecretType type;
				type.name = Identifier("s3");
				type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
				type.default_provider = "rotation_test";
				manager.RegisterSecretType(type);
				CreateSecretFunction function;
				function.secret_type = "s3";
				function.provider = Identifier("rotation_test");
				function.function = Refresh;
				manager.RegisterSecretFunction(function, OnCreateConflict::ERROR_ON_CONFLICT);
				auto secret = make_uniq<KeyValueSecret>(vector<string> {"s3://lake/"}, Identifier("s3"),
				                                      Identifier("rotation_test"), Identifier("rotating"));
				secret->secret_map["key_id"] = Value("expired");
				secret->secret_map["secret"] = Value("expired");
				secret->secret_map["refresh_info"] = Value::STRUCT({{"marker", Value::INTEGER(42)}});
				manager.RegisterSecret(CatalogTransaction::GetSystemCatalogTransaction(*connection.context),
				                       std::move(secret), OnCreateConflict::ERROR_ON_CONFLICT,
				                       SecretPersistType::TEMPORARY);
				MoraineS3Config config {};
				S3SecretStrings strings;
				Require(ResolveS3Config(*connection.context, "s3://lake/catalog", config, strings), "S3 not resolved");
				Require(config.credential_source != nullptr, "refreshable secret was copied as static credentials");
				source = *config.credential_source;
				source.retain(source.context);
				connection.Commit();
			}
			Received result;
			Require(source.load(source.context, Receive, &result), "refresh after connection close failed");
			Require(result.key == "key-1" && result.expiry > 0, "first refresh did not return credentials and expiry");
			Require(source.load(source.context, Receive, &result), "second refresh failed");
			Require(result.key == "key-2", "provider was not called again");
			fail = true;
			Require(!source.load(source.context, Receive, &result), "provider exception was not contained");
			fail = false;
		}
		for (int attempt = 0; attempt < 5000 && !database.expired(); ++attempt) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		Require(database.expired(), "provider kept the database alive");
		Received result;
		Require(!source.load(source.context, Receive, &result), "provider kept the database alive");
		source.release(source.context);
		std::cout << "S3 credential rotation and lifetime checks passed\n";
		return 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
