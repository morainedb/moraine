#include "s3_secret.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace moraine_duckdb {

namespace {

// Database destruction may close the store waiting for this callback.
// Release its last reference on a separate thread after handing back credentials.
class DatabaseReaper {
public:
	struct Entry {
		duckdb::shared_ptr<duckdb::DatabaseInstance> database;
		Entry *next = nullptr;
	};

	DatabaseReaper() : worker([this] { Run(); }) {
	}
	~DatabaseReaper() {
		{
			std::lock_guard<std::mutex> guard(mutex);
			stopping = true;
		}
		ready.notify_one();
		worker.join();
	}
	void Retire(Entry *entry) {
		{
			std::lock_guard<std::mutex> guard(mutex);
			entry->next = pending;
			pending = entry;
		}
		ready.notify_one();
	}

private:
	void Run() {
		for (;;) {
			Entry *entries;
			{
				std::unique_lock<std::mutex> guard(mutex);
				ready.wait(guard, [&] { return stopping || pending; });
				if (!pending && stopping) {
					return;
				}
				entries = pending;
				pending = nullptr;
			}
			while (entries) {
				auto next = entries->next;
				delete entries;
				entries = next;
			}
		}
	}
	std::mutex mutex;
	std::condition_variable ready;
	Entry *pending = nullptr;
	bool stopping = false;
	std::thread worker;
};

static DatabaseReaper &Reaper() {
	static DatabaseReaper reaper;
	return reaper;
}

struct CredentialSource {
	std::atomic<size_t> references {1};
	duckdb::weak_ptr<duckdb::DatabaseInstance> database;
	duckdb::CreateSecretInput input;
	duckdb::create_secret_function_t create;
};

static void Retain(void *context) {
	static_cast<CredentialSource *>(context)->references.fetch_add(1, std::memory_order_relaxed);
}

static void Release(void *context) {
	auto source = static_cast<CredentialSource *>(context);
	if (source->references.fetch_sub(1, std::memory_order_acq_rel) == 1) {
		delete source;
	}
}

static bool Load(void *context, MoraineS3CredentialReceiver receive, void *output) {
	try {
		auto &source = *static_cast<CredentialSource *>(context);
		auto retire = [](DatabaseReaper::Entry *entry) { Reaper().Retire(entry); };
		std::unique_ptr<DatabaseReaper::Entry, decltype(retire)> lease(new DatabaseReaper::Entry, retire);
		lease->database = source.database.lock();
		if (!lease->database) {
			return false;
		}
		duckdb::ClientContext client(lease->database);
		auto input = source.input;
		auto secret = source.create(client, input);
		if (!secret) {
			return false;
		}
		auto &values = dynamic_cast<const duckdb::KeyValueSecret &>(*secret);
		auto take = [&](const char *key) {
			auto value = values.TryGetValue(key);
			return value.IsNull() ? std::string() : value.ToString();
		};
		auto key = take("key_id");
		auto secret_key = take("secret");
		auto token = take("session_token");
		auto expiry = values.TryGetValue("expiration_epoch_ms");
		receive(output, key.c_str(), secret_key.c_str(), token.c_str(),
		        expiry.IsNull() ? 0 : expiry.GetValue<int64_t>());
		return true;
	} catch (...) {
		return false;
	}
}

static void ResolveCredentialSource(duckdb::ClientContext &context, const duckdb::KeyValueSecret &secret,
                                    MoraineS3Config &config, S3SecretStrings &storage) {
	duckdb::Value refresh;
	if (!secret.TryGetValue("refresh_info", refresh) || refresh.IsNull()) {
		return;
	}
	if (refresh.type().id() != duckdb::LogicalTypeId::STRUCT) {
		throw duckdb::InvalidInputException("moraine: invalid S3 secret refresh information");
	}
	auto source = std::make_unique<CredentialSource>();
	source->database = context.db;
	source->input.type = secret.GetType();
	source->input.provider = secret.GetProvider();
	source->input.name = secret.GetName();
	source->input.scope = secret.GetScope();
	source->input.persist_type = duckdb::SecretPersistType::TEMPORARY;
	source->input.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
	const auto &children = duckdb::StructValue::GetChildren(refresh);
	for (duckdb::idx_t index = 0; index < children.size(); ++index) {
		source->input.options[duckdb::StructType::GetChildName(refresh.type(), index).GetIdentifierName()] = children[index];
	}
	for (const auto &function : duckdb::SecretManager::Get(context).AllSecretFunctions()) {
		if (duckdb::Identifier(function.secret_type) == secret.GetType() && function.provider == secret.GetProvider()) {
			source->create = function.function;
			Reaper();
			storage.credential_owner = std::shared_ptr<void>(source.release(), Release);
			storage.credential_source = {storage.credential_owner.get(), Retain, Release, Load};
			config.credential_source = &storage.credential_source;
			return;
		}
	}

	throw duckdb::InvalidInputException("moraine: S3 secret refresh provider is unavailable");
}

} // namespace

bool ResolveS3Config(duckdb::ClientContext &context, const std::string &path, MoraineS3Config &config,
                     S3SecretStrings &storage) {
	config = MoraineS3Config {};
	config.use_ssl = -1;
	if (!duckdb::StringUtil::StartsWith(path, "s3://")) {
		return false;
	}

	auto &secret_manager = duckdb::SecretManager::Get(context);
	auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
	auto match = secret_manager.LookupSecret(transaction, path, "s3");
	if (!match.HasMatch()) {
		return true;
	}

	auto &kv = dynamic_cast<const duckdb::KeyValueSecret &>(match.GetSecret());
	ResolveCredentialSource(context, kv, config, storage);
	auto take = [&](const char *key, std::string &into, const char *&field) {
		duckdb::Value value;
		if (kv.TryGetValue(key, value) && !value.IsNull()) {
			into = value.ToString();
			field = into.c_str();
		}
	};
	take("key_id", storage.key_id, config.key_id);
	take("secret", storage.secret, config.secret);
	take("region", storage.region, config.region);
	take("session_token", storage.session_token, config.session_token);
	take("endpoint", storage.endpoint, config.endpoint);
	take("url_style", storage.url_style, config.url_style);
	duckdb::Value ssl;
	if (kv.TryGetValue("use_ssl", ssl) && !ssl.IsNull()) {
		config.use_ssl = ssl.GetValue<bool>() ? 1 : 0;
	}
	return true;
}

} // namespace moraine_duckdb
