// Resolving S3 credentials from the DuckDB secret that matches a store
// path. Shared by attach and by migrate, which reach the same stores by
// different routes.
#pragma once

#include "duckdb.hpp"

#include "moraine_abi.h"

#include <string>
#include <memory>

namespace moraine_duckdb {

// Backing storage for the strings a `MoraineS3Config` points into. The ABI
// borrows them, so this must outlive the call that reads the config.
struct S3SecretStrings {
	std::string key_id;
	std::string secret;
	std::string region;
	std::string session_token;
	std::string endpoint;
	std::string url_style;
	MoraineS3CredentialSource credential_source {};
	std::shared_ptr<void> credential_owner;
};

// Fills `config` from the DuckDB secret matching `path` — the same secret
// DuckLake and httpfs use for DATA_PATH — when `path` is an `s3://` URL.
// With no matching secret, all fields remain unset and the core uses AWS_*
// environment settings. A matching secret supplies its own configuration.
//
// Returns whether `path` is an `s3://` URL, which is what decides whether
// the caller passes `&config` or null.
bool ResolveS3Config(duckdb::ClientContext &context, const std::string &path, MoraineS3Config &config,
                     S3SecretStrings &storage);

} // namespace moraine_duckdb
