#include "index_cache.hpp"

#include "catalog.hpp"
#include "transaction_manager.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

#include <map>

namespace moraine_duckdb {
namespace {

constexpr auto STATE_KEY = "moraine_index_dependencies";

bool IsIndexRead(const std::string &name) {
	return name == "moraine_index_lookup" || name == "moraine_index_in" || name == "moraine_index_range" ||
	       name == "moraine_index_nulls";
}

bool LiteralArgument(duckdb::ClientContext &context, const duckdb::ParsedExpression &expression) {
	if (expression.GetExpressionClass() == duckdb::ExpressionClass::CONSTANT) {
		return true;
	}
	if (expression.GetExpressionClass() == duckdb::ExpressionClass::CAST) {
		auto &cast = expression.Cast<duckdb::CastExpression>();
		auto type = duckdb::UnboundType::TryDefaultBind(cast.cast_type);
		return (type.IsIntegral() || type.id() == duckdb::LogicalTypeId::BOOLEAN) &&
		       LiteralArgument(context, *cast.child);
	}
	if (expression.GetExpressionClass() != duckdb::ExpressionClass::FUNCTION) {
		return false;
	}
	auto &function = expression.Cast<duckdb::FunctionExpression>();
	if (function.function_name != "list_value" && function.function_name != "range" &&
	    function.function_name != "row" && function.function_name != "struct_pack") {
		return false;
	}
	duckdb::EntryLookupInfo lookup(duckdb::CatalogType::SCALAR_FUNCTION_ENTRY, function.function_name);
	auto entry = duckdb::Catalog::GetEntry(context, function.catalog, function.schema, lookup,
	                                       duckdb::OnEntryNotFound::RETURN_NULL);
	if (!entry || entry->type != duckdb::CatalogType::SCALAR_FUNCTION_ENTRY || !entry->internal) {
		return false;
	}
	for (auto &child : function.children) {
		if (!LiteralArgument(context, *child)) {
			return false;
		}
	}
	return true;
}

// Hidden invocations in views/macros and arguments depending on execution state rebind.
bool LiteralIndexStatement(duckdb::ClientContext &context, duckdb::SQLStatement &statement) {
	if (statement.type != duckdb::StatementType::SELECT_STATEMENT || !statement.named_param_map.empty()) {
		return false;
	}
	bool supported = true;
	bool found = false;
	auto expression = [&](duckdb::unique_ptr<duckdb::ParsedExpression> &expression) {
		duckdb::ParsedExpressionIterator::VisitExpressionClass(*expression, duckdb::ExpressionClass::SUBQUERY,
		    [&](const duckdb::ParsedExpression &) { supported = false; });
	};
	auto reference = [&](duckdb::TableRef &reference) {
		if (reference.type == duckdb::TableReferenceType::TABLE_FUNCTION) {
			auto &function = reference.Cast<duckdb::TableFunctionRef>().function->Cast<duckdb::FunctionExpression>();
			if (!IsIndexRead(function.function_name)) {
				supported = false;
				return;
			}
			duckdb::EntryLookupInfo lookup(duckdb::CatalogType::TABLE_FUNCTION_ENTRY, function.function_name);
			auto entry = duckdb::Catalog::GetEntry(context, function.catalog, function.schema, lookup,
			                                       duckdb::OnEntryNotFound::RETURN_NULL);
			if (!entry || entry->type != duckdb::CatalogType::TABLE_FUNCTION_ENTRY) {
				supported = false;
			}
			found = true;
			for (auto &argument : function.children) {
				auto literal = LiteralArgument(context, *argument);
				if (!literal) {
					WriteMoraineLog(duckdb::DatabaseInstance::GetDatabase(context), duckdb::LogLevel::LOG_DEBUG,
					    "index plan requires argument rebinding");
				}
				supported = literal && supported;
			}
		} else if (reference.type == duckdb::TableReferenceType::BASE_TABLE) {
			auto &table = reference.Cast<duckdb::BaseTableRef>();
			duckdb::EntryLookupInfo lookup(duckdb::CatalogType::TABLE_ENTRY, table.table_name);
			auto entry = duckdb::Catalog::GetEntry(context, table.catalog_name, table.schema_name, lookup,
			                                       duckdb::OnEntryNotFound::RETURN_NULL);
			if (!entry || entry->type != duckdb::CatalogType::TABLE_ENTRY || table.at_clause) {
				supported = false;
			}
		}
	};
	try {
		duckdb::ParsedExpressionIterator::EnumerateQueryNodeChildren(*statement.Cast<duckdb::SelectStatement>().node,
		                                                            expression, reference);
	} catch (duckdb::NotImplementedException &) {
		return false;
	}
	return found && supported;
}

MoraineTransaction &ReadTransaction(duckdb::ClientContext &context, MoraineCatalog &catalog,
                                    const std::string &lake_name, bool &clean);

struct IndexDependencyState : public duckdb::ClientContextState {
	struct Association {
		duckdb::idx_t metadata_oid;
		std::string lake;
		bool ambiguous = false;
	};
	std::map<std::string, Association> catalogs;

	void Prune(duckdb::ClientContext &context) {
		for (auto entry = catalogs.begin(); entry != catalogs.end();) {
			auto catalog = duckdb::Catalog::GetCatalogEntry(context, entry->first);
			if (!catalog || catalog->GetOid() != entry->second.metadata_oid) {
				entry = catalogs.erase(entry);
			} else {
				++entry;
			}
		}
	}

	duckdb::RebindQueryInfo Check(duckdb::ClientContext &context, duckdb::PreparedStatementData &prepared) {
		bool dependent = false;
		for (auto &read : prepared.properties.read_databases) {
			dependent = dependent || catalogs.count(read.first) != 0;
		}
		if (dependent && (!prepared.unbound_statement || !LiteralIndexStatement(context, *prepared.unbound_statement))) {
			return duckdb::RebindQueryInfo::ATTEMPT_TO_REBIND;
		}
		for (auto &read : prepared.properties.read_databases) {
			auto association = catalogs.find(read.first);
			if (association == catalogs.end()) {
				continue;
			}
			auto catalog = duckdb::Catalog::GetCatalogEntry(context, read.first);
			if (!catalog || catalog->GetCatalogType() != "moraine" || catalog->GetOid() != read.second.catalog_oid ||
			    association->second.ambiguous) {
				return duckdb::RebindQueryInfo::ATTEMPT_TO_REBIND;
			}
			if (!association->second.lake.empty()) {
				auto lake = duckdb::Catalog::GetCatalogEntry(context, association->second.lake);
				if (!lake || lake->GetCatalogType() != "ducklake" ||
				    lake->Cast<duckdb::DuckLakeCatalog>().MetadataDatabaseName() != catalog->GetName()) {
					return duckdb::RebindQueryInfo::ATTEMPT_TO_REBIND;
				}
			}
			bool clean = true;
			auto &transaction = ReadTransaction(context, catalog->Cast<MoraineCatalog>(), association->second.lake, clean);
			uint64_t revision = 0;
			if (!clean || !moraine_snapshot_read_revision(transaction.Snapshot(), &revision) ||
			    read.second.catalog_version != duckdb::optional_idx(revision)) {
				return duckdb::RebindQueryInfo::ATTEMPT_TO_REBIND;
			}
		}
		return duckdb::RebindQueryInfo::DO_NOT_REBIND;
	}

	duckdb::RebindQueryInfo OnExecutePrepared(duckdb::ClientContext &context,
	    duckdb::PreparedStatementCallbackInfo &info, duckdb::RebindQueryInfo) override {
		if (info.prepared_statement.statement_type == duckdb::StatementType::PREPARE_STATEMENT ||
		    info.prepared_statement.statement_type == duckdb::StatementType::EXECUTE_STATEMENT) {
			return duckdb::RebindQueryInfo::DO_NOT_REBIND;
		}
		return Check(context, info.prepared_statement);
	}
	duckdb::RebindQueryInfo OnRebindPreparedStatement(duckdb::ClientContext &context,
	    duckdb::BindPreparedStatementCallbackInfo &info, duckdb::RebindQueryInfo) override {
		return Check(context, info.prepared_statement);
	}
};

// `clean` is whether index reads may pin the transaction's starting
// revision. Staged writes do not clear it: index entries carry no
// transaction-local overlay either way, and the pin is the view consistent
// with the DuckLake snapshot the transaction scans at. A time-travel attach
// reads an older DuckLake snapshot than the pin and stays unpinned.
MoraineTransaction &ReadTransaction(duckdb::ClientContext &context, MoraineCatalog &catalog,
                                    const std::string &lake_name, bool &clean) {
	auto *metadata_context = &context;
	if (!lake_name.empty()) {
		auto &lake = duckdb::Catalog::GetCatalog(context, lake_name).Cast<duckdb::DuckLakeCatalog>();
		auto &transaction = duckdb::DuckLakeTransaction::Get(context, lake);
		clean = !lake.CatalogSnapshot();
		metadata_context = transaction.GetConnection().context.get();
	}
	auto transaction = catalog.GetCatalogTransaction(*metadata_context);
	return transaction.transaction->Cast<MoraineTransaction>();
}

} // namespace

duckdb::optional_idx IndexCatalogVersion(duckdb::ClientContext &context, MoraineCatalog &catalog) {
	auto state = context.registered_state->Get<IndexDependencyState>(STATE_KEY);
	if (!state) {
		return {};
	}
	auto association = state->catalogs.find(catalog.GetName());
	if (association == state->catalogs.end() || association->second.metadata_oid != catalog.GetOid() ||
	    association->second.ambiguous) {
		return {};
	}
	bool clean = true;
	auto &transaction = ReadTransaction(context, catalog, "", clean);
	uint64_t revision = 0;
	if (!clean || !moraine_snapshot_read_revision(transaction.Snapshot(), &revision)) {
		return {};
	}
	return duckdb::optional_idx(revision);
}

MoraineCatalogHandle *BindIndexRead(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                    bool &cacheable) {
	auto name = input.inputs[0].GetValue<std::string>();
	auto named = duckdb::Catalog::GetCatalogEntry(context, name);
	auto &catalog = named && named->GetCatalogType() == "ducklake"
	    ? ResolveMoraineCatalog(context, named->Cast<duckdb::DuckLakeCatalog>().MetadataDatabaseName())
	    : ResolveMoraineCatalog(context, name);
	std::string lake_name;
	bool ambiguous = false;
	if (named && named->GetCatalogType() == "ducklake") {
		auto &lake = named->Cast<duckdb::DuckLakeCatalog>();
		if (lake.MetadataDatabaseName() != catalog.GetName()) {
			throw duckdb::InvalidInputException("moraine: index metadata catalog does not match the lake");
		}
		lake_name = named->GetName();
	} else {
		for (auto &database : duckdb::DatabaseManager::Get(context).GetDatabases(context)) {
			auto &candidate = database->GetCatalog();
			if (candidate.GetCatalogType() == "ducklake" &&
			    candidate.Cast<duckdb::DuckLakeCatalog>().MetadataDatabaseName() == catalog.GetName()) {
				ambiguous = ambiguous || !lake_name.empty();
				lake_name = candidate.GetName();
			}
		}
		if (ambiguous) {
			lake_name.clear();
		}
	}
	auto state = context.registered_state->GetOrCreate<IndexDependencyState>(STATE_KEY);
	state->Prune(context);
	auto existing = state->catalogs.find(catalog.GetName());
	if (existing == state->catalogs.end() || existing->second.metadata_oid != catalog.GetOid()) {
		state->catalogs[catalog.GetName()] = {catalog.GetOid(), lake_name, ambiguous};
	} else if (existing->second.lake != lake_name) {
		existing->second.ambiguous = true;
	}
	bool clean = true;
	auto &transaction = ReadTransaction(context, catalog, lake_name, clean);
	uint64_t revision = 0;
	cacheable = clean && input.binder && !state->catalogs.at(catalog.GetName()).ambiguous &&
	            moraine_snapshot_read_revision(transaction.Snapshot(), &revision);
	if (input.binder) {
		auto &properties = input.binder->GetStatementProperties();
		duckdb::StatementProperties::CatalogIdentity identity {catalog.GetOid(),
		    cacheable ? duckdb::optional_idx(revision) : duckdb::optional_idx()};
		auto existing_read = properties.read_databases.find(catalog.GetName());
		if (existing_read != properties.read_databases.end() && existing_read->second != identity) {
			cacheable = false;
			input.binder->SetAlwaysRequireRebind();
		}
		properties.read_databases[catalog.GetName()] = identity;
		if (!lake_name.empty()) {
			input.binder->GetStatementProperties().RegisterDBRead(duckdb::Catalog::GetCatalog(context, lake_name), context);
		}
	}
	return clean ? transaction.IndexReadHandle() : catalog.Handle();
}

} // namespace moraine_duckdb
