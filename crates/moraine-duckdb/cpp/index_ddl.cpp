#include "catalog.hpp"
#include "moraine_abi.h"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry_retriever.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/planner/operator/logical_drop.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "duckdb/planner/planner_extension.hpp"
#include "storage/ducklake_catalog.hpp"

namespace moraine_duckdb {
namespace {

using namespace duckdb;

struct IndexDdlBindData : public duckdb::FunctionData {
	bool is_create = false;
	bool ignore_missing_or_existing = false;
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	std::string index_name;
	std::vector<std::string> columns;
	// Per-column direction: 1 = descending, parallel to `columns`. Empty
	// leaves the index ascending.
	std::vector<uint8_t> descending;
	// Per-column NULL placement: 1 = NULLS FIRST, parallel to `columns`.
	// Empty leaves the index NULLS LAST.
	std::vector<uint8_t> nulls_first;
	bool unique = false;
	// Commit additions after the data snapshot in bounded repair steps.
	bool deferred_maintenance = false;
	// Run the multi-commit build instead of backfilling in one commit.
	bool staged = false;
	// Bounds on one step of that build, 0 for moraine's default. Only
	// meaningful with `staged`.
	uint64_t step_entries = 0;
	uint64_t step_bytes = 0;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<IndexDdlBindData>();
		*result = *this;
		return result;
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<IndexDdlBindData>();
		return is_create == other.is_create &&
		       ignore_missing_or_existing == other.ignore_missing_or_existing && catalog_name == other.catalog_name &&
		       schema_name == other.schema_name && table_name == other.table_name &&
		       index_name == other.index_name && columns == other.columns &&
		       descending == other.descending && nulls_first == other.nulls_first &&
		       unique == other.unique && deferred_maintenance == other.deferred_maintenance &&
		       staged == other.staged &&
		       step_entries == other.step_entries && step_bytes == other.step_bytes;
	}
};

struct IndexDdlGlobalState : public duckdb::GlobalTableFunctionState {
	bool emitted = false;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

// The DDL is applied once, at execution start.
duckdb::unique_ptr<duckdb::GlobalTableFunctionState> IndexDdlInitGlobal(duckdb::ClientContext &context,
                                                                        duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<IndexDdlBindData>();
	if (!context.transaction.IsAutoCommit()) {
		throw duckdb::TransactionException("Moraine index DDL requires autocommit; commit or roll back the transaction first");
	}
	auto handle = ResolveMoraineCatalog(context, bind_data.catalog_name).Handle();
	MoraineError err {};
	int32_t code;
	if (bind_data.is_create) {
		std::vector<const char *> column_ptrs;
		column_ptrs.reserve(bind_data.columns.size());
		for (auto &column : bind_data.columns) {
			column_ptrs.push_back(column.c_str());
		}
		// `descending`/`nulls_first` store one 0/1 byte per column; the ABI
		// takes `uint8_t` arrays (not `bool`, which is UB to alias here), so
		// pass the vector storage directly, or null to default that axis.
		const uint8_t *directions = bind_data.descending.empty() ? nullptr : bind_data.descending.data();
		const uint8_t *nulls_first = bind_data.nulls_first.empty() ? nullptr : bind_data.nulls_first.data();
		code = moraine_index_create(handle, bind_data.schema_name.c_str(), bind_data.table_name.c_str(),
		                            bind_data.index_name.c_str(), column_ptrs.data(), column_ptrs.size(),
		                            directions, nulls_first, bind_data.unique,
		                            bind_data.deferred_maintenance, bind_data.staged,
		                            bind_data.step_entries, bind_data.step_bytes,
		                            bind_data.ignore_missing_or_existing,
		                            moraine_shim_is_interrupted, &context, &err);
	} else {
		code = moraine_index_drop(handle, bind_data.schema_name.c_str(),
		                          bind_data.index_name.c_str(), bind_data.ignore_missing_or_existing,
		                          moraine_shim_is_interrupted, &context, &err);
	}
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	return duckdb::make_uniq<IndexDdlGlobalState>();
}

void IndexDdlImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<IndexDdlGlobalState>();
	if (state.emitted) {
		output.SetChildCardinality(0);
		return;
	}
	output.SetValue(0, 0, duckdb::Value::BOOLEAN(true));
	output.SetChildCardinality(1);
	state.emitted = true;
}

void BindCreateOptions(IndexDdlBindData &data, const duckdb::named_argument_map_t &options) {
	auto maintenance = options.find("maintenance");
	if (maintenance != options.end()) {
		const std::string mode = maintenance->second.GetValue<std::string>();
		if (mode == "deferred" || mode == "DEFERRED") {
			data.deferred_maintenance = true;
		} else if (mode != "synchronous" && mode != "SYNCHRONOUS") {
			throw duckdb::InvalidInputException(
			    "CREATE INDEX: maintenance must be 'synchronous' or 'deferred', got \"%s\"", mode);
		}
	}
	if (data.deferred_maintenance && data.unique) {
		throw duckdb::InvalidInputException(
		    "CREATE INDEX: deferred maintenance is supported only for non-unique indexes");
	}
	auto ordering = [&](const char *name, const char *default_order, const char *other_order) {
		std::vector<uint8_t> result;
		auto option = options.find(name);
		if (option == options.end()) {
			return result;
		}
		for (auto &child : duckdb::ListValue::GetChildren(option->second)) {
			if (child.IsNull()) {
				throw duckdb::InvalidInputException("CREATE INDEX: %s entries cannot be NULL", name);
			}
			auto value = child.GetValue<std::string>();
			if (duckdb::StringUtil::CIEquals(value, other_order)) {
				result.push_back(1);
			} else if (duckdb::StringUtil::CIEquals(value, default_order)) {
				result.push_back(0);
			} else {
				throw duckdb::InvalidInputException("CREATE INDEX: %s entries must be '%s' or '%s', got '%s'",
				                                    name, default_order, other_order, value);
			}
		}
		if (result.size() != data.columns.size()) {
			throw duckdb::InvalidInputException("CREATE INDEX: %s must have one entry per column", name);
		}
		return result;
	};
	data.descending = ordering("directions", "asc", "desc");
	data.nulls_first = ordering("nulls", "last", "first");
	// A staged build advances across bounded commits.
	auto staged = options.find("staged");
	if (staged != options.end()) {
		data.staged = staged->second.GetValue<bool>();
	}
	// Zero selects the core default; explicit bounds must be positive.
	auto positive = [&options](const char *name) -> uint64_t {
		auto found = options.find(name);
		if (found == options.end()) {
			return 0;
		}
		const int64_t value = found->second.GetValue<int64_t>();
		if (value <= 0) {
			throw duckdb::InvalidInputException(
			    "CREATE INDEX: `%s` must be greater than zero, got %lld", name,
			    static_cast<long long>(value));
		}
		return static_cast<uint64_t>(value);
	};
	data.step_entries = positive("step_entries");
	data.step_bytes = positive("step_bytes");
}

duckdb::BoundStatement BindNativeIndexDdl(duckdb::ClientContext &context, duckdb::Binder &binder,
                                         duckdb::unique_ptr<IndexDdlBindData> data,
                                         duckdb::named_argument_map_t options = {}) {
	if (!context.transaction.IsAutoCommit()) {
		throw duckdb::TransactionException("Moraine index DDL requires autocommit; commit or roll back the transaction first");
	}
	for (auto &option : options) {
		duckdb::LogicalType type;
		if (option.first == "staged") {
			type = duckdb::LogicalType::BOOLEAN;
		} else if (option.first == "step_entries" || option.first == "step_bytes") {
			type = duckdb::LogicalType::BIGINT;
		} else if (option.first == "directions" || option.first == "nulls") {
			type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		} else if (option.first == "maintenance") {
			type = duckdb::LogicalType::VARCHAR;
		} else {
			throw duckdb::BinderException("Unknown Moraine index option: %s", option.first);
		}
		if (option.second.IsNull()) {
			throw duckdb::BinderException("Moraine index option %s requires a value", option.first);
		}
		option.second = option.second.DefaultCastAs(type);
	}
	if (data->is_create) {
		BindCreateOptions(*data, options);
	}
	duckdb::TableFunction function(data->is_create ? "CREATE_INDEX" : "DROP_INDEX", {},
	                               IndexDdlImpl, nullptr, IndexDdlInitGlobal);
	duckdb::BoundTableFunction bound_function(function);
	duckdb::BoundStatement result;
	result.types = {duckdb::LogicalType::BOOLEAN};
	result.names = {"Success"};
	auto get = duckdb::make_uniq<duckdb::LogicalGet>(binder.GenerateTableIndex(), std::move(bound_function),
	                                                std::move(data), result.types, result.names);
	get->AddColumnId(0);
	result.plan = std::move(get);
	auto &properties = binder.GetStatementProperties();
	properties.return_type = duckdb::StatementReturnType::NOTHING;
	properties.always_require_rebind = true;
	return result;
}

bool IsMoraineLake(ClientContext &context, Catalog &catalog) {
	if (catalog.GetCatalogType() != "ducklake") {
		return false;
	}
	auto &lake = catalog.Cast<DuckLakeCatalog>();
	auto metadata = Catalog::GetCatalogEntry(context, Identifier(lake.MetadataDatabaseName()));
	return metadata && metadata->GetCatalogType() == "moraine";
}

void RequireWritableLake(ClientContext &context, Binder &binder, Catalog &catalog,
                         DatabaseModificationType modification) {
	if (catalog.GetAttached().IsReadOnly() || catalog.Cast<DuckLakeCatalog>().CatalogSnapshot()) {
		throw BinderException("Cannot change indexes on a read-only Moraine lake");
	}
	binder.GetStatementProperties().RegisterDBModify(catalog, context, modification);
}

BoundStatement BindCreate(ClientContext &context, Binder &binder, CreateIndexInfo &info) {
	auto table = Catalog::GetEntry<TableCatalogEntry>(context,
	    info.GetQualifiedName().WithName(info.table), OnEntryNotFound::RETURN_NULL);
	if (!table || !IsMoraineLake(context, table->ParentCatalog())) {
		return {};
	}
	if (!StringUtil::CIEquals(info.index_type, "ART") && !StringUtil::CIEquals(info.index_type, "moraine")) {
		throw BinderException("Unsupported Moraine index type: %s", info.index_type);
	}
	if (info.temporary || info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw BinderException("Moraine does not support temporary or replacement index DDL");
	}
	auto data = make_uniq<IndexDdlBindData>();
	for (auto &expression : info.expressions) {
		if (expression->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
			throw BinderException("Moraine indexes require stored column names");
		}
		auto &reference = expression->Cast<ColumnRefExpression>();
		auto &parts = reference.ColumnNames();
		if (parts.size() > 2 || (parts.size() == 2 && parts[0] != table->name)) {
			throw BinderException("Index column must belong to table %s", table->name);
		}
		auto &column = table->GetColumn(reference.GetColumnName());
		data->columns.push_back(column.Name().GetIdentifierName());
	}
	auto &catalog = table->ParentCatalog();
	RequireWritableLake(context, binder, catalog, DatabaseModificationType::CREATE_INDEX);
	data->is_create = true;
	data->catalog_name = catalog.GetName().GetIdentifierName();
	data->schema_name = table->ParentSchema().name.GetIdentifierName();
	data->table_name = table->name.GetIdentifierName();
	data->index_name = info.GetIndexName().GetIdentifierName();
	data->unique = info.constraint_type == IndexConstraintType::UNIQUE;
	data->ignore_missing_or_existing = info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT;
	named_argument_map_t options;
	for (auto &option : info.options) {
		options[Identifier(option.first)] = option.second;
	}
	return BindNativeIndexDdl(context, binder, std::move(data), std::move(options));
}

BoundStatement BindDrop(ClientContext &context, Binder &binder, const DropInfo &info) {
	auto name = binder.BindTableName(info.GetQualifiedName());
	auto catalog = Catalog::GetCatalogEntry(context, name.Catalog());
	if (!catalog || !IsMoraineLake(context, *catalog)) {
		return {};
	}
	if (info.cascade) {
		throw BinderException("Moraine does not support DROP INDEX CASCADE");
	}
	RequireWritableLake(context, binder, *catalog, DatabaseModificationType::DROP_CATALOG_ENTRY);
	auto schema_name = name.Schema();
	if (schema_name.empty()) {
		CatalogEntryRetriever retriever(context);
		auto default_schema = retriever.GetSearchPath().GetDefaultSchema(context, catalog->GetName());
		if (!default_schema) {
			throw BinderException("Specify a schema when dropping a Moraine index");
		}
		schema_name = *default_schema;
	}
	auto schema = Catalog::GetSchema(context, catalog->GetName(), schema_name, OnEntryNotFound::RETURN_NULL);
	auto data = make_uniq<IndexDdlBindData>();
	data->catalog_name = catalog->GetName().GetIdentifierName();
	data->schema_name = (schema ? schema->name : schema_name).GetIdentifierName();
	data->index_name = name.Name().GetIdentifierName();
	data->ignore_missing_or_existing = info.if_not_found == OnEntryNotFound::RETURN_NULL;
	return BindNativeIndexDdl(context, binder, std::move(data));
}

class MoraineIndexDdl : public OperatorExtension {
public:
	MoraineIndexDdl() {
		Bind = BindStatement;
	}

	std::string GetName() override {
		return "moraine_index_ddl";
	}

	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &) override {
		throw NotImplementedException("Moraine index DDL uses ordinary table-function plans");
	}

	static BoundStatement BindStatement(ClientContext &context, Binder &binder, OperatorExtensionInfo *,
	                                    SQLStatement &statement) {
		if (statement.type == StatementType::EXPLAIN_STATEMENT) {
			auto &explain = statement.Cast<ExplainStatement>();
			if (explain.explain_type == ExplainType::EXPLAIN_SQL) {
				return {};
			}
			auto result = BindStatement(context, binder, nullptr, *explain.stmt);
			if (result.plan) {
				auto text = result.plan->ToString(context, explain.format);
				auto plan = make_uniq<LogicalExplain>(std::move(result.plan), explain.explain_type, explain.format);
				plan->logical_plan_unopt = std::move(text);
				result.plan = std::move(plan);
				result.names = {"explain_key", "explain_value"};
				result.types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
				binder.GetStatementProperties().return_type = StatementReturnType::QUERY_RESULT;
			}
			return result;
		} else if (statement.type == StatementType::CREATE_STATEMENT) {
			auto &create = statement.Cast<CreateStatement>();
			if (create.info && create.info->type == CatalogType::INDEX_ENTRY) {
				return BindCreate(context, binder, create.info->Cast<CreateIndexInfo>());
			}
		} else if (statement.type == StatementType::DROP_STATEMENT) {
			auto &drop = statement.Cast<DropStatement>();
			if (drop.info && drop.info->type == CatalogType::INDEX_ENTRY) {
				return BindDrop(context, binder, *drop.info);
			}
		}
		return {};
	}
};

void RewriteDrops(PlannerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		RewriteDrops(input, child);
	}
	if (plan->type != LogicalOperatorType::LOGICAL_DROP) {
		return;
	}
	auto &drop = plan->Cast<LogicalDrop>();
	if (drop.info->type != CatalogType::INDEX_ENTRY) {
		return;
	}
	auto replacement = BindDrop(input.context, input.binder, *drop.info);
	if (replacement.plan) {
		plan = std::move(replacement.plan);
	}
}

void RewriteBoundDrop(PlannerExtensionInput &input, BoundStatement &statement) {
	auto return_type = input.binder.GetStatementProperties().return_type;
	if (statement.plan) {
		RewriteDrops(input, statement.plan);
	}
	input.binder.GetStatementProperties().return_type = return_type;
}

} // namespace

void RegisterMoraineIndexDdl(duckdb::DBConfig &config) {
	duckdb::OperatorExtension::Register(config, duckdb::make_shared_ptr<MoraineIndexDdl>());
	duckdb::PlannerExtension planner;
	planner.post_bind_function = RewriteBoundDrop;
	duckdb::PlannerExtension::Register(config, std::move(planner));
}

} // namespace moraine_duckdb
