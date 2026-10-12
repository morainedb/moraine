#include "catalog.hpp"
#include "index_functions.hpp"
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
	vector<Value> columns;
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
		columns.emplace_back(column.Name().GetIdentifierName());
	}
	auto &catalog = table->ParentCatalog();
	RequireWritableLake(context, binder, catalog, DatabaseModificationType::CREATE_INDEX);
	vector<Value> arguments {Value(catalog.GetName().GetIdentifierName()),
	                         Value(table->ParentSchema().name.GetIdentifierName()),
	                         Value(table->name.GetIdentifierName()),
	                         Value(info.GetIndexName().GetIdentifierName()),
	                         Value::LIST(LogicalType::VARCHAR, std::move(columns)),
	                         Value::BOOLEAN(info.constraint_type == IndexConstraintType::UNIQUE)};
	named_argument_map_t options;
	for (auto &option : info.options) {
		options[Identifier(option.first)] = option.second;
	}
	return BindNativeIndexDdl(context, binder, true, std::move(arguments), std::move(options),
	                          info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
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
	vector<Value> arguments {Value(catalog->GetName().GetIdentifierName()),
	                         Value((schema ? schema->name : schema_name).GetIdentifierName()), Value(""),
	                         Value(name.Name().GetIdentifierName())};
	return BindNativeIndexDdl(context, binder, false, std::move(arguments), {},
	                          info.if_not_found == OnEntryNotFound::RETURN_NULL);
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
