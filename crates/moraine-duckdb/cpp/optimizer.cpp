#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace moraine_duckdb {

namespace {

bool IsIndexRead(const std::string &name) {
	return name == "moraine_index_lookup" || name == "moraine_index_in" || name == "moraine_index_range" ||
	       name == "moraine_index_nulls";
}

void ReplaceEmptyIndexReads(duckdb::ClientContext &context, duckdb::unique_ptr<duckdb::LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		ReplaceEmptyIndexReads(context, child);
	}

	if (plan->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = plan->Cast<duckdb::LogicalGet>();
	if (!IsIndexRead(get.function.name) || !get.function.cardinality) {
		return;
	}
	auto cardinality = get.function.cardinality(context, get.bind_data.get());
	if (!cardinality || !cardinality->has_max_cardinality || cardinality->max_cardinality != 0) {
		return;
	}
	plan = duckdb::make_uniq<duckdb::LogicalEmptyResult>(std::move(plan));
}

class MoraineOptimizer : public duckdb::OptimizerExtension {
public:
	MoraineOptimizer() {
		pre_optimize_function = Optimize;
	}

	static void Optimize(duckdb::OptimizerExtensionInput &input, duckdb::unique_ptr<duckdb::LogicalOperator> &plan) {
		ReplaceEmptyIndexReads(input.context, plan);
	}
};

} // namespace

void RegisterMoraineOptimizer(duckdb::DBConfig &config) {
	duckdb::OptimizerExtension::Register(config, MoraineOptimizer());
}

} // namespace moraine_duckdb
