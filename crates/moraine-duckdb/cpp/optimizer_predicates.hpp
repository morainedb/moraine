#pragma once

#include "index_functions.hpp"

namespace moraine_duckdb {

// Owns the constants borrowed by the predicate ABI descriptors.
struct PredicateArena {
	LookupValueArena arena;
	std::vector<std::vector<MoraineLookupValue>> constants;
	std::vector<MoraineIndexPredicate> predicates;

	void Add(uint64_t column, uint8_t kind, const duckdb::vector<duckdb::Value> &values);
};

bool ExtractPredicates(const duckdb::Expression &expression, uint64_t column, PredicateArena &output);
void ExposeTuplePredicates(duckdb::ClientContext &context, duckdb::LogicalOperator &plan);

} // namespace moraine_duckdb
