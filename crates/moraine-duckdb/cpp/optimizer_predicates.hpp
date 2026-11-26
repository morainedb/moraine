#pragma once

#include <optional>

#include "index_functions.hpp"

namespace duckdb {
class LogicalGet;
class LogicalComparisonJoin;
class DuckLakeTableEntry;
class TableFilter;
} // namespace duckdb

namespace moraine_duckdb {

// Owns the constants borrowed by the predicate ABI descriptors.
struct PredicateArena {
	LookupValueArena arena;
	std::vector<std::vector<MoraineLookupValue>> constants;
	std::vector<MoraineIndexPredicate> predicates;

	void Add(uint64_t column, uint8_t kind, const duckdb::vector<duckdb::Value> &values);
};

bool ExtractPredicates(const duckdb::Expression &expression, uint64_t column, PredicateArena &output);
void ExtractTuplePredicates(duckdb::ClientContext &context, const duckdb::Expression &expression,
                            const duckdb::LogicalGet &get, const duckdb::DuckLakeTableEntry &table,
                            PredicateArena &output);
void ExposeTuplePredicates(duckdb::ClientContext &context, duckdb::LogicalOperator &plan);

// A set of complete equality keys over `columns`, each key one non-NULL
// constant per column. No keys denotes no row.
struct KeySet {
	std::vector<duckdb::idx_t> columns;
	std::vector<duckdb::vector<duckdb::Value>> keys;
};

// The key set a filter expression over `get`'s columns denotes: a constant
// equality, an IN list, a tuple equality or IN list, or an AND or OR of
// those. Anything else, or a conjunction past 4,096 keys, is nullopt.
std::optional<KeySet> ExtractKeySet(duckdb::ClientContext &context, const duckdb::Expression &expression,
                                    const duckdb::LogicalGet &get);

// The key set a MARK join of `get` against a chunk of constants denotes,
// which is how DuckDB plans an IN list of five or more constants. nullopt for
// any other join.
std::optional<KeySet> MarkJoinKeySet(duckdb::ClientContext &context, const duckdb::LogicalComparisonJoin &join,
                                     const duckdb::LogicalGet &get);

// The key set one pushed table filter on `column` denotes, or nullopt for a
// filter that is not an equality list.
std::optional<KeySet> TableFilterKeySet(const duckdb::TableFilter &filter, duckdb::idx_t column);

// The keys satisfying both sets, or nullopt past 4,096 keys.
std::optional<KeySet> ConjoinKeySets(const KeySet &left, const KeySet &right);

} // namespace moraine_duckdb
