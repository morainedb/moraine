// Answering a scan's pushed-down filter from an equality index, so an
// ordinary `WHERE key = 'x'` reads only the files that can hold a match.
//
// DuckLake hands the filters to the metadata manager while it builds the
// file list, which is the one place a file can be dropped before any reader
// opens it. What arrives is keyed by field id — the same identity an index
// is declared on — so matching a filter to an index needs no name lookup.
//
// Pruning is only ever a narrowing of a correct list: every path that
// cannot answer the filter returns nothing and leaves the list whole. The
// bar is that a file holding a matching row is never dropped, which is why
// an unplaced row, a shape the index cannot answer, and a list built for
// another snapshot than this transaction reads all decline.

#include "index_pushdown.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

#include "storage/ducklake_catalog.hpp"

#include "catalog.hpp"
#include "index_functions.hpp"
#include "moraine_abi.h"
#include "owned_array.hpp"
#include "transaction_manager.hpp"

namespace moraine_duckdb {

namespace {

// The constant an equality filter compares its column against, or nothing
// for every other shape. Ranges, `IN` lists and null checks are left to the
// scan: an index answers them, but through a different probe.
duckdb::optional_ptr<const duckdb::Value> EqualityConstant(const duckdb::ExpressionFilter &filter) {
	// `Left`/`Right` below index the children unchecked, so the shape has to
	// be established first: a filter expression is also how an optional
	// wrapper or a scalar call arrives.
	if (!filter.expr || filter.expr->GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION ||
	    !duckdb::BoundComparisonExpression::IsComparison(*filter.expr)) {
		return nullptr;
	}
	auto &comparison = filter.expr->Cast<duckdb::BoundFunctionExpression>();
	if (comparison.GetChildren().size() != 2) {
		return nullptr;
	}
	auto comparison_type = duckdb::ExpressionType::INVALID;
	auto constant = duckdb::ExpressionFilter::TryGetColumnConstantComparison(comparison, comparison_type);
	if (!constant || comparison_type != duckdb::ExpressionType::COMPARE_EQUAL) {
		return nullptr;
	}
	return &constant->GetValue();
}

// The moraine catalog serving this lake's metadata, or nothing when the
// metadata database is not one.
MoraineCatalog *MetadataCatalog(duckdb::ClientContext &context, const duckdb::string &metadata_name) {
	auto entry = duckdb::Catalog::GetCatalogEntry(context, duckdb::Identifier(metadata_name));
	if (!entry || entry->GetCatalogType() != "moraine") {
		return nullptr;
	}
	return &entry->Cast<MoraineCatalog>();
}

} // namespace

std::optional<std::unordered_set<uint64_t>>
IndexCandidateFiles(duckdb::DuckLakeTransaction &transaction, duckdb::DuckLakeTableEntry &table,
                    duckdb::DuckLakeSnapshot snapshot, const duckdb::FilterPushdownInfo *filter_info) {
	if (!filter_info || filter_info->column_filters.empty()) {
		return std::nullopt;
	}

	auto connection_context = transaction.GetConnection().context;
	if (!connection_context) {
		return std::nullopt;
	}
	auto &context = *connection_context;
	auto catalog = MetadataCatalog(context, transaction.GetCatalog().MetadataDatabaseName());
	if (!catalog) {
		return std::nullopt;
	}
	auto catalog_transaction = catalog->GetCatalogTransaction(context);
	if (!catalog_transaction.transaction) {
		return std::nullopt;
	}
	auto *pinned = catalog_transaction.transaction->Cast<MoraineTransaction>().Snapshot();
	if (!pinned) {
		return std::nullopt;
	}

	// The index resolves against the revision this transaction's metadata
	// reads are pinned to. A file list for any other snapshot — time travel,
	// most of all — is a different cut, and a lookup at this one says nothing
	// about which files held a row then.
	uint64_t pinned_id = 0;
	MoraineError snapshot_error {};
	if (moraine_snapshot_id(pinned, &pinned_id, &snapshot_error) != MORAINE_OK) {
		moraine_error_free(snapshot_error.message);
		return std::nullopt;
	}
	if (pinned_id != snapshot.snapshot_id) {
		return std::nullopt;
	}
	auto *handle = moraine_snapshot_read_handle(pinned);
	if (!handle) {
		return std::nullopt;
	}

	auto schema_name = table.ParentSchema().name.GetIdentifierName();
	auto table_name = table.name.GetIdentifierName();
	std::optional<std::unordered_set<uint64_t>> candidates;
	for (auto &filter : filter_info->column_filters) {
		if (!filter.second.table_filter) {
			continue;
		}
		auto constant = EqualityConstant(*filter.second.table_filter);
		if (!constant) {
			continue;
		}
		LookupValueArena arena;
		MoraineLookupValue value {};
		try {
			value = BuildLookupValue(*constant, arena, "moraine index pushdown");
		} catch (const std::exception &) {
			// A type equality indexes do not cover: nothing to ask.
			continue;
		}
		if (value.kind == 0) {
			// A NULL is not an equality key, and `= NULL` matches nothing
			// anyway; `IS NULL` arrives as its own filter shape.
			continue;
		}

		OwnedArray<uint64_t> files(moraine_index_candidate_files_free);
		bool prunable = false;
		MoraineError error {};
		auto code = moraine_index_candidate_files(handle, schema_name.c_str(), table_name.c_str(),
		                                          filter.second.column_field_index, &value, files.OutItems(),
		                                          files.OutLen(), &prunable, moraine_shim_is_interrupted, &context,
		                                          &error);
		if (code == MORAINE_INTERRUPTED) {
			ThrowMoraineError(error);
		}
		if (code != MORAINE_OK) {
			// A read that failed leaves the list whole rather than the
			// statement failed: pruning is an optimization, and the scan can
			// still answer from every file.
			WriteMoraineLog(duckdb::DatabaseInstance::GetDatabase(context), duckdb::LogLevel::LOG_DEBUG,
			                std::string("index pushdown declined: ") +
			                    (error.message != nullptr ? error.message : "unknown error"));
			moraine_error_free(error.message);
			continue;
		}
		if (!prunable) {
			continue;
		}

		std::unordered_set<uint64_t> matched(files.begin(), files.end());
		if (!candidates) {
			candidates = std::move(matched);
			continue;
		}
		// Filters hold together, so a file has to satisfy every one of them.
		for (auto held = candidates->begin(); held != candidates->end();) {
			held = matched.count(*held) > 0 ? std::next(held) : candidates->erase(held);
		}
	}
	return candidates;
}

} // namespace moraine_duckdb
