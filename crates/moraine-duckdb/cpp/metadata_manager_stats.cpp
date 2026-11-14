#include "metadata_manager.hpp"

#include "catalog.hpp"
#include "staged_write.hpp"
#include "transaction_manager.hpp"

#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parser.hpp"

#include <unordered_set>

namespace moraine_duckdb {

namespace {

template <class T>
bool ReadOptional(const duckdb::Value &value, T &target) {
	if (value.IsNull()) {
		return false;
	}
	target = value.GetValue<T>();
	return true;
}

duckdb::Value OptionalBoolean(bool present, bool value) {
	return present ? duckdb::Value::BOOLEAN(value) : duckdb::Value();
}

duckdb::Value StatisticsBound(bool present, const duckdb::string &value) {
	// DuckLake's SQL serializer drops bounds containing NUL bytes.
	return present && value.find('\0') == duckdb::string::npos ? duckdb::Value(value) : duckdb::Value();
}

} // namespace

MetadataRows MoraineMetadataManager::TableStatisticsRows(const char *name, duckdb::TableIndex table_id) {
	auto &context = *transaction.GetConnection().context;
	auto &catalog = ResolveMoraineCatalog(context, transaction.GetCatalog().MetadataDatabaseName());
	auto &moraine_transaction = catalog.GetCatalogTransaction(context).transaction->Cast<MoraineTransaction>();
	const auto &spec = MetadataSpec(name);
	auto *staged = moraine_transaction.StagedTxIfOpen();
	MoraineMetadataCursor *cursor = nullptr;
	MoraineError error {};
	auto code =
	    staged ? moraine_tx_metadata_cursor_open(staged, spec.write_table_kind, table_id.index, kEveryVersion, &cursor,
	                                             moraine_shim_is_interrupted, &context, &error)
	           : moraine_metadata_cursor_open(moraine_transaction.ReadHandle(), spec.write_table_kind, table_id.index,
	                                          kEveryVersion, &cursor, moraine_shim_is_interrupted, &context, &error);
	if (code != MORAINE_OK) {
		ThrowMoraineError(error);
	}
	std::unique_ptr<MoraineMetadataCursor, decltype(&moraine_metadata_cursor_free)> owned_cursor(
	    cursor, moraine_metadata_cursor_free);
	MetadataRows rows;
	while (true) {
		auto page = spec.page_provider(cursor, moraine_shim_is_interrupted, &context);
		if (page.empty()) {
			return rows;
		}
		for (auto &row : page) {
			rows.push_back(std::move(row));
		}
	}
}

bool MoraineMetadataManager::TryGetTableStats(duckdb::DuckLakeSnapshot, duckdb::TableIndex table_id,
                                              duckdb::vector<duckdb::DuckLakeGlobalStatsInfo> &stats) {
	RecordDrivingContext();
	auto rows = TableStatisticsRows("ducklake_table_stats", table_id);
	if (rows.empty()) {
		return true;
	}
	if (rows.size() != 1) {
		throw duckdb::InvalidInputException("moraine: ambiguous table statistics");
	}
	const auto &row = rows[0];
	duckdb::DuckLakeGlobalStatsInfo entry {};
	entry.table_id = table_id;
	entry.initialized = true;
	entry.record_count_unknown = !ReadOptional(row[1], entry.record_count);
	entry.next_row_id = row[2].GetValue<duckdb::idx_t>();
	entry.missing_table_size = !ReadOptional(row[3], entry.table_size_bytes);
	if (entry.missing_table_size) {
		// Older catalogs can require DuckLake's data-file size recovery query.
		return false;
	}
	for (const auto &column : TableStatisticsRows("ducklake_table_column_stats", table_id)) {
		duckdb::DuckLakeGlobalColumnStatsInfo column_stats {};
		column_stats.column_id = duckdb::FieldIndex(column[1].GetValue<duckdb::idx_t>());
		column_stats.has_contains_null = ReadOptional(column[2], column_stats.contains_null);
		column_stats.has_contains_nan = ReadOptional(column[3], column_stats.contains_nan);
		column_stats.has_min = ReadOptional(column[4], column_stats.min_val);
		column_stats.has_max = ReadOptional(column[5], column_stats.max_val);
		column_stats.has_extra_stats = ReadOptional(column[6], column_stats.extra_stats);
		if (column.size() > 7) {
			ReadOptional(column[7], column_stats.min_is_exact);
			ReadOptional(column[8], column_stats.max_is_exact);
		}
		entry.column_stats.push_back(std::move(column_stats));
	}
	stats.push_back(std::move(entry));
	return true;
}

bool MoraineMetadataManager::TryWriteAppendStats(const duckdb::DuckLakeGlobalStatsInfo &stats) {
	auto pending = stats;
	for (auto &column : pending.column_stats) {
		if (!column.has_extra_stats) {
			continue;
		}
		// DuckLake serializes extra statistics as a SQL string literal.
		duckdb::Parser parser(*transaction.GetConnection().context);
		auto expressions = parser.ParseExpressionList(column.extra_stats);
		if (expressions.size() != 1 || expressions[0]->GetExpressionClass() != duckdb::ExpressionClass::CONSTANT) {
			return false;
		}
		auto value = expressions[0]->Cast<duckdb::ConstantExpression>().GetLiteral().ToValue();
		column.has_extra_stats = ReadOptional(value, column.extra_stats);
	}
	pending_statistics.push_back(std::move(pending));
	return true;
}

void MoraineMetadataManager::ClearAppendStats() {
	pending_statistics.clear();
}

void MoraineMetadataManager::FlushAppendStats() {
	auto pending = std::move(pending_statistics);
	pending_statistics.clear();
	auto &context = *transaction.GetConnection().context;
	auto &catalog = ResolveMoraineCatalog(context, transaction.GetCatalog().MetadataDatabaseName());
	auto &moraine_transaction = catalog.GetCatalogTransaction(context).transaction->Cast<MoraineTransaction>();
	const auto &table_spec = MetadataSpec("ducklake_table_stats");
	const auto &column_spec = MetadataSpec("ducklake_table_column_stats");
	for (const auto &stats : pending) {
		// SQL UPDATE does not create absent table or column statistics.
		bool table_exists = !stats.initialized || !TableStatisticsRows(table_spec.name, stats.table_id).empty();
		std::unordered_set<duckdb::idx_t> existing_columns;
		if (stats.initialized) {
			for (const auto &column : TableStatisticsRows(column_spec.name, stats.table_id)) {
				existing_columns.insert(column[1].GetValue<duckdb::idx_t>());
			}
		}
		auto *staged = moraine_transaction.StagedTx();
		auto table_id = duckdb::Value::UBIGINT(stats.table_id.index);
		if (table_exists) {
			StageMetadataInsert(
			    staged, table_spec,
			    {table_id, stats.record_count_unknown ? duckdb::Value() : duckdb::Value::UBIGINT(stats.record_count),
			     duckdb::Value::UBIGINT(stats.next_row_id), duckdb::Value::UBIGINT(stats.table_size_bytes)});
		}
		for (const auto &column : stats.column_stats) {
			if (stats.initialized && existing_columns.count(column.column_id.index) == 0) {
				continue;
			}
			std::vector<duckdb::Value> row {table_id,
			                                duckdb::Value::UBIGINT(column.column_id.index),
			                                OptionalBoolean(column.has_contains_null, column.contains_null),
			                                OptionalBoolean(column.has_contains_nan, column.contains_nan),
			                                StatisticsBound(column.has_min, column.min_val),
			                                StatisticsBound(column.has_max, column.max_val),
			                                column.has_extra_stats ? duckdb::Value(column.extra_stats)
			                                                       : duckdb::Value()};
			if (catalog.ServesExtendedCatalog()) {
				row.push_back(OptionalBoolean(column.has_min, column.min_is_exact));
				row.push_back(OptionalBoolean(column.has_max, column.max_is_exact));
			}
			StageMetadataInsert(staged, column_spec, row);
		}
	}
}

bool MoraineMetadataManager::TryWriteSnapshotChanges(duckdb::DuckLakeSnapshot snapshot,
                                                     const duckdb::SnapshotChangeInfo &changes,
                                                     const duckdb::DuckLakeSnapshotCommit &commit_info) {
	auto &context = *transaction.GetConnection().context;
	auto &catalog = ResolveMoraineCatalog(context, transaction.GetCatalog().MetadataDatabaseName());
	auto &moraine_transaction = catalog.GetCatalogTransaction(context).transaction->Cast<MoraineTransaction>();
	StageMetadataInsert(moraine_transaction.StagedTx(), MetadataSpec("ducklake_snapshot_changes"),
	                    {duckdb::Value::UBIGINT(snapshot.snapshot_id),
	                     changes.changes_made.empty() ? duckdb::Value() : duckdb::Value(changes.changes_made),
	                     commit_info.author, commit_info.commit_message, commit_info.commit_extra_info});
	return true;
}

} // namespace moraine_duckdb
