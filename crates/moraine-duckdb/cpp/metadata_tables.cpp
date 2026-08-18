#include "metadata_tables.hpp"

#include <algorithm>
#include <optional>
#include <set>
#include <string>

// The bound-expression shapes `MetadataScanPushdownComplexFilter` matches,
// and the `LogicalGet` it reads the scan's projection from.
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

// Re-resolving a deferred scan's catalog at execution time.
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

#include "catalog.hpp"
#include "inline_tables.hpp"
#include "owned_array.hpp"
#include "transaction_manager.hpp"

namespace moraine_duckdb {

namespace {

// The store state a dump's rows stand at.
struct MoraineHeadStamp {
	uint64_t snapshot_id;
	uint64_t batch_seq;
};

// Reads the head stamp, reporting whether one exists. A store with no
// head yet (mid-bootstrap) and a failed read both report false, which
// costs a caller only the caching it would otherwise have done — never
// correctness, so the error is swallowed rather than raised.
bool ReadHeadStamp(MoraineCatalogHandle *handle, duckdb::ClientContext &context, MoraineHeadStamp &out) {
	MoraineError err {};
	bool present = false;
	if (moraine_head_stamp(handle, &out.snapshot_id, &out.batch_seq, &present, moraine_shim_is_interrupted, &context,
	                       &err) != MORAINE_OK) {
		if (err.message != nullptr) {
			moraine_error_free(err.message);
		}
		return false;
	}
	return present;
}

duckdb::Value OptVarchar(const char *s) {
	if (s == nullptr) {
		return duckdb::Value(duckdb::LogicalType::VARCHAR);
	}
	return duckdb::Value(std::string(s));
}

duckdb::Value Varchar(const char *s) {
	return OptVarchar(s);
}

duckdb::Value Bigint(uint64_t v) {
	return duckdb::Value::BIGINT(static_cast<int64_t>(v));
}

duckdb::Value OptBigint(bool has, uint64_t v) {
	if (!has) {
		return duckdb::Value(duckdb::LogicalType::BIGINT);
	}
	return Bigint(v);
}

duckdb::Value Boolean(bool v) {
	return duckdb::Value::BOOLEAN(v);
}

duckdb::Value OptBoolean(bool has, bool v) {
	if (!has) {
		return duckdb::Value(duckdb::LogicalType::BOOLEAN);
	}
	return Boolean(v);
}

duckdb::Value Uuid(const char *s) {
	if (s == nullptr) {
		return duckdb::Value(duckdb::LogicalType::UUID);
	}
	return duckdb::Value::UUID(std::string(s));
}

duckdb::Value TimestampTz(int64_t micros) {
	return duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t(micros));
}

// `ducklake_column.column_type` must carry DuckLake's own lowercase type
// vocabulary ("int64", "float64", "timestamptz", ...), not the DuckDB SQL
// type names moraine stores in this field. Re-derives the
// `duckdb::LogicalType` via `MapColumnType`, then names it DuckLake's way.
// `DECIMAL` reproduces its width/scale suffix ("decimal(18,4)"), which
// DuckLake needs to reconstruct the type; every other supported type maps
// exactly.
duckdb::Value DuckLakeColumnType(const char *sql_type) {
	if (sql_type == nullptr) {
		return duckdb::Value(duckdb::LogicalType::VARCHAR);
	}
	// A nested type is stored as its DuckLake marker ("list"/"struct"/"map")
	// with the element/field types carried by child `ducklake_column` rows
	// (linked by `parent_column`). Pass the marker through unchanged so
	// DuckLake reconstructs the type from the hierarchy; there is no scalar
	// `LogicalType` to normalize it against.
	if (duckdb::StringUtil::CIEquals(sql_type, "list") || duckdb::StringUtil::CIEquals(sql_type, "struct") ||
	    duckdb::StringUtil::CIEquals(sql_type, "map")) {
		return duckdb::Value(duckdb::StringUtil::Lower(sql_type));
	}
	auto type = MapColumnType(sql_type);
	// `JSON` is a VARCHAR carrying a `JSON` alias, so it must be named before
	// the id switch would collapse it to "varchar" (matches DuckLake's own
	// `DuckLakeTypes::ToString`, which checks the alias first).
	if (type.IsJSONType()) {
		return duckdb::Value("json");
	}
	switch (type.id()) {
	case duckdb::LogicalTypeId::BOOLEAN:
		return duckdb::Value("boolean");
	case duckdb::LogicalTypeId::TINYINT:
		return duckdb::Value("int8");
	case duckdb::LogicalTypeId::SMALLINT:
		return duckdb::Value("int16");
	case duckdb::LogicalTypeId::INTEGER:
		return duckdb::Value("int32");
	case duckdb::LogicalTypeId::BIGINT:
		return duckdb::Value("int64");
	case duckdb::LogicalTypeId::HUGEINT:
		return duckdb::Value("int128");
	case duckdb::LogicalTypeId::UHUGEINT:
		return duckdb::Value("uint128");
	case duckdb::LogicalTypeId::UTINYINT:
		return duckdb::Value("uint8");
	case duckdb::LogicalTypeId::USMALLINT:
		return duckdb::Value("uint16");
	case duckdb::LogicalTypeId::UINTEGER:
		return duckdb::Value("uint32");
	case duckdb::LogicalTypeId::UBIGINT:
		return duckdb::Value("uint64");
	case duckdb::LogicalTypeId::FLOAT:
		return duckdb::Value("float32");
	case duckdb::LogicalTypeId::DOUBLE:
		return duckdb::Value("float64");
	case duckdb::LogicalTypeId::DECIMAL:
		return duckdb::Value(duckdb::StringUtil::Format("decimal(%d,%d)", duckdb::DecimalType::GetWidth(type),
		                                                duckdb::DecimalType::GetScale(type)));
	case duckdb::LogicalTypeId::INTERVAL:
		return duckdb::Value("interval");
	case duckdb::LogicalTypeId::TIME:
		return duckdb::Value("time");
	case duckdb::LogicalTypeId::TIME_NS:
		return duckdb::Value("time_ns");
	case duckdb::LogicalTypeId::TIME_TZ:
		return duckdb::Value("timetz");
	case duckdb::LogicalTypeId::DATE:
		return duckdb::Value("date");
	case duckdb::LogicalTypeId::TIMESTAMP:
		return duckdb::Value("timestamp");
	case duckdb::LogicalTypeId::TIMESTAMP_SEC:
		return duckdb::Value("timestamp_s");
	case duckdb::LogicalTypeId::TIMESTAMP_MS:
		return duckdb::Value("timestamp_ms");
	case duckdb::LogicalTypeId::TIMESTAMP_NS:
		return duckdb::Value("timestamp_ns");
	case duckdb::LogicalTypeId::TIMESTAMP_TZ:
		return duckdb::Value("timestamptz");
	case duckdb::LogicalTypeId::VARCHAR:
		return duckdb::Value("varchar");
	case duckdb::LogicalTypeId::BLOB:
		return duckdb::Value("blob");
	case duckdb::LogicalTypeId::UUID:
		return duckdb::Value("uuid");
	case duckdb::LogicalTypeId::GEOMETRY:
		return duckdb::Value("geometry");
	default:
		// `MapColumnType` only ever returns one of the ids above (it
		// throws NotImplementedException for anything else), so this is
		// unreachable by construction, not a silent fallback.
		throw duckdb::InternalException("moraine: unmapped DuckLake type for \"%s\"", sql_type);
	}
}
// Runs one dump entry point into an `OwnedArray` and shapes each row into
// its `ducklake_*` column list — the shared body of every `Provide*`
// function below.
template <typename Row, typename DumpFn, typename ShapeFn>
std::vector<std::vector<duckdb::Value>> DumpRows(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                 void *probe_ctx, DumpFn dump, void (*free_fn)(Row *, size_t),
                                                 ShapeFn shape) {
	OwnedArray<Row> rows(free_fn);
	MoraineError err {};
	auto code = dump(handle, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back(shape(r));
	}
	return result;
}

// One dump call (`moraine_dump_snapshots`) feeds both ProvideSnapshots and
// ProvideSnapshotChanges, since the store models them as one merged record;
// each emits its columns in the declared order of its `ducklake_*` table.
// The two column shapes of one snapshot record — `ducklake_snapshot` and
// `ducklake_snapshot_changes` — shared by the committed dumps and the
// tx-aware dump.
std::vector<duckdb::Value> SnapshotShape(const MoraineSnapshotRow &r) {
	return {
	    Bigint(r.snapshot_id),    TimestampTz(r.snapshot_time_micros),
	    Bigint(r.schema_version), Bigint(r.next_catalog_id),
	    Bigint(r.next_file_id),
	};
}

std::vector<duckdb::Value> SnapshotChangesShape(const MoraineSnapshotRow &r) {
	return {
	    Bigint(r.snapshot_id),        Varchar(r.changes_made),         OptVarchar(r.author),
	    OptVarchar(r.commit_message), OptVarchar(r.commit_extra_info),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideSnapshots(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                         void *probe_ctx) {
	return DumpRows<MoraineSnapshotRow>(handle, probe, probe_ctx, moraine_dump_snapshots, moraine_dump_snapshots_free,
	                                    SnapshotShape);
}

std::vector<std::vector<duckdb::Value>> ProvideSnapshotChanges(MoraineCatalogHandle *handle,
                                                               MoraineInterruptProbe probe, void *probe_ctx) {
	return DumpRows<MoraineSnapshotRow>(handle, probe, probe_ctx, moraine_dump_snapshots, moraine_dump_snapshots_free,
	                                    SnapshotChangesShape);
}

// One `ducklake_schema` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> SchemaShape(const MoraineSchemaRow &r) {
	return {
	    Bigint(r.schema_id),         Uuid(r.schema_uuid),
	    Bigint(r.begin_snapshot),    OptBigint(r.has_end_snapshot, r.end_snapshot),
	    Varchar(r.schema_name),      Varchar(r.path),
	    Boolean(r.path_is_relative),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideSchemas(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                       void *probe_ctx) {
	return DumpRows<MoraineSchemaRow>(handle, probe, probe_ctx, moraine_dump_schemas, moraine_dump_schemas_free,
	                                  SchemaShape);
}

// One `ducklake_table` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> TableShape(const MoraineTableRow &r) {
	return {
	    Bigint(r.table_id),       Uuid(r.table_uuid),
	    Bigint(r.begin_snapshot), OptBigint(r.has_end_snapshot, r.end_snapshot),
	    Bigint(r.schema_id),      Varchar(r.table_name),
	    Varchar(r.path),          Boolean(r.path_is_relative),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideTables(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                      void *probe_ctx) {
	return DumpRows<MoraineTableRow>(handle, probe, probe_ctx, moraine_dump_tables, moraine_dump_tables_free,
	                                 TableShape);
}

// One `ducklake_view` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> ViewShape(const MoraineViewRow &r) {
	return {
	    Bigint(r.view_id),
	    Uuid(r.view_uuid),
	    Bigint(r.begin_snapshot),
	    OptBigint(r.has_end_snapshot, r.end_snapshot),
	    Bigint(r.schema_id),
	    Varchar(r.view_name),
	    Varchar(r.dialect),
	    Varchar(r.sql),
	    OptVarchar(r.column_aliases),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideViews(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                     void *probe_ctx) {
	return DumpRows<MoraineViewRow>(handle, probe, probe_ctx, moraine_dump_views, moraine_dump_views_free, ViewShape);
}

// One `ducklake_macro` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> MacroShape(const MoraineMacroRow &r) {
	return {
	    Bigint(r.schema_id),
	    Bigint(r.macro_id),
	    Varchar(r.macro_name),
	    Bigint(r.begin_snapshot),
	    OptBigint(r.has_end_snapshot, r.end_snapshot),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideMacros(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                      void *probe_ctx) {
	return DumpRows<MoraineMacroRow>(handle, probe, probe_ctx, moraine_dump_macros, moraine_dump_macros_free,
	                                 MacroShape);
}

// Impl and parameter rows come back flattened from the embedded children
// in `(macro_id, impl_id[, column_id])` order, and that order is served
// as-is: DuckLake reconstructs macros with LIST() aggregations that carry
// no ORDER BY, so served row order is the reconstruction order.
// One `ducklake_macro_impl` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> MacroImplShape(const MoraineMacroImplRow &r) {
	return {
	    Bigint(r.macro_id), Bigint(r.impl_id), Varchar(r.dialect), Varchar(r.sql), Varchar(r.macro_type),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideMacroImpls(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                          void *probe_ctx) {
	return DumpRows<MoraineMacroImplRow>(handle, probe, probe_ctx, moraine_dump_macro_impls,
	                                     moraine_dump_macro_impls_free, MacroImplShape);
}

// One `ducklake_macro_parameters` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> MacroParameterShape(const MoraineMacroParameterRow &r) {
	return {
	    Bigint(r.macro_id),
	    Bigint(r.impl_id),
	    Bigint(r.column_id),
	    Varchar(r.parameter_name),
	    Varchar(r.parameter_type),
	    OptVarchar(r.default_value),
	    Varchar(r.default_value_type),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideMacroParameters(MoraineCatalogHandle *handle,
                                                               MoraineInterruptProbe probe, void *probe_ctx) {
	return DumpRows<MoraineMacroParameterRow>(handle, probe, probe_ctx, moraine_dump_macro_parameters,
	                                          moraine_dump_macro_parameters_free, MacroParameterShape);
}

// One `ducklake_column_mapping` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> ColumnMappingShape(const MoraineColumnMappingRow &r) {
	return {
	    Bigint(r.mapping_id),
	    Bigint(r.table_id),
	    Varchar(r.map_type),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideColumnMappings(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                              void *probe_ctx) {
	return DumpRows<MoraineColumnMappingRow>(handle, probe, probe_ctx, moraine_dump_column_mappings,
	                                         moraine_dump_column_mappings_free, ColumnMappingShape);
}

// One `ducklake_name_mapping` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> NameMappingShape(const MoraineNameMappingRow &r) {
	return {
	    Bigint(r.mapping_id),
	    Bigint(r.column_id),
	    Varchar(r.source_name),
	    Bigint(r.target_field_id),
	    OptBigint(r.has_parent_column, r.parent_column),
	    Boolean(r.is_partition),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideNameMappings(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                            void *probe_ctx) {
	return DumpRows<MoraineNameMappingRow>(handle, probe, probe_ctx, moraine_dump_name_mappings,
	                                       moraine_dump_name_mappings_free, NameMappingShape);
}

// One `ducklake_column` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> ColumnShape(const MoraineColumnRow &r) {
	return {
	    Bigint(r.column_id),
	    Bigint(r.begin_snapshot),
	    OptBigint(r.has_end_snapshot, r.end_snapshot),
	    Bigint(r.table_id),
	    Bigint(r.column_order),
	    Varchar(r.column_name),
	    DuckLakeColumnType(r.column_type),
	    OptVarchar(r.initial_default),
	    OptVarchar(r.default_value),
	    Boolean(r.nulls_allowed),
	    OptBigint(r.has_parent_column, r.parent_column),
	    OptVarchar(r.default_value_type),
	    OptVarchar(r.default_value_dialect),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideColumns(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                       void *probe_ctx) {
	return DumpRows<MoraineColumnRow>(handle, probe, probe_ctx, moraine_dump_columns, moraine_dump_columns_free,
	                                  ColumnShape);
}

// One `ducklake_data_file` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> DataFileShape(const MoraineDataFileRow &r) {
	return {
	    Bigint(r.data_file_id),
	    Bigint(r.table_id),
	    Bigint(r.begin_snapshot),
	    OptBigint(r.has_end_snapshot, r.end_snapshot),
	    OptBigint(r.has_file_order, r.file_order),
	    Varchar(r.path),
	    Boolean(r.path_is_relative),
	    Varchar(r.file_format),
	    Bigint(r.record_count),
	    Bigint(r.file_size_bytes),
	    Bigint(r.footer_size),
	    OptBigint(r.has_row_id_start, r.row_id_start),
	    OptBigint(r.has_partition_id, r.partition_id),
	    OptVarchar(r.encryption_key),
	    OptBigint(r.has_mapping_id, r.mapping_id),
	    OptBigint(r.has_partial_max, r.partial_max),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideDataFiles(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                         void *probe_ctx) {
	return DumpRows<MoraineDataFileRow>(handle, probe, probe_ctx, moraine_dump_data_files, moraine_dump_data_files_free,
	                                    DataFileShape);
}

// The `ducklake_data_file` rows live at `live_bound`. The core compares the
// bound against the read point it serves this dump from, so a bound that
// has fallen behind one simply reads every version.
std::vector<std::vector<duckdb::Value>> ProvideDataFilesLiveAt(MoraineCatalogHandle *handle, uint64_t live_bound,
                                                               MoraineInterruptProbe probe, void *probe_ctx) {
	OwnedArray<MoraineDataFileRow> rows(moraine_dump_data_files_free);
	MoraineError err {};
	if (moraine_dump_data_files_live_at(handle, live_bound, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err) !=
	    MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back(DataFileShape(r));
	}
	return result;
}

// One `ducklake_delete_file` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> DeleteFileShape(const MoraineDeleteFileRow &r) {
	return {
	    Bigint(r.delete_file_id),
	    Bigint(r.table_id),
	    Bigint(r.begin_snapshot),
	    OptBigint(r.has_end_snapshot, r.end_snapshot),
	    Bigint(r.data_file_id),
	    Varchar(r.path),
	    Boolean(r.path_is_relative),
	    Varchar(r.format),
	    Bigint(r.delete_count),
	    Bigint(r.file_size_bytes),
	    Bigint(r.footer_size),
	    OptVarchar(r.encryption_key),
	    OptBigint(r.has_partial_max, r.partial_max),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideDeleteFiles(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                           void *probe_ctx) {
	return DumpRows<MoraineDeleteFileRow>(handle, probe, probe_ctx, moraine_dump_delete_files,
	                                      moraine_dump_delete_files_free, DeleteFileShape);
}

// One `ducklake_table_stats` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> TableStatsShape(const MoraineTableStatsRow &r) {
	return {
	    Bigint(r.table_id),
	    Bigint(r.record_count),
	    Bigint(r.next_row_id),
	    Bigint(r.file_size_bytes),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideTableStats(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                          void *probe_ctx) {
	return DumpRows<MoraineTableStatsRow>(handle, probe, probe_ctx, moraine_dump_table_stats,
	                                      moraine_dump_table_stats_free, TableStatsShape);
}

// One `ducklake_table_column_stats` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> TableColumnStatsShape(const MoraineTableColumnStatsRow &r) {
	return {
	    Bigint(r.table_id),
	    Bigint(r.column_id),
	    OptBoolean(r.has_contains_null, r.contains_null),
	    OptBoolean(r.has_contains_nan, r.contains_nan),
	    OptVarchar(r.min_value),
	    OptVarchar(r.max_value),
	    OptVarchar(r.extra_stats),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideTableColumnStats(MoraineCatalogHandle *handle,
                                                                MoraineInterruptProbe probe, void *probe_ctx) {
	return DumpRows<MoraineTableColumnStatsRow>(handle, probe, probe_ctx, moraine_dump_table_column_stats,
	                                            moraine_dump_table_column_stats_free, TableColumnStatsShape);
}

// One `ducklake_file_column_stats` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> FileColumnStatsShape(const MoraineFileColumnStatsRow &r) {
	return {
	    Bigint(r.data_file_id),      Bigint(r.table_id),      Bigint(r.column_id),
	    Bigint(r.column_size_bytes), Bigint(r.value_count),   Bigint(r.null_count),
	    OptVarchar(r.min_value),     OptVarchar(r.max_value), OptBoolean(r.has_contains_nan, r.contains_nan),
	    OptVarchar(r.extra_stats),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideFileColumnStats(MoraineCatalogHandle *handle,
                                                               MoraineInterruptProbe probe, void *probe_ctx) {
	return DumpRows<MoraineFileColumnStatsRow>(handle, probe, probe_ctx, moraine_dump_file_column_stats,
	                                           moraine_dump_file_column_stats_free, FileColumnStatsShape);
}

// One table's file column statistics. `DumpRows` cannot serve this: the
// scoped entry point takes a table id.
std::vector<std::vector<duckdb::Value>> ProvideFileColumnStatsOf(MoraineCatalogHandle *handle, uint64_t table_id,
                                                                 MoraineInterruptProbe probe, void *probe_ctx) {
	OwnedArray<MoraineFileColumnStatsRow> rows(moraine_dump_file_column_stats_free);
	MoraineError err {};
	auto code =
	    moraine_dump_file_column_stats_of(handle, table_id, rows.OutItems(), rows.OutLen(), probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back(FileColumnStatsShape(r));
	}
	return result;
}

// `ducklake_schema_versions` rows are flattened out of the snapshot
// records they fold into (the staged path stores only the per-snapshot
// table-id set — begin_snapshot/schema_version are the snapshot's own
// values, revalidated at commit).
// One `ducklake_schema_versions` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> SchemaVersionShape(const MoraineSchemaVersionRow &r) {
	return {
	    Bigint(r.begin_snapshot),
	    Bigint(r.schema_version),
	    Bigint(r.table_id),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideSchemaVersions(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                              void *probe_ctx) {
	return DumpRows<MoraineSchemaVersionRow>(handle, probe, probe_ctx, moraine_dump_schema_versions,
	                                         moraine_dump_schema_versions_free, SchemaVersionShape);
}

// Always-empty stand-in for a `ducklake_*` table covering a feature the
// store doesn't model (variant statistics). The table must still exist
// as a SQL table: DuckLake's attach/snapshot-load query joins every one
// of them unconditionally, so a missing table is a bind-time Catalog
// Error even where the query would return zero rows for it.
std::vector<std::vector<duckdb::Value>> ProvideEmpty(MoraineCatalogHandle *, MoraineInterruptProbe, void *) {
	return {};
}

// One `ducklake_partition_info` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> PartitionInfoShape(const MorainePartitionInfoRow &r) {
	return {
	    Bigint(r.partition_id),
	    Bigint(r.table_id),
	    Bigint(r.begin_snapshot),
	    OptBigint(r.has_end_snapshot, r.end_snapshot),
	};
}

std::vector<std::vector<duckdb::Value>> ProvidePartitionInfo(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                             void *probe_ctx) {
	return DumpRows<MorainePartitionInfoRow>(handle, probe, probe_ctx, moraine_dump_partition_info,
	                                         moraine_dump_partition_info_free, PartitionInfoShape);
}

// One `ducklake_partition_column` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> PartitionColumnShape(const MorainePartitionColumnRow &r) {
	return {
	    Bigint(r.partition_id), Bigint(r.table_id),   Bigint(r.partition_key_index),
	    Bigint(r.column_id),    Varchar(r.transform),
	};
}

std::vector<std::vector<duckdb::Value>> ProvidePartitionColumns(MoraineCatalogHandle *handle,
                                                                MoraineInterruptProbe probe, void *probe_ctx) {
	return DumpRows<MorainePartitionColumnRow>(handle, probe, probe_ctx, moraine_dump_partition_columns,
	                                           moraine_dump_partition_columns_free, PartitionColumnShape);
}

// One `ducklake_file_partition_value` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> FilePartitionValueShape(const MoraineFilePartitionValueRow &r) {
	return {
	    Bigint(r.data_file_id),
	    Bigint(r.table_id),
	    Bigint(r.partition_key_index),
	    Varchar(r.partition_value),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideFilePartitionValues(MoraineCatalogHandle *handle,
                                                                   MoraineInterruptProbe probe, void *probe_ctx) {
	return DumpRows<MoraineFilePartitionValueRow>(handle, probe, probe_ctx, moraine_dump_file_partition_values,
	                                              moraine_dump_file_partition_values_free, FilePartitionValueShape);
}

// One `ducklake_sort_info` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> SortInfoShape(const MoraineSortInfoRow &r) {
	return {
	    Bigint(r.sort_id),
	    Bigint(r.table_id),
	    Bigint(r.begin_snapshot),
	    OptBigint(r.has_end_snapshot, r.end_snapshot),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideSortInfo(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                        void *probe_ctx) {
	return DumpRows<MoraineSortInfoRow>(handle, probe, probe_ctx, moraine_dump_sort_info, moraine_dump_sort_info_free,
	                                    SortInfoShape);
}

// One `ducklake_sort_expression` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> SortExpressionShape(const MoraineSortExpressionRow &r) {
	return {
	    Bigint(r.sort_id),  Bigint(r.table_id),        Bigint(r.sort_key_index), Varchar(r.expression),
	    Varchar(r.dialect), Varchar(r.sort_direction), Varchar(r.null_order),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideSortExpressions(MoraineCatalogHandle *handle,
                                                               MoraineInterruptProbe probe, void *probe_ctx) {
	return DumpRows<MoraineSortExpressionRow>(handle, probe, probe_ctx, moraine_dump_sort_expressions,
	                                          moraine_dump_sort_expressions_free, SortExpressionShape);
}

// One `ducklake_tag` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> TagShape(const MoraineTagRow &r) {
	return {
	    Bigint(r.object_id), Bigint(r.begin_snapshot), OptBigint(r.has_end_snapshot, r.end_snapshot),
	    Varchar(r.key),      Varchar(r.value),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideTags(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                    void *probe_ctx) {
	return DumpRows<MoraineTagRow>(handle, probe, probe_ctx, moraine_dump_tags, moraine_dump_tags_free, TagShape);
}

// One `ducklake_column_tag` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> ColumnTagShape(const MoraineColumnTagRow &r) {
	return {
	    Bigint(r.table_id),       Bigint(r.column_id),
	    Bigint(r.begin_snapshot), OptBigint(r.has_end_snapshot, r.end_snapshot),
	    Varchar(r.key),           Varchar(r.value),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideColumnTags(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                          void *probe_ctx) {
	return DumpRows<MoraineColumnTagRow>(handle, probe, probe_ctx, moraine_dump_column_tags,
	                                     moraine_dump_column_tags_free, ColumnTagShape);
}

// One `ducklake_files_scheduled_for_deletion` record's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> ScheduledDeletionShape(const MoraineScheduledDeletionRow &r) {
	return {
	    Bigint(r.data_file_id),
	    Varchar(r.path),
	    duckdb::Value::BOOLEAN(r.path_is_relative),
	    TimestampTz(r.schedule_start_micros),
	};
}

std::vector<std::vector<duckdb::Value>> ProvideScheduledDeletions(MoraineCatalogHandle *handle,
                                                                  MoraineInterruptProbe probe, void *probe_ctx) {
	return DumpRows<MoraineScheduledDeletionRow>(handle, probe, probe_ctx, moraine_dump_scheduled_deletions,
	                                             moraine_dump_scheduled_deletions_free, ScheduledDeletionShape);
}

// `ducklake_metadata` rows. All are fixed here except `encrypted`, which
// is the store's creation-time flag. Constraints on the values DuckLake
// reads back:
//   - "version": must be "1.0"; any other value triggers migration logic.
//   - "encrypted": the stored flag (moraine_catalog_encrypted). DuckLake
//     compares it against the attach's requested encryption and, when
//     "true", encrypts new data files and records their keys in
//     `ducklake_data_file`/`ducklake_delete_file` rows.
//   - "data_path" is served only when the store recorded one at creation
//     (from the DATA_PATH given then). DuckLake reads it back on attach and
//     uses it when no DATA_PATH is supplied, so a re-attach need not repeat
//     it; a store with none served leaves the ATTACH DATA_PATH as authority.
//   - "created_by": never read back; served because it costs nothing.
// No row is served for "data_inlining_row_limit". DuckLake resolves that
// limit from its config options first and falls back to the DuckDB setting
// and then a compiled default of 10, and an ATTACH option lands in the same
// map an option row does — so a row served here would outrank both
// `ATTACH ... (DATA_INLINING_ROW_LIMIT n)` and
// `SET ducklake_default_data_inlining_row_limit`, silently. Serving none
// leaves inlining on at that same default of 10 and leaves both knobs
// meaningful; a store that wants another limit records a real option row.
// All rows are global (scope/scope_id NULL).
// The `ducklake_metadata` rows moraine serves from its own facts rather
// than from stored options: the protocol version, the writer, the
// creation-time `encrypted` flag, and the recorded data root in the
// normalized form DuckLake compares against.
std::vector<std::vector<duckdb::Value>> FixedMetadataRows(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                          void *probe_ctx) {
	bool encrypted = false;
	MoraineError err {};
	auto code = moraine_catalog_encrypted(handle, &encrypted, probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	auto null_varchar = duckdb::Value(duckdb::LogicalType::VARCHAR);
	auto null_bigint = duckdb::Value(duckdb::LogicalType::BIGINT);
	std::vector<std::vector<duckdb::Value>> rows = {
	    {Varchar("version"), Varchar("1.0"), null_varchar, null_bigint},
	    {Varchar("created_by"), Varchar("moraine"), null_varchar, null_bigint},
	    {Varchar("encrypted"), Varchar(encrypted ? "true" : "false"), null_varchar, null_bigint},
	};
	// The recorded data root, when the store has one, so DuckLake reads it
	// back on attach instead of requiring DATA_PATH again.
	char *data_path = nullptr;
	MoraineError dp_err {};
	if (moraine_data_path(handle, probe, probe_ctx, &data_path, &dp_err) != MORAINE_OK) {
		ThrowMoraineError(dp_err);
	}
	if (data_path != nullptr) {
		// DuckLake normalizes a data path to a single trailing separator and
		// compares that to this served value (or adopts it verbatim when no
		// DATA_PATH is given), so serve the same normalized form. moraine's
		// supported stores (local, s3://) all use `/`.
		std::string served(data_path);
		moraine_string_free(data_path);
		while (!served.empty() && served.back() == '/') {
			served.pop_back();
		}
		served += '/';
		rows.push_back({Varchar("data_path"), Varchar(served.c_str()), null_varchar, null_bigint});
	}

	// Stored options last. Most override a fixed row of the same key and
	// scope — the rows above are what a store *without* a setting serves,
	// and `set_option` is what replaces one; without the override a user
	// could set an option and never see it take effect.
	//
	// The exceptions are the keys above that are *store facts* rather than
	// defaults: `version` is the protocol constant, `encrypted` is fixed
	// when the catalog is created, and `data_path` is served in the
	// normalized form DuckLake compares against the ATTACH value (the
	// stored option holds it verbatim, which would not match). Those are
	// moraine's to project, so a stored row of the same key is dropped
	// rather than served twice or allowed to win.
	return rows;
}

// One `ducklake_metadata` option row's column list, shared by the committed
// dump and the transaction-aware one.
std::vector<duckdb::Value> OptionShape(const MoraineOptionRow &r) {
	return {
	    Varchar(r.key),
	    Varchar(r.value),
	    OptVarchar(r.scope),
	    r.has_scope_id ? Bigint(r.scope_id) : duckdb::Value(duckdb::LogicalType::BIGINT),
	};
}

// Merges the store's own option rows over the fixed ones. Most override a
// fixed row of the same key and scope — the fixed rows are what a store
// *without* a setting serves, and `set_option` is what replaces one; without
// the override a user could set an option and never see it take effect.
//
// The exceptions are the keys that are *store facts* rather than defaults:
// `version` is the protocol constant, `encrypted` is fixed when the catalog
// is created, and `data_path` is served in the normalized form DuckLake
// compares against the ATTACH value (the stored option holds it verbatim,
// which would not match). Those are moraine's to project, so a stored row of
// the same key is dropped rather than served twice or allowed to win.
void MergeStoredMetadataRows(std::vector<std::vector<duckdb::Value>> &rows,
                             std::vector<std::vector<duckdb::Value>> stored) {
	static const std::set<std::string> kServedByMoraine = {"version", "created_by", "encrypted", "data_path"};
	for (auto &row : stored) {
		// Only the global scope: a table-scoped option of the same name is
		// a user setting, not one of these store facts.
		if (row[2].IsNull() && kServedByMoraine.count(row[0].GetValue<std::string>()) > 0) {
			continue;
		}
		auto same_key = [&row](const std::vector<duckdb::Value> &existing) {
			// Null-aware: a global option's scope and scope_id are both
			// NULL, and duckdb::Value's operator== throws on those rather
			// than returning false.
			auto same = [](const duckdb::Value &a, const duckdb::Value &b) {
				if (a.IsNull() || b.IsNull()) {
					return a.IsNull() && b.IsNull();
				}
				return a == b;
			};
			return same(existing[0], row[0]) && same(existing[2], row[2]) && same(existing[3], row[3]);
		};
		rows.erase(std::remove_if(rows.begin(), rows.end(), same_key), rows.end());
		rows.push_back(std::move(row));
	}
}

std::vector<std::vector<duckdb::Value>> ProvideMetadata(MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                                                        void *probe_ctx) {
	auto rows = FixedMetadataRows(handle, probe, probe_ctx);
	MergeStoredMetadataRows(rows, DumpRows<MoraineOptionRow>(handle, probe, probe_ctx, moraine_dump_options,
	                                                         moraine_dump_options_free, OptionShape));
	return rows;
}

// Feeds `ducklake_inlined_data_tables`: one row per `(table_id,
// schema_version)` with a recorded `inline/schema`. The table_name column
// carries `InlinedDataTableName` (inline_tables.cpp), matching DuckLake's
// own inline-table naming.
std::vector<std::vector<duckdb::Value>> ProvideInlinedDataTables(MoraineCatalogHandle *handle,
                                                                 MoraineInterruptProbe probe, void *probe_ctx) {
	return DumpRows<MoraineInlineTableRow>(handle, probe, probe_ctx, moraine_inline_registered_tables,
	                                       moraine_inline_registered_tables_free,
	                                       [](const MoraineInlineTableRow &r) -> std::vector<duckdb::Value> {
		                                       return {
		                                           Bigint(r.table_id),
		                                           Varchar(InlinedDataTableName(r.table_id, r.schema_version).c_str()),
		                                           Bigint(r.schema_version),
		                                       };
	                                       });
}

// Column shapes below match each `ducklake_*` table's own
// `CREATE TABLE` shape (name, type, declared nullability). `not_null` is set
// only for columns DuckLake declares `NOT NULL` or `PRIMARY KEY`; every
// other column is left nullable to match DuckLake's literal schema, even ids
// it always populates in practice.
const std::vector<MetadataTableSpec> &MetadataTableSpecsImpl() {
	static const std::vector<MetadataTableSpec> specs = {
	    {
	        "ducklake_snapshot",
	        {
	            {"snapshot_id", "BIGINT", true},
	            {"snapshot_time", "TIMESTAMPTZ", false},
	            {"schema_version", "BIGINT", false},
	            {"next_catalog_id", "BIGINT", false},
	            {"next_file_id", "BIGINT", false},
	        },
	        ProvideSnapshots,
	        0,
	        {},
	        0,
	        /* delete key: snapshot_id */ {0},
	    },
	    {
	        "ducklake_snapshot_changes",
	        {
	            {"snapshot_id", "BIGINT", true},
	            {"changes_made", "VARCHAR", false},
	            {"author", "VARCHAR", false},
	            {"commit_message", "VARCHAR", false},
	            {"commit_extra_info", "VARCHAR", false},
	        },
	        ProvideSnapshotChanges,
	        1,
	        {},
	        0,
	        /* delete key: snapshot_id */ {0},
	    },
	    {
	        "ducklake_schema",
	        {
	            {"schema_id", "BIGINT", true},
	            {"schema_uuid", "UUID", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"schema_name", "VARCHAR", false},
	            {"path", "VARCHAR", false},
	            {"path_is_relative", "BOOLEAN", false},
	        },
	        ProvideSchemas,
	        2,
	        /* end key: schema_id */ {0},
	        /* end_snapshot col */ 3,
	        /* delete key: schema_id, end_snapshot */ {0, 3},
	    },
	    {
	        "ducklake_table",
	        {
	            {"table_id", "BIGINT", false},
	            {"table_uuid", "UUID", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"schema_id", "BIGINT", false},
	            {"table_name", "VARCHAR", false},
	            {"path", "VARCHAR", false},
	            {"path_is_relative", "BOOLEAN", false},
	        },
	        ProvideTables,
	        3,
	        /* end key: table_id */ {0},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, end_snapshot */ {0, 3},
	    },
	    {
	        "ducklake_view",
	        {
	            {"view_id", "BIGINT", false},
	            {"view_uuid", "UUID", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"schema_id", "BIGINT", false},
	            {"view_name", "VARCHAR", false},
	            {"dialect", "VARCHAR", false},
	            {"sql", "VARCHAR", false},
	            {"column_aliases", "VARCHAR", false},
	        },
	        ProvideViews,
	        4,
	        /* end key: view_id */ {0},
	        /* end_snapshot col */ 3,
	        /* delete key: view_id, end_snapshot */ {0, 3},
	    },
	    {
	        "ducklake_column",
	        {
	            {"column_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"column_order", "BIGINT", false},
	            {"column_name", "VARCHAR", false},
	            {"column_type", "VARCHAR", false},
	            {"initial_default", "VARCHAR", false},
	            {"default_value", "VARCHAR", false},
	            {"nulls_allowed", "BOOLEAN", false},
	            {"parent_column", "BIGINT", false},
	            {"default_value_type", "VARCHAR", false},
	            {"default_value_dialect", "VARCHAR", false},
	        },
	        ProvideColumns,
	        5,
	        /* end key: table_id, column_id (decoder order) */ {3, 0},
	        /* end_snapshot col */ 2,
	        /* delete key: table_id, column_id, end_snapshot */ {3, 0, 2},
	    },
	    {
	        "ducklake_data_file",
	        {
	            {"data_file_id", "BIGINT", true},
	            {"table_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"file_order", "BIGINT", false},
	            {"path", "VARCHAR", false},
	            {"path_is_relative", "BOOLEAN", false},
	            {"file_format", "VARCHAR", false},
	            {"record_count", "BIGINT", false},
	            {"file_size_bytes", "BIGINT", false},
	            {"footer_size", "BIGINT", false},
	            {"row_id_start", "BIGINT", false},
	            {"partition_id", "BIGINT", false},
	            {"encryption_key", "VARCHAR", false},
	            {"mapping_id", "BIGINT", false},
	            {"partial_max", "BIGINT", false},
	        },
	        ProvideDataFiles,
	        6,
	        /* end key: table_id, data_file_id (decoder order) */ {1, 0},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, data_file_id, end_snapshot */ {1, 0, 3},
	        /* overlay_updatable */ false,
	        /* scope_column */ -1,
	        /* live_narrowable */ true,
	    },
	    {
	        "ducklake_delete_file",
	        {
	            {"delete_file_id", "BIGINT", true},
	            {"table_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"data_file_id", "BIGINT", false},
	            {"path", "VARCHAR", false},
	            {"path_is_relative", "BOOLEAN", false},
	            {"format", "VARCHAR", false},
	            {"delete_count", "BIGINT", false},
	            {"file_size_bytes", "BIGINT", false},
	            {"footer_size", "BIGINT", false},
	            {"encryption_key", "VARCHAR", false},
	            {"partial_max", "BIGINT", false},
	        },
	        ProvideDeleteFiles,
	        7,
	        /* end key: table_id, delete_file_id (decoder order) */ {1, 0},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, delete_file_id, end_snapshot */ {1, 0, 3},
	    },
	    {
	        "ducklake_table_stats",
	        {
	            {"table_id", "BIGINT", false},
	            {"record_count", "BIGINT", false},
	            {"next_row_id", "BIGINT", false},
	            {"file_size_bytes", "BIGINT", false},
	        },
	        ProvideTableStats,
	        8,
	        {},
	        0,
	        /* delete key: table_id */ {0},
	        /* overlay updates */ true,
	    },
	    {
	        "ducklake_table_column_stats",
	        {
	            {"table_id", "BIGINT", false},
	            {"column_id", "BIGINT", false},
	            {"contains_null", "BOOLEAN", false},
	            {"contains_nan", "BOOLEAN", false},
	            {"min_value", "VARCHAR", false},
	            {"max_value", "VARCHAR", false},
	            {"extra_stats", "VARCHAR", false},
	        },
	        ProvideTableColumnStats,
	        9,
	        {},
	        0,
	        /* delete key: table_id, column_id */ {0, 1},
	        /* overlay updates */ true,
	    },
	    {
	        "ducklake_file_column_stats",
	        {
	            {"data_file_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"column_id", "BIGINT", false},
	            {"column_size_bytes", "BIGINT", false},
	            {"value_count", "BIGINT", false},
	            {"null_count", "BIGINT", false},
	            {"min_value", "VARCHAR", false},
	            {"max_value", "VARCHAR", false},
	            {"contains_nan", "BOOLEAN", false},
	            {"extra_stats", "VARCHAR", false},
	        },
	        ProvideFileColumnStats,
	        10,
	        {},
	        0,
	        /* delete key: data_file_id, table_id, column_id (decoder order) */ {0, 1, 2},
	        /* overlay updates */ true,
	        /* scope column: table_id */ 1,
	    },
	    {
	        // Three-column form: (begin_snapshot, schema_version, table_id).
	        "ducklake_schema_versions",
	        {
	            {"begin_snapshot", "BIGINT", false},
	            {"schema_version", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	        },
	        ProvideSchemaVersions,
	        11,
	        {},
	        0,
	        /* delete key: begin_snapshot, schema_version, table_id */ {0, 1, 2},
	    },
	    {
	        "ducklake_tag",
	        {
	            {"object_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"key", "VARCHAR", false},
	            {"value", "VARCHAR", false},
	        },
	        ProvideTags,
	        17,
	        /* end key: object_id, key (decoder order) */ {0, 3},
	        /* end_snapshot col */ 2,
	        /* delete key: object_id, key, begin_snapshot */ {0, 3, 1},
	    },
	    {
	        "ducklake_column_tag",
	        {
	            {"table_id", "BIGINT", false},
	            {"column_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	            {"key", "VARCHAR", false},
	            {"value", "VARCHAR", false},
	        },
	        ProvideColumnTags,
	        18,
	        /* end key: table_id, column_id, key (decoder order) */ {0, 1, 4},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, column_id, key, begin_snapshot */ {0, 1, 4, 2},
	    },
	    // Always-empty stand-ins (see `ProvideEmpty`): no dump ABI call backs
	    // them — the store models none of these kinds.
	    {
	        "ducklake_inlined_data_tables",
	        {
	            {"table_id", "BIGINT", false},
	            {"table_name", "VARCHAR", false},
	            {"schema_version", "BIGINT", false},
	        },
	        ProvideInlinedDataTables,
	        kVoidInsertable,
	    },
	    {
	        "ducklake_macro",
	        {
	            {"schema_id", "BIGINT", false},
	            {"macro_id", "BIGINT", false},
	            {"macro_name", "VARCHAR", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	        },
	        ProvideMacros,
	        20,
	        /* end key: macro_id */ {1},
	        /* end_snapshot col */ 4,
	        /* delete key: macro_id, end_snapshot */ {1, 4},
	    },
	    {
	        "ducklake_macro_impl",
	        {
	            {"macro_id", "BIGINT", false},
	            {"impl_id", "BIGINT", false},
	            {"dialect", "VARCHAR", false},
	            {"sql", "VARCHAR", false},
	            {"type", "VARCHAR", false},
	        },
	        ProvideMacroImpls,
	        21,
	        {},
	        0,
	        /* delete key: macro_id */ {0},
	    },
	    {
	        "ducklake_macro_parameters",
	        {
	            {"macro_id", "BIGINT", false},
	            {"impl_id", "BIGINT", false},
	            {"column_id", "BIGINT", false},
	            {"parameter_name", "VARCHAR", false},
	            {"parameter_type", "VARCHAR", false},
	            {"default_value", "VARCHAR", false},
	            {"default_value_type", "VARCHAR", false},
	        },
	        ProvideMacroParameters,
	        22,
	        {},
	        0,
	        /* delete key: macro_id */ {0},
	    },
	    {
	        "ducklake_partition_info",
	        {
	            {"partition_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	        },
	        ProvidePartitionInfo,
	        12,
	        /* end key: table_id, partition_id (decoder order) */ {1, 0},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, partition_id, end_snapshot */ {1, 0, 3},
	    },
	    {
	        "ducklake_partition_column",
	        {
	            {"partition_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"partition_key_index", "BIGINT", false},
	            {"column_id", "BIGINT", false},
	            {"transform", "VARCHAR", false},
	        },
	        ProvidePartitionColumns,
	        13,
	        {},
	        0,
	        /* delete key: partition_id, table_id */ {0, 1},
	    },
	    {
	        "ducklake_file_partition_value",
	        {
	            {"data_file_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"partition_key_index", "BIGINT", false},
	            {"partition_value", "VARCHAR", false},
	        },
	        ProvideFilePartitionValues,
	        14,
	        {},
	        0,
	        /* delete key: data_file_id, table_id */ {0, 1},
	    },
	    {
	        "ducklake_file_variant_stats",
	        {
	            {"data_file_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"column_id", "BIGINT", false},
	            {"variant_path", "VARCHAR", false},
	            {"shredded_type", "VARCHAR", false},
	            {"column_size_bytes", "BIGINT", false},
	            {"value_count", "BIGINT", false},
	            {"null_count", "BIGINT", false},
	            {"min_value", "VARCHAR", false},
	            {"max_value", "VARCHAR", false},
	            {"contains_nan", "BOOLEAN", false},
	            {"extra_stats", "VARCHAR", false},
	        },
	        ProvideEmpty,
	    },
	    {
	        "ducklake_files_scheduled_for_deletion",
	        {
	            {"data_file_id", "BIGINT", false},
	            {"path", "VARCHAR", false},
	            {"path_is_relative", "BOOLEAN", false},
	            {"schedule_start", "TIMESTAMPTZ", false},
	        },
	        ProvideScheduledDeletions,
	        19,
	        {},
	        0,
	        /* delete key: data_file_id */ {0},
	    },
	    {
	        "ducklake_column_mapping",
	        {
	            {"mapping_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"type", "VARCHAR", false},
	        },
	        ProvideColumnMappings,
	        23,
	        {},
	        0,
	        /* delete key: mapping_id, table_id */ {0, 1},
	    },
	    {
	        "ducklake_name_mapping",
	        {
	            {"mapping_id", "BIGINT", false},
	            {"column_id", "BIGINT", false},
	            {"source_name", "VARCHAR", false},
	            {"target_field_id", "BIGINT", false},
	            {"parent_column", "BIGINT", false},
	            {"is_partition", "BOOLEAN", false},
	        },
	        ProvideNameMappings,
	        24,
	        {},
	        0,
	        /* delete key: mapping_id */ {0},
	    },
	    {
	        "ducklake_sort_info",
	        {
	            {"sort_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"begin_snapshot", "BIGINT", false},
	            {"end_snapshot", "BIGINT", false},
	        },
	        ProvideSortInfo,
	        15,
	        /* end key: table_id, sort_id (decoder order) */ {1, 0},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, sort_id, end_snapshot */ {1, 0, 3},
	    },
	    {
	        "ducklake_sort_expression",
	        {
	            {"sort_id", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	            {"sort_key_index", "BIGINT", false},
	            {"expression", "VARCHAR", false},
	            {"dialect", "VARCHAR", false},
	            {"sort_direction", "VARCHAR", false},
	            {"null_order", "VARCHAR", false},
	        },
	        ProvideSortExpressions,
	        16,
	        {},
	        0,
	        /* delete key: sort_id, table_id */ {0, 1},
	    },
	    {
	        "ducklake_metadata",
	        {
	            {"key", "VARCHAR", true},
	            {"value", "VARCHAR", true},
	            {"scope", "VARCHAR", false},
	            {"scope_id", "BIGINT", false},
	        },
	        ProvideMetadata,
	        // Options are unversioned and outside the snapshot protocol.
	        // `set_option` counts the rows already holding the key at that
	        // scope and writes an INSERT only when there are none, so every
	        // later set arrives as `SET value` on the matched row — the
	        // overlay below. A key moraine serves a default for takes that
	        // branch on the first set, since a synthesized row counts.
	        25,
	        {},
	        0,
	        /* delete key: key, scope, scope_id */ {0, 2, 3},
	        /* overlay_updatable */ true,
	    },
	};
	return specs;
}

duckdb::BindInfo MetadataScanBindInfo(const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
	auto &data = bind_data->Cast<MetadataScanBindData>();
	duckdb::BindInfo info(duckdb::ScanType::TABLE);
	info.table = data.table_entry;
	return info;
}

// The catalog a deferred scan reads through, proven still attached.
//
// Bind data outlives its bind: `PREPARE`, `DETACH`, then `EXECUTE` reaches
// here with `bind_data.catalog` pointing at a freed catalog. The pointer is
// therefore only ever compared, never followed — the reference returned is
// the one the database manager resolves now.
duckdb::Catalog &LiveBoundCatalog(duckdb::ClientContext &context, const MetadataScanBindData &bind_data) {
	auto database = duckdb::DatabaseManager::Get(context).GetDatabase(context, bind_data.catalog_name);
	if (database && &database->GetCatalog() == bind_data.catalog) {
		return *bind_data.catalog;
	}
	throw duckdb::CatalogException("moraine: database \"%s\" was detached after this statement was bound",
	                               bind_data.catalog_name);
}

struct MetadataScanGlobalState : public duckdb::GlobalTableFunctionState {
	duckdb::idx_t offset = 0;
	// The columns DuckDB asked for, by index into a materialized row, in
	// output order. Empty for a zero-column "virtual column" probe (e.g.
	// `SELECT NULL FROM ducklake_metadata LIMIT 1`), which DuckDB emits only
	// when the table function advertises `projection_pushdown = true`.
	std::vector<duckdb::column_t> column_ids;
	// Set only for a scan whose bind deferred materialization, holding what
	// this scan built.
	std::shared_ptr<const MetadataRows> rows;
	// The row id this scan's first row carries, taken from the transaction's
	// run registry. Zero for a scan emitting no row ids, and for the inline
	// tables, whose own Sinks index a re-materialization directly.
	uint64_t row_id_base = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

bool EmitsRowIds(const std::vector<duckdb::column_t> &column_ids) {
	return std::find(column_ids.begin(), column_ids.end(), duckdb::COLUMN_IDENTIFIER_ROW_ID) != column_ids.end();
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> MetadataScanInitGlobal(duckdb::ClientContext &context,
                                                                            duckdb::TableFunctionInitInput &input) {
	auto state = duckdb::make_uniq<MetadataScanGlobalState>();
	state->column_ids = input.column_ids;

	auto &bind_data = input.bind_data->Cast<MetadataScanBindData>();
	if (bind_data.spec == nullptr || bind_data.catalog == nullptr) {
		return state;
	}
	auto &catalog = LiveBoundCatalog(context, bind_data);
	if (bind_data.rows == nullptr) {
		if (bind_data.scope.IsValid()) {
			state->rows = ScopedMetadataRowsFor(context, catalog, *bind_data.spec, bind_data.scope.GetIndex());
		} else {
			// A scan emitting row ids has its rows resolved back from them by
			// the staged-write Sink, so it reads the same materialization
			// every other writer of this table does — never a narrowed one.
			auto live_bound = EmitsRowIds(state->column_ids) ? duckdb::optional_idx() : bind_data.live_bound;
			state->rows = MetadataRowsFor(context, catalog, *bind_data.spec, live_bound);
		}
	}

	// Only a scan feeding an UPDATE or a DELETE asks for row ids, and only
	// its rows are ever resolved back from one, so only it is registered.
	if (EmitsRowIds(state->column_ids)) {
		auto catalog_transaction = catalog.GetCatalogTransaction(context);
		auto &transaction = catalog_transaction.transaction->Cast<MoraineTransaction>();
		state->row_id_base = transaction.RegisterScannedRows(
		    *bind_data.spec, state->rows != nullptr ? state->rows : bind_data.rows);
	}
	return state;
}

// Where `column` sits in this scan's projection, if it is projected at
// all. A filter's column reference binds to the projection, not to the
// table's own column order.
duckdb::optional_idx ProjectedColumn(const duckdb::LogicalGet &get, duckdb::idx_t column) {
	auto &column_ids = get.GetColumnIds();
	for (duckdb::idx_t i = 0; i < column_ids.size(); i++) {
		if (!column_ids[i].IsVirtualColumn() && column_ids[i].GetPrimaryIndex() == column) {
			return i;
		}
	}
	return duckdb::optional_idx();
}

// Whether `expression` is a reference to the scan's `projected` column.
bool IsColumnRef(const duckdb::Expression &expression, const duckdb::LogicalGet &get, duckdb::idx_t projected) {
	if (expression.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &column = expression.Cast<duckdb::BoundColumnRefExpression>();
	return column.binding.table_index == get.table_index && column.binding.column_index == projected;
}

// The non-negative BIGINT constant `expression` holds, if it is one.
duckdb::optional_idx ConstantSnapshot(const duckdb::Expression &expression) {
	if (expression.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
		return duckdb::optional_idx();
	}
	auto &value = expression.Cast<duckdb::BoundConstantExpression>().value;
	if (value.IsNull() || value.type().id() != duckdb::LogicalTypeId::BIGINT) {
		return duckdb::optional_idx();
	}
	// Read signed and refused if negative: no snapshot carries a negative
	// id, and `optional_idx` throws on the sentinel an unsigned read of -1
	// would produce.
	auto snapshot = value.GetValue<int64_t>();
	if (snapshot < 0) {
		return duckdb::optional_idx();
	}
	return static_cast<duckdb::idx_t>(snapshot);
}

// The snapshot `S` of a `S < end_snapshot OR end_snapshot IS NULL` filter
// over this scan's `end_snapshot` column — the shape every DuckLake read of
// a versioned table carries, alongside its `S >= begin_snapshot` half.
//
// Only rows the disjunction keeps matter, so recognizing it is what proves
// the ended half unreachable: an ended version's `end_snapshot` is the
// snapshot that ended it, never past the head, and is never null. Both arms
// therefore reject every ended version once `S` has reached the head — which
// the core, not this function, is what checks.
duckdb::optional_idx LiveBoundOf(const duckdb::Expression &filter, const duckdb::LogicalGet &get,
                                 duckdb::idx_t projected) {
	if (filter.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONJUNCTION) {
		return duckdb::optional_idx();
	}
	auto &disjunction = filter.Cast<duckdb::BoundConjunctionExpression>();
	if (disjunction.GetExpressionType() != duckdb::ExpressionType::CONJUNCTION_OR ||
	    disjunction.children.size() != 2) {
		return duckdb::optional_idx();
	}

	duckdb::optional_idx bound;
	bool saw_is_null = false;
	for (auto &child : disjunction.children) {
		if (child->GetExpressionType() == duckdb::ExpressionType::OPERATOR_IS_NULL) {
			auto &is_null = child->Cast<duckdb::BoundOperatorExpression>();
			if (is_null.children.size() == 1 && IsColumnRef(*is_null.children[0], get, projected)) {
				saw_is_null = true;
			}
			continue;
		}
		auto type = child->GetExpressionType();
		if (type != duckdb::ExpressionType::COMPARE_LESSTHAN &&
		    type != duckdb::ExpressionType::COMPARE_GREATERTHAN) {
			continue;
		}
		auto &comparison = child->Cast<duckdb::BoundComparisonExpression>();
		// `S < end_snapshot` either way round; anything else, including the
		// inclusive forms, is left alone.
		auto &constant = type == duckdb::ExpressionType::COMPARE_LESSTHAN ? comparison.left : comparison.right;
		auto &column = type == duckdb::ExpressionType::COMPARE_LESSTHAN ? comparison.right : comparison.left;
		if (!IsColumnRef(*column, get, projected)) {
			continue;
		}
		if (auto snapshot = ConstantSnapshot(*constant); snapshot.IsValid()) {
			bound = snapshot;
		}
	}

	return saw_is_null ? bound : duckdb::optional_idx();
}

// Records the snapshot a lifecycle filter keeps this scan's rows against,
// so `InitGlobal` can leave the ended half unread.
//
// **Consumes nothing**, as with the scope pushdown below: the filter stays
// in `filters` and DuckLake keeps applying it, so a bound recognized too
// generously costs a narrower materialization, never a wrong row — the core
// still refuses to narrow a bound that has fallen behind its read point.
void MetadataScanPushdownLiveBound(duckdb::LogicalGet &get, MetadataScanBindData &bind_data,
                                   const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &filters) {
	// `end_snapshot_column` only names a column on a kind that declares an
	// end key, so a kind marked narrowable without one narrows nothing
	// rather than reading column zero as a lifecycle.
	if (!bind_data.spec->live_narrowable || bind_data.spec->end_key_columns.empty() ||
	    bind_data.live_bound.IsValid()) {
		return;
	}
	auto projected = ProjectedColumn(get, bind_data.spec->end_snapshot_column);
	if (!projected.IsValid()) {
		return;
	}
	for (auto &filter : filters) {
		if (auto bound = LiveBoundOf(*filter, get, projected.GetIndex()); bound.IsValid()) {
			bind_data.live_bound = bound;
			return;
		}
	}
}

// Records the `table_id` an equality filter pins this scan to, so
// `InitGlobal` can materialize just that table's run.
//
// **Consumes nothing.** Every filter stays in `filters`, so DuckDB keeps
// applying all of them and this scan never owes a predicate it did not
// implement — a shape not matched here costs a wider materialization, never
// a wrong row. That is also why `filter_pushdown` stays off: setting it
// would delete the filter operator and make the scan responsible for the
// whole of `TableFilterType`.
void MetadataScanPushdownComplexFilter(duckdb::ClientContext &, duckdb::LogicalGet &get,
                                       duckdb::FunctionData *bind_data_p,
                                       duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &filters) {
	if (bind_data_p == nullptr) {
		return;
	}
	auto &bind_data = bind_data_p->Cast<MetadataScanBindData>();
	if (bind_data.spec == nullptr) {
		return;
	}
	MetadataScanPushdownLiveBound(get, bind_data, filters);
	if (bind_data.spec->scope_column < 0 || bind_data.scope.IsValid()) {
		return;
	}

	auto projected = ProjectedColumn(get, static_cast<duckdb::idx_t>(bind_data.spec->scope_column));
	if (!projected.IsValid()) {
		return;
	}

	for (auto &filter : filters) {
		if (filter->GetExpressionType() != duckdb::ExpressionType::COMPARE_EQUAL) {
			continue;
		}
		auto &comparison = filter->Cast<duckdb::BoundComparisonExpression>();
		// Either side may hold the column reference.
		duckdb::Expression *orderings[2][2] = {
		    {comparison.left.get(), comparison.right.get()},
		    {comparison.right.get(), comparison.left.get()},
		};
		for (auto &ordering : orderings) {
			auto *reference = ordering[0];
			auto *constant = ordering[1];
			if (reference->GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF ||
			    constant->GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
				continue;
			}
			auto &column = reference->Cast<duckdb::BoundColumnRefExpression>();
			if (column.binding.table_index != get.table_index ||
			    column.binding.column_index != projected.GetIndex()) {
				continue;
			}
			auto &value = constant->Cast<duckdb::BoundConstantExpression>().value;
			if (value.IsNull() || value.type().id() != duckdb::LogicalTypeId::BIGINT) {
				continue;
			}
			// Read signed and refused if negative: no table carries a
			// negative id, and `optional_idx` throws on the sentinel an
			// unsigned read of -1 would produce.
			auto table_id = value.GetValue<int64_t>();
			if (table_id < 0) {
				continue;
			}
			bind_data.scope = static_cast<duckdb::idx_t>(table_id);
			return;
		}
	}
}

void MetadataScanFunctionImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<MetadataScanBindData>();
	auto &state = data.global_state->Cast<MetadataScanGlobalState>();
	// A scan that deferred materialization holds its rows on the state;
	// every other one holds them on the bind data.
	auto &materialized = state.rows != nullptr ? state.rows : bind_data.rows;
	if (materialized == nullptr) {
		throw duckdb::InternalException("moraine: metadata scan bound without a materialized row set");
	}
	auto &rows = *materialized;
	if (state.offset >= rows.size()) {
		output.SetCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, rows.size() - state.offset);
	for (duckdb::idx_t out_row = 0; out_row < count; out_row++) {
		auto &row = rows[state.offset + out_row];
		for (duckdb::idx_t out_col = 0; out_col < state.column_ids.size(); out_col++) {
			auto col_id = state.column_ids[out_col];
			if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
				// An id in the transaction's run registry, which the
				// staged-write Sink (staged_write.cpp) resolves back to this
				// very row — never to the same position in a second
				// materialization, which a narrowed scan and a commit
				// landing under it can each make a different list.
				output.SetValue(
				    out_col, out_row,
				    duckdb::Value::BIGINT(static_cast<int64_t>(state.row_id_base + state.offset + out_row)));
				continue;
			}
			if (duckdb::IsVirtualColumn(col_id) || col_id >= row.size()) {
				// Any other virtual column has no synthesized value, and an
				// out-of-range id would be a DuckDB/shim mismatch. Serve an
				// untyped NULL rather than read out of bounds:
				// `Vector::SetValue` accepts a null `Value` of any type.
				output.SetValue(out_col, out_row, duckdb::Value());
				continue;
			}
			output.SetValue(out_col, out_row, row[col_id]);
		}
	}
	state.offset += count;
	output.SetCardinality(count);
}

} // namespace

duckdb::unique_ptr<duckdb::FunctionData> MetadataScanBindData::Copy() const {
	auto result = duckdb::make_uniq<MetadataScanBindData>();
	result->rows = rows;
	result->spec = spec;
	result->catalog = catalog;
	result->catalog_name = catalog_name;
	result->scope = scope;
	result->live_bound = live_bound;
	result->table_entry = table_entry;
	return result;
}

bool MetadataScanBindData::Equals(const duckdb::FunctionData &other_p) const {
	auto &other = other_p.Cast<MetadataScanBindData>();
	// `spec` and `scope` carry the identity `rows` does elsewhere: a
	// deferred scan has no row set at bind to compare, and two scans of one
	// kind narrowed to different tables are not interchangeable.
	auto same_scope = scope.IsValid() == other.scope.IsValid() &&
	                  (!scope.IsValid() || scope.GetIndex() == other.scope.GetIndex());
	auto same_live_bound = live_bound.IsValid() == other.live_bound.IsValid() &&
	                       (!live_bound.IsValid() || live_bound.GetIndex() == other.live_bound.GetIndex());
	return rows == other.rows && spec == other.spec && same_scope && same_live_bound &&
	       table_entry.get() == other.table_entry.get();
}

duckdb::TableFunction MetadataScanTableFunction() {
	// No `bind` callback (as in `MoraineScanFunction`, scan.cpp): the caller
	// already produces complete bind data itself.
	duckdb::TableFunction function("moraine_metadata_scan", {}, MetadataScanFunctionImpl, nullptr,
	                               MetadataScanInitGlobal, nullptr);
	// Required for the zero-real-column "virtual column" scan shape the
	// exists-probe query uses (see `MetadataScanGlobalState::column_ids`);
	// real projection pushdown falls out of the same mechanism.
	function.projection_pushdown = true;
	// Reads the equality a narrowable kind scopes its materialization by,
	// without consuming it (`MetadataScanPushdownComplexFilter`).
	// `filter_pushdown` deliberately stays off: it would delete the filter
	// operator and make this scan answerable for every `TableFilterType`.
	function.pushdown_complex_filter = MetadataScanPushdownComplexFilter;
	// Resolves `LogicalGet::GetTable()` so UPDATE/DELETE statements bind
	// against these tables.
	function.get_bind_info = MetadataScanBindInfo;
	return function;
}

const std::vector<MetadataTableSpec> &MoraineMetadataTableSpecs() {
	return MetadataTableSpecsImpl();
}

MoraineMetadataTableEntry::MoraineMetadataTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
                                                     duckdb::CreateTableInfo &info, const MetadataTableSpec &spec,
                                                     MoraineCatalogHandle *handle)
    : duckdb::TableCatalogEntry(catalog, schema, info), spec_(spec), handle_(handle) {
}

duckdb::unique_ptr<duckdb::BaseStatistics> MoraineMetadataTableEntry::GetStatistics(duckdb::ClientContext &,
                                                                                    duckdb::column_t) {
	throw duckdb::NotImplementedException("moraine: column statistics are not supported yet");
}

namespace {

// The shared body of every tx-aware provider: run one `moraine_tx_dump_*`
// and shape its rows exactly as the committed dump shapes its own. The
// mirror of `DumpRows`, against an open transaction rather than the
// catalog handle.
template <typename Row, typename DumpFn, typename ShapeFn>
std::vector<std::vector<duckdb::Value>> TxDumpRows(MoraineTxHandle *tx, DumpFn dump, void (*free_fn)(Row *, size_t),
                                                   ShapeFn shape) {
	OwnedArray<Row> rows(free_fn);
	MoraineError err {};
	auto code = dump(tx, rows.OutItems(), rows.OutLen(), &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back(shape(r));
	}
	return result;
}

// As `TxDumpRows`, for a dump that takes the snapshot a reader keeps rows
// against.
template <typename Row, typename DumpFn, typename ShapeFn>
std::vector<std::vector<duckdb::Value>> TxDumpRowsLiveAt(MoraineTxHandle *tx, uint64_t live_bound, DumpFn dump,
                                                         void (*free_fn)(Row *, size_t), ShapeFn shape) {
	OwnedArray<Row> rows(free_fn);
	MoraineError err {};
	if (dump(tx, live_bound, rows.OutItems(), rows.OutLen(), &err) != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back(shape(r));
	}
	return result;
}

// The transaction-aware rows for one `write_table_kind`, or an empty
// optional where no such dump exists yet.
//
// A scan inside a write transaction that already staged rows must observe
// them: DuckLake's expiry and cleanup cascades re-read these tables after
// staging their own deletes, so a committed-state scan would make them
// re-plan work they have already done. Kinds absent from this switch — and
// every scan outside a write transaction — serve committed state, which is
// right for a kind the transaction has not written and is exactly the gap
// this table closes for one it has.
//
// The cases are `write_table_kind` values — the same integers the spec
// table above declares, which are the staged ABI's `table_kind`
// discriminants. Ordered by DuckLake's own cascade: the file tables first,
// then the catalog tables they hang off.
std::optional<std::vector<std::vector<duckdb::Value>>> TxAwareRows(MoraineTxHandle *tx, MoraineCatalogHandle *handle,
                                                                   duckdb::ClientContext &context,
                                                                   int32_t write_table_kind,
                                                                   duckdb::optional_idx live_bound) {
	if (write_table_kind == 6 && live_bound.IsValid()) {
		return TxDumpRowsLiveAt<MoraineDataFileRow>(tx, static_cast<uint64_t>(live_bound.GetIndex()),
		                                            moraine_tx_dump_data_files_live_at, moraine_dump_data_files_free,
		                                            DataFileShape);
	}
	switch (write_table_kind) {
	case 0:
		return TxDumpRows<MoraineSnapshotRow>(tx, moraine_tx_dump_snapshots, moraine_dump_snapshots_free,
		                                      SnapshotShape);
	case 1:
		return TxDumpRows<MoraineSnapshotRow>(tx, moraine_tx_dump_snapshots, moraine_dump_snapshots_free,
		                                      SnapshotChangesShape);
	case 6:
		return TxDumpRows<MoraineDataFileRow>(tx, moraine_tx_dump_data_files, moraine_dump_data_files_free,
		                                      DataFileShape);
	case 7:
		return TxDumpRows<MoraineDeleteFileRow>(tx, moraine_tx_dump_delete_files, moraine_dump_delete_files_free,
		                                        DeleteFileShape);
	case 10:
		return TxDumpRows<MoraineFileColumnStatsRow>(tx, moraine_tx_dump_file_column_stats,
		                                             moraine_dump_file_column_stats_free, FileColumnStatsShape);
	case 19:
		return TxDumpRows<MoraineScheduledDeletionRow>(tx, moraine_tx_dump_scheduled_deletions,
		                                               moraine_dump_scheduled_deletions_free, ScheduledDeletionShape);
	case 5:
		return TxDumpRows<MoraineColumnRow>(tx, moraine_tx_dump_columns, moraine_dump_columns_free, ColumnShape);
	case 3:
		return TxDumpRows<MoraineTableRow>(tx, moraine_tx_dump_tables, moraine_dump_tables_free, TableShape);
	case 2:
		return TxDumpRows<MoraineSchemaRow>(tx, moraine_tx_dump_schemas, moraine_dump_schemas_free, SchemaShape);
	case 4:
		return TxDumpRows<MoraineViewRow>(tx, moraine_tx_dump_views, moraine_dump_views_free, ViewShape);
	case 8:
		return TxDumpRows<MoraineTableStatsRow>(tx, moraine_tx_dump_table_stats, moraine_dump_table_stats_free,
		                                        TableStatsShape);
	case 9:
		return TxDumpRows<MoraineTableColumnStatsRow>(tx, moraine_tx_dump_table_column_stats,
		                                              moraine_dump_table_column_stats_free, TableColumnStatsShape);
	case 11:
		return TxDumpRows<MoraineSchemaVersionRow>(tx, moraine_tx_dump_schema_versions,
		                                           moraine_dump_schema_versions_free, SchemaVersionShape);
	case 12:
		return TxDumpRows<MorainePartitionInfoRow>(tx, moraine_tx_dump_partition_info, moraine_dump_partition_info_free,
		                                           PartitionInfoShape);
	case 13:
		return TxDumpRows<MorainePartitionColumnRow>(tx, moraine_tx_dump_partition_columns,
		                                             moraine_dump_partition_columns_free, PartitionColumnShape);
	case 14:
		return TxDumpRows<MoraineFilePartitionValueRow>(tx, moraine_tx_dump_file_partition_values,
		                                                moraine_dump_file_partition_values_free,
		                                                FilePartitionValueShape);
	case 15:
		return TxDumpRows<MoraineSortInfoRow>(tx, moraine_tx_dump_sort_info, moraine_dump_sort_info_free,
		                                      SortInfoShape);
	case 16:
		return TxDumpRows<MoraineSortExpressionRow>(tx, moraine_tx_dump_sort_expressions,
		                                            moraine_dump_sort_expressions_free, SortExpressionShape);
	case 17:
		return TxDumpRows<MoraineTagRow>(tx, moraine_tx_dump_tags, moraine_dump_tags_free, TagShape);
	case 18:
		return TxDumpRows<MoraineColumnTagRow>(tx, moraine_tx_dump_column_tags, moraine_dump_column_tags_free,
		                                       ColumnTagShape);
	case 20:
		return TxDumpRows<MoraineMacroRow>(tx, moraine_tx_dump_macros, moraine_dump_macros_free, MacroShape);
	case 21:
		return TxDumpRows<MoraineMacroImplRow>(tx, moraine_tx_dump_macro_impls, moraine_dump_macro_impls_free,
		                                       MacroImplShape);
	case 22:
		return TxDumpRows<MoraineMacroParameterRow>(tx, moraine_tx_dump_macro_parameters,
		                                            moraine_dump_macro_parameters_free, MacroParameterShape);
	case 23:
		return TxDumpRows<MoraineColumnMappingRow>(tx, moraine_tx_dump_column_mappings,
		                                           moraine_dump_column_mappings_free, ColumnMappingShape);
	case 24:
		return TxDumpRows<MoraineNameMappingRow>(tx, moraine_tx_dump_name_mappings, moraine_dump_name_mappings_free,
		                                         NameMappingShape);
	case 25: {
		// The fixed rows are store facts, not stored options, so they are
		// the same either way; only the stored half is overlaid.
		auto rows = FixedMetadataRows(handle, moraine_shim_is_interrupted, &context);
		MergeStoredMetadataRows(
		    rows, TxDumpRows<MoraineOptionRow>(tx, moraine_tx_dump_options, moraine_dump_options_free, OptionShape));
		return rows;
	}
	default:
		return std::nullopt;
	}
}

} // namespace

std::shared_ptr<const MetadataRows> MetadataRowsFor(duckdb::ClientContext &context, duckdb::Catalog &catalog,
                                                    const MetadataTableSpec &spec,
                                                    duckdb::optional_idx live_bound) {
	auto catalog_transaction = catalog.GetCatalogTransaction(context);
	auto &transaction = catalog_transaction.transaction->Cast<MoraineTransaction>();
	auto *handle = catalog.Cast<MoraineCatalog>().Handle();
	auto *staged_tx = transaction.StagedTxIfOpen();

	if (staged_tx != nullptr) {
		// A writing transaction reads through its staged tx, which pins one
		// read point for its whole life and overlays the rows staged so far.
		// That is the pinning this function otherwise supplies.
		//
		// The read point cannot move under a materialization, so what makes
		// one stale is this transaction staging into the same table —
		// `StagedTxFor` drops exactly that entry as it hands out the tx.
		// A commit attempt takes the staged tx, so the retry DuckLake drives
		// between attempts opens a fresh one and re-reads through it.
		// The staged tx pins one read point for its whole life, so a
		// narrowed and an unnarrowed read of one table cannot stand at two
		// heads — which is what confines the narrowing to this branch.
		const bool live_only = live_bound.IsValid() && spec.live_narrowable;
		if (auto cached = transaction.GetMetadataRows(spec, live_only)) {
			return cached;
		}
		std::shared_ptr<const MetadataRows> rows;
		if (spec.write_table_kind != kNotWritable) {
			if (auto staged = TxAwareRows(staged_tx, handle, context, spec.write_table_kind,
			                              live_only ? live_bound : duckdb::optional_idx())) {
				rows = std::make_shared<const MetadataRows>(std::move(*staged));
			}
		}
		if (rows == nullptr) {
			rows = std::make_shared<const MetadataRows>(spec.provider(handle, moraine_shim_is_interrupted, &context));
		}
		transaction.PutMetadataRows(spec, rows, live_only);
		return rows;
	}

	if (auto cached = transaction.GetMetadataRows(spec)) {
		return cached;
	}

	// The rows this attach dumped last time are byte-identical to a fresh
	// dump whenever no batch landed since, so ask the store where it
	// stands (one point read) before paying the ABI crossing again. A
	// store with no head yet, or a stamp read that fails, simply dumps.
	auto &moraine_catalog = catalog.Cast<MoraineCatalog>();
	MoraineHeadStamp before;
	const bool stamped = ReadHeadStamp(handle, context, before);
	if (stamped) {
		if (auto held = moraine_catalog.HeldMetadataRows(spec, before.snapshot_id, before.batch_seq)) {
			transaction.PutMetadataRows(spec, held);
			return held;
		}
	}

	auto rows = std::make_shared<const MetadataRows>(spec.provider(handle, moraine_shim_is_interrupted, &context));
	transaction.PutMetadataRows(spec, rows);

	// Hold them for the next transaction only if the store did not move
	// under the dump: rows that straddled a commit stand at no single
	// stamp, and holding them under the earlier one would serve a
	// concurrent reader at that stamp a row set from beyond it.
	MoraineHeadStamp after;
	if (stamped && ReadHeadStamp(handle, context, after) && after.snapshot_id == before.snapshot_id &&
	    after.batch_seq == before.batch_seq) {
		moraine_catalog.HoldMetadataRows(spec, before.snapshot_id, before.batch_seq, rows);
	}
	return rows;
}

std::shared_ptr<const MetadataRows> ScopedMetadataRowsFor(duckdb::ClientContext &context, duckdb::Catalog &catalog,
                                                          const MetadataTableSpec &spec, uint64_t table_id) {
	auto catalog_transaction = catalog.GetCatalogTransaction(context);
	auto &transaction = catalog_transaction.transaction->Cast<MoraineTransaction>();
	// Mid-write the staged tx's overlay is what a read owes, and only the
	// unscoped dump carries it, so the scope is dropped rather than served
	// without it. `MetadataRowsFor` holds that set for the transaction.
	if (spec.scope_column < 0 || transaction.StagedTxIfOpen() != nullptr) {
		return MetadataRowsFor(context, catalog, spec);
	}
	// A transaction that already materialized this table narrows what it
	// holds. Reading the store again here would serve one scan of the table
	// rows from beyond the point every other scan of it stands at.
	if (auto held = transaction.GetMetadataRows(spec)) {
		auto scope = static_cast<duckdb::idx_t>(spec.scope_column);
		MetadataRows scoped;
		for (auto &row : *held) {
			if (scope < row.size() && !row[scope].IsNull() &&
			    row[scope].GetValue<int64_t>() == static_cast<int64_t>(table_id)) {
				scoped.push_back(row);
			}
		}
		return std::make_shared<const MetadataRows>(std::move(scoped));
	}
	auto *handle = catalog.Cast<MoraineCatalog>().Handle();
	return std::make_shared<const MetadataRows>(
	    ProvideFileColumnStatsOf(handle, table_id, moraine_shim_is_interrupted, &context));
}

duckdb::TableFunction MoraineMetadataTableEntry::GetScanFunction(duckdb::ClientContext &context,
                                                                 duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	auto scan_bind_data = duckdb::make_uniq<MetadataScanBindData>();
	scan_bind_data->spec = &spec_;
	scan_bind_data->catalog = &ParentCatalog();
	scan_bind_data->catalog_name = ParentCatalog().GetName();
	// A narrowable kind is left unmaterialized for `InitGlobal`: the filter
	// deciding how much of it to build has not been pushed down yet.
	if (spec_.scope_column < 0 && !spec_.live_narrowable) {
		scan_bind_data->rows = MetadataRowsFor(context, ParentCatalog(), spec_);
	}
	scan_bind_data->table_entry = this;
	bind_data = std::move(scan_bind_data);
	return MetadataScanTableFunction();
}

duckdb::TableStorageInfo MoraineMetadataTableEntry::GetStorageInfo(duckdb::ClientContext &) {
	return duckdb::TableStorageInfo();
}

void PopulateMetadataTables(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, MoraineCatalogHandle *handle,
                            duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CatalogEntry>> &tables) {
	for (auto &spec : MoraineMetadataTableSpecs()) {
		duckdb::CreateTableInfo info(schema, spec.name);
		duckdb::idx_t column_index = 0;
		for (auto &col : spec.columns) {
			info.columns.AddColumn(duckdb::ColumnDefinition(col.name, MapColumnType(col.ducklake_type)));
			if (col.not_null) {
				info.constraints.push_back(duckdb::make_uniq_base<duckdb::Constraint, duckdb::NotNullConstraint>(
				    duckdb::LogicalIndex(column_index)));
			}
			column_index++;
		}
		tables.emplace(spec.name, duckdb::make_uniq<MoraineMetadataTableEntry>(catalog, schema, info, spec, handle));
	}
}

} // namespace moraine_duckdb
