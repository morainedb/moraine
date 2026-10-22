#include "metadata_tables.hpp"

#include <algorithm>
#include <initializer_list>
#include <set>
#include <string>

#include "catalog.hpp"
#include "inline_tables.hpp"
#include "owned_array.hpp"

namespace moraine_duckdb {
namespace {

duckdb::Value OptVarchar(const char *s) {
	if (s == nullptr) {
		return duckdb::Value(duckdb::LogicalType::VARCHAR);
	}
	return duckdb::Value(std::string(s));
}

duckdb::Value Varchar(const char *s) {
	return OptVarchar(s);
}

// Serve both the stable and superseded development labels as catalog version 1.1.
constexpr const char *kExtendedCatalogVersion = "1.1";
constexpr const char *kSupersededExtendedCatalogVersion = "1.1-dev1";

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

// Translate stored SQL type names into DuckLake's type vocabulary, preserving
// DECIMAL width/scale and the JSON alias.
duckdb::Value DuckLakeColumnType(const char *sql_type) {
	if (sql_type == nullptr) {
		return duckdb::Value(duckdb::LogicalType::VARCHAR);
	}
	// Nested markers name the container; child columns carry its element/field types.
	if (duckdb::StringUtil::CIEquals(sql_type, "list") || duckdb::StringUtil::CIEquals(sql_type, "struct") ||
	    duckdb::StringUtil::CIEquals(sql_type, "map")) {
		return duckdb::Value(duckdb::StringUtil::Lower(sql_type));
	}
	auto type = MapColumnType(sql_type);
	// Check JSON's alias before its VARCHAR type ID.
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
	case duckdb::LogicalTypeId::VARIANT:
		return duckdb::Value("variant");
	case duckdb::LogicalTypeId::GEOMETRY:
		return duckdb::Value("geometry");
	default:
		// MapColumnType rejects types outside this switch.
		throw duckdb::InternalException("moraine: unmapped DuckLake type for \"%s\"", sql_type);
	}
}
// Convert a dump to SQL rows, releasing its ABI allocation even if conversion throws.
template <typename Row, typename ShapeFn, typename CallFn>
MetadataRows CollectRows(void (*free_fn)(Row *, size_t), ShapeFn shape, CallFn call) {
	OwnedArray<Row> rows(free_fn);
	MoraineError err {};
	if (call(rows.OutItems(), rows.OutLen(), &err) != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	MetadataRows result;
	result.reserve(rows.size());
	for (auto &r : rows) {
		result.push_back(shape(r));
	}
	return result;
}

// Committed dumps take narrowing arguments before the outputs and a cancellation probe after them.
template <typename Row, typename DumpFn, typename ShapeFn, typename... Args>
MetadataRows DumpRows(MoraineCatalogHandle *handle, MoraineInterruptProbe probe, void *probe_ctx, DumpFn dump,
                      void (*free_fn)(Row *, size_t), ShapeFn shape, Args... args) {
	return CollectRows<Row>(free_fn, shape, [&](Row **out_items, size_t *out_len, MoraineError *err) {
		return dump(handle, args..., out_items, out_len, probe, probe_ctx, err);
	});
}

// Bind committed readers without per-table wrappers or runtime captures.
template <auto Dump, auto Free, auto Shape>
MetadataRows ProvideRows(MoraineCatalogHandle *handle, MoraineInterruptProbe probe, void *probe_ctx) {
	return DumpRows(handle, probe, probe_ctx, Dump, Free, Shape);
}

template <auto Dump, auto Free, auto Shape>
MetadataRows ProvideScopedRows(MoraineCatalogHandle *handle, uint64_t table_id, MoraineInterruptProbe probe,
                               void *probe_ctx) {
	return DumpRows(handle, probe, probe_ctx, Dump, Free, Shape, table_id);
}

// Staged dumps take narrowing arguments before the outputs and no cancellation probe.
template <typename Row, typename DumpFn, typename ShapeFn, typename... Args>
MetadataRows TxDumpRows(MoraineTxHandle *tx, DumpFn dump, void (*free_fn)(Row *, size_t), ShapeFn shape, Args... args) {
	return CollectRows<Row>(free_fn, shape, [&](Row **out_items, size_t *out_len, MoraineError *err) {
		return dump(tx, args..., out_items, out_len, err);
	});
}

// ABI signatures for whole-kind, live-bounded, and table-scoped staged dumps.
template <typename Row>
using RowFreeFn = void (*)(Row *, size_t);
template <typename Row>
using RowShapeFn = std::vector<duckdb::Value> (*)(const Row &);
template <typename Row>
using TxDumpFn = int32_t (*)(MoraineTxHandle *, Row **, size_t *, MoraineError *);
template <typename Row>
using TxDumpByFn = int32_t (*)(MoraineTxHandle *, uint64_t, Row **, size_t *, MoraineError *);
template <typename Row>
using TxDumpOfFn = int32_t (*)(MoraineTxHandle *, uint64_t, uint64_t, Row **, size_t *, MoraineError *);

// Build staged readers using one row shape. Absent narrowing dumps leave empty providers.
template <typename Row>
MetadataTxProviders TxProviders(RowFreeFn<Row> free_fn, RowShapeFn<Row> shape, TxDumpFn<Row> dump,
                                TxDumpByFn<Row> dump_live_at = nullptr, TxDumpOfFn<Row> dump_of = nullptr) {
	MetadataTxProviders providers;
	providers.provider = [=](MoraineTxHandle *tx, MoraineCatalogHandle *, MoraineInterruptProbe, void *) {
		return TxDumpRows<Row>(tx, dump, free_fn, shape);
	};
	if (dump_live_at != nullptr) {
		providers.live_provider = [=](MoraineTxHandle *tx, uint64_t live_bound) {
			return TxDumpRows<Row>(tx, dump_live_at, free_fn, shape, live_bound);
		};
	}
	if (dump_of != nullptr) {
		providers.scoped_provider = [=](MoraineTxHandle *tx, uint64_t table_id, uint64_t filter_snapshot) {
			return TxDumpRows<Row>(tx, dump_of, free_fn, shape, table_id, filter_snapshot);
		};
	}
	return providers;
}

// Unversioned scoped dumps ignore the snapshot bound.
template <typename Row>
MetadataTxProviders UnversionedTxProviders(RowFreeFn<Row> free_fn, RowShapeFn<Row> shape, TxDumpFn<Row> dump,
                                           TxDumpByFn<Row> dump_of) {
	auto providers = TxProviders<Row>(free_fn, shape, dump);
	providers.scoped_provider = [=](MoraineTxHandle *tx, uint64_t table_id, uint64_t) {
		return TxDumpRows<Row>(tx, dump_of, free_fn, shape, table_id);
	};
	return providers;
}

// Snapshot and change tables project different columns from the same stored record.
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

template <bool EXTENDED>
std::vector<duckdb::Value> SchemaShape(const MoraineSchemaRow &r) {
	std::vector<duckdb::Value> values = {
	    Bigint(r.schema_id),         Uuid(r.schema_uuid),
	    Bigint(r.begin_snapshot),    OptBigint(r.has_end_snapshot, r.end_snapshot),
	    Varchar(r.schema_name),      Varchar(r.path),
	    Boolean(r.path_is_relative),
	};
	if constexpr (EXTENDED) {
		values.push_back(OptBigint(r.has_parent_schema_id, r.parent_schema_id));
	}
	return values;
}

std::vector<duckdb::Value> TableShape(const MoraineTableRow &r) {
	return {
	    Bigint(r.table_id),       Uuid(r.table_uuid),
	    Bigint(r.begin_snapshot), OptBigint(r.has_end_snapshot, r.end_snapshot),
	    Bigint(r.schema_id),      Varchar(r.table_name),
	    Varchar(r.path),          Boolean(r.path_is_relative),
	};
}

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

std::vector<duckdb::Value> MacroShape(const MoraineMacroRow &r) {
	return {
	    Bigint(r.schema_id),
	    Bigint(r.macro_id),
	    Varchar(r.macro_name),
	    Bigint(r.begin_snapshot),
	    OptBigint(r.has_end_snapshot, r.end_snapshot),
	};
}

// Preserve (macro_id, impl_id, column_id) order: DuckLake reconstructs macro
// children with LIST aggregations without ORDER BY.
std::vector<duckdb::Value> MacroImplShape(const MoraineMacroImplRow &r) {
	return {
	    Bigint(r.macro_id), Bigint(r.impl_id), Varchar(r.dialect), Varchar(r.sql), Varchar(r.macro_type),
	};
}

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

std::vector<duckdb::Value> ColumnMappingShape(const MoraineColumnMappingRow &r) {
	return {
	    Bigint(r.mapping_id),
	    Bigint(r.table_id),
	    Varchar(r.map_type),
	};
}

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

template <bool EXTENDED>
std::vector<duckdb::Value> DataFileShape(const MoraineDataFileRow &r) {
	std::vector<duckdb::Value> values = {
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
	if constexpr (EXTENDED) {
		values.push_back(OptBigint(r.has_row_group_count, r.row_group_count));
	}
	return values;
}

template <bool EXTENDED>
std::vector<duckdb::Value> DeleteFileShape(const MoraineDeleteFileRow &r) {
	std::vector<duckdb::Value> values = {
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
	if constexpr (EXTENDED) {
		values.push_back(OptBigint(r.has_row_group_count, r.row_group_count));
	}
	return values;
}

std::vector<duckdb::Value> TableStatsShape(const MoraineTableStatsRow &r) {
	return {
	    Bigint(r.table_id),
	    Bigint(r.record_count),
	    Bigint(r.next_row_id),
	    Bigint(r.file_size_bytes),
	};
}

template <bool EXTENDED>
std::vector<duckdb::Value> TableColumnStatsShape(const MoraineTableColumnStatsRow &r) {
	std::vector<duckdb::Value> values = {
	    Bigint(r.table_id),
	    Bigint(r.column_id),
	    OptBoolean(r.has_contains_null, r.contains_null),
	    OptBoolean(r.has_contains_nan, r.contains_nan),
	    OptVarchar(r.min_value),
	    OptVarchar(r.max_value),
	    OptVarchar(r.extra_stats),
	};
	if constexpr (EXTENDED) {
		values.push_back(OptBoolean(r.has_min_is_exact, r.min_is_exact));
		values.push_back(OptBoolean(r.has_max_is_exact, r.max_is_exact));
	}
	return values;
}

template <bool EXTENDED>
std::vector<duckdb::Value> FileColumnStatsShape(const MoraineFileColumnStatsRow &r) {
	std::vector<duckdb::Value> values = {
	    Bigint(r.data_file_id),
	    Bigint(r.table_id),
	    Bigint(r.column_id),
	    Bigint(r.column_size_bytes),
	    OptBigint(r.has_value_count, r.value_count),
	    OptBigint(r.has_null_count, r.null_count),
	    OptVarchar(r.min_value),
	    OptVarchar(r.max_value),
	    OptBoolean(r.has_contains_nan, r.contains_nan),
	    OptVarchar(r.extra_stats),
	};
	if constexpr (EXTENDED) {
		values.push_back(OptBoolean(r.has_min_is_exact, r.min_is_exact));
		values.push_back(OptBoolean(r.has_max_is_exact, r.max_is_exact));
	}
	return values;
}

std::vector<duckdb::Value> FileVariantStatsShape(const MoraineFileVariantStatsRow &r) {
	return {
	    Bigint(r.data_file_id),
	    Bigint(r.table_id),
	    Bigint(r.column_id),
	    Varchar(r.variant_path),
	    Varchar(r.shredded_type),
	    Bigint(r.column_size_bytes),
	    OptBigint(r.has_value_count, r.value_count),
	    OptBigint(r.has_null_count, r.null_count),
	    OptVarchar(r.min_value),
	    OptVarchar(r.max_value),
	    OptBoolean(r.has_contains_nan, r.contains_nan),
	    OptVarchar(r.extra_stats),
	};
}

// Schema versions inherit begin_snapshot and schema_version from their snapshot record.
std::vector<duckdb::Value> SchemaVersionShape(const MoraineSchemaVersionRow &r) {
	return {
	    Bigint(r.begin_snapshot),
	    Bigint(r.schema_version),
	    Bigint(r.table_id),
	};
}

std::vector<duckdb::Value> PartitionInfoShape(const MorainePartitionInfoRow &r) {
	return {
	    Bigint(r.partition_id),
	    Bigint(r.table_id),
	    Bigint(r.begin_snapshot),
	    OptBigint(r.has_end_snapshot, r.end_snapshot),
	};
}

std::vector<duckdb::Value> PartitionColumnShape(const MorainePartitionColumnRow &r) {
	return {
	    Bigint(r.partition_id), Bigint(r.table_id),   Bigint(r.partition_key_index),
	    Bigint(r.column_id),    Varchar(r.transform),
	};
}

std::vector<duckdb::Value> FilePartitionValueShape(const MoraineFilePartitionValueRow &r) {
	return {
	    Bigint(r.data_file_id),
	    Bigint(r.table_id),
	    Bigint(r.partition_key_index),
	    Varchar(r.partition_value),
	};
}

std::vector<duckdb::Value> SortInfoShape(const MoraineSortInfoRow &r) {
	return {
	    Bigint(r.sort_id),
	    Bigint(r.table_id),
	    Bigint(r.begin_snapshot),
	    OptBigint(r.has_end_snapshot, r.end_snapshot),
	};
}

std::vector<duckdb::Value> SortExpressionShape(const MoraineSortExpressionRow &r) {
	return {
	    Bigint(r.sort_id),  Bigint(r.table_id),        Bigint(r.sort_key_index), Varchar(r.expression),
	    Varchar(r.dialect), Varchar(r.sort_direction), Varchar(r.null_order),
	};
}

std::vector<duckdb::Value> TagShape(const MoraineTagRow &r) {
	return {
	    Bigint(r.object_id), Bigint(r.begin_snapshot), OptBigint(r.has_end_snapshot, r.end_snapshot),
	    Varchar(r.key),      Varchar(r.value),
	};
}

std::vector<duckdb::Value> ColumnTagShape(const MoraineColumnTagRow &r) {
	return {
	    Bigint(r.table_id),       Bigint(r.column_id),
	    Bigint(r.begin_snapshot), OptBigint(r.has_end_snapshot, r.end_snapshot),
	    Varchar(r.key),           Varchar(r.value),
	};
}

// Catalog version 1.1 adds view-column tags.
std::vector<duckdb::Value> ViewColumnTagShape(const MoraineViewColumnTagRow &r) {
	return {
	    Bigint(r.view_id),        Varchar(r.column_name),
	    Bigint(r.begin_snapshot), OptBigint(r.has_end_snapshot, r.end_snapshot),
	    Varchar(r.key),           Varchar(r.value),
	};
}

std::vector<duckdb::Value> ScheduledDeletionShape(const MoraineScheduledDeletionRow &r) {
	return {
	    Bigint(r.data_file_id),
	    Varchar(r.path),
	    duckdb::Value::BOOLEAN(r.path_is_relative),
	    TimestampTz(r.schedule_start_micros),
	};
}

// Serve the attach-pinned version, writer, encryption flag, and normalized data root
// as global options. Leave data_inlining_row_limit to stored options and DuckLake defaults.
MetadataRows FixedMetadataRows(MoraineCatalogHandle *handle, MoraineInterruptProbe probe, void *probe_ctx,
                               bool extended) {
	bool encrypted = false;
	MoraineError err {};
	auto code = moraine_catalog_encrypted(handle, &encrypted, probe, probe_ctx, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	auto null_varchar = duckdb::Value(duckdb::LogicalType::VARCHAR);
	auto null_bigint = duckdb::Value(duckdb::LogicalType::BIGINT);

	MetadataRows rows = {
	    {Varchar("version"), Varchar(extended ? kExtendedCatalogVersion : "1.0"), null_varchar, null_bigint},
	    {Varchar("created_by"), Varchar("moraine"), null_varchar, null_bigint},
	    {Varchar("encrypted"), Varchar(encrypted ? "true" : "false"), null_varchar, null_bigint},
	};
	// Reuse the recorded data root when a later attach omits DATA_PATH.
	char *data_path = nullptr;
	MoraineError dp_err {};
	if (moraine_data_path(handle, probe, probe_ctx, &data_path, &dp_err) != MORAINE_OK) {
		ThrowMoraineError(dp_err);
	}
	if (data_path != nullptr) {
		// Match DuckLake's single trailing separator for local and S3 paths.
		std::string served(data_path);
		moraine_string_free(data_path);
		while (!served.empty() && served.back() == '/') {
			served.pop_back();
		}
		served += '/';
		rows.push_back({Varchar("data_path"), Varchar(served.c_str()), null_varchar, null_bigint});
	}

	return rows;
}

std::vector<duckdb::Value> OptionShape(const MoraineOptionRow &r) {
	return {
	    Varchar(r.key),
	    Varchar(r.value),
	    OptVarchar(r.scope),
	    r.has_scope_id ? Bigint(r.scope_id) : duckdb::Value(duckdb::LogicalType::BIGINT),
	};
}

// Stored options replace matching defaults, except global store facts.
// Table-scoped settings remain overridable, including names used by store facts.
void MergeStoredMetadataRows(MetadataRows &rows, MetadataRows stored) {
	static const std::set<std::string> kServedByMoraine = {"version", "created_by", "encrypted", "data_path"};
	for (auto &row : stored) {
		if (row[2].IsNull() && kServedByMoraine.count(row[0].GetValue<std::string>()) > 0) {
			continue;
		}
		auto same_key = [&row](const std::vector<duckdb::Value> &existing) {
			// Global scope keys are NULL; Value::operator== rejects NULLs.
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

template <bool EXTENDED>
MetadataRows ProvideMetadata(MoraineCatalogHandle *handle, MoraineInterruptProbe probe, void *probe_ctx) {
	auto rows = FixedMetadataRows(handle, probe, probe_ctx, EXTENDED);
	MergeStoredMetadataRows(rows, DumpRows<MoraineOptionRow>(handle, probe, probe_ctx, moraine_dump_options,
	                                                         moraine_dump_options_free, OptionShape));
	return rows;
}

// Overlay staged options on store facts that transactions cannot change.
template <bool EXTENDED>
MetadataRows TxProvideMetadata(MoraineTxHandle *tx, MoraineCatalogHandle *handle, MoraineInterruptProbe probe,
                               void *probe_ctx) {
	auto rows = FixedMetadataRows(handle, probe, probe_ctx, EXTENDED);
	MergeStoredMetadataRows(
	    rows, TxDumpRows<MoraineOptionRow>(tx, moraine_tx_dump_options, moraine_dump_options_free, OptionShape));
	return rows;
}

// Project committed inline registrations using DuckLake's generated table names.
// DuckLake reads this registry before staging a registration or after committing it.
MetadataRows ProvideInlinedDataTables(MoraineCatalogHandle *handle, MoraineInterruptProbe probe, void *probe_ctx) {
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

template <bool EXTENDED>
std::vector<MetadataColumnSpec> VersionedColumns(std::vector<MetadataColumnSpec> columns,
                                                 std::initializer_list<MetadataColumnSpec> additions) {
	if constexpr (EXTENDED) {
		columns.insert(columns.end(), additions.begin(), additions.end());
	}
	return columns;
}

// Match DuckLake's column order, types, and nullability, including nullable IDs.
template <bool EXTENDED>
std::vector<MetadataTableSpec> MetadataTableSpecsImpl() {
	std::vector<MetadataTableSpec> specs = {
	    {
	        "ducklake_snapshot",
	        {
	            {"snapshot_id", "BIGINT", true},
	            {"snapshot_time", "TIMESTAMPTZ", false},
	            {"schema_version", "BIGINT", false},
	            {"next_catalog_id", "BIGINT", false},
	            {"next_file_id", "BIGINT", false},
	        },
	        ProvideRows<moraine_dump_snapshots, moraine_dump_snapshots_free, SnapshotShape>,
	        TxProviders<MoraineSnapshotRow>(moraine_dump_snapshots_free, SnapshotShape, moraine_tx_dump_snapshots),
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
	        ProvideRows<moraine_dump_snapshots, moraine_dump_snapshots_free, SnapshotChangesShape>,
	        TxProviders<MoraineSnapshotRow>(moraine_dump_snapshots_free, SnapshotChangesShape,
	                                        moraine_tx_dump_snapshots),
	        1,
	        {},
	        0,
	        /* delete key: snapshot_id */ {0},
	    },
	    {
	        "ducklake_schema",
	        VersionedColumns<EXTENDED>(
	            {
	                {"schema_id", "BIGINT", true},
	                {"schema_uuid", "UUID", false},
	                {"begin_snapshot", "BIGINT", false},
	                {"end_snapshot", "BIGINT", false},
	                {"schema_name", "VARCHAR", false},
	                {"path", "VARCHAR", false},
	                {"path_is_relative", "BOOLEAN", false},
	            },
	            {{"parent_schema_id", "BIGINT", false}}),
	        ProvideRows<moraine_dump_schemas, moraine_dump_schemas_free, SchemaShape<EXTENDED>>,
	        TxProviders<MoraineSchemaRow>(moraine_dump_schemas_free, SchemaShape<EXTENDED>, moraine_tx_dump_schemas),
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
	        ProvideRows<moraine_dump_tables, moraine_dump_tables_free, TableShape>,
	        TxProviders<MoraineTableRow>(moraine_dump_tables_free, TableShape, moraine_tx_dump_tables,
	                                     moraine_tx_dump_tables_live_at),
	        3,
	        /* end key: table_id */ {0},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, end_snapshot */ {0, 3},
	        /* overlay_updatable */ false,
	        /* scope_column */ -1,
	        /* live_narrowable */ true,
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
	        ProvideRows<moraine_dump_views, moraine_dump_views_free, ViewShape>,
	        TxProviders<MoraineViewRow>(moraine_dump_views_free, ViewShape, moraine_tx_dump_views,
	                                    moraine_tx_dump_views_live_at),
	        4,
	        /* end key: view_id */ {0},
	        /* end_snapshot col */ 3,
	        /* delete key: view_id, end_snapshot */ {0, 3},
	        /* overlay_updatable */ false,
	        /* scope_column */ -1,
	        /* live_narrowable */ true,
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
	        ProvideRows<moraine_dump_columns, moraine_dump_columns_free, ColumnShape>,
	        TxProviders<MoraineColumnRow>(moraine_dump_columns_free, ColumnShape, moraine_tx_dump_columns,
	                                      moraine_tx_dump_columns_live_at),
	        5,
	        /* end key: table_id, column_id (decoder order) */ {3, 0},
	        /* end_snapshot col */ 2,
	        /* delete key: table_id, column_id, end_snapshot */ {3, 0, 2},
	        /* overlay_updatable */ false,
	        /* scope_column */ -1,
	        /* live_narrowable */ true,
	    },
	    {
	        "ducklake_data_file",
	        VersionedColumns<EXTENDED>(
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
	            {{"row_group_count", "BIGINT", false}}),
	        ProvideRows<moraine_dump_data_files, moraine_dump_data_files_free, DataFileShape<EXTENDED>>,
	        TxProviders<MoraineDataFileRow>(moraine_dump_data_files_free, DataFileShape<EXTENDED>,
	                                        moraine_tx_dump_data_files, moraine_tx_dump_data_files_live_at,
	                                        moraine_tx_dump_data_files_of),
	        6,
	        /* end key: table_id, data_file_id (decoder order) */ {1, 0},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, data_file_id, end_snapshot */ {1, 0, 3},
	        /* overlay_updatable */ false,
	        /* scope column: table_id */ 1,
	        /* live_narrowable */ true,
	        ProvideScopedRows<moraine_dump_data_files_of, moraine_dump_data_files_free, DataFileShape<EXTENDED>>,
	    },
	    {
	        "ducklake_delete_file",
	        VersionedColumns<EXTENDED>(
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
	            {{"row_group_count", "BIGINT", false}}),
	        ProvideRows<moraine_dump_delete_files, moraine_dump_delete_files_free, DeleteFileShape<EXTENDED>>,
	        TxProviders<MoraineDeleteFileRow>(moraine_dump_delete_files_free, DeleteFileShape<EXTENDED>,
	                                          moraine_tx_dump_delete_files, moraine_tx_dump_delete_files_live_at,
	                                          moraine_tx_dump_delete_files_of),
	        7,
	        /* end key: table_id, delete_file_id (decoder order) */ {1, 0},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, delete_file_id, end_snapshot */ {1, 0, 3},
	        /* overlay_updatable */ false,
	        /* scope column: table_id */ 1,
	        /* live_narrowable */ true,
	        ProvideScopedRows<moraine_dump_delete_files_of, moraine_dump_delete_files_free, DeleteFileShape<EXTENDED>>,
	    },
	    {
	        "ducklake_table_stats",
	        {
	            {"table_id", "BIGINT", false},
	            {"record_count", "BIGINT", false},
	            {"next_row_id", "BIGINT", false},
	            {"file_size_bytes", "BIGINT", false},
	        },
	        ProvideRows<moraine_dump_table_stats, moraine_dump_table_stats_free, TableStatsShape>,
	        TxProviders<MoraineTableStatsRow>(moraine_dump_table_stats_free, TableStatsShape,
	                                          moraine_tx_dump_table_stats),
	        8,
	        {},
	        0,
	        /* delete key: table_id */ {0},
	        /* overlay updates */ true,
	    },
	    {
	        "ducklake_table_column_stats",
	        VersionedColumns<EXTENDED>(
	            {
	                {"table_id", "BIGINT", false},
	                {"column_id", "BIGINT", false},
	                {"contains_null", "BOOLEAN", false},
	                {"contains_nan", "BOOLEAN", false},
	                {"min_value", "VARCHAR", false},
	                {"max_value", "VARCHAR", false},
	                {"extra_stats", "VARCHAR", false},
	            },
	            {{"min_is_exact", "BOOLEAN", false}, {"max_is_exact", "BOOLEAN", false}}),
	        ProvideRows<moraine_dump_table_column_stats, moraine_dump_table_column_stats_free,
	                    TableColumnStatsShape<EXTENDED>>,
	        TxProviders<MoraineTableColumnStatsRow>(moraine_dump_table_column_stats_free,
	                                                TableColumnStatsShape<EXTENDED>,
	                                                moraine_tx_dump_table_column_stats),
	        9,
	        {},
	        0,
	        /* delete key: table_id, column_id */ {0, 1},
	        /* overlay updates */ true,
	    },
	    {
	        "ducklake_file_column_stats",
	        VersionedColumns<EXTENDED>(
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
	            {{"min_is_exact", "BOOLEAN", false}, {"max_is_exact", "BOOLEAN", false}}),
	        ProvideRows<moraine_dump_file_column_stats, moraine_dump_file_column_stats_free,
	                    FileColumnStatsShape<EXTENDED>>,
	        UnversionedTxProviders<MoraineFileColumnStatsRow>(
	            moraine_dump_file_column_stats_free, FileColumnStatsShape<EXTENDED>, moraine_tx_dump_file_column_stats,
	            moraine_tx_dump_file_column_stats_of),
	        10,
	        {},
	        0,
	        /* delete key: data_file_id, table_id, column_id (decoder order) */ {0, 1, 2},
	        /* overlay updates */ true,
	        /* scope column: table_id */ 1,
	        /* live_narrowable */ false,
	        ProvideScopedRows<moraine_dump_file_column_stats_of, moraine_dump_file_column_stats_free,
	                          FileColumnStatsShape<EXTENDED>>,
	    },
	    {
	        // Three-column form: (begin_snapshot, schema_version, table_id).
	        "ducklake_schema_versions",
	        {
	            {"begin_snapshot", "BIGINT", false},
	            {"schema_version", "BIGINT", false},
	            {"table_id", "BIGINT", false},
	        },
	        ProvideRows<moraine_dump_schema_versions, moraine_dump_schema_versions_free, SchemaVersionShape>,
	        TxProviders<MoraineSchemaVersionRow>(moraine_dump_schema_versions_free, SchemaVersionShape,
	                                             moraine_tx_dump_schema_versions),
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
	        ProvideRows<moraine_dump_tags, moraine_dump_tags_free, TagShape>,
	        TxProviders<MoraineTagRow>(moraine_dump_tags_free, TagShape, moraine_tx_dump_tags),
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
	        ProvideRows<moraine_dump_column_tags, moraine_dump_column_tags_free, ColumnTagShape>,
	        TxProviders<MoraineColumnTagRow>(moraine_dump_column_tags_free, ColumnTagShape,
	                                         moraine_tx_dump_column_tags),
	        18,
	        /* end key: table_id, column_id, key (decoder order) */ {0, 1, 4},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, column_id, key, begin_snapshot */ {0, 1, 4, 2},
	    },
	    {
	        "ducklake_inlined_data_tables",
	        {
	            {"table_id", "BIGINT", false},
	            {"table_name", "VARCHAR", false},
	            {"schema_version", "BIGINT", false},
	        },
	        ProvideInlinedDataTables,
	        {},
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
	        ProvideRows<moraine_dump_macros, moraine_dump_macros_free, MacroShape>,
	        TxProviders<MoraineMacroRow>(moraine_dump_macros_free, MacroShape, moraine_tx_dump_macros),
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
	        ProvideRows<moraine_dump_macro_impls, moraine_dump_macro_impls_free, MacroImplShape>,
	        TxProviders<MoraineMacroImplRow>(moraine_dump_macro_impls_free, MacroImplShape,
	                                         moraine_tx_dump_macro_impls),
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
	        ProvideRows<moraine_dump_macro_parameters, moraine_dump_macro_parameters_free, MacroParameterShape>,
	        TxProviders<MoraineMacroParameterRow>(moraine_dump_macro_parameters_free, MacroParameterShape,
	                                              moraine_tx_dump_macro_parameters),
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
	        ProvideRows<moraine_dump_partition_info, moraine_dump_partition_info_free, PartitionInfoShape>,
	        TxProviders<MorainePartitionInfoRow>(moraine_dump_partition_info_free, PartitionInfoShape,
	                                             moraine_tx_dump_partition_info,
	                                             moraine_tx_dump_partition_info_live_at),
	        12,
	        /* end key: table_id, partition_id (decoder order) */ {1, 0},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, partition_id, end_snapshot */ {1, 0, 3},
	        /* overlay_updatable */ false,
	        /* scope_column */ -1,
	        /* live_narrowable */ true,
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
	        ProvideRows<moraine_dump_partition_columns, moraine_dump_partition_columns_free, PartitionColumnShape>,
	        TxProviders<MorainePartitionColumnRow>(moraine_dump_partition_columns_free, PartitionColumnShape,
	                                               moraine_tx_dump_partition_columns),
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
	        ProvideRows<moraine_dump_file_partition_values, moraine_dump_file_partition_values_free,
	                    FilePartitionValueShape>,
	        TxProviders<MoraineFilePartitionValueRow>(moraine_dump_file_partition_values_free, FilePartitionValueShape,
	                                                  moraine_tx_dump_file_partition_values),
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
	        ProvideRows<moraine_dump_file_variant_stats, moraine_dump_file_variant_stats_free, FileVariantStatsShape>,
	        UnversionedTxProviders<MoraineFileVariantStatsRow>(
	            moraine_dump_file_variant_stats_free, FileVariantStatsShape, moraine_tx_dump_file_variant_stats,
	            moraine_tx_dump_file_variant_stats_of),
	        27,
	        {},
	        0,
	        /* delete key: data_file_id, table_id */ {0, 1},
	        /* overlay updates */ false,
	        /* scope column: table_id */ 1,
	        /* live_narrowable */ false,
	        ProvideScopedRows<moraine_dump_file_variant_stats_of, moraine_dump_file_variant_stats_free,
	                          FileVariantStatsShape>,
	    },
	    {
	        "ducklake_files_scheduled_for_deletion",
	        {
	            {"data_file_id", "BIGINT", false},
	            {"path", "VARCHAR", false},
	            {"path_is_relative", "BOOLEAN", false},
	            {"schedule_start", "TIMESTAMPTZ", false},
	        },
	        ProvideRows<moraine_dump_scheduled_deletions, moraine_dump_scheduled_deletions_free,
	                    ScheduledDeletionShape>,
	        TxProviders<MoraineScheduledDeletionRow>(moraine_dump_scheduled_deletions_free, ScheduledDeletionShape,
	                                                 moraine_tx_dump_scheduled_deletions),
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
	        ProvideRows<moraine_dump_column_mappings, moraine_dump_column_mappings_free, ColumnMappingShape>,
	        TxProviders<MoraineColumnMappingRow>(moraine_dump_column_mappings_free, ColumnMappingShape,
	                                             moraine_tx_dump_column_mappings),
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
	        ProvideRows<moraine_dump_name_mappings, moraine_dump_name_mappings_free, NameMappingShape>,
	        TxProviders<MoraineNameMappingRow>(moraine_dump_name_mappings_free, NameMappingShape,
	                                           moraine_tx_dump_name_mappings),
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
	        ProvideRows<moraine_dump_sort_info, moraine_dump_sort_info_free, SortInfoShape>,
	        TxProviders<MoraineSortInfoRow>(moraine_dump_sort_info_free, SortInfoShape, moraine_tx_dump_sort_info,
	                                        moraine_tx_dump_sort_info_live_at),
	        15,
	        /* end key: table_id, sort_id (decoder order) */ {1, 0},
	        /* end_snapshot col */ 3,
	        /* delete key: table_id, sort_id, end_snapshot */ {1, 0, 3},
	        /* overlay_updatable */ false,
	        /* scope_column */ -1,
	        /* live_narrowable */ true,
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
	        ProvideRows<moraine_dump_sort_expressions, moraine_dump_sort_expressions_free, SortExpressionShape>,
	        TxProviders<MoraineSortExpressionRow>(moraine_dump_sort_expressions_free, SortExpressionShape,
	                                              moraine_tx_dump_sort_expressions),
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
	        ProvideMetadata<EXTENDED>,
	        {TxProvideMetadata<EXTENDED>},
	        // Options are unversioned. DuckLake updates existing keys, including synthesized defaults.
	        25,
	        {},
	        0,
	        /* delete key: key, scope, scope_id */ {0, 2, 3},
	        /* overlay_updatable */ true,
	    },
	};
	if constexpr (EXTENDED) {
		specs.push_back({
		    "ducklake_view_column_tag",
		    {
		        {"view_id", "BIGINT", false},
		        {"column_name", "VARCHAR", false},
		        {"begin_snapshot", "BIGINT", false},
		        {"end_snapshot", "BIGINT", false},
		        {"key", "VARCHAR", false},
		        {"value", "VARCHAR", false},
		    },
		    ProvideRows<moraine_dump_view_column_tags, moraine_dump_view_column_tags_free, ViewColumnTagShape>,
		    TxProviders<MoraineViewColumnTagRow>(moraine_dump_view_column_tags_free, ViewColumnTagShape,
		                                         moraine_tx_dump_view_column_tags),
		    26,
		    /* end key: view_id, column_name, key (decoder order) */ {0, 1, 4},
		    /* end_snapshot col */ 3,
		    /* delete key: view_id, column_name, key, begin_snapshot */ {0, 1, 4, 2},
		});
	}
	return specs;
}

} // namespace

bool MoraineServesExtendedCatalog(MoraineCatalogHandle *handle) {
	char *version = nullptr;
	MoraineError err {};
	if (moraine_catalog_version(handle, nullptr, nullptr, &version, &err) != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::string recorded(version == nullptr ? "" : version);
	moraine_string_free(version);
	return recorded == kExtendedCatalogVersion || recorded == kSupersededExtendedCatalogVersion;
}

const std::vector<MetadataTableSpec> &MoraineMetadataTableSpecs(bool extended) {
	// Entries and row caches retain spec addresses; each version needs stable storage.
	if (extended) {
		static const auto specs = MetadataTableSpecsImpl<true>();
		return specs;
	}
	static const auto specs = MetadataTableSpecsImpl<false>();
	return specs;
}

} // namespace moraine_duckdb
