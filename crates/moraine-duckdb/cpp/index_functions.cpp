// The equality-index SQL surface: autonomous-commit DDL, introspection, and
// explicit point, IN, range, and NULL-prefix lookups. These are the first
// user-callable functions this extension registers; every other TableFunction
// in the shim is reached through a catalog-entry override, not a global
// registration.
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "catalog.hpp"
#include "moraine_abi.h"
#include "owned_array.hpp"

#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/uuid.hpp"

#include <deque>

namespace moraine_duckdb {

namespace {

// Resolves a moraine catalog handle from `catalog_name`. The lake name
// and its metadata catalog's name are both accepted; `ResolveMoraineCatalog`
// owns the mapping between them, so this and the maintenance functions
// resolve identically.
MoraineCatalogHandle *ResolveHandle(duckdb::ClientContext &context, const std::string &catalog_name) {
	return ResolveMoraineCatalog(context, catalog_name).Handle();
}

// moraine_indexes: lists a table's live equality indexes.

struct IndexesBindData : public duckdb::FunctionData {
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	struct Row {
		int64_t index_id;
		std::string name;
		bool unique;
		bool building;
	};
	std::vector<Row> rows;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<IndexesBindData>();
		result->catalog_name = catalog_name;
		result->schema_name = schema_name;
		result->table_name = table_name;
		result->rows = rows;
		return result;
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<IndexesBindData>();
		return catalog_name == other.catalog_name && schema_name == other.schema_name &&
		       table_name == other.table_name;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> IndexesBind(duckdb::ClientContext &context,
                                                     duckdb::TableFunctionBindInput &input,
                                                     duckdb::vector<duckdb::LogicalType> &return_types,
                                                     duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<IndexesBindData>();
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();

	auto handle = ResolveHandle(context, bind_data->catalog_name);
	OwnedArray<MoraineIndexDesc> descs(moraine_indexes_free);
	MoraineError err {};
	auto code = moraine_indexes(handle, bind_data->schema_name.c_str(), bind_data->table_name.c_str(),
	                            descs.OutItems(), descs.OutLen(), moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	for (auto &desc : descs) {
		bind_data->rows.push_back(
		    {static_cast<int64_t>(desc.index_id), std::string(desc.name), desc.unique, desc.building});
	}

	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN,
	                duckdb::LogicalType::BOOLEAN};
	names = {"index_id", "index_name", "is_unique", "is_building"};
	return bind_data;
}

struct IndexesGlobalState : public duckdb::GlobalTableFunctionState {
	duckdb::idx_t offset = 0;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> IndexesInitGlobal(duckdb::ClientContext &,
                                                                       duckdb::TableFunctionInitInput &) {
	return duckdb::make_uniq<IndexesGlobalState>();
}

void IndexesImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IndexesBindData>();
	auto &state = data.global_state->Cast<IndexesGlobalState>();
	if (state.offset >= bind_data.rows.size()) {
		output.SetCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - state.offset);
	for (duckdb::idx_t i = 0; i < count; i++) {
		auto &row = bind_data.rows[state.offset + i];
		output.SetValue(0, i, duckdb::Value::BIGINT(row.index_id));
		output.SetValue(1, i, duckdb::Value(row.name));
		output.SetValue(2, i, duckdb::Value::BOOLEAN(row.unique));
		output.SetValue(3, i, duckdb::Value::BOOLEAN(row.building));
	}
	state.offset += count;
	output.SetCardinality(count);
}

// moraine_index_create / moraine_index_drop: autonomous-commit DDL.

struct IndexDdlBindData : public duckdb::FunctionData {
	bool is_create = false;
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
		return is_create == other.is_create && catalog_name == other.catalog_name &&
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
	auto handle = ResolveHandle(context, bind_data.catalog_name);
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
		                            moraine_shim_is_interrupted, &context, &err);
	} else {
		code = moraine_index_drop(handle, bind_data.schema_name.c_str(), bind_data.table_name.c_str(),
		                          bind_data.index_name.c_str(), moraine_shim_is_interrupted, &context, &err);
	}
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	return duckdb::make_uniq<IndexDdlGlobalState>();
}

void IndexDdlImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<IndexDdlGlobalState>();
	auto &bind_data = data.bind_data->Cast<IndexDdlBindData>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	std::string message = (bind_data.is_create ? "created index " : "dropped index ") + bind_data.index_name;
	output.SetValue(0, 0, duckdb::Value(message));
	output.SetCardinality(1);
	state.emitted = true;
}

duckdb::unique_ptr<duckdb::FunctionData> CreateBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                    duckdb::vector<duckdb::LogicalType> &return_types,
                                                    duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<IndexDdlBindData>();
	bind_data->is_create = true;
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	bind_data->index_name = input.inputs[3].GetValue<std::string>();
	for (auto &child : duckdb::ListValue::GetChildren(input.inputs[4])) {
		bind_data->columns.push_back(child.GetValue<std::string>());
	}
	bind_data->unique = input.inputs[5].GetValue<bool>();
	auto maintenance = input.named_parameters.find("maintenance");
	if (maintenance != input.named_parameters.end() && !maintenance->second.IsNull()) {
		const std::string mode = maintenance->second.GetValue<std::string>();
		if (mode == "deferred" || mode == "DEFERRED") {
			bind_data->deferred_maintenance = true;
		} else if (mode != "synchronous" && mode != "SYNCHRONOUS") {
			throw duckdb::InvalidInputException(
			    "moraine_index_create: maintenance must be 'synchronous' or 'deferred', got \"%s\"", mode);
		}
	}
	if (bind_data->deferred_maintenance && bind_data->unique) {
		throw duckdb::InvalidInputException(
		    "moraine_index_create: deferred maintenance is supported only for non-unique indexes");
	}
	// Optional `directions := ['asc'|'desc', ...]`, parallel to the columns.
	auto directions = input.named_parameters.find("directions");
	if (directions != input.named_parameters.end() && !directions->second.IsNull()) {
		for (auto &child : duckdb::ListValue::GetChildren(directions->second)) {
			const std::string dir = child.GetValue<std::string>();
			if (dir == "desc" || dir == "DESC") {
				bind_data->descending.push_back(1);
			} else if (dir == "asc" || dir == "ASC") {
				bind_data->descending.push_back(0);
			} else {
				throw duckdb::InvalidInputException(
				    "moraine_index_create: direction must be 'asc' or 'desc', got \"%s\"", dir);
			}
		}
		if (bind_data->descending.size() != bind_data->columns.size()) {
			throw duckdb::InvalidInputException(
			    "moraine_index_create: `directions` must have one entry per column");
		}
	}
	// Optional `nulls := ['first'|'last', ...]`, parallel to the columns.
	auto nulls = input.named_parameters.find("nulls");
	if (nulls != input.named_parameters.end() && !nulls->second.IsNull()) {
		for (auto &child : duckdb::ListValue::GetChildren(nulls->second)) {
			const std::string placement = child.GetValue<std::string>();
			if (placement == "first" || placement == "FIRST") {
				bind_data->nulls_first.push_back(1);
			} else if (placement == "last" || placement == "LAST") {
				bind_data->nulls_first.push_back(0);
			} else {
				throw duckdb::InvalidInputException(
				    "moraine_index_create: nulls placement must be 'first' or 'last', got \"%s\"", placement);
			}
		}
		if (bind_data->nulls_first.size() != bind_data->columns.size()) {
			throw duckdb::InvalidInputException(
			    "moraine_index_create: `nulls` must have one entry per column");
		}
	}
	// Optional `staged := true`: build across several commits, for a table
	// whose backfill exceeds what one commit may stage.
	auto staged = input.named_parameters.find("staged");
	if (staged != input.named_parameters.end() && !staged->second.IsNull()) {
		bind_data->staged = staged->second.GetValue<bool>();
	}
	// Optional bounds on one step of that build. Rejected here rather than
	// in the core so the message names the parameter the user typed, and so
	// nothing is committed before the refusal.
	auto positive = [&input](const char *name) -> uint64_t {
		auto found = input.named_parameters.find(name);
		if (found == input.named_parameters.end() || found->second.IsNull()) {
			return 0;
		}
		const int64_t value = found->second.GetValue<int64_t>();
		if (value <= 0) {
			throw duckdb::InvalidInputException(
			    "moraine_index_create: `%s` must be greater than zero, got %lld", name,
			    static_cast<long long>(value));
		}
		return static_cast<uint64_t>(value);
	};
	bind_data->step_entries = positive("step_entries");
	bind_data->step_bytes = positive("step_bytes");
	return_types = {duckdb::LogicalType::VARCHAR};
	names = {"result"};
	return bind_data;
}

duckdb::unique_ptr<duckdb::FunctionData> DropBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                  duckdb::vector<duckdb::LogicalType> &return_types,
                                                  duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<IndexDdlBindData>();
	bind_data->is_create = false;
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	bind_data->index_name = input.inputs[3].GetValue<std::string>();
	return_types = {duckdb::LogicalType::VARCHAR};
	names = {"result"};
	return bind_data;
}

// moraine_index_lookup: resolves an equality key to the rows holding it. The
// variadic values are one per indexed column, in the index's column order.

// Emits one located row: its stable id, and the file currently holding it.
// A NULL file id is a live inlined row or one the lookup could not place,
// so a consumer joins the second column with null-safe equality.
void SetRowId(duckdb::DataChunk &output, duckdb::idx_t row_index, const MoraineRowId &row_id) {
	output.SetValue(0, row_index, duckdb::Value::BIGINT(static_cast<int64_t>(row_id.value)));
	output.SetValue(1, row_index,
	                row_id.has_data_file_id ? duckdb::Value::UBIGINT(row_id.data_file_id)
	                                        : duckdb::Value(duckdb::LogicalType::UBIGINT));
}

struct LookupBindData : public duckdb::FunctionData {
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	std::string index_name;
	// The looked-up values in text form, so `Equals` distinguishes lookups of
	// different keys (the rows they resolve to differ).
	std::string value_repr;
	std::vector<MoraineRowId> rows;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<LookupBindData>();
		*result = *this;
		return result;
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<LookupBindData>();
		return catalog_name == other.catalog_name && schema_name == other.schema_name &&
		       table_name == other.table_name && index_name == other.index_name && value_repr == other.value_repr;
	}
};

template <class BindData>
duckdb::unique_ptr<duckdb::NodeStatistics> ExactIndexCardinality(duckdb::ClientContext &,
                                                                 const duckdb::FunctionData *bind_data) {
	auto count = bind_data->Cast<BindData>().rows.size();
	return duckdb::make_uniq<duckdb::NodeStatistics>(count, count);
}

// Backing storage for a `MoraineLookupValue`'s string/bytes fields, which
// point into these members and so must outlive any use of the value.
struct LookupValueBacking {
	std::string str;
	std::vector<uint8_t> bytes;
};

using LookupValueArena = std::deque<LookupValueBacking>;

// A value's text tagged by type and length, so that two different argument
// lists cannot render alike: without the tag `('a', 'b,c')` and `('a,b', 'c')`
// share a rendering, as do a NULL bound and the string `'NULL'`.
std::string ValueRepr(const duckdb::Value &value) {
	auto text = value.ToString();
	return std::to_string(static_cast<int>(value.type().id())) + ":" + std::to_string(text.size()) + ":" + text;
}

// Translates a DuckDB `Value` into the tagged `MoraineLookupValue` the ABI
// coerces to the indexed column's type. `caller` names the SQL function in
// any error. Throws on a type equality indexes do not cover.
//
// A NULL becomes the `IS NULL` predicate kind; which callers accept one is
// the core's rule, so it is not judged here. Reading a typed NULL's payload
// would raise an internal error, so the kind is set before the switch.
MoraineLookupValue BuildLookupValue(const duckdb::Value &value, LookupValueArena &arena, const char *caller) {
	MoraineLookupValue lookup {};
	if (value.IsNull()) {
		lookup.kind = 0;
		return lookup;
	}

	auto &backing = arena.emplace_back();
	switch (value.type().id()) {
	case duckdb::LogicalTypeId::TINYINT:
	case duckdb::LogicalTypeId::SMALLINT:
	case duckdb::LogicalTypeId::INTEGER:
	case duckdb::LogicalTypeId::BIGINT:
	case duckdb::LogicalTypeId::DATE:
	case duckdb::LogicalTypeId::TIME:
	case duckdb::LogicalTypeId::TIMESTAMP:
	case duckdb::LogicalTypeId::TIMESTAMP_TZ:
	case duckdb::LogicalTypeId::TIMESTAMP_SEC:
	case duckdb::LogicalTypeId::TIMESTAMP_MS:
	case duckdb::LogicalTypeId::TIMESTAMP_NS:
		lookup.kind = 1;
		lookup.i64_value = value.GetValue<int64_t>();
		break;
	case duckdb::LogicalTypeId::UTINYINT:
	case duckdb::LogicalTypeId::USMALLINT:
	case duckdb::LogicalTypeId::UINTEGER:
	case duckdb::LogicalTypeId::UBIGINT:
		lookup.kind = 2;
		lookup.u64_value = value.GetValue<uint64_t>();
		break;
	case duckdb::LogicalTypeId::FLOAT:
	case duckdb::LogicalTypeId::DOUBLE:
		lookup.kind = 3;
		lookup.f64_value = value.GetValue<double>();
		break;
	case duckdb::LogicalTypeId::BOOLEAN:
		lookup.kind = 4;
		lookup.bool_value = value.GetValue<bool>();
		break;
	case duckdb::LogicalTypeId::VARCHAR:
		lookup.kind = 5;
		backing.str = duckdb::StringValue::Get(value);
		lookup.str_value = backing.str.c_str();
		break;
	case duckdb::LogicalTypeId::UUID: {
		lookup.kind = 6;
		backing.bytes.resize(16);
		duckdb::BaseUUID::ToBlob(value.GetValue<duckdb::hugeint_t>(), backing.bytes.data());
		lookup.bytes_value = backing.bytes.data();
		lookup.bytes_len = backing.bytes.size();
		break;
	}
	case duckdb::LogicalTypeId::BLOB: {
		lookup.kind = 6;
		const auto &blob = duckdb::StringValue::Get(value);
		backing.bytes.assign(blob.begin(), blob.end());
		lookup.bytes_value = backing.bytes.data();
		lookup.bytes_len = backing.bytes.size();
		break;
	}
	default:
		throw duckdb::InvalidInputException("%s: unsupported value type \"%s\"", caller,
		                                    value.type().ToString());
	}
	return lookup;
}

// The variadic args after the index name are the equality key: one value per
// indexed column, in the index's column order. The count must match the index
// width — the core refuses a partial key.
duckdb::unique_ptr<duckdb::FunctionData> LookupBind(duckdb::ClientContext &context,
                                                    duckdb::TableFunctionBindInput &input,
                                                    duckdb::vector<duckdb::LogicalType> &return_types,
                                                    duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<LookupBindData>();
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	bind_data->index_name = input.inputs[3].GetValue<std::string>();

	const idx_t value_count = input.inputs.size() - 4;
	LookupValueArena backings;
	std::vector<MoraineLookupValue> values;
	values.reserve(value_count);
	for (idx_t i = 4; i < input.inputs.size(); i++) {
		bind_data->value_repr += ValueRepr(input.inputs[i]) + ",";
		values.push_back(BuildLookupValue(input.inputs[i], backings, "moraine_index_lookup"));
	}

	auto handle = ResolveHandle(context, bind_data->catalog_name);
	OwnedArray<MoraineRowId> row_ids(moraine_index_lookup_free);
	MoraineError err {};
	auto code = moraine_index_lookup(handle, bind_data->schema_name.c_str(), bind_data->table_name.c_str(),
	                                 bind_data->index_name.c_str(), values.data(), values.size(),
	                                 row_ids.OutItems(), row_ids.OutLen(), moraine_shim_is_interrupted, &context,
	                                 &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	for (auto row_id : row_ids) {
		bind_data->rows.push_back(row_id);
	}

	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::UBIGINT};
	names = {"row_id", "data_file_id"};
	return bind_data;
}

// moraine_index_in: resolves a list of complete equality keys under one
// catalog read. A scalar list addresses a one-column index; a list of
// STRUCT/row values addresses a composite index.
duckdb::unique_ptr<duckdb::FunctionData> InBind(duckdb::ClientContext &context,
                                                duckdb::TableFunctionBindInput &input,
                                                duckdb::vector<duckdb::LogicalType> &return_types,
                                                duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<LookupBindData>();
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	bind_data->index_name = input.inputs[3].GetValue<std::string>();
	bind_data->value_repr = ValueRepr(input.inputs[4]);

	LookupValueArena backings;
	std::vector<std::vector<MoraineLookupValue>> key_values;
	if (!input.inputs[4].IsNull()) {
		auto &keys = duckdb::ListValue::GetChildren(input.inputs[4]);
		key_values.reserve(keys.size());
		for (auto &key : keys) {
			// A whole NULL row cannot make an IN predicate true. Skipping it
			// here also avoids asking StructValue for a NULL payload.
			if (key.IsNull() && key.type().id() == duckdb::LogicalTypeId::STRUCT) {
				continue;
			}
			auto &values = key_values.emplace_back();
			if (key.type().id() == duckdb::LogicalTypeId::STRUCT) {
				auto &children = duckdb::StructValue::GetChildren(key);
				values.reserve(children.size());
				for (auto &child : children) {
					values.push_back(BuildLookupValue(child, backings, "moraine_index_in"));
				}
			} else {
				values.push_back(BuildLookupValue(key, backings, "moraine_index_in"));
			}
		}
	}

	std::vector<MoraineLookupKey> keys;
	keys.reserve(key_values.size());
	for (auto &values : key_values) {
		keys.push_back({values.data(), values.size()});
	}

	auto handle = ResolveHandle(context, bind_data->catalog_name);
	OwnedArray<MoraineRowId> row_ids(moraine_index_in_free);
	MoraineError err {};
	auto code = moraine_index_in(handle, bind_data->schema_name.c_str(), bind_data->table_name.c_str(),
	                             bind_data->index_name.c_str(), keys.data(), keys.size(), row_ids.OutItems(),
	                             row_ids.OutLen(),
	                             moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	for (auto row_id : row_ids) {
		bind_data->rows.push_back(row_id);
	}

	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::UBIGINT};
	names = {"row_id", "data_file_id"};
	return bind_data;
}

struct LookupGlobalState : public duckdb::GlobalTableFunctionState {
	duckdb::idx_t offset = 0;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> LookupInitGlobal(duckdb::ClientContext &,
                                                                      duckdb::TableFunctionInitInput &) {
	return duckdb::make_uniq<LookupGlobalState>();
}

void LookupImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<LookupBindData>();
	auto &state = data.global_state->Cast<LookupGlobalState>();
	if (state.offset >= bind_data.rows.size()) {
		output.SetCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - state.offset);
	for (duckdb::idx_t i = 0; i < count; i++) {
		auto &row = bind_data.rows[state.offset + i];
		SetRowId(output, i, row);
	}
	state.offset += count;
	output.SetCardinality(count);
}

struct RangeBindData : public duckdb::FunctionData {
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	std::string index_name;
	// Both bounds and their inclusivity in text form, so `Equals`
	// distinguishes different ranges (they resolve to different rows).
	std::string bounds_repr;
	std::vector<MoraineRowId> rows;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<RangeBindData>();
		*result = *this;
		return result;
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<RangeBindData>();
		return catalog_name == other.catalog_name && schema_name == other.schema_name &&
		       table_name == other.table_name && index_name == other.index_name && bounds_repr == other.bounds_repr;
	}
};

// Each bound is a scalar (single-column index) or a STRUCT / `row(...)` of
// values naming the index's leading columns, in column order; a NULL bound
// is an open (unbounded) side. A present bound is Included or Excluded per
// its inclusivity flag.
duckdb::unique_ptr<duckdb::FunctionData> RangeBind(duckdb::ClientContext &context,
                                                   duckdb::TableFunctionBindInput &input,
                                                   duckdb::vector<duckdb::LogicalType> &return_types,
                                                   duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<RangeBindData>();
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	bind_data->index_name = input.inputs[3].GetValue<std::string>();
	const bool lower_inclusive = input.inputs[6].GetValue<bool>();
	const bool upper_inclusive = input.inputs[7].GetValue<bool>();
	bind_data->bounds_repr = ValueRepr(input.inputs[4]) + (lower_inclusive ? "[" : "(") + "," +
	                         ValueRepr(input.inputs[5]) + (upper_inclusive ? "]" : ")");

	// A scalar is a one-column bound; a STRUCT/`row(...)` is a composite bound
	// whose fields are the leading columns in order. Each bound's arena
	// outlives the call, keeping the string/bytes its values borrow valid.
	LookupValueArena lower_backing;
	LookupValueArena upper_backing;
	std::vector<MoraineLookupValue> lower_values;
	std::vector<MoraineLookupValue> upper_values;
	auto build_bound = [](const duckdb::Value &value, LookupValueArena &backing,
	                      std::vector<MoraineLookupValue> &out) {
		// A whole-bound NULL is an open side, not a value: it names no column.
		if (value.IsNull()) {
			return;
		}
		duckdb::vector<duckdb::Value> columns;
		if (value.type().id() == duckdb::LogicalTypeId::STRUCT) {
			columns = duckdb::StructValue::GetChildren(value);
		} else {
			columns.push_back(value);
		}
		out.reserve(columns.size());
		for (auto &column : columns) {
			out.push_back(BuildLookupValue(column, backing, "moraine_index_range"));
		}
	};
	build_bound(input.inputs[4], lower_backing, lower_values);
	build_bound(input.inputs[5], upper_backing, upper_values);

	// Optional `reverse := true` returns the rows in the opposite of the
	// index's declared order.
	auto reverse_it = input.named_parameters.find("reverse");
	const bool reverse =
	    reverse_it != input.named_parameters.end() && !reverse_it->second.IsNull() && reverse_it->second.GetValue<bool>();
	bind_data->bounds_repr += reverse ? "|rev" : "";

	auto handle = ResolveHandle(context, bind_data->catalog_name);
	OwnedArray<MoraineRowId> row_ids(moraine_index_range_free);
	MoraineError err {};
	auto code = moraine_index_range(handle, bind_data->schema_name.c_str(), bind_data->table_name.c_str(),
	                                bind_data->index_name.c_str(), lower_values.data(), lower_values.size(),
	                                lower_inclusive, upper_values.data(), upper_values.size(), upper_inclusive, reverse,
	                                row_ids.OutItems(), row_ids.OutLen(), moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	for (auto row_id : row_ids) {
		bind_data->rows.push_back(row_id);
	}

	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::UBIGINT};
	names = {"row_id", "data_file_id"};
	return bind_data;
}

struct RangeGlobalState : public duckdb::GlobalTableFunctionState {
	duckdb::idx_t offset = 0;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> RangeInitGlobal(duckdb::ClientContext &,
                                                                     duckdb::TableFunctionInitInput &) {
	return duckdb::make_uniq<RangeGlobalState>();
}

void RangeImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RangeBindData>();
	auto &state = data.global_state->Cast<RangeGlobalState>();
	if (state.offset >= bind_data.rows.size()) {
		output.SetCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - state.offset);
	for (duckdb::idx_t i = 0; i < count; i++) {
		auto &row = bind_data.rows[state.offset + i];
		SetRowId(output, i, row);
	}
	state.offset += count;
	output.SetCardinality(count);
}

struct NullsBindData : public duckdb::FunctionData {
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	std::string index_name;
	// The prefix predicates in text form, so `Equals` distinguishes queries.
	std::string prefix_repr;
	std::vector<MoraineRowId> rows;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<NullsBindData>();
		*result = *this;
		return result;
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<NullsBindData>();
		return catalog_name == other.catalog_name && schema_name == other.schema_name &&
		       table_name == other.table_name && index_name == other.index_name && prefix_repr == other.prefix_repr;
	}
};

// The variadic args after the index name are the leading-prefix predicates: a
// NULL arg is `IS NULL` for that column (ABI kind 0), any other is `= value`.
duckdb::unique_ptr<duckdb::FunctionData> NullsBind(duckdb::ClientContext &context,
                                                   duckdb::TableFunctionBindInput &input,
                                                   duckdb::vector<duckdb::LogicalType> &return_types,
                                                   duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<NullsBindData>();
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	bind_data->index_name = input.inputs[3].GetValue<std::string>();

	const idx_t prefix_count = input.inputs.size() - 4;
	LookupValueArena backings;
	std::vector<MoraineLookupValue> prefix;
	prefix.reserve(prefix_count);
	for (idx_t i = 4; i < input.inputs.size(); i++) {
		bind_data->prefix_repr += ValueRepr(input.inputs[i]) + ",";
		if (input.inputs[i].IsNull()) {
			MoraineLookupValue is_null {};
			is_null.kind = 0;
			prefix.push_back(is_null);
		} else {
			prefix.push_back(BuildLookupValue(input.inputs[i], backings, "moraine_index_nulls"));
		}
	}

	// Optional `reverse := true` returns the rows in the opposite order.
	auto reverse_it = input.named_parameters.find("reverse");
	const bool reverse =
	    reverse_it != input.named_parameters.end() && !reverse_it->second.IsNull() && reverse_it->second.GetValue<bool>();
	bind_data->prefix_repr += reverse ? "rev" : "";

	auto handle = ResolveHandle(context, bind_data->catalog_name);
	OwnedArray<MoraineRowId> row_ids(moraine_index_nulls_free);
	MoraineError err {};
	auto code = moraine_index_nulls(handle, bind_data->schema_name.c_str(), bind_data->table_name.c_str(),
	                                bind_data->index_name.c_str(), prefix.data(), prefix.size(), reverse,
	                                row_ids.OutItems(), row_ids.OutLen(), moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	for (auto row_id : row_ids) {
		bind_data->rows.push_back(row_id);
	}

	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::UBIGINT};
	names = {"row_id", "data_file_id"};
	return bind_data;
}

struct NullsGlobalState : public duckdb::GlobalTableFunctionState {
	duckdb::idx_t offset = 0;
	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> NullsInitGlobal(duckdb::ClientContext &,
                                                                     duckdb::TableFunctionInitInput &) {
	return duckdb::make_uniq<NullsGlobalState>();
}

void NullsImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<NullsBindData>();
	auto &state = data.global_state->Cast<NullsGlobalState>();
	if (state.offset >= bind_data.rows.size()) {
		output.SetCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - state.offset);
	for (duckdb::idx_t i = 0; i < count; i++) {
		auto &row = bind_data.rows[state.offset + i];
		SetRowId(output, i, row);
	}
	state.offset += count;
	output.SetCardinality(count);
}

} // namespace

void RegisterMoraineIndexFunctions(duckdb::ExtensionLoader &loader) {
	using duckdb::LogicalType;

	duckdb::TableFunction list("moraine_indexes",
	                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, IndexesImpl,
	                           IndexesBind, IndexesInitGlobal);
	loader.RegisterFunction(list);

	duckdb::TableFunction create(
	    "moraine_index_create",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	     LogicalType::LIST(LogicalType::VARCHAR), LogicalType::BOOLEAN},
	    IndexDdlImpl, CreateBind, IndexDdlInitGlobal);
	// Optional per-column sort directions ['asc'|'desc', ...] and NULL
	// placement ['first'|'last', ...], each parallel to the columns.
	create.named_parameters["directions"] = LogicalType::LIST(LogicalType::VARCHAR);
	create.named_parameters["nulls"] = LogicalType::LIST(LogicalType::VARCHAR);
	create.named_parameters["maintenance"] = LogicalType::VARCHAR;
	// Multi-commit build, for a backfill too large for one commit, and the
	// bounds on one step of it — each step is one object-store request, so a
	// slow link wants them smaller than the default.
	create.named_parameters["staged"] = LogicalType::BOOLEAN;
	create.named_parameters["step_entries"] = LogicalType::BIGINT;
	create.named_parameters["step_bytes"] = LogicalType::BIGINT;
	loader.RegisterFunction(create);

	// (catalog, schema, table, index, then the equality key as variadic args
	// — one value per indexed column, in the index's column order.)
	duckdb::TableFunction lookup("moraine_index_lookup",
	                             {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                              LogicalType::VARCHAR},
	                             LookupImpl, LookupBind, LookupInitGlobal);
	lookup.varargs = LogicalType::ANY;
	lookup.cardinality = ExactIndexCardinality<LookupBindData>;
	loader.RegisterFunction(lookup);

	// A homogeneous scalar list addresses a one-column index. A list of
	// `row(...)` structs carries complete composite keys.
	duckdb::TableFunction in("moraine_index_in",
	                         {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                          LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	                         LookupImpl, InBind, LookupInitGlobal);
	in.cardinality = ExactIndexCardinality<LookupBindData>;
	loader.RegisterFunction(in);

	duckdb::TableFunction drop("moraine_index_drop",
	                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                            LogicalType::VARCHAR},
	                           IndexDdlImpl, DropBind, IndexDdlInitGlobal);
	loader.RegisterFunction(drop);

	// (catalog, schema, table, index, lower, upper, lower_inclusive,
	// upper_inclusive); a NULL bound is an open side.
	duckdb::TableFunction range("moraine_index_range",
	                            {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                             LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY,
	                             LogicalType::BOOLEAN, LogicalType::BOOLEAN},
	                            RangeImpl, RangeBind, RangeInitGlobal);
	range.named_parameters["reverse"] = LogicalType::BOOLEAN;
	range.cardinality = ExactIndexCardinality<RangeBindData>;
	loader.RegisterFunction(range);

	// (catalog, schema, table, index, then a leading prefix of predicates as
	// variadic args — a NULL arg is IS NULL, any other is = value).
	duckdb::TableFunction nulls("moraine_index_nulls",
	                            {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                             LogicalType::VARCHAR},
	                            NullsImpl, NullsBind, NullsInitGlobal);
	nulls.varargs = LogicalType::ANY;
	nulls.named_parameters["reverse"] = LogicalType::BOOLEAN;
	nulls.cardinality = ExactIndexCardinality<NullsBindData>;
	loader.RegisterFunction(nulls);
}

} // namespace moraine_duckdb
