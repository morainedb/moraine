// Index introspection, verification, and explicit lookups.
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "catalog.hpp"
#include "index_functions.hpp"
#include "index_cache.hpp"
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

// `is_building` is true for every state but ready, so a poisoned index —
// which no build is advancing — reads the same as one mid-build. This names
// the state itself.
const char *IndexStateName(MoraineIndexState state) {
	switch (state) {
	case MoraineIndexState_Ready:
		return "ready";
	case MoraineIndexState_Building:
		return "building";
	case MoraineIndexState_Maintaining:
		return "maintaining";
	case MoraineIndexState_Poisoned:
		return "poisoned";
	}
	throw duckdb::InternalException("moraine_indexes: unknown index state %d", static_cast<int>(state));
}

struct IndexesBindData : public duckdb::FunctionData {
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	struct Row {
		int64_t index_id;
		std::string name;
		bool unique;
		bool building;
		std::string state;
		uint64_t certified_at;
		bool has_certificate;
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
		return catalog_name == other.catalog_name && schema_name == other.schema_name && table_name == other.table_name;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> IndexesBind(duckdb::ClientContext &context,
                                                     duckdb::TableFunctionBindInput &input,
                                                     duckdb::vector<duckdb::LogicalType> &return_types,
                                                     duckdb::vector<duckdb::Identifier> &names) {
	auto bind_data = duckdb::make_uniq<IndexesBindData>();
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();

	auto handle = ResolveHandle(context, bind_data->catalog_name);
	OwnedArray<MoraineIndexDesc> descs(moraine_indexes_free);
	MoraineError err {};
	auto code = moraine_indexes(handle, bind_data->schema_name.c_str(), bind_data->table_name.c_str(), descs.OutItems(),
	                            descs.OutLen(), moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	for (auto &desc : descs) {
		bind_data->rows.push_back({static_cast<int64_t>(desc.index_id), std::string(desc.name), desc.unique,
		                           desc.building, IndexStateName(desc.state), desc.certified_at, desc.has_certificate});
	}

	return_types = {duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN,
	                duckdb::LogicalType::BOOLEAN, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::UBIGINT};
	names = {"index_id", "index_name", "is_unique", "is_building", "state", "certified_at"};
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
		output.SetChildCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - state.offset);
	for (duckdb::idx_t i = 0; i < count; i++) {
		auto &row = bind_data.rows[state.offset + i];
		output.SetValue(0, i, duckdb::Value::BIGINT(row.index_id));
		output.SetValue(1, i, duckdb::Value(row.name));
		output.SetValue(2, i, duckdb::Value::BOOLEAN(row.unique));
		output.SetValue(3, i, duckdb::Value::BOOLEAN(row.building));
		output.SetValue(4, i, duckdb::Value(row.state));
		output.SetValue(5, i,
		                row.has_certificate ? duckdb::Value::UBIGINT(row.certified_at)
		                                    : duckdb::Value(duckdb::LogicalType::UBIGINT));
	}
	state.offset += count;
	output.SetChildCardinality(count);
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

struct IndexReadBindData : public duckdb::FunctionData {
	bool cacheable = false;
	bool SupportStatementCache() const override {
		return cacheable;
	}
};

template <class BindData>
void FinishIndexBind(duckdb::ClientContext &context, BindData &data) {
	const auto bytes = data.rows.capacity() * sizeof(MoraineRowId);
	data.cacheable = data.cacheable && bytes <= 8 * 1024 * 1024;
	WriteMoraineLog(duckdb::DatabaseInstance::GetDatabase(context), duckdb::LogLevel::LOG_DEBUG,
	                "index bind resolved rows=" + std::to_string(data.rows.size()) +
	                    " retained_bytes=" + std::to_string(data.cacheable ? bytes : 0));
}

struct LookupBindData : public IndexReadBindData {
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

// A value's text tagged by type and length, so that two different argument
// lists cannot render alike: without the tag `('a', 'b,c')` and `('a,b', 'c')`
// share a rendering, as do a NULL bound and the string `'NULL'`.
std::string ValueRepr(const duckdb::Value &value) {
	auto text = value.ToString();
	return std::to_string(static_cast<int>(value.type().id())) + ":" + std::to_string(text.size()) + ":" + text;
}

} // namespace

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
		throw duckdb::InvalidInputException("%s: unsupported value type \"%s\"", caller, value.type().ToString());
	}
	return lookup;
}

namespace {

// The variadic args after the index name are the equality key: one value per
// indexed column, in the index's column order. The count must match the index
// width — the core refuses a partial key.
duckdb::unique_ptr<duckdb::FunctionData> LookupBind(duckdb::ClientContext &context,
                                                    duckdb::TableFunctionBindInput &input,
                                                    duckdb::vector<duckdb::LogicalType> &return_types,
                                                    duckdb::vector<duckdb::Identifier> &names) {
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

	auto handle = BindIndexRead(context, input, bind_data->cacheable);
	OwnedArray<MoraineRowId> row_ids(moraine_index_lookup_free);
	MoraineError err {};
	auto code = moraine_index_lookup(handle, bind_data->schema_name.c_str(), bind_data->table_name.c_str(),
	                                 bind_data->index_name.c_str(), values.data(), values.size(), row_ids.OutItems(),
	                                 row_ids.OutLen(), moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	for (auto row_id : row_ids) {
		bind_data->rows.push_back(row_id);
	}

	FinishIndexBind(context, *bind_data);
	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::UBIGINT};
	names = {"row_id", "data_file_id"};
	return bind_data;
}

// moraine_index_in: resolves a list of complete equality keys under one
// catalog read. A scalar list addresses a one-column index; a list of
// STRUCT/row values addresses a composite index.
duckdb::unique_ptr<duckdb::FunctionData> InBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                duckdb::vector<duckdb::LogicalType> &return_types,
                                                duckdb::vector<duckdb::Identifier> &names) {
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
			if (key.IsNull() && duckdb::StructType::IsStruct(key.type())) {
				continue;
			}
			auto &values = key_values.emplace_back();
			if (duckdb::StructType::IsStruct(key.type())) {
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

	auto handle = BindIndexRead(context, input, bind_data->cacheable);
	OwnedArray<MoraineRowId> row_ids(moraine_index_in_free);
	MoraineError err {};
	auto code = moraine_index_in(handle, bind_data->schema_name.c_str(), bind_data->table_name.c_str(),
	                             bind_data->index_name.c_str(), keys.data(), keys.size(), row_ids.OutItems(),
	                             row_ids.OutLen(), moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	for (auto row_id : row_ids) {
		bind_data->rows.push_back(row_id);
	}

	FinishIndexBind(context, *bind_data);
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
		output.SetChildCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - state.offset);
	for (duckdb::idx_t i = 0; i < count; i++) {
		auto &row = bind_data.rows[state.offset + i];
		SetRowId(output, i, row);
	}
	state.offset += count;
	output.SetChildCardinality(count);
}

struct RangeBindData : public IndexReadBindData {
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
                                                   duckdb::vector<duckdb::Identifier> &names) {
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
	auto build_bound = [](const duckdb::Value &value, LookupValueArena &backing, std::vector<MoraineLookupValue> &out) {
		// A whole-bound NULL is an open side, not a value: it names no column.
		if (value.IsNull()) {
			return;
		}
		duckdb::vector<duckdb::Value> columns;
		if (duckdb::StructType::IsStruct(value.type())) {
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
	const bool reverse = reverse_it != input.named_parameters.end() && !reverse_it->second.IsNull() &&
	                     reverse_it->second.GetValue<bool>();
	bind_data->bounds_repr += reverse ? "|rev" : "";

	auto handle = BindIndexRead(context, input, bind_data->cacheable);
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

	FinishIndexBind(context, *bind_data);
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
		output.SetChildCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - state.offset);
	for (duckdb::idx_t i = 0; i < count; i++) {
		auto &row = bind_data.rows[state.offset + i];
		SetRowId(output, i, row);
	}
	state.offset += count;
	output.SetChildCardinality(count);
}

struct NullsBindData : public IndexReadBindData {
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
                                                   duckdb::vector<duckdb::Identifier> &names) {
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
	const bool reverse = reverse_it != input.named_parameters.end() && !reverse_it->second.IsNull() &&
	                     reverse_it->second.GetValue<bool>();
	bind_data->prefix_repr += reverse ? "rev" : "";

	auto handle = BindIndexRead(context, input, bind_data->cacheable);
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

	FinishIndexBind(context, *bind_data);
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
		output.SetChildCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, bind_data.rows.size() - state.offset);
	for (duckdb::idx_t i = 0; i < count; i++) {
		auto &row = bind_data.rows[state.offset + i];
		SetRowId(output, i, row);
	}
	state.offset += count;
	output.SetChildCardinality(count);
}

} // namespace

bool IsIndexRead(const std::string &name) {
	return name == "moraine_index_lookup" || name == "moraine_index_in" || name == "moraine_index_range" ||
	       name == "moraine_index_nulls";
}

const std::vector<MoraineRowId> *IndexReadRows(const duckdb::TableFunction &function,
                                               const duckdb::FunctionData *bind_data) {
	if (!bind_data) {
		return nullptr;
	}
	if (function.name == "moraine_index_lookup" || function.name == "moraine_index_in") {
		return &bind_data->Cast<LookupBindData>().rows;
	}
	if (function.name == "moraine_index_range") {
		return &bind_data->Cast<RangeBindData>().rows;
	}
	if (function.name == "moraine_index_nulls") {
		return &bind_data->Cast<NullsBindData>().rows;
	}
	return nullptr;
}

template <class BindData>
void CopyIndexTable(const duckdb::FunctionData &data, std::string &catalog, std::string &schema, std::string &table) {
	auto &bound = data.Cast<BindData>();
	catalog = bound.catalog_name;
	schema = bound.schema_name;
	table = bound.table_name;
}

bool IndexReadTable(const duckdb::TableFunction &function, const duckdb::FunctionData *data, std::string &catalog,
                    std::string &schema, std::string &table) {
	if (!data) {
		return false;
	}
	if (function.function == LookupImpl) {
		CopyIndexTable<LookupBindData>(*data, catalog, schema, table);
	} else if (function.function == RangeImpl) {
		CopyIndexTable<RangeBindData>(*data, catalog, schema, table);
	} else if (function.function == NullsImpl) {
		CopyIndexTable<NullsBindData>(*data, catalog, schema, table);
	} else {
		return false;
	}
	return true;
}

// moraine_index_verify: recomputes the entries from the rows and reports the
// difference. One row of counts, then the snapshot the index is certified
// at, NULL when it is not. A clean index reports zeros for missing, dead
// and mismatched. A read-write attach records the outcome; a read-only one
// reports without writing.

struct IndexVerifyBindData : public duckdb::FunctionData {
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	std::string index_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<IndexVerifyBindData>();
		*result = *this;
		return result;
	}
	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<IndexVerifyBindData>();
		return catalog_name == other.catalog_name && schema_name == other.schema_name &&
		       table_name == other.table_name && index_name == other.index_name;
	}
};

struct IndexVerifyGlobalState : public duckdb::GlobalTableFunctionState {
	MoraineIndexVerification report {};
	bool emitted = false;

	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

// The pass runs once, at execution start, so a cancelled statement stops it
// rather than a later chunk.
duckdb::unique_ptr<duckdb::GlobalTableFunctionState> IndexVerifyInitGlobal(duckdb::ClientContext &context,
                                                                           duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<IndexVerifyBindData>();
	auto handle = ResolveHandle(context, bind_data.catalog_name);
	auto state = duckdb::make_uniq<IndexVerifyGlobalState>();
	MoraineError err {};
	auto code =
	    moraine_index_verify(handle, bind_data.schema_name.c_str(), bind_data.table_name.c_str(),
	                         bind_data.index_name.c_str(), &state->report, moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	return std::move(state);
}

void IndexVerifyImpl(duckdb::ClientContext &, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &state = data.global_state->Cast<IndexVerifyGlobalState>();
	if (state.emitted) {
		output.SetChildCardinality(0);
		return;
	}
	auto &report = state.report;
	const uint64_t counts[] = {report.stored, report.expected, report.missing, report.dead, report.mismatched};
	for (duckdb::idx_t column = 0; column < 5; column++) {
		output.SetValue(column, 0, duckdb::Value::UBIGINT(counts[column]));
	}
	output.SetValue(5, 0,
	                report.has_certificate ? duckdb::Value::UBIGINT(report.certified_at)
	                                       : duckdb::Value(duckdb::LogicalType::UBIGINT));
	output.SetChildCardinality(1);
	state.emitted = true;
}

duckdb::unique_ptr<duckdb::FunctionData> VerifyBind(duckdb::ClientContext &, duckdb::TableFunctionBindInput &input,
                                                    duckdb::vector<duckdb::LogicalType> &return_types,
                                                    duckdb::vector<duckdb::Identifier> &names) {
	auto bind_data = duckdb::make_uniq<IndexVerifyBindData>();
	bind_data->catalog_name = input.inputs[0].GetValue<std::string>();
	bind_data->schema_name = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	bind_data->index_name = input.inputs[3].GetValue<std::string>();
	return_types = {duckdb::LogicalType::UBIGINT, duckdb::LogicalType::UBIGINT, duckdb::LogicalType::UBIGINT,
	                duckdb::LogicalType::UBIGINT, duckdb::LogicalType::UBIGINT, duckdb::LogicalType::UBIGINT};
	names = {"stored", "expected", "missing", "dead", "mismatched", "certified_at"};
	return bind_data;
}

void RegisterMoraineIndexFunctions(duckdb::ExtensionLoader &loader) {
	using duckdb::LogicalType;

	duckdb::TableFunction list("moraine_indexes", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                           IndexesImpl, IndexesBind, IndexesInitGlobal);
	loader.RegisterFunction(list);

	// (catalog, schema, table, index, then the equality key as variadic args
	// — one value per indexed column, in the index's column order.)
	duckdb::TableFunction lookup(
	    "moraine_index_lookup",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LookupImpl,
	    LookupBind, LookupInitGlobal);
	lookup.GetSignature().AddArgs("keys", LogicalType::ANY);
	lookup.cardinality = ExactIndexCardinality<LookupBindData>;
	loader.RegisterFunction(lookup);

	// A homogeneous scalar list addresses a one-column index. A list of
	// `row(...)` structs carries complete composite keys.
	duckdb::TableFunction in("moraine_index_in",
	                         {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                          LogicalType::LIST(LogicalType::ANY)},
	                         LookupImpl, InBind, LookupInitGlobal);
	in.cardinality = ExactIndexCardinality<LookupBindData>;
	loader.RegisterFunction(in);

	duckdb::TableFunction verify(
	    "moraine_index_verify",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, IndexVerifyImpl,
	    VerifyBind, IndexVerifyInitGlobal);
	loader.RegisterFunction(verify);

	// (catalog, schema, table, index, lower, upper, lower_inclusive,
	// upper_inclusive); a NULL bound is an open side.
	duckdb::TableFunction range("moraine_index_range",
	                            {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                             LogicalType::ANY, LogicalType::ANY, LogicalType::BOOLEAN, LogicalType::BOOLEAN},
	                            RangeImpl, RangeBind, RangeInitGlobal);
	range.GetSignature().WithTypedKwargs(
	    "options", [](duckdb::TypedKwargs &options) { options.Add("reverse", LogicalType::BOOLEAN); });
	range.cardinality = ExactIndexCardinality<RangeBindData>;
	loader.RegisterFunction(range);

	// (catalog, schema, table, index, then a leading prefix of predicates as
	// variadic args — a NULL arg is IS NULL, any other is = value).
	duckdb::TableFunction nulls(
	    "moraine_index_nulls", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    NullsImpl, NullsBind, NullsInitGlobal);
	nulls.GetSignature().AddArgs("keys", LogicalType::ANY);
	nulls.GetSignature().WithTypedKwargs(
	    "options", [](duckdb::TypedKwargs &options) { options.Add("reverse", LogicalType::BOOLEAN); });
	nulls.cardinality = ExactIndexCardinality<NullsBindData>;
	loader.RegisterFunction(nulls);
}

} // namespace moraine_duckdb
