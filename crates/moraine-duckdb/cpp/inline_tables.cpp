#include "inline_tables.hpp"

#include "catalog.hpp"
#include "metadata_tables.hpp"
#include "owned_array.hpp"
#include "transaction_manager.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <optional>

namespace moraine_duckdb {

namespace {

duckdb::Value Bigint(uint64_t v) {
	return duckdb::Value::BIGINT(static_cast<int64_t>(v));
}

duckdb::Value OptBigint(bool has, uint64_t v) {
	if (!has) {
		return duckdb::Value(duckdb::LogicalType::BIGINT);
	}
	return Bigint(v);
}

uint64_t CellAsU64(const duckdb::Value &v) {
	return static_cast<uint64_t>(v.GetValue<int64_t>());
}

} // namespace

std::string InlinedDataTableName(uint64_t table_id, uint64_t schema_version) {
	return "ducklake_inlined_data_" + std::to_string(table_id) + "_" + std::to_string(schema_version);
}

std::string InlinedDeleteTableName(uint64_t table_id) {
	return "ducklake_inlined_delete_" + std::to_string(table_id);
}

namespace {

// Parses a run of ASCII digits from `s` starting at `pos`, requiring at
// least one digit and consuming to the end of `s`. Returns nullopt on any
// non-digit, empty digit run, or overflow.
std::optional<uint64_t> ParseTrailingU64(const std::string &s, size_t pos) {
	if (pos >= s.size()) {
		return std::nullopt;
	}
	uint64_t value = 0;
	for (size_t i = pos; i < s.size(); i++) {
		char c = s[i];
		if (c < '0' || c > '9') {
			return std::nullopt;
		}
		auto digit = static_cast<uint64_t>(c - '0');
		if (value > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
			return std::nullopt;
		}
		value = value * 10 + digit;
	}
	return value;
}

} // namespace

std::optional<InlinedDataTableId> ParseInlinedDataTableName(const std::string &name) {
	static const std::string prefix = "ducklake_inlined_data_";
	if (name.rfind(prefix, 0) != 0) {
		return std::nullopt;
	}
	auto rest = name.substr(prefix.size());
	auto underscore = rest.rfind('_');
	if (underscore == std::string::npos || underscore == 0) {
		return std::nullopt;
	}
	auto table_id = ParseTrailingU64(rest.substr(0, underscore), 0);
	auto schema_version = ParseTrailingU64(rest, underscore + 1);
	if (!table_id || !schema_version) {
		return std::nullopt;
	}
	return InlinedDataTableId {*table_id, *schema_version};
}

std::optional<uint64_t> ParseInlinedDeleteTableName(const std::string &name) {
	static const std::string prefix = "ducklake_inlined_delete_";
	if (name.rfind(prefix, 0) != 0) {
		return std::nullopt;
	}
	return ParseTrailingU64(name, prefix.size());
}

MoraineArrowBytes EncodeInlineSchema(duckdb::ClientContext &context,
                                     const std::vector<DecodedInlineColumn> &user_columns) {
	duckdb::vector<duckdb::LogicalType> types;
	duckdb::vector<std::string> names;
	types.reserve(user_columns.size());
	names.reserve(user_columns.size());
	for (auto &col : user_columns) {
		// The core refuses this column too, but not in time: DuckLake builds the
		// inlined data table while flushing the CREATE TABLE, before the
		// `ducklake_column` rows the core validates reach a commit. Without this
		// refusal the user sees DuckDB's bare "Unsupported Arrow type VARIANT"
		// instead, which names neither moraine nor a way forward.
		if (col.type.id() == duckdb::LogicalTypeId::VARIANT) {
			throw duckdb::NotImplementedException(
			    "moraine: column \"%s\" is VARIANT, which moraine cannot store — its inline "
			    "data is serialized through Arrow, and DuckDB's Arrow format has no VARIANT "
			    "support. Use JSON (or another type) instead.",
			    col.name);
		}
		types.push_back(col.type);
		names.push_back(col.name);
	}

	// Lossless: encode UUID (and other extension types) type-faithfully — a
	// UUID as a 16-byte blob, byte-identical to how DuckDB writes it to
	// Parquet — so an equality index derives the same value whether a row is
	// inlined or written to a data file. The lossy default would render a
	// UUID as a string, diverging from the Parquet path.
	auto options = context.GetClientProperties();
	options.arrow_lossless_conversion = true;
	ArrowSchema c_schema;
	duckdb::ArrowConverter::ToArrowSchema(&c_schema, types, names, options);

	MoraineArrowBytes bytes {};
	MoraineError err {};
	// Consumes `c_schema` (releases DuckDB's buffers); do not release it here.
	if (moraine_arrow_encode_schema(&c_schema, &bytes, &err) != 0) {
		ThrowMoraineError(err);
	}
	return bytes;
}

std::vector<DecodedInlineColumn> DecodeInlineSchema(duckdb::ClientContext &context, const uint8_t *data, size_t len) {
	ArrowSchema c_schema;
	ArrowArray c_array;
	MoraineError err {};
	if (moraine_arrow_decode_stream(data, len, &c_schema, &c_array, &err) != 0) {
		ThrowMoraineError(err);
	}
	// A schema-only stream still yields a (zero-row) array; release it.
	if (c_array.release) {
		c_array.release(&c_array);
	}

	std::vector<DecodedInlineColumn> result;
	result.reserve(static_cast<size_t>(c_schema.n_children));
	for (int64_t i = 0; i < c_schema.n_children; i++) {
		ArrowSchema &child = *c_schema.children[i];
		std::string name = child.name ? child.name : "";
		auto arrow_type = duckdb::ArrowType::GetTypeFromSchema(context, child);
		result.push_back(DecodedInlineColumn {std::move(name), arrow_type->GetDuckType()});
	}
	if (c_schema.release) {
		c_schema.release(&c_schema);
	}
	return result;
}

MoraineArrowBytes EncodeInlineChunkRows(duckdb::ClientContext &context, duckdb::DataChunk &chunk,
                                        duckdb::idx_t user_col_start, duckdb::idx_t row_offset,
                                        duckdb::idx_t row_count) {
	auto user_count = chunk.ColumnCount() - user_col_start;
	duckdb::vector<duckdb::LogicalType> types;
	duckdb::vector<std::string> names;
	types.reserve(user_count);
	names.reserve(user_count);
	for (duckdb::idx_t i = 0; i < user_count; i++) {
		types.push_back(chunk.data[user_col_start + i].GetType());
		// Column identity comes from the `inline/schema` record; these names
		// only ride the chunk stream and are never read back.
		names.push_back("c" + std::to_string(i));
	}

	// Export just the user columns of just this row range: `ToArrowArray`
	// serializes every column and every row of the chunk it is handed, so
	// slice the tail into a standalone view.
	duckdb::DataChunk user_chunk;
	user_chunk.InitializeEmpty(types);
	for (duckdb::idx_t i = 0; i < user_count; i++) {
		user_chunk.data[i].Slice(chunk.data[user_col_start + i], row_offset, row_offset + row_count);
	}
	user_chunk.SetCardinality(row_count);

	// Lossless, matching the inline schema registration: a UUID encodes as a
	// 16-byte blob (as in Parquet), not a string, so index maintenance sees
	// the same value across the inline and data-file paths.
	auto options = context.GetClientProperties();
	options.arrow_lossless_conversion = true;
	ArrowSchema c_schema;
	duckdb::ArrowConverter::ToArrowSchema(&c_schema, types, names, options);
	ArrowArray c_array;
	duckdb::ArrowConverter::ToArrowArray(user_chunk, &c_array, options, {});

	MoraineArrowBytes bytes {};
	MoraineError err {};
	// Consumes both `c_schema` and `c_array`; do not release them here.
	if (moraine_arrow_encode_chunk(&c_schema, &c_array, &bytes, &err) != 0) {
		ThrowMoraineError(err);
	}
	return bytes;
}

DecodedInlineSchema::DecodedInlineSchema(const uint8_t *schema_ipc, size_t schema_ipc_len) : handle_(nullptr) {
	MoraineError err {};
	if (moraine_arrow_schema_decode(schema_ipc, schema_ipc_len, &handle_, &err) != MORAINE_OK) {
		ThrowMoraineError(err);
	}
}

DecodedInlineSchema::~DecodedInlineSchema() {
	moraine_arrow_schema_free(handle_);
}

std::vector<duckdb::unique_ptr<duckdb::DataChunk>>
DecodeInlineChunkPieces(duckdb::ClientContext &context, const DecodedInlineSchema &schema, MoraineInlineChunk &chunk,
                        const std::vector<duckdb::LogicalType> &user_types) {
	ArrowSchema c_schema;
	ArrowArray c_array;
	MoraineError err {};
	if (moraine_arrow_decode_inline_chunk_with_schema(schema.Get(), &chunk, &c_schema, &c_array, &err) != 0) {
		ThrowMoraineError(err);
	}

	// Build a per-column `ArrowType` map from the stream's own embedded schema.
	duckdb::ArrowTableSchema arrow_table;
	duckdb::ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, c_schema);
	auto &columns = arrow_table.GetColumns();
	if (columns.size() != user_types.size()) {
		if (c_schema.release) {
			c_schema.release(&c_schema);
		}
		if (c_array.release) {
			c_array.release(&c_array);
		}
		throw duckdb::InternalException("moraine: inline chunk has %llu columns, expected %llu — schema/body mismatch",
		                                static_cast<unsigned long long>(columns.size()),
		                                static_cast<unsigned long long>(user_types.size()));
	}

	// Drive DuckDB's own record-batch importer (the arrow-scan path), which
	// applies each column's validity and offset. The decoded array is a
	// struct whose children are the columns; the scan state owns it and
	// releases it once, at end of scope, after every value is copied out.
	// `arrow_scan_is_projected = false` maps output columns 1:1 to the arrow
	// columns, so no projection `column_ids` are needed.
	auto chunk_wrapper = duckdb::make_uniq<duckdb::ArrowArrayWrapper>();
	chunk_wrapper->arrow_array = c_array;
	auto total = static_cast<duckdb::idx_t>(chunk_wrapper->arrow_array.length);

	duckdb::ArrowScanLocalState scan_state(std::move(chunk_wrapper), context);
	for (duckdb::idx_t col = 0; col < user_types.size(); col++) {
		scan_state.column_ids.push_back(col);
	}

	duckdb::vector<duckdb::LogicalType> chunk_types(user_types.begin(), user_types.end());
	std::vector<duckdb::unique_ptr<duckdb::DataChunk>> pieces;
	pieces.reserve((total + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE);
	while (scan_state.chunk_offset < total) {
		auto size = std::min<duckdb::idx_t>(total - scan_state.chunk_offset, STANDARD_VECTOR_SIZE);
		auto out = duckdb::make_uniq<duckdb::DataChunk>();
		out->Initialize(context, chunk_types);
		// `ArrowToDuckDB` reads `output.size()` as the row count to convert, so
		// the cardinality must be set before the call, not after.
		out->SetCardinality(size);
		duckdb::ArrowTableFunction::ArrowToDuckDB(scan_state, columns, *out, /* arrow_scan_is_projected */ false);
		pieces.push_back(std::move(out));
		scan_state.chunk_offset += size;
	}

	if (c_schema.release) {
		c_schema.release(&c_schema);
	}
	return pieces;
}

namespace {

duckdb::CreateTableInfo BuildInlineDataTableInfo(duckdb::SchemaCatalogEntry &schema, uint64_t table_id,
                                                 uint64_t schema_version,
                                                 const std::vector<DecodedInlineColumn> &user_columns) {
	duckdb::CreateTableInfo info(schema, InlinedDataTableName(table_id, schema_version));
	info.columns.AddColumn(duckdb::ColumnDefinition("row_id", duckdb::LogicalType::BIGINT));
	info.columns.AddColumn(duckdb::ColumnDefinition("begin_snapshot", duckdb::LogicalType::BIGINT));
	info.columns.AddColumn(duckdb::ColumnDefinition("end_snapshot", duckdb::LogicalType::BIGINT));
	for (auto &col : user_columns) {
		info.columns.AddColumn(duckdb::ColumnDefinition(col.name, col.type));
	}
	return info;
}

// One row of the scan's current window: the three metadata columns
// verbatim, plus where the row's user values live in the window's decoded
// chunks. The user values themselves are never copied out of those
// chunks — the scan slices them across column-wise.
struct InlineDataRow {
	uint64_t row_id;
	uint64_t begin_snapshot;
	bool has_end_snapshot;
	uint64_t end_snapshot;
	// Index into the window's `pieces`.
	size_t piece;
	duckdb::idx_t row_in_piece;
};

// What a physical rowid resolves back to.
struct InlineLedgerRow {
	uint64_t row_id;
	uint64_t begin_snapshot;
};

// The rows a statement's scan has emitted, indexed by physical rowid.
// Shared between that scan and the DML operator resolving its rowids, so
// both agree on what position `n` was, and filled only when the plan asked
// for rowids — UPDATE and DELETE do, the flush's read does not.
struct InlineRowLedger {
	std::vector<InlineLedgerRow> rows;
};

// `row_id`, `begin_snapshot`, `end_snapshot` precede the user columns in
// `ducklake_inlined_data_<t>_<v>`.
constexpr duckdb::column_t kInlineUserColumnStart = 3;

// Rows one refill pulls from the core. Sized so a window's chunk bodies
// fill the core's point-read concurrency rather than arriving one round
// trip at a time, while a whole-table read still decodes one window's
// worth at once.
constexpr duckdb::idx_t kInlineScanWindowRows = 8 * STANDARD_VECTOR_SIZE;

// What one bound `ducklake_inlined_data_<t>_<v>` statement needs: where to
// open its scan, and the ledger its own DML resolves rowids against.
struct InlineDataScanBindData : public duckdb::FunctionData {
	MoraineCatalogHandle *handle = nullptr;
	uint64_t table_id = 0;
	uint64_t schema_version = 0;
	// This entry's user columns, in order — what a chunk body decodes to.
	std::vector<duckdb::LogicalType> user_types;
	// Shared rather than deep-copied: `Copy` is called per plan, and a DML
	// must resolve rowids against what its own plan's scan emitted.
	std::shared_ptr<InlineRowLedger> ledger;
	// Exposed through `get_bind_info` so `LogicalGet::GetTable()` resolves
	// this entry: the binder's UPDATE/DELETE paths require a resolvable base
	// table.
	duckdb::optional_ptr<duckdb::TableCatalogEntry> table_entry;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<InlineDataScanBindData>();
		result->handle = handle;
		result->table_id = table_id;
		result->schema_version = schema_version;
		result->user_types = user_types;
		result->ledger = ledger;
		result->table_entry = table_entry;
		return result;
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		auto &that = other.Cast<InlineDataScanBindData>();
		return ledger == that.ledger && table_entry.get() == that.table_entry.get();
	}
};

// One open scan: the core cursor, the window it last served, and how far
// emission has walked that window. Single-threaded, so a row's physical
// rowid is its position in the order the windows arrive.
struct InlineDataScanGlobalState : public duckdb::GlobalTableFunctionState {
	InlineDataScanGlobalState() : schemas(moraine_inline_schemas_free) {
	}
	~InlineDataScanGlobalState() override {
		moraine_inline_scan_close(cursor);
	}

	MoraineInlineScanCursor *cursor = nullptr;
	// The columns DuckDB asked for, by index into the table's column list,
	// in output order. Empty for a zero-column probe, which DuckDB emits
	// only because this function advertises projection pushdown.
	std::vector<duckdb::column_t> column_ids;
	// Whether any user column was asked for. A scan of the metadata columns
	// alone — the flush's own `DELETE`, or a `COUNT(*)` — reads no body.
	bool wants_user_columns = false;
	bool wants_row_ids = false;

	std::vector<InlineDataRow> rows;
	std::vector<duckdb::unique_ptr<duckdb::DataChunk>> pieces;
	// Position within `rows`, and rows emitted across every window so far.
	duckdb::idx_t offset = 0;
	duckdb::idx_t emitted = 0;

	// This version's schema-only stream (`inline/schema`), read on the first
	// body decode and not before, then reused by every window. `schemas`
	// owns the bytes `decoded_schema` is built from, so it outlives every
	// decode.
	OwnedArray<MoraineInlineSchemaRow> schemas;
	duckdb::unique_ptr<DecodedInlineSchema> decoded_schema;

	duckdb::idx_t MaxThreads() const override {
		return 1;
	}
};

// This entry serves one `(table_id, schema_version)`; body-only chunks of
// that version decode against its schema-only stream. A flushed table has
// no body left to touch — its registration outlives its rows, so every
// read of the base table scans it.
const DecodedInlineSchema &InlineScanSchema(duckdb::ClientContext &context,
                                            const InlineDataScanBindData &bind_data,
                                            InlineDataScanGlobalState &state) {
	if (state.decoded_schema) {
		return *state.decoded_schema;
	}
	MoraineError err {};
	if (moraine_inline_schemas(bind_data.handle, bind_data.table_id, state.schemas.OutItems(), state.schemas.OutLen(),
	                           moraine_shim_is_interrupted, &context, &err) != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	for (auto &s : state.schemas) {
		if (s.schema_version == bind_data.schema_version) {
			state.decoded_schema = duckdb::make_uniq<DecodedInlineSchema>(s.arrow_schema, s.arrow_schema_len);
			return *state.decoded_schema;
		}
	}
	throw duckdb::InternalException("moraine: no inline schema recorded for table %llu schema version %llu",
	                                static_cast<unsigned long long>(bind_data.table_id),
	                                static_cast<unsigned long long>(bind_data.schema_version));
}

// Pulls the scan's next window and decodes the chunks its rows reference,
// replacing the window before it; false once the scan is exhausted.
bool RefillInlineWindow(duckdb::ClientContext &context, const InlineDataScanBindData &bind_data,
                        InlineDataScanGlobalState &state) {
	// A window's rows and the bodies they reference are owned together and
	// freed by the one moraine_inline_scan_free call below, pass or throw.
	struct WindowGuard {
		MoraineInlineRow *rows = nullptr;
		size_t rows_len = 0;
		MoraineInlineChunk *chunks = nullptr;
		size_t chunks_len = 0;
		~WindowGuard() {
			moraine_inline_scan_free(rows, rows_len, chunks, chunks_len);
		}
	} window;
	MoraineError err {};
	auto code = moraine_inline_scan_next(state.cursor, kInlineScanWindowRows, &window.rows, &window.rows_len,
	                                     &window.chunks, &window.chunks_len, moraine_shim_is_interrupted, &context,
	                                     &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	state.rows.clear();
	state.pieces.clear();
	state.offset = 0;
	if (window.rows_len == 0) {
		return false;
	}
	state.rows.reserve(window.rows_len);

	// Each chunk this window references decodes once, on first use, however
	// many of the window's rows it holds. `first_piece[c]` is where chunk
	// `c`'s pieces start in `pieces`; `npos` means "not decoded yet".
	std::vector<size_t> first_piece(window.chunks_len, std::numeric_limits<size_t>::max());
	std::vector<duckdb::idx_t> chunk_rows(window.chunks_len, 0);
	for (size_t i = 0; i < window.rows_len; i++) {
		auto &r = window.rows[i];
		// The core scoped the scan to this version; another version's chunk
		// would decode against the wrong schema.
		if (r.schema_version != bind_data.schema_version) {
			throw duckdb::InternalException("moraine: a version-scoped inline scan returned a row of another version");
		}
		if (r.chunk_index >= window.chunks_len) {
			throw duckdb::InternalException("moraine: inline scan chunk index out of range");
		}

		size_t piece = 0;
		duckdb::idx_t row_in_piece = 0;
		if (state.wants_user_columns) {
			if (first_piece[r.chunk_index] == std::numeric_limits<size_t>::max()) {
				auto &chunk = window.chunks[r.chunk_index];
				auto decoded = DecodeInlineChunkPieces(context, InlineScanSchema(context, bind_data, state), chunk,
				                                       bind_data.user_types);
				first_piece[r.chunk_index] = state.pieces.size();
				chunk_rows[r.chunk_index] = 0;
				for (auto &p : decoded) {
					chunk_rows[r.chunk_index] += p->size();
					state.pieces.push_back(std::move(p));
				}
			}
			if (r.offset_in_chunk >= chunk_rows[r.chunk_index]) {
				throw duckdb::InternalException("moraine: inline scan row offset out of range");
			}
			piece = first_piece[r.chunk_index] + r.offset_in_chunk / STANDARD_VECTOR_SIZE;
			row_in_piece = r.offset_in_chunk % STANDARD_VECTOR_SIZE;
		}

		state.rows.push_back(
		    InlineDataRow {r.row_id, r.begin_snapshot, r.has_end_snapshot, r.end_snapshot, piece, row_in_piece});
	}

	return true;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InlineDataScanInitGlobal(duckdb::ClientContext &context,
                                                                              duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<InlineDataScanBindData>();
	auto state = duckdb::make_uniq<InlineDataScanGlobalState>();
	state->column_ids = input.column_ids;
	for (auto col_id : state->column_ids) {
		if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
			state->wants_row_ids = true;
			continue;
		}
		if (!duckdb::IsVirtualColumn(col_id) && col_id >= kInlineUserColumnStart) {
			state->wants_user_columns = true;
		}
	}

	// The `ForFlush` variant at the maximum snapshot selects every row of
	// this version, so DuckDB's query engine applies the WHERE clause; the
	// shim serves raw rows, never interprets the predicate. The scan is
	// version-scoped in the core, so a schema-evolved table's other
	// versions cost this entry neither rows nor chunk bodies.
	MoraineError err {};
	auto code = moraine_inline_scan_open(bind_data.handle, bind_data.table_id, /* SCAN_FOR_FLUSH */ 3,
	                                     std::numeric_limits<uint64_t>::max(), 0, bind_data.schema_version,
	                                     state->wants_user_columns, &state->cursor, moraine_shim_is_interrupted,
	                                     &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}

	return state;
}

duckdb::BindInfo InlineDataScanBindInfo(const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
	auto &data = bind_data->Cast<InlineDataScanBindData>();
	duckdb::BindInfo info(duckdb::ScanType::TABLE);
	info.table = data.table_entry;
	return info;
}

// The three metadata columns, written straight into the output's flat
// vectors — no `Value` in the loop.
void EmitMetadataColumn(const std::vector<InlineDataRow> &rows, duckdb::idx_t offset, duckdb::idx_t count,
                        duckdb::column_t col_id, duckdb::Vector &target) {
	auto data = duckdb::FlatVector::GetData<int64_t>(target);
	auto &validity = duckdb::FlatVector::Validity(target);
	for (duckdb::idx_t out_row = 0; out_row < count; out_row++) {
		auto &row = rows[offset + out_row];
		switch (col_id) {
		case 0:
			data[out_row] = static_cast<int64_t>(row.row_id);
			break;
		case 1:
			data[out_row] = static_cast<int64_t>(row.begin_snapshot);
			break;
		default:
			if (!row.has_end_snapshot) {
				validity.SetInvalid(out_row);
				break;
			}
			data[out_row] = static_cast<int64_t>(row.end_snapshot);
			break;
		}
	}
}

// One user column, copied run by run out of the window's decoded chunks. A
// run is a maximal stretch of output rows that is also contiguous in one
// source piece, which in practice is most of the chunk: the scan orders by
// `row_id`, and row ids within a chunk follow insertion order.
void EmitUserColumn(const InlineDataScanGlobalState &state, duckdb::idx_t offset, duckdb::idx_t count,
                    duckdb::idx_t user_col, duckdb::Vector &target) {
	duckdb::idx_t at = 0;
	while (at < count) {
		auto &first = state.rows[offset + at];
		duckdb::idx_t run = 1;
		while (at + run < count) {
			auto &next = state.rows[offset + at + run];
			if (next.piece != first.piece || next.row_in_piece != first.row_in_piece + run) {
				break;
			}
			run++;
		}
		duckdb::VectorOperations::Copy(state.pieces[first.piece]->data[user_col], target, first.row_in_piece + run,
		                               first.row_in_piece, at);
		at += run;
	}
}

void InlineDataScanImpl(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<InlineDataScanBindData>();
	auto &state = data.global_state->Cast<InlineDataScanGlobalState>();
	if (state.offset >= state.rows.size() && !RefillInlineWindow(context, bind_data, state)) {
		output.SetCardinality(0);
		return;
	}
	duckdb::idx_t count = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.offset);

	for (duckdb::idx_t out_col = 0; out_col < state.column_ids.size(); out_col++) {
		auto col_id = state.column_ids[out_col];
		auto &target = output.data[out_col];
		if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
			// Physical rowids distinguish historical versions sharing a logical id.
			auto rowids = duckdb::FlatVector::GetData<int64_t>(target);
			for (duckdb::idx_t out_row = 0; out_row < count; out_row++) {
				rowids[out_row] = static_cast<int64_t>(state.emitted + out_row);
			}
			continue;
		}
		if (duckdb::IsVirtualColumn(col_id) || col_id >= kInlineUserColumnStart + bind_data.user_types.size()) {
			// Any other virtual column has no synthesized value; serve an
			// untyped NULL rather than read out of bounds.
			target.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
			duckdb::ConstantVector::SetNull(target, true);
			continue;
		}
		if (col_id < kInlineUserColumnStart) {
			EmitMetadataColumn(state.rows, state.offset, count, col_id, target);
			continue;
		}
		EmitUserColumn(state, state.offset, count, col_id - kInlineUserColumnStart, target);
	}

	// A rowid this scan hands out must resolve back to its row for the
	// length of the statement, whatever window it came from.
	if (state.wants_row_ids && bind_data.ledger) {
		auto &ledger = bind_data.ledger->rows;
		if (ledger.size() < state.emitted + count) {
			ledger.resize(state.emitted + count);
		}
		for (duckdb::idx_t out_row = 0; out_row < count; out_row++) {
			auto &row = state.rows[state.offset + out_row];
			ledger[state.emitted + out_row] = InlineLedgerRow {row.row_id, row.begin_snapshot};
		}
	}

	state.offset += count;
	state.emitted += count;
	output.SetCardinality(count);
}

} // namespace

MoraineInlineDataTableEntry::MoraineInlineDataTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
                                                         duckdb::CreateTableInfo &info, MoraineCatalogHandle *handle,
                                                         uint64_t table_id, uint64_t schema_version)
    : duckdb::TableCatalogEntry(catalog, schema, info), handle_(handle), table_id_(table_id),
      schema_version_(schema_version) {
}

duckdb::unique_ptr<duckdb::BaseStatistics> MoraineInlineDataTableEntry::GetStatistics(duckdb::ClientContext &,
                                                                                      duckdb::column_t) {
	throw duckdb::NotImplementedException("moraine: column statistics are not supported yet");
}

std::vector<duckdb::LogicalType> MoraineInlineDataTableEntry::UserColumnTypes() const {
	std::vector<duckdb::LogicalType> types;
	duckdb::idx_t index = 0;
	for (auto &col : GetColumns().Logical()) {
		if (index >= 3) {
			types.push_back(col.Type());
		}
		index++;
	}
	return types;
}

duckdb::TableFunction
MoraineInlineDataTableEntry::GetScanFunction(duckdb::ClientContext &,
                                             duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	// Nothing is read here: the scan opens, and its first window arrives,
	// when the plan initializes it — so a bind that never scans, which is
	// how DuckLake probes for the table, costs no store read at all.
	auto scan_bind_data = duckdb::make_uniq<InlineDataScanBindData>();
	scan_bind_data->handle = handle_;
	scan_bind_data->table_id = table_id_;
	scan_bind_data->schema_version = schema_version_;
	scan_bind_data->user_types = UserColumnTypes();
	scan_bind_data->ledger = std::make_shared<InlineRowLedger>();
	scan_bind_data->table_entry = this;
	bind_data = std::move(scan_bind_data);

	duckdb::TableFunction function("moraine_inline_data_scan", {}, InlineDataScanImpl, nullptr,
	                               InlineDataScanInitGlobal, nullptr);
	// Required for the zero-real-column probe shape, and real projection
	// pushdown falls out of the same mechanism — which matters more here than
	// for a metadata table: an unasked-for user column is never copied.
	function.projection_pushdown = true;
	// Resolves `LogicalGet::GetTable()` so UPDATE/DELETE bind against this
	// entry.
	function.get_bind_info = InlineDataScanBindInfo;
	return function;
}

duckdb::TableStorageInfo MoraineInlineDataTableEntry::GetStorageInfo(duckdb::ClientContext &) {
	return duckdb::TableStorageInfo();
}

MoraineInlineDeleteTableEntry::MoraineInlineDeleteTableEntry(duckdb::Catalog &catalog,
                                                             duckdb::SchemaCatalogEntry &schema,
                                                             duckdb::CreateTableInfo &info,
                                                             MoraineCatalogHandle *handle, uint64_t table_id)
    : duckdb::TableCatalogEntry(catalog, schema, info), handle_(handle), table_id_(table_id) {
}

duckdb::unique_ptr<duckdb::BaseStatistics> MoraineInlineDeleteTableEntry::GetStatistics(duckdb::ClientContext &,
                                                                                        duckdb::column_t) {
	throw duckdb::NotImplementedException("moraine: column statistics are not supported yet");
}

// The `(file_id, row_id, begin_snapshot)` rows of
// `ducklake_inlined_delete_<table_id>`. Shared by the scan and by the
// DELETE that resolves rowids back into records, so both see one
// definition of what the table holds — a rowid is an index into this
// output, and two independent materializations could disagree on the
// order.
std::vector<std::vector<duckdb::Value>> ProvideInlineFileDeleteRows(duckdb::ClientContext &context,
                                                                    MoraineCatalogHandle *handle, uint64_t table_id) {
	OwnedArray<MoraineInlineFileDeleteRow> file_deletes(moraine_inline_file_deletes_free);
	MoraineError err {};
	auto code = moraine_inline_file_deletes(handle, table_id, file_deletes.OutItems(), file_deletes.OutLen(),
	                                        moraine_shim_is_interrupted, &context, &err);
	if (code != MORAINE_OK) {
		ThrowMoraineError(err);
	}
	std::vector<std::vector<duckdb::Value>> rows;
	rows.reserve(file_deletes.size());
	for (auto &r : file_deletes) {
		rows.push_back({Bigint(r.file_id), Bigint(r.row_id), Bigint(r.begin_snapshot)});
	}
	return rows;
}

duckdb::TableFunction
MoraineInlineDeleteTableEntry::GetScanFunction(duckdb::ClientContext &context,
                                               duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	auto scan_bind_data = duckdb::make_uniq<MetadataScanBindData>();
	scan_bind_data->rows =
	    std::make_shared<const MetadataRows>(ProvideInlineFileDeleteRows(context, handle_, table_id_));
	scan_bind_data->table_entry = this;
	bind_data = std::move(scan_bind_data);
	return MetadataScanTableFunction();
}

duckdb::TableStorageInfo MoraineInlineDeleteTableEntry::GetStorageInfo(duckdb::ClientContext &) {
	return duckdb::TableStorageInfo();
}

duckdb::unique_ptr<MoraineInlineDataTableEntry>
MakeInlineDataTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, MoraineCatalogHandle *handle,
                         uint64_t table_id, uint64_t schema_version,
                         const std::vector<DecodedInlineColumn> &user_columns) {
	auto info = BuildInlineDataTableInfo(schema, table_id, schema_version, user_columns);
	return duckdb::make_uniq<MoraineInlineDataTableEntry>(catalog, schema, info, handle, table_id, schema_version);
}

duckdb::unique_ptr<MoraineInlineDeleteTableEntry> MakeInlineDeleteTableEntry(duckdb::Catalog &catalog,
                                                                             duckdb::SchemaCatalogEntry &schema,
                                                                             MoraineCatalogHandle *handle,
                                                                             uint64_t table_id) {
	duckdb::CreateTableInfo info(schema, InlinedDeleteTableName(table_id));
	info.columns.AddColumn(duckdb::ColumnDefinition("file_id", duckdb::LogicalType::BIGINT));
	info.columns.AddColumn(duckdb::ColumnDefinition("row_id", duckdb::LogicalType::BIGINT));
	info.columns.AddColumn(duckdb::ColumnDefinition("begin_snapshot", duckdb::LogicalType::BIGINT));
	return duckdb::make_uniq<MoraineInlineDeleteTableEntry>(catalog, schema, info, handle, table_id);
}

duckdb::unique_ptr<duckdb::CatalogEntry> LookupInlineTableEntry(duckdb::ClientContext &context,
                                                                duckdb::Catalog &catalog,
                                                                duckdb::SchemaCatalogEntry &schema,
                                                                MoraineCatalogHandle *handle, const std::string &name) {
	if (auto parsed = ParseInlinedDataTableName(name)) {
		OwnedArray<MoraineInlineSchemaRow> schemas(moraine_inline_schemas_free);
		MoraineError err {};
		auto code = moraine_inline_schemas(handle, parsed->table_id, schemas.OutItems(), schemas.OutLen(),
		                                   moraine_shim_is_interrupted, &context, &err);
		if (code != MORAINE_OK) {
			ThrowMoraineError(err);
		}
		for (auto &row : schemas) {
			if (row.schema_version != parsed->schema_version) {
				continue;
			}
			auto user_columns = DecodeInlineSchema(context, row.arrow_schema, row.arrow_schema_len);
			return MakeInlineDataTableEntry(catalog, schema, handle, parsed->table_id, parsed->schema_version,
			                                user_columns);
		}
		return nullptr;
	}
	if (auto table_id = ParseInlinedDeleteTableName(name)) {
		bool exists = false;
		MoraineError err {};
		auto code = moraine_inline_file_delete_table_exists(handle, *table_id, &exists, moraine_shim_is_interrupted,
		                                                    &context, &err);
		if (code != MORAINE_OK) {
			ThrowMoraineError(err);
		}
		if (!exists) {
			return nullptr;
		}
		return MakeInlineDeleteTableEntry(catalog, schema, handle, *table_id);
	}
	return nullptr;
}

duckdb::unique_ptr<duckdb::CatalogEntry> CreateInlineDataTable(duckdb::ClientContext &context, duckdb::Catalog &catalog,
                                                               duckdb::SchemaCatalogEntry &schema,
                                                               MoraineCatalogHandle *handle, MoraineTxHandle *tx,
                                                               duckdb::BoundCreateTableInfo &info, uint64_t table_id,
                                                               uint64_t schema_version) {
	// Registration, not the retained record, is what "already exists" means:
	// a flush deregisters an emptied version while its record stays so the
	// name resolves. Re-creating it is the only thing that registers it again.
	bool registered = false;
	MoraineError lookup_err {};
	auto lookup_code = moraine_inline_table_registered(handle, table_id, schema_version, &registered,
	                                                   moraine_shim_is_interrupted, &context, &lookup_err);
	if (lookup_code != MORAINE_OK) {
		ThrowMoraineError(lookup_err);
	}
	if (registered) {
		if (info.Base().on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
			return nullptr;
		}
		throw duckdb::CatalogException("moraine: \"%s\" already exists", info.Base().table);
	}

	std::vector<DecodedInlineColumn> user_columns;
	duckdb::idx_t index = 0;
	for (auto &col : info.Base().columns.Logical()) {
		if (index >= 3) {
			user_columns.push_back(DecodedInlineColumn {col.Name(), col.Type()});
		}
		index++;
	}
	auto schema_bytes = EncodeInlineSchema(context, user_columns);
	MoraineError stage_err {};
	auto stage_code = moraine_tx_stage_inline_schema_owned(tx, table_id, schema_version, schema_bytes, &stage_err);
	if (stage_code != MORAINE_OK) {
		ThrowMoraineError(stage_err);
	}
	return MakeInlineDataTableEntry(catalog, schema, handle, table_id, schema_version, user_columns);
}

namespace {

// The ledger of the statement's own source scan, so physical rowids
// resolve without store I/O.
std::shared_ptr<InlineRowLedger> FindInlineRowLedger(const duckdb::PhysicalOperator &op,
                                                     MoraineCatalogHandle *handle, uint64_t table_id,
                                                     uint64_t schema_version) {
	if (auto *table_scan = dynamic_cast<const duckdb::PhysicalTableScan *>(&op)) {
		auto *data = dynamic_cast<const InlineDataScanBindData *>(table_scan->bind_data.get());
		if (data && data->handle == handle && data->table_id == table_id &&
		    data->schema_version == schema_version) {
			return data->ledger;
		}
	}
	for (auto &child : op.children) {
		auto ledger = FindInlineRowLedger(child.get(), handle, table_id, schema_version);
		if (ledger) {
			return ledger;
		}
	}
	return nullptr;
}

struct InlineDmlState : public duckdb::GlobalSinkState {
	duckdb::idx_t affected_count = 0;
	bool emitted = false;
	std::shared_ptr<InlineRowLedger> old_rows;
	// The delete table's own rows, whose rowids resolve against a different
	// materialization entirely (`ProvideInlineFileDeleteRows`).
	bool old_delete_rows_loaded = false;
	std::vector<std::vector<duckdb::Value>> old_delete_rows;
	// DELETE only: the maximum `begin_snapshot` among matched rows, standing
	// in for the flush-snapshot threshold.
	std::optional<uint64_t> max_begin_snapshot;
};

class MoraineInlineDml : public duckdb::PhysicalOperator {
public:
	static constexpr const duckdb::PhysicalOperatorType TYPE = duckdb::PhysicalOperatorType::EXTENSION;

	MoraineInlineDml(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                 duckdb::Catalog &catalog, duckdb::idx_t estimated_cardinality)
	    : duckdb::PhysicalOperator(physical_plan, TYPE, std::move(types), estimated_cardinality), catalog_(catalog) {
	}

	duckdb::Catalog &catalog_;

	duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(duckdb::ClientContext &) const override {
		return duckdb::make_uniq<InlineDmlState>();
	}
	bool IsSink() const override {
		return true;
	}
	bool IsSource() const override {
		return true;
	}

protected:
	MoraineTxHandle *StagedTx(duckdb::ClientContext &client) const {
		auto catalog_transaction = catalog_.GetCatalogTransaction(client);
		auto &moraine_tx = catalog_transaction.transaction->Cast<MoraineTransaction>();
		return moraine_tx.StagedTxForInline();
	}

	// Physical rowids index the rows the source scan emitted, in order.
	InlineLedgerRow ResolveRow(InlineDmlState &state, MoraineCatalogHandle *handle, uint64_t table_id,
	                           uint64_t schema_version, const duckdb::Value &row_id) const {
		if (!state.old_rows) {
			state.old_rows = FindInlineRowLedger(*this, handle, table_id, schema_version);
			if (!state.old_rows) {
				throw duckdb::InternalException("moraine: staged write has no inline source scan");
			}
		}
		if (row_id.IsNull()) {
			throw duckdb::InternalException("moraine: staged write received a NULL rowid");
		}
		auto wanted = static_cast<uint64_t>(row_id.GetValue<int64_t>());
		if (wanted >= state.old_rows->rows.size()) {
			throw duckdb::InternalException("moraine: staged write rowid is outside its source scan");
		}
		return state.old_rows->rows[wanted];
	}

public:
	duckdb::SourceResultType GetDataInternal(duckdb::ExecutionContext &, duckdb::DataChunk &chunk,
	                                         duckdb::OperatorSourceInput &) const override {
		auto &state = sink_state->Cast<InlineDmlState>();
		if (state.emitted) {
			chunk.SetCardinality(0);
			return duckdb::SourceResultType::FINISHED;
		}
		chunk.SetValue(0, 0, duckdb::Value::BIGINT(static_cast<int64_t>(state.affected_count)));
		chunk.SetCardinality(1);
		state.emitted = true;
		return duckdb::SourceResultType::FINISHED;
	}
};

class MoraineInlineDataInsertOp : public MoraineInlineDml {
public:
	MoraineInlineDataInsertOp(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                          duckdb::Catalog &catalog, duckdb::idx_t estimated_cardinality, uint64_t table_id,
	                          uint64_t schema_version)
	    : MoraineInlineDml(physical_plan, std::move(types), catalog, estimated_cardinality), table_id_(table_id),
	      schema_version_(schema_version) {
	}

	uint64_t table_id_;
	uint64_t schema_version_;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<InlineDmlState>();
		if (chunk.size() == 0) {
			return duckdb::SinkResultType::NEED_MORE_INPUT;
		}
		auto *tx = StagedTx(context.client);
		// Columns 0/1 are `row_id`/`begin_snapshot`; every row of one chunk
		// shares one `begin_snapshot`, so the first row's value is enough.
		// A chunk records only its first row id and a count, so its rows must
		// be contiguous — which an UPDATE's preserved row ids are not, since
		// DuckLake emits each row's own id and mints fresh ones only for the
		// rest. Stage one chunk per contiguous run.
		auto begin_snapshot = CellAsU64(chunk.GetValue(1, 0));

		// Flattened once: the run scan reads every row, and a `Value` apiece
		// would price an ordinary insert by its chunk size.
		duckdb::UnifiedVectorFormat row_ids;
		chunk.data[0].ToUnifiedFormat(chunk.size(), row_ids);
		auto ids = duckdb::UnifiedVectorFormat::GetData<int64_t>(row_ids);
		auto id_at = [&](duckdb::idx_t row) {
			return static_cast<uint64_t>(ids[row_ids.sel->get_index(row)]);
		};

		duckdb::idx_t run_start = 0;
		while (run_start < chunk.size()) {
			auto run_first_id = id_at(run_start);
			duckdb::idx_t run_end = run_start + 1;
			while (run_end < chunk.size() && id_at(run_end) == run_first_id + (run_end - run_start)) {
				run_end++;
			}
			auto run_count = run_end - run_start;
			auto body = EncodeInlineChunkRows(context.client, chunk, /* user_col_start */ 3, run_start, run_count);
			MoraineError err {};
			auto code = moraine_tx_stage_inline_insert_owned(tx, table_id_, schema_version_, begin_snapshot,
			                                                 run_first_id, run_count, body, &err);
			if (code != MORAINE_OK) {
				ThrowMoraineError(err);
			}
			run_start = run_end;
		}
		state.affected_count += chunk.size();
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}
};

class MoraineInlineDataUpdateOp : public MoraineInlineDml {
public:
	MoraineInlineDataUpdateOp(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                          duckdb::Catalog &catalog, duckdb::idx_t estimated_cardinality,
	                          MoraineCatalogHandle *handle, uint64_t table_id, uint64_t schema_version,
	                          duckdb::idx_t set_ref)
	    : MoraineInlineDml(physical_plan, std::move(types), catalog, estimated_cardinality), handle_(handle),
	      table_id_(table_id), schema_version_(schema_version), set_ref_(set_ref) {
	}

	MoraineCatalogHandle *handle_;
	uint64_t table_id_;
	uint64_t schema_version_;
	duckdb::idx_t set_ref_;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<InlineDmlState>();
		auto *tx = StagedTx(context.client);
		// The appended physical rowid identifies the version being ended.
		auto row_id_col = chunk.ColumnCount() - 1;
		for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
			auto old_row = ResolveRow(state, handle_, table_id_, schema_version_,
			                          chunk.GetValue(row_id_col, row));
			auto end_snapshot = CellAsU64(chunk.GetValue(set_ref_, row));
			MoraineError err {};
			auto code = moraine_tx_stage_inline_inline_delete(tx, table_id_, old_row.row_id, end_snapshot, &err);
			if (code != MORAINE_OK) {
				ThrowMoraineError(err);
			}
			state.affected_count++;
		}
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}
};

class MoraineInlineDataDeleteOp : public MoraineInlineDml {
public:
	MoraineInlineDataDeleteOp(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                          duckdb::Catalog &catalog, duckdb::idx_t estimated_cardinality,
	                          MoraineCatalogHandle *handle, uint64_t table_id, uint64_t schema_version,
	                          duckdb::idx_t row_id_chunk_index)
	    : MoraineInlineDml(physical_plan, std::move(types), catalog, estimated_cardinality), handle_(handle),
	      table_id_(table_id), schema_version_(schema_version), row_id_chunk_index_(row_id_chunk_index) {
	}

	MoraineCatalogHandle *handle_;
	uint64_t table_id_;
	uint64_t schema_version_;
	duckdb::idx_t row_id_chunk_index_;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<InlineDmlState>();
		for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
			auto old_row = ResolveRow(state, handle_, table_id_, schema_version_,
			                          chunk.GetValue(row_id_chunk_index_, row));
			auto begin_snapshot = old_row.begin_snapshot;
			if (!state.max_begin_snapshot.has_value() || begin_snapshot > *state.max_begin_snapshot) {
				state.max_begin_snapshot = begin_snapshot;
			}
			state.affected_count++;
		}
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}

	duckdb::SourceResultType GetDataInternal(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                                         duckdb::OperatorSourceInput &input) const override {
		auto &state = sink_state->Cast<InlineDmlState>();
		if (!state.emitted && state.max_begin_snapshot.has_value()) {
			auto *tx = StagedTx(context.client);
			MoraineError err {};
			auto code =
			    moraine_tx_stage_inline_flush_delete(tx, table_id_, schema_version_, *state.max_begin_snapshot, &err);
			if (code != MORAINE_OK) {
				ThrowMoraineError(err);
			}
		}
		return MoraineInlineDml::GetDataInternal(context, chunk, input);
	}
};

class MoraineInlineDeleteInsertOp : public MoraineInlineDml {
public:
	MoraineInlineDeleteInsertOp(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                            duckdb::Catalog &catalog, duckdb::idx_t estimated_cardinality, uint64_t table_id)
	    : MoraineInlineDml(physical_plan, std::move(types), catalog, estimated_cardinality), table_id_(table_id) {
	}

	uint64_t table_id_;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<InlineDmlState>();
		auto *tx = StagedTx(context.client);
		for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
			auto file_id = CellAsU64(chunk.GetValue(0, row));
			auto row_id = CellAsU64(chunk.GetValue(1, row));
			auto begin_snapshot = CellAsU64(chunk.GetValue(2, row));
			MoraineError err {};
			auto code = moraine_tx_stage_inline_file_delete(tx, table_id_, file_id, row_id, begin_snapshot, &err);
			if (code != MORAINE_OK) {
				ThrowMoraineError(err);
			}
			state.affected_count++;
		}
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}
};

// Translates every row of its input into one
// `stage_inline_file_delete_remove` call. Row-grain rather than a
// table-wide clear: DuckLake's flush happens to delete the whole table,
// but what it issues is an ordinary SQL DELETE, and resolving each rowid
// means a filtered one removes exactly what it matched.
class MoraineInlineDeleteDeleteOp : public MoraineInlineDml {
public:
	MoraineInlineDeleteDeleteOp(duckdb::PhysicalPlan &physical_plan, std::vector<duckdb::LogicalType> types,
	                            duckdb::Catalog &catalog, duckdb::idx_t estimated_cardinality,
	                            MoraineCatalogHandle *handle, uint64_t table_id, duckdb::idx_t row_id_chunk_index)
	    : MoraineInlineDml(physical_plan, std::move(types), catalog, estimated_cardinality), handle_(handle),
	      table_id_(table_id), row_id_chunk_index_(row_id_chunk_index) {
	}

	MoraineCatalogHandle *handle_;
	uint64_t table_id_;
	duckdb::idx_t row_id_chunk_index_;

	duckdb::SinkResultType Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                            duckdb::OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<InlineDmlState>();
		auto *tx = StagedTx(context.client);
		for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
			if (!state.old_delete_rows_loaded) {
				state.old_delete_rows = ProvideInlineFileDeleteRows(context.client, handle_, table_id_);
				state.old_delete_rows_loaded = true;
			}
			auto row_id_value = chunk.GetValue(row_id_chunk_index_, row);
			if (row_id_value.IsNull()) {
				throw duckdb::InternalException("moraine: staged write received a NULL rowid");
			}
			auto index = static_cast<duckdb::idx_t>(row_id_value.GetValue<int64_t>());
			if (index >= state.old_delete_rows.size()) {
				throw duckdb::InternalException(
				    "moraine: staged write rowid is out of range — the committed head moved between this "
				    "statement's scan and its write, which the supported topology excludes");
			}
			// Columns 0/1 of the scan are `file_id`/`row_id`.
			auto &old_row = state.old_delete_rows[index];
			MoraineError err {};
			auto code = moraine_tx_stage_inline_file_delete_remove(tx, table_id_, CellAsU64(old_row[0]),
			                                                       CellAsU64(old_row[1]), &err);
			if (code != MORAINE_OK) {
				ThrowMoraineError(err);
			}
			state.affected_count++;
		}
		return duckdb::SinkResultType::NEED_MORE_INPUT;
	}
};

} // namespace

duckdb::PhysicalOperator &PlanInlineDataInsert(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalInsert &op,
                                               MoraineInlineDataTableEntry &table_entry) {
	return planner.Make<MoraineInlineDataInsertOp>(op.types, op.table.catalog, op.estimated_cardinality,
	                                               table_entry.TableId(), table_entry.SchemaVersion());
}

duckdb::PhysicalOperator &PlanInlineDataUpdate(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalUpdate &op,
                                               MoraineInlineDataTableEntry &table_entry) {
	if (op.return_chunk) {
		throw duckdb::NotImplementedException("moraine: UPDATE ... RETURNING is not supported on \"%s\"",
		                                      op.table.name);
	}
	if (op.columns.size() != 1 ||
	    !duckdb::StringUtil::CIEquals(table_entry.GetColumns().GetColumn(op.columns[0]).GetName(), "end_snapshot")) {
		throw duckdb::NotImplementedException(
		    "moraine: the only UPDATE supported on \"%s\" is SET end_snapshot (the staged-row lifecycle "
		    "convention)",
		    op.table.name);
	}
	if (op.expressions.size() != 1 || op.expressions[0]->GetExpressionClass() != duckdb::ExpressionClass::BOUND_REF) {
		throw duckdb::NotImplementedException(
		    "moraine: UPDATE with a non-column SET expression is not supported on \"%s\"", op.table.name);
	}
	auto set_ref = op.expressions[0]->Cast<duckdb::BoundReferenceExpression>().index;
	return planner.Make<MoraineInlineDataUpdateOp>(op.types, op.table.catalog, op.estimated_cardinality,
	                                               table_entry.Handle(), table_entry.TableId(),
	                                               table_entry.SchemaVersion(), set_ref);
}

duckdb::PhysicalOperator &PlanInlineDataDelete(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalDelete &op,
                                               MoraineInlineDataTableEntry &table_entry) {
	if (op.return_chunk) {
		throw duckdb::NotImplementedException("moraine: DELETE ... RETURNING is not supported on \"%s\"",
		                                      op.table.name);
	}
	if (op.expressions.size() != 1) {
		throw duckdb::InternalException("moraine: expected exactly one row-id expression for DELETE on \"%s\"",
		                                op.table.name);
	}
	auto &bound_ref = op.expressions[0]->Cast<duckdb::BoundReferenceExpression>();
	return planner.Make<MoraineInlineDataDeleteOp>(
	    op.types, op.table.catalog, op.estimated_cardinality, table_entry.Handle(), table_entry.TableId(),
	    table_entry.SchemaVersion(), bound_ref.index);
}

duckdb::PhysicalOperator &PlanInlineDeleteInsert(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalInsert &op,
                                                 MoraineInlineDeleteTableEntry &table_entry) {
	return planner.Make<MoraineInlineDeleteInsertOp>(op.types, op.table.catalog, op.estimated_cardinality,
	                                                 table_entry.TableId());
}

duckdb::PhysicalOperator &PlanInlineDeleteDelete(duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalDelete &op,
                                                 MoraineInlineDeleteTableEntry &table_entry) {
	if (op.return_chunk) {
		throw duckdb::NotImplementedException("moraine: DELETE ... RETURNING is not supported on \"%s\"",
		                                      op.table.name);
	}
	if (op.expressions.size() != 1) {
		throw duckdb::InternalException("moraine: expected exactly one row-id expression for DELETE on \"%s\"",
		                                op.table.name);
	}
	auto &bound_ref = op.expressions[0]->Cast<duckdb::BoundReferenceExpression>();
	return planner.Make<MoraineInlineDeleteDeleteOp>(op.types, op.table.catalog, op.estimated_cardinality,
	                                                 table_entry.Handle(), table_entry.TableId(), bound_ref.index);
}

} // namespace moraine_duckdb
