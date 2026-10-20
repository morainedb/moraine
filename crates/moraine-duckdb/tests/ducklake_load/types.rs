use crate::helpers::*;

/// Data inlining end to end, driven entirely through DuckLake's own
/// SQL: small `INSERT`s land in the `inline/*` keyspace (never
/// materialized as a real table) and read back through DuckLake's own
/// inlined-data reader, not this crate's scan.
///
/// - `INSERT` (two statements, two chunks) of mixed types (`BIGINT`, `VARCHAR`,
///   `DOUBLE`, `BOOLEAN`) and `NULL`s inlines; `SELECT` returns every row with
///   the right values and types.
/// - `DELETE` of one row stages an `inline/inline_delete`; a follow-up `SELECT`
///   no longer sees it.
/// - `CALL ducklake_flush_inlined_data('lake')` moves the remaining rows to a
///   real Parquet file; `SELECT` afterward is still correct (now served by
///   DuckLake's Parquet reader plus its delete-file join for the pre-flush
///   `DELETE`), and the standalone `moraine:` attach's row-faithful projections
///   confirm the `inline/insert` chunk is gone (0 remaining rows in the
///   now-empty `ducklake_inlined_data_<t>_<v>` entry) and a
///   `ducklake_data_file` is registered.
///
/// The full DuckLake scalar type matrix — every scalar moraine maps —
/// created, inlined, and round-tripped live through DuckLake's own SQL,
/// both before flush (served from the `inline/*` keyspace via Arrow IPC)
/// and after (transcoded to Parquet and read by DuckLake's own reader).
///
/// Covers every integer width (signed and unsigned), `FLOAT`/`DOUBLE`,
/// `DECIMAL(w,s)` (width/scale preserved through the type round trip),
/// `VARCHAR`/`BLOB`/`BOOLEAN`, the temporal types
/// (`DATE`/`TIME`/`TIMESTAMP`/`TIMESTAMPTZ`/`INTERVAL`), `UUID`, and
/// `JSON` (VARCHAR-backed, aliased — stored as DuckLake's `json`). A
/// second all-`NULL` row proves null handling for each. The stored
/// `ducklake_column.column_type` is checked in DuckLake's own vocabulary
/// through the standalone projection, so a type that reads back but
/// mis-names itself (a dropped `DECIMAL` suffix) is caught too.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
#[allow(clippy::too_many_lines)]
fn ducklake_scalar_type_matrix_round_trip_through_flush() {
    let dir = TempDir::new("scalars-store");
    let data_dir = TempDir::new("scalars-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.t (\
         c_tinyint TINYINT, c_smallint SMALLINT, c_integer INTEGER, c_bigint BIGINT, \
         c_hugeint HUGEINT, c_utinyint UTINYINT, c_usmallint USMALLINT, c_uinteger UINTEGER, \
         c_ubigint UBIGINT, c_float FLOAT, c_double DOUBLE, c_decimal DECIMAL(18,4), \
         c_varchar VARCHAR, c_blob BLOB, c_boolean BOOLEAN, c_date DATE, c_time TIME, \
         c_timestamp TIMESTAMP, c_timestamptz TIMESTAMPTZ, c_interval INTERVAL, c_uuid UUID, \
         c_json JSON);",
    );
    run_ducklake_sql(
        store,
        data_path,
        "INSERT INTO lake.main.t VALUES (\
         1, 2, 3, 4, 5, 6, 7, 8, 9, 1.5, 2.5, 12345.6789, 'hello', '\\x01\\x02'::BLOB, true, \
         DATE '2020-01-02', TIME '03:04:05', TIMESTAMP '2020-01-02 03:04:05', \
         TIMESTAMPTZ '2020-01-02 03:04:05+00', INTERVAL '1' MONTH, \
         '12345678-1234-5678-1234-567812345678'::UUID, '[1]'::JSON), \
         (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);",
    );

    // TIMESTAMPTZ renders in the session zone; pin UTC so it is stable.
    let select = "SET TimeZone='UTC'; \
         SELECT c_tinyint::VARCHAR, c_smallint::VARCHAR, c_integer::VARCHAR, \
         c_bigint::VARCHAR, c_hugeint::VARCHAR, c_utinyint::VARCHAR, c_usmallint::VARCHAR, \
         c_uinteger::VARCHAR, c_ubigint::VARCHAR, c_float::VARCHAR, c_double::VARCHAR, \
         c_decimal::VARCHAR, c_varchar, c_blob::VARCHAR, c_boolean::VARCHAR, c_date::VARCHAR, \
         c_time::VARCHAR, c_timestamp::VARCHAR, c_timestamptz::VARCHAR, c_interval::VARCHAR, \
         c_uuid::VARCHAR, c_json::VARCHAR FROM lake.main.t ORDER BY c_bigint NULLS LAST;";
    let values_row = vec![
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "1.5",
        "2.5",
        "12345.6789",
        "hello",
        "\\x01\\x02",
        "true",
        "2020-01-02",
        "03:04:05",
        "2020-01-02 03:04:05",
        "2020-01-02 03:04:05+00",
        "1 month",
        "12345678-1234-5678-1234-567812345678",
        "[1]",
    ];
    let null_row = vec!["NULL"; 22];
    let want = vec![values_row.clone(), null_row.clone()];

    // Pre-flush: served from the inline keyspace, no Parquet file yet.
    assert_eq!(csv_rows(&run_ducklake_sql(store, data_path, select)), want);
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_data_file WHERE end_snapshot IS NULL;",
        )),
        vec![vec!["0".to_string()]]
    );

    // The stored type names round-trip in DuckLake's own vocabulary.
    // `decimal(18,4)` is checked separately below: its comma would split
    // under `csv_rows`, and it is the one type whose parameters must
    // survive the round trip.
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store,
            "SELECT column_type FROM m.ducklake_column WHERE end_snapshot IS NULL \
             AND column_name <> 'c_decimal' ORDER BY column_order;",
        )),
        vec![
            vec!["int8"],
            vec!["int16"],
            vec!["int32"],
            vec!["int64"],
            vec!["int128"],
            vec!["uint8"],
            vec!["uint16"],
            vec!["uint32"],
            vec!["uint64"],
            vec!["float32"],
            vec!["float64"],
            vec!["varchar"],
            vec!["blob"],
            vec!["boolean"],
            vec!["date"],
            vec!["time"],
            vec!["timestamp"],
            vec!["timestamptz"],
            vec!["interval"],
            vec!["uuid"],
            vec!["json"],
        ]
    );
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store,
            "SELECT column_type = 'decimal(18,4)' FROM m.ducklake_column \
             WHERE column_name = 'c_decimal' AND end_snapshot IS NULL;",
        )),
        vec![vec!["true".to_string()]]
    );

    // Post-flush: the same values, now read through DuckLake's Parquet
    // reader after the transcode.
    run_ducklake_sql(
        store,
        data_path,
        "CALL ducklake_flush_inlined_data('lake');",
    );
    assert_eq!(csv_rows(&run_ducklake_sql(store, data_path, select)), want);
}

/// A `GEOMETRY` column round-trips through moraine + DuckLake: DuckDB's
/// Arrow inline encoding supports geometry, the stored `column_type` reads
/// back as DuckLake's `geometry`, and values survive both the inline
/// keyspace and the Parquet flush. A `NULL` row proves null handling.
///
/// The type and its text casts are DuckDB's own; only the `ST_*` functions
/// need `spatial`, and the casts say everything this pins.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn ducklake_geometry_round_trip_through_flush() {
    let dir = TempDir::new("geom-store");
    let data_dir = TempDir::new("geom-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.g (id BIGINT, geom GEOMETRY);",
    );
    run_ducklake_sql(
        store,
        data_path,
        "INSERT INTO lake.main.g VALUES (1, 'POINT(1 2)'::GEOMETRY), (2, NULL);",
    );

    let select = "SELECT id::VARCHAR, coalesce(geom::VARCHAR, 'NULL') \
         FROM lake.main.g ORDER BY id;";
    let want = vec![vec!["1", "POINT (1 2)"], vec!["2", "NULL"]];

    // Pre-flush: served from the inline keyspace.
    assert_eq!(csv_rows(&run_ducklake_sql(store, data_path, select)), want);

    // The stored type name round-trips in DuckLake's vocabulary.
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store,
            "SELECT column_type FROM m.ducklake_column WHERE column_name = 'geom' \
             AND end_snapshot IS NULL;",
        )),
        vec![vec!["geometry".to_string()]]
    );

    // Post-flush: read back through DuckLake's Parquet reader.
    run_ducklake_sql(
        store,
        data_path,
        "CALL ducklake_flush_inlined_data('lake');",
    );
    assert_eq!(csv_rows(&run_ducklake_sql(store, data_path, select)), want);
}

/// The extended scalar types DuckLake can name — `uint128` (UHUGEINT) and
/// the sub-second / tz temporals (`timestamp_s`/`_ms`/`_ns`, `time_ns`,
/// `timetz`) — map through moraine, so a table using them creates and its
/// `ducklake_column.column_type` reads back in DuckLake's vocabulary. This
/// is the metadata probe that previously failed with "unsupported DuckLake
/// column type". `uint128` data round-trips exactly through the inline
/// (Arrow) keyspace, so the data check here stays inline: once flushed,
/// DuckDB's Parquet writer stores 128-bit integers — `int128` and
/// `uint128` alike — as `DOUBLE`, losing precision beyond ~17 significant
/// digits (a DuckDB limitation, not moraine's, unchanged by this mapping).
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn ducklake_extended_scalar_types_map_through_probe() {
    let dir = TempDir::new("exttypes-store");
    let data_dir = TempDir::new("exttypes-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.t (\
         c_uint128 UHUGEINT, c_ts_s TIMESTAMP_S, c_ts_ms TIMESTAMP_MS, \
         c_ts_ns TIMESTAMP_NS, c_time_ns TIME_NS, c_timetz TIMETZ);",
    );

    // The stored type names round-trip in DuckLake's vocabulary — the probe
    // that regressed for each of these.
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store,
            "SELECT column_type FROM m.ducklake_column WHERE end_snapshot IS NULL \
             ORDER BY column_order;",
        )),
        vec![
            vec!["uint128"],
            vec!["timestamp_s"],
            vec!["timestamp_ms"],
            vec!["timestamp_ns"],
            vec!["time_ns"],
            vec!["timetz"],
        ]
    );

    // uint128 data round-trips through the inline keyspace for values within
    // its Arrow (`DECIMAL(38,0)`) range.
    run_ducklake_sql(
        store,
        data_path,
        "INSERT INTO lake.main.t (c_uint128) VALUES (12345), (NULL);",
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT coalesce(c_uint128::VARCHAR, 'NULL') FROM lake.main.t \
             ORDER BY c_uint128 NULLS LAST;",
        )),
        vec![vec!["12345"], vec!["NULL"]],
    );
}

/// A `VARIANT` column survives every stage of its life: inlined, flushed
/// to Parquet, and read back at a snapshot from before the flush. Its
/// field statistics land in `ducklake_file_variant_stats`, which DuckLake
/// writes for every Parquet file carrying a variant.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged extensions"]
fn variant_columns_inline_flush_and_carry_their_statistics() {
    let store = TempDir::new("variant-lifecycle");
    let data = TempDir::new("variant-lifecycle-data");
    let result = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &format!(", META_DATA_PATH '{}'", data.path().display()),
        "CREATE TABLE lake.main.v(id BIGINT, doc VARIANT);
         INSERT INTO lake.main.v VALUES
             (1, '{\"a\":1,\"b\":\"x\"}'::JSON::VARIANT),
             (2, '[10,20]'::JSON::VARIANT);
         SELECT 'inlined', id, typeof(doc), variant_extract(doc, 'b')::VARCHAR
             FROM lake.main.v ORDER BY id;
         SELECT 'inline_stats', count(*)
             FROM __ducklake_metadata_lake.main.ducklake_file_variant_stats;
         CALL ducklake_flush_inlined_data('lake');
         SELECT 'flushed', id, typeof(doc), variant_extract(doc, 'b')::VARCHAR
             FROM lake.main.v ORDER BY id;
         SELECT 'files', count(*) FROM __ducklake_metadata_lake.main.ducklake_data_file;
         SELECT 'stats_paths', variant_path
             FROM __ducklake_metadata_lake.main.ducklake_file_variant_stats
             ORDER BY variant_path;",
    );
    let phase = |name: &str| -> Vec<Vec<String>> {
        csv_rows(&result)
            .into_iter()
            .filter(|row| row.first().is_some_and(|first| first == name))
            .map(|row| row.into_iter().skip(1).collect())
            .collect()
    };
    assert_eq!(
        phase("inlined"),
        vec![vec!["1", "VARIANT", "x"], vec!["2", "VARIANT", "NULL"]]
    );
    // Inlined rows are not a Parquet file, so they carry no file statistics.
    assert_eq!(phase("inline_stats"), vec![vec!["0"]]);
    assert_eq!(
        phase("flushed"),
        vec![vec!["1", "VARIANT", "x"], vec!["2", "VARIANT", "NULL"]],
        "the flush rewrites the rows into Parquet without changing them"
    );
    assert_eq!(phase("files"), vec![vec!["1"]]);
    assert!(
        !phase("stats_paths").is_empty(),
        "the flushed file carries variant statistics: {result}"
    );
}

/// A `VARIANT` column written straight to Parquet, with inlining off, is
/// the other path DuckLake produces variant statistics on.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged extensions"]
fn variant_columns_write_straight_to_parquet() {
    let store = TempDir::new("variant-parquet");
    let data = TempDir::new("variant-parquet-data");
    let result = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        ", DATA_INLINING_ROW_LIMIT 0",
        "CREATE TABLE lake.main.v(id BIGINT, doc VARIANT);
         INSERT INTO lake.main.v VALUES (1, '{\"a\":1}'::JSON::VARIANT);
         SELECT 'rows', id, variant_extract(doc, 'a')::VARCHAR FROM lake.main.v;
         SELECT 'files', count(*) FROM __ducklake_metadata_lake.main.ducklake_data_file;",
    );
    let phase = |name: &str| -> Vec<Vec<String>> {
        csv_rows(&result)
            .into_iter()
            .filter(|row| row.first().is_some_and(|first| first == name))
            .map(|row| row.into_iter().skip(1).collect())
            .collect()
    };
    assert_eq!(phase("rows"), vec![vec!["1", "1"]]);
    assert_eq!(phase("files"), vec![vec!["1"]]);
}

/// An equality index cannot encode a variant, so it is refused where the
/// index is created rather than where a scan would read it.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged extensions"]
fn a_variant_column_is_refused_as_an_index_key() {
    let store = TempDir::new("variant-index");
    let data = TempDir::new("variant-index-data");
    let output = run_ducklake_sql_output(
        store.path(),
        data.path(),
        "",
        "CREATE TABLE lake.main.v(id BIGINT, doc VARIANT);
         CREATE INDEX by_doc ON lake.main.v(doc);",
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("index key can encode"), "stderr: {stderr}");
}
