LOAD parquet;

ATTACH 'ducklake:release-smoke.ducklake' AS lake (
    DATA_PATH 'release-smoke-data',
    METADATA_CATALOG 'release_smoke',
    DATA_INLINING_ROW_LIMIT 0
);
CREATE TABLE lake.items(value INTEGER);
INSERT INTO lake.items VALUES (0), (1), (2);
INSERT INTO lake.items VALUES (3), (4);
INSERT INTO lake.items VALUES (5), (6), (7), (8);

DELETE FROM release_smoke.ducklake_file_column_stats
WHERE column_id = 2147483540;

SELECT schema_name, table_name, files_backfilled, files_remaining
FROM ducklake_backfill_row_id_stats('lake', schema => 'main', table_name => 'items');

SELECT stats.column_id, stats.min_value, stats.max_value
FROM release_smoke.ducklake_data_file AS data
JOIN release_smoke.ducklake_file_column_stats AS stats
USING (data_file_id, table_id)
WHERE data.end_snapshot IS NULL
  AND stats.column_id = 2147483540
ORDER BY data.data_file_id;

EXPLAIN ANALYZE SELECT * FROM lake.items WHERE rowid = 3;

-- The positional delete takes the caller's committed delete positions as a
-- third struct field. A bundled DuckLake without that patch rejects the
-- struct shape, which is exactly the skew this smoke exists to catch.
SET VARIABLE first_file = (
    SELECT min(data_file_id) FROM release_smoke.ducklake_data_file WHERE end_snapshot IS NULL
);
SELECT file_rows_deleted, inline_rows_deleted, delete_files_written
FROM ducklake_delete_positions('lake', 'main', 'items',
    [{'data_file_id': getvariable('first_file')::UBIGINT,
      'positions': [1::UBIGINT],
      'existing_positions': NULL::BLOB}],
    inlined_rows => []::UBIGINT[]);
SELECT 'remaining=' || count(*) AS remaining FROM lake.items;
