LOAD parquet;

-- moraine serves as the metadata catalog, so the indexed table's row-id
-- bounds it records as each file commits are what the statistics query reads.
ATTACH 'ducklake:moraine:release-smoke-store' AS lake (
    DATA_PATH 'release-smoke-data',
    DATA_INLINING_ROW_LIMIT 0
);
CREATE TABLE lake.items(value INTEGER);
CREATE INDEX by_value ON lake.main.items(value);
INSERT INTO lake.items VALUES (0), (1), (2);
INSERT INTO lake.items VALUES (3), (4);
INSERT INTO lake.items VALUES (5), (6), (7), (8);

SELECT stats.column_id, stats.min_value, stats.max_value
FROM __ducklake_metadata_lake.ducklake_data_file AS data
JOIN __ducklake_metadata_lake.ducklake_file_column_stats AS stats
USING (data_file_id, table_id)
WHERE data.end_snapshot IS NULL
  AND stats.column_id = 2147483540
ORDER BY data.data_file_id;


-- moraine's positional delete stages located rows through the bundled
-- DuckLake's transaction; a bundle whose DuckLake drifted from what it
-- links against fails here rather than at a user's DELETE.
SET VARIABLE first_file = (
    SELECT min(data_file_id) FROM __ducklake_metadata_lake.ducklake_data_file WHERE end_snapshot IS NULL
);
SELECT file_rows_deleted, delete_files_written
FROM moraine_delete_positions('lake', 'main', 'items',
    [{'data_file_id': getvariable('first_file')::UBIGINT,
      'positions': [1::UBIGINT],
      'existing_positions': NULL::BLOB}]);
SELECT 'remaining=' || count(*) AS remaining FROM lake.items;
