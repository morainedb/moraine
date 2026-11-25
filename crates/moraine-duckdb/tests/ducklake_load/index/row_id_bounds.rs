//! Moraine records each data file's row-id bounds itself, in the file
//! statistics under the reserved row-id column: at commit for an indexed
//! table's files, and for every file an index build reads. The file
//! directory reads them to pick the files that can hold a row id.

use crate::helpers::*;

/// The recorded row-id bounds of every live data file, as
/// `data_file_id,record_count,min,max` lines.
const BOUNDS: &str = "SELECT data.data_file_id, data.record_count, stats.min_value, stats.max_value
    FROM __ducklake_metadata_lake.ducklake_data_file AS data
    JOIN __ducklake_metadata_lake.ducklake_file_column_stats AS stats USING (data_file_id, table_id)
    WHERE data.end_snapshot IS NULL AND stats.column_id = 2147483540
    ORDER BY data.data_file_id;";

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_indexed_tables_files_carry_their_row_id_bounds() {
    let store = TempDir::new("row-id-bounds-store");
    let data = TempDir::new("row-id-bounds-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    run("CREATE TABLE lake.main.t AS SELECT (i*7919)%100000 AS a,
             repeat(md5(i::VARCHAR),6) AS b FROM range(50000) r(i);
         INSERT INTO lake.main.t SELECT (i*7919)%100000,
             repeat(md5(i::VARCHAR),6) FROM range(50000,100000) r(i);");
    assert_eq!(
        csv_rows(&run(BOUNDS)),
        Vec::<Vec<String>>::new(),
        "an unindexed table's files record no bounds"
    );

    // The build reads both files and records what it read.
    run("CREATE UNIQUE INDEX by_a ON lake.main.t(a);");
    assert_eq!(
        csv_rows(&run(BOUNDS)),
        vec![
            vec!["0", "50000", "0", "49999"],
            vec!["1", "50000", "50000", "99999"],
        ]
    );

    // A dense file committed afterwards records its range from the catalog
    // alone; an UPDATE output carries its preserved id in its footer.
    let row_id = csv_rows(&run("SELECT rowid FROM lake.main.t WHERE a = 30001;"))[0][0].clone();
    run(
        "INSERT INTO lake.main.t SELECT 100000 + i, 'late' FROM range(1000) r(i);
         UPDATE lake.main.t SET b = 'z' WHERE a = 30001;",
    );
    let bounds = csv_rows(&run(BOUNDS));
    assert_eq!(bounds.len(), 4, "{bounds:?}");
    assert_eq!(bounds[2][1..], ["1000", "100000", "100999"]);
    assert_eq!(bounds[3][1..], ["1", row_id.as_str(), row_id.as_str()]);
}
