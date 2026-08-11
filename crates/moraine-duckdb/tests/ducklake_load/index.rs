use crate::helpers::*;

/// The equality-index SQL surface end to end: `moraine_index_create`
/// backfills an index over existing data by scoped-reading the table's
/// Parquet from `DATA_PATH` (autonomous commit), `moraine_indexes` lists it,
/// `moraine_index_lookup` resolves a value to its row, and
/// `moraine_index_drop` removes it — each through a fresh attach, so the
/// definition and entries round-trip through the persisted store.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_functions_create_list_lookup_and_drop() {
    let store = TempDir::new("index-fns-store");
    let data = TempDir::new("index-fns-data");
    // DATA_PATH is declared once, at attach: DuckLake's own `DATA_PATH`
    // plus `META_DATA_PATH` (the passthrough that reaches moraine), so the
    // index functions source it from the handle — no per-call argument.
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

    // A 100-row insert writes a Parquet data file under DATA_PATH.
    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");

    // Create a unique index over the existing rows — backfilled by the
    // scoped read of the DATA_PATH files, no DATA_PATH argument.
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

    let listed = csv_rows(&run(
        "SELECT index_name, is_unique FROM moraine_indexes('lake', 'main', 't');",
    ));
    assert_eq!(
        listed,
        vec![vec!["by_a".to_string(), "true".to_string()]],
        "the created unique index is listed"
    );

    let lookup = csv_rows(&run("SELECT * FROM \
         moraine_index_lookup('lake', 'main', 't', 'by_a', 42);"));
    assert_eq!(
        lookup,
        vec![vec!["42".to_string()]],
        "a lookup surfaces only the stable row id"
    );

    // A value that exists reads through one relational query.
    let hit = csv_rows(&run("SELECT data.a \
         FROM lake.main.t data \
         JOIN moraine_index_lookup('lake', 'main', 't', 'by_a', 42) hits \
           ON hits.row_id = data.rowid;"));
    assert_eq!(
        hit,
        vec![vec!["42".to_string()]],
        "value 42 is read through its stable row id"
    );
    // A value that does not exist resolves to none.
    let miss = csv_rows(&run("SELECT count(*) FROM \
         moraine_index_lookup('lake', 'main', 't', 'by_a', 9999);"));
    assert_eq!(miss, vec![vec!["0".to_string()]], "value 9999 is absent");

    run("CALL moraine_index_drop('lake', 'main', 't', 'by_a');");
    let after = csv_rows(&run(
        "SELECT count(*) FROM moraine_indexes('lake', 'main', 't');",
    ));
    assert_eq!(
        after,
        vec![vec!["0".to_string()]],
        "the index is gone after drop"
    );
}

/// An absent lookup is known while its table function binds. The optimizer
/// must turn that exact zero-row input into an empty result before DuckLake
/// opens the table's Parquet files for the surrounding row-id join.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn absent_index_lookup_eliminates_the_ducklake_scan() {
    let store = TempDir::new("index-empty-result-store");
    let data = TempDir::new("index-empty-result-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    // More than DuckLake's inline threshold, so a scan would have to open a
    // real Parquet file and report it in the analyzed plan.
    run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

    // Prepared callers supply a different key on each execution. The rewrite
    // must therefore see the execution-time bind, not rely on a literal in
    // the original SQL text.
    let plan = run("PREPARE absent_lookup AS \
         SELECT count(*) FROM lake.main.t data \
         JOIN moraine_index_lookup('lake', 'main', 't', 'by_a', $1) hits \
           ON hits.row_id = data.rowid; \
         EXPLAIN ANALYZE EXECUTE absent_lookup(9999);");
    assert!(
        plan.contains("EMPTY_RESULT"),
        "the known-empty lookup did not eliminate the join:\n{plan}"
    );
    assert!(
        !plan.contains("Total Files Read"),
        "DuckLake opened data files for an absent index key:\n{plan}"
    );
}

/// A composite index over `(a, b)` resolves a full multi-column equality key
/// from SQL: the variadic values, in the index's column order, pin the one
/// matching row, and an absent key resolves to none.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_lookup_resolves_a_composite_key() {
    let store = TempDir::new("index-composite-store");
    let data = TempDir::new("index-composite-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    let count = |sql: &str| csv_rows(&run(sql));

    // Rows 0 and 1 share a=5; only (a, b) together are unique.
    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    run("INSERT INTO lake.main.t VALUES (5, 'x'), (5, 'y'), (7, 'x');");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_ab', ['a', 'b'], true);");

    // The full key (5, 'y') pins exactly the second row.
    assert_eq!(
        csv_rows(&run(
            "SELECT row_id FROM moraine_index_lookup('lake', 'main', 't', 'by_ab', 5, 'y');"
        )),
        vec![vec!["1".to_string()]],
        "(5, 'y') resolves to row 1"
    );
    // The same leading value with a different second column is a distinct key.
    assert_eq!(
        csv_rows(&run(
            "SELECT row_id FROM moraine_index_lookup('lake', 'main', 't', 'by_ab', 5, 'x');"
        )),
        vec![vec!["0".to_string()]],
        "(5, 'x') resolves to row 0"
    );
    // A key present in neither column combination resolves to no rows.
    assert_eq!(
        count("SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_ab', 9, 'x');"),
        vec![vec!["0".to_string()]],
        "(9, 'x') matches no row"
    );
}

/// The batched equality surface implements SQL `IN` over one-column and
/// composite indexes, including duplicate, absent, NULL, and empty keys.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_in_resolves_a_list_of_keys() {
    let store = TempDir::new("index-in-store");
    let data = TempDir::new("index-in-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    run("INSERT INTO lake.main.t VALUES (5, 'x'), (5, 'y'), (7, 'x');");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], false);");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_ab', ['a', 'b'], true);");

    assert_eq!(
        csv_rows(&run("SELECT row_id FROM moraine_index_in(\
                'lake', 'main', 't', 'by_a', [5, 7, 5, NULL, 99]) ORDER BY row_id;")),
        vec![
            vec!["0".to_string()],
            vec!["1".to_string()],
            vec!["2".to_string()]
        ],
        "scalar IN returns each matching row once"
    );
    assert_eq!(
        csv_rows(&run("SELECT row_id FROM moraine_index_in(\
                'lake', 'main', 't', 'by_ab', \
                [row(5, 'x'), row(7, 'x'), row(5, 'x')]) ORDER BY row_id;")),
        vec![vec!["0".to_string()], vec!["2".to_string()]],
        "composite IN accepts a list of full row keys"
    );
    assert_eq!(
        csv_rows(&run("SELECT count(*) FROM moraine_index_in(\
                'lake', 'main', 't', 'by_a', []::BIGINT[]);")),
        vec![vec!["0".to_string()]],
        "an empty typed list returns no rows"
    );
}

/// A composite index answers a range whose bounds are `row(...)` tuples over
/// its leading columns: a leading-column equality window (a one-field tuple),
/// a full-tuple window, and a half-open window with a NULL open side.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_range_spans_a_composite_window() {
    let store = TempDir::new("index-composite-range-store");
    let data = TempDir::new("index-composite-range-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    run("INSERT INTO lake.main.t VALUES (5, 'x'), (5, 'y'), (7, 'x');");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_ab', ['a', 'b'], true);");

    // a = 5 as a one-field tuple bound: both rows sharing the leading value.
    assert_eq!(
        csv_rows(&run("SELECT row_id FROM \
             moraine_index_range('lake', 'main', 't', 'by_ab', row(5), row(5), true, true) \
             ORDER BY row_id;")),
        vec![vec!["0".to_string()], vec!["1".to_string()]],
        "a = 5 spans rows 0 and 1"
    );
    // (5, 'y') ..= (7, 'x') over the full tuple: excludes (5, 'x').
    assert_eq!(
        csv_rows(&run("SELECT row_id FROM \
             moraine_index_range('lake', 'main', 't', 'by_ab', row(5, 'y'), row(7, 'x'), true, true) \
             ORDER BY row_id;")),
        vec![vec!["1".to_string()], vec!["2".to_string()]],
        "(5, 'y')..=(7, 'x') spans rows 1 and 2"
    );
    // a >= 7 with a NULL (open) upper side: only the (7, _) row.
    assert_eq!(
        csv_rows(&run("SELECT row_id FROM \
             moraine_index_range('lake', 'main', 't', 'by_ab', row(7), NULL, true, true);")),
        vec![vec!["2".to_string()]],
        "a >= 7 spans only row 2"
    );
}

/// The range SQL surface: `moraine_index_range` resolves a comparison
/// window over the same index the lookup uses — closed, open, and half-open
/// (a NULL bound is an open side).
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_range_selects_a_comparison_window() {
    let store = TempDir::new("index-range-store");
    let data = TempDir::new("index-range-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    let count = |sql: &str| csv_rows(&run(sql));

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

    // BETWEEN 10 AND 20, both inclusive: values 10..=20 -> 11 rows.
    assert_eq!(
        count(
            "SELECT count(*) FROM \
             moraine_index_range('lake', 'main', 't', 'by_a', 10, 20, true, true);"
        ),
        vec![vec!["11".to_string()]],
        "closed [10, 20] selects eleven rows"
    );
    // 10 < a < 20, both exclusive: values 11..=19 -> 9 rows.
    assert_eq!(
        count(
            "SELECT count(*) FROM \
             moraine_index_range('lake', 'main', 't', 'by_a', 10, 20, false, false);"
        ),
        vec![vec!["9".to_string()]],
        "open (10, 20) selects nine rows"
    );
    // a >= 90 with an open upper side (NULL bound): values 90..=99 -> 10 rows.
    assert_eq!(
        count(
            "SELECT count(*) FROM \
             moraine_index_range('lake', 'main', 't', 'by_a', 90, NULL::BIGINT, true, true);"
        ),
        vec![vec!["10".to_string()]],
        "half-open [90, +inf) selects ten rows"
    );
    // a < 5 with an open lower side: values 0..=4 -> 5 rows.
    assert_eq!(
        count(
            "SELECT count(*) FROM \
             moraine_index_range('lake', 'main', 't', 'by_a', NULL::BIGINT, 5, true, false);"
        ),
        vec![vec!["5".to_string()]],
        "half-open (-inf, 5) selects five rows"
    );

    // `reverse := true` returns the window high value first: row_id == value
    // here (row i holds value i), so [10, 20] reversed leads with 20.
    assert_eq!(
        csv_rows(&run("SELECT row_id FROM \
             moraine_index_range('lake', 'main', 't', 'by_a', 10, 20, true, true, reverse := true) \
             LIMIT 1;")),
        vec![vec!["20".to_string()]],
        "reverse leads with the high end of the window"
    );
    // Ascending (the default) leads with the low end.
    assert_eq!(
        csv_rows(&run("SELECT row_id FROM \
             moraine_index_range('lake', 'main', 't', 'by_a', 10, 20, true, true) LIMIT 1;")),
        vec![vec!["10".to_string()]],
        "the default order leads with the low end"
    );
}

/// A descending index created via the `directions := ['desc']` named
/// parameter answers both a range window and an equality lookup — the
/// per-column direction rides `moraine_index_create` into the stored order.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_create_descending_answers_range_and_lookup() {
    let store = TempDir::new("index-desc-store");
    let data = TempDir::new("index-desc-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    let count = |sql: &str| csv_rows(&run(sql));

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");
    run(
        "CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true, \
         directions := ['desc']);",
    );

    // A descending index answers a closed value window: 10..=20 -> 11 rows.
    assert_eq!(
        count(
            "SELECT count(*) FROM \
             moraine_index_range('lake', 'main', 't', 'by_a', 10, 20, true, true);"
        ),
        vec![vec!["11".to_string()]],
        "the descending index answers a closed window"
    );
    // And an equality lookup on the same descending index.
    assert_eq!(
        count("SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', 42);"),
        vec![vec!["1".to_string()]],
        "the descending index answers an equality lookup"
    );
}

/// A NULLS FIRST index created via `nulls := ['first']`: the placement rides
/// into the stored key's null flag, and the same flag is used when encoding a
/// query bound — so values still resolve, proving the parameter threads
/// through both the entry and the query paths.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_create_nulls_first_still_resolves_values() {
    let store = TempDir::new("index-nulls-store");
    let data = TempDir::new("index-nulls-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    let count = |sql: &str| csv_rows(&run(sql));

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");
    run(
        "CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true, \
         directions := ['desc'], nulls := ['first']);",
    );

    assert_eq!(
        count("SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', 42);"),
        vec![vec!["1".to_string()]],
        "a NULLS FIRST index still resolves an equality lookup"
    );
    assert_eq!(
        count(
            "SELECT count(*) FROM \
             moraine_index_range('lake', 'main', 't', 'by_a', 10, 20, true, true);"
        ),
        vec![vec!["11".to_string()]],
        "a NULLS FIRST index still answers a range window"
    );
}

/// The `IS NULL` SQL surface: `moraine_index_nulls` finds the rows whose
/// leading indexed column is NULL (a NULL variadic arg means `IS NULL`), and
/// a unique index admits multiple NULL rows — both a bulk (Parquet) and an
/// inline NULL are covered.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_nulls_finds_null_rows() {
    let store = TempDir::new("index-isnull-store");
    let data = TempDir::new("index-isnull-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    let count = |sql: &str| csv_rows(&run(sql));

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    // 98 non-null rows in a bulk (Parquet) file, plus two NULL rows.
    run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(98) t(i);");
    run("INSERT INTO lake.main.t VALUES (NULL, 'n1'), (NULL, 'n2');");
    // A unique index must accept the two NULL rows (SQL treats NULLs distinct).
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

    // a IS NULL resolves to exactly the two NULL rows.
    assert_eq!(
        count("SELECT count(*) FROM moraine_index_nulls('lake', 'main', 't', 'by_a', NULL);"),
        vec![vec!["2".to_string()]],
        "IS NULL finds both NULL rows"
    );
    // A non-null value is still uniquely resolvable, unaffected.
    assert_eq!(
        count("SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', 5);"),
        vec![vec!["1".to_string()]],
        "a non-null value is still uniquely resolvable"
    );
    // An inline NULL added after the index also becomes findable.
    run("INSERT INTO lake.main.t VALUES (NULL, 'n3');");
    assert_eq!(
        count("SELECT count(*) FROM moraine_index_nulls('lake', 'main', 't', 'by_a', NULL);"),
        vec![vec!["3".to_string()]],
        "an inline NULL inserted after create is indexed too"
    );
}

/// Write-path coverage: a bulk INSERT *after* the index exists is
/// maintained by the staged commit scoped-reading the new Parquet from
/// `DATA_PATH`, and a duplicate INSERT is rejected on the unique index.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_maintained_on_bulk_insert() {
    let store = TempDir::new("index-maint-store");
    let data = TempDir::new("index-maint-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

    // A bulk INSERT after create is maintained by the staged commit.
    run("INSERT INTO lake.main.t SELECT i, 'y' FROM range(100, 200) t(i);");
    let post = csv_rows(&run("SELECT count(*) FROM \
         moraine_index_lookup('lake', 'main', 't', 'by_a', 150);"));
    assert_eq!(
        post,
        vec![vec!["1".to_string()]],
        "value 150 from the post-create INSERT is indexed"
    );
    // The backfilled rows are still resolvable too.
    let pre = csv_rows(&run("SELECT count(*) FROM \
         moraine_index_lookup('lake', 'main', 't', 'by_a', 42);"));
    assert_eq!(
        pre,
        vec![vec!["1".to_string()]],
        "the backfilled value 42 is indexed"
    );

    // A small INSERT (one row, under the 10-row inline limit) is inlined
    // as an Arrow chunk, not a Parquet file, and is maintained too.
    run("INSERT INTO lake.main.t VALUES (500, 'z');");
    let inline = csv_rows(&run("SELECT data.a \
         FROM lake.main.t data \
         JOIN moraine_index_lookup('lake', 'main', 't', 'by_a', 500) hits \
           ON hits.row_id = data.rowid;"));
    assert_eq!(
        inline,
        vec![vec!["500".to_string()]],
        "the inlined value 500 is read through its stable row id"
    );

    // Duplicates are rejected on both write paths: a bulk (Parquet) INSERT
    // and a small (inline) INSERT of an already-indexed value.
    let reject = |sql: &str| {
        let out = run_ducklake_sql_output(store.path(), data.path(), &meta, sql);
        assert!(
            !out.status.success(),
            "`{sql}` should fail; stdout: {}",
            String::from_utf8_lossy(&out.stdout)
        );
        let stderr = String::from_utf8_lossy(&out.stderr).to_lowercase();
        assert!(
            stderr.contains("duplicate value violates equality index"),
            "expected the equality-index constraint error for `{sql}`, got: {stderr}"
        );
        // The message must avoid DuckLake's retry substrings, or its
        // commit loop spins instead of surfacing the error.
        for retry_word in ["conflict", "concurrent", "unique", "primary key"] {
            assert!(
                !stderr.contains(retry_word),
                "the constraint message must not contain `{retry_word}`: {stderr}"
            );
        }
    };
    reject("INSERT INTO lake.main.t SELECT i, 'dup' FROM range(20) t(i);");
    reject("INSERT INTO lake.main.t VALUES (500, 'dup');");
}

/// Delete-path coverage: entries are live-only, so a DELETE frees the
/// value it killed and the same key can be written again. Covers both
/// residences — an inlined row and a row in a flushed Parquet file —
/// and the replace-in-one-transaction shape a writer depends on.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_entries_are_removed_by_delete() {
    let store = TempDir::new("index-delete-store");
    let data = TempDir::new("index-delete-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    let count = |sql: &str| csv_rows(&run(sql));

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

    // A row in the flushed Parquet file: deleting it frees its value.
    run("DELETE FROM lake.main.t WHERE a = 42;");
    assert_eq!(
        count("SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', 42);"),
        vec![vec!["0".to_string()]],
        "the deleted row's entry is gone"
    );
    run("INSERT INTO lake.main.t VALUES (42, 'again');");
    assert_eq!(
        count("SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', 42);"),
        vec![vec!["1".to_string()]],
        "the freed value is insertable again"
    );

    // A small INSERT stays inline; deleting it frees its value too.
    run("INSERT INTO lake.main.t VALUES (500, 'z');");
    run("DELETE FROM lake.main.t WHERE a = 500;");
    assert_eq!(
        count("SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', 500);"),
        vec![vec!["0".to_string()]],
        "the inlined row's entry is gone"
    );
    run("INSERT INTO lake.main.t VALUES (500, 'again');");
    assert_eq!(
        count("SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', 500);"),
        vec![vec!["1".to_string()]],
        "the freed inlined value is insertable again"
    );

    // The replace shape: delete and reinsert one key in one transaction.
    run("BEGIN; DELETE FROM lake.main.t WHERE a = 7; \
         INSERT INTO lake.main.t VALUES (7, 'replaced'); COMMIT;");
    assert_eq!(
        count("SELECT b FROM lake.main.t WHERE a = 7;"),
        vec![vec!["replaced".to_string()]],
        "the replacement row is the live one"
    );
    assert_eq!(
        count("SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', 7);"),
        vec![vec!["1".to_string()]],
        "the replaced key resolves to exactly one row"
    );

    // Every lookup still resolves to a row that exists: no entry outlives
    // its row, which is what a non-unique leak would show as.
    assert_eq!(
        count("SELECT count(*) FROM lake.main.t;"),
        vec![vec!["101".to_string()]],
        "100 original rows, plus the extra key 500; every delete was reinserted"
    );
}

/// The data root, given once at creation via `META_DATA_PATH`, is
/// persisted at bootstrap and served back — so later attaches maintain
/// the index with `DATA_PATH` alone, and even with no data-path option at
/// all (DuckLake reads the root moraine serves).
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_data_path_persists_across_attach() {
    let store = TempDir::new("index-persist-store");
    let data = TempDir::new("index-persist-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());

    // Creation: DATA_PATH + META_DATA_PATH seeds and persists the root.
    let create = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    create("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    create("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");
    create("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

    // Re-attach with DATA_PATH only (no META_DATA_PATH): a bulk INSERT
    // (a Parquet file, not inline) is maintained by scoped-reading it from
    // the data store moraine resolves off the *recorded* root — the value
    // never came over this attach.
    let dp = |sql: &str| run_ducklake_sql(store.path(), data.path(), sql);
    dp("INSERT INTO lake.main.t SELECT i, 'y' FROM range(100, 120) t(i);");
    assert_eq!(
        csv_rows(&dp("SELECT count(*) FROM \
             moraine_index_lookup('lake','main','t','by_a',110);")),
        vec![vec!["1".to_string()]],
        "a bulk value indexed through a DATA_PATH-only attach is found"
    );
    let dup_dp = run_ducklake_sql_output(
        store.path(),
        data.path(),
        "",
        "INSERT INTO lake.main.t SELECT i, 'dup' FROM range(100, 120) t(i);",
    );
    assert!(
        !dup_dp.status.success(),
        "a bulk duplicate is rejected on a DATA_PATH-only attach; stdout: {}",
        String::from_utf8_lossy(&dup_dp.stdout)
    );

    // Re-attach with no data-path option: DuckLake reads the served root,
    // and maintenance still works.
    let bare_count = run_ducklake_sql_bare(store.path(), "SELECT count(*) FROM lake.main.t;");
    assert!(
        bare_count.status.success(),
        "a bare attach reads the served data root; stderr: {}",
        String::from_utf8_lossy(&bare_count.stderr)
    );
    assert_eq!(
        csv_rows(&String::from_utf8_lossy(&bare_count.stdout)),
        vec![vec!["120".to_string()]],
        "the lake is readable through a bare attach (100 seed + 20 bulk)"
    );
    let dup_bare =
        run_ducklake_sql_bare(store.path(), "INSERT INTO lake.main.t VALUES (110, 'z');");
    assert!(
        !dup_bare.status.success(),
        "a duplicate is rejected on a bare attach; stdout: {}",
        String::from_utf8_lossy(&dup_bare.stdout)
    );
}

/// A catalog name that is not a moraine-backed lake is refused with a
/// clear error, not a crash — the handle downcast behind the index
/// functions is unchecked, so the kind is verified first.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_functions_reject_a_non_moraine_catalog() {
    let store = TempDir::new("index-badcat-store");
    let data = TempDir::new("index-badcat-data");
    // `memory` is DuckDB's built-in in-memory catalog — present, but not
    // a moraine catalog, and not a DuckLake lake either.
    let out = run_ducklake_sql_output(
        store.path(),
        data.path(),
        "",
        "CALL moraine_index_create('memory', 'main', 't', 'by_a', ['a'], true);",
    );
    assert!(
        !out.status.success(),
        "a non-moraine catalog must be refused; stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    // The clean error proves it did not crash: a SIGSEGV would carry no
    // such message.
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("not a moraine-backed lake"),
        "expected a clear rejection, got: {stderr}"
    );
}

/// A unique index over a `UUID` column works across write paths and is
/// queryable by UUID: a UUID written to a Parquet file backfills and
/// looks up, and an inline duplicate of it is rejected — proving the
/// inline Arrow encoding and the Parquet encoding derive the same key.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_uuid_across_paths_and_lookup() {
    const KNOWN: &str = "550e8400-e29b-41d4-a716-446655440000";
    let store = TempDir::new("index-uuid-store");
    let data = TempDir::new("index-uuid-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

    run("CREATE TABLE lake.main.t(id UUID, name VARCHAR);");
    // A bulk insert (>10 rows → a Parquet file) that includes the known
    // UUID at row 0, the rest random.
    run(&format!(
        "INSERT INTO lake.main.t SELECT \
             CASE WHEN i = 0 THEN '{KNOWN}'::UUID ELSE gen_random_uuid() END, 'x' \
         FROM range(20) t(i);"
    ));
    run("CALL moraine_index_create('lake', 'main', 't', 'by_id', ['id'], true);");

    // The Parquet-stored UUID is found by an equality lookup.
    let hit = csv_rows(&run(&format!(
        "SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_id', '{KNOWN}'::UUID);"
    )));
    assert_eq!(
        hit,
        vec![vec!["1".to_string()]],
        "the UUID is indexed and found"
    );
    let miss = csv_rows(&run(
        "SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_id', \
             '00000000-0000-0000-0000-000000000000'::UUID);",
    ));
    assert_eq!(
        miss,
        vec![vec!["0".to_string()]],
        "an absent UUID resolves to none"
    );

    // An inline (single-row) INSERT of the same UUID is rejected: inline
    // maintenance derives the same 16-byte key the Parquet path stored.
    let dup = run_ducklake_sql_output(
        store.path(),
        data.path(),
        &meta,
        &format!("INSERT INTO lake.main.t VALUES ('{KNOWN}'::UUID, 'dup');"),
    );
    assert!(
        !dup.status.success(),
        "a cross-path UUID duplicate must be rejected; stdout: {}",
        String::from_utf8_lossy(&dup.stdout)
    );
}

/// A unique index over a `TIMESTAMP_MS` column: the scoped read must read
/// the millisecond array (not misread it as microseconds), and the inline
/// path must derive the same millisecond count — so backfill succeeds and
/// a cross-path duplicate is rejected.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_millisecond_timestamp() {
    let store = TempDir::new("index-ts-store");
    let data = TempDir::new("index-ts-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

    run("CREATE TABLE lake.main.t(ts TIMESTAMP_MS, name VARCHAR);");
    // A bulk insert (>10 rows → a Parquet file) of 20 distinct sub-second
    // millisecond timestamps, row 0 at the epoch base.
    run("INSERT INTO lake.main.t SELECT \
             '2023-01-01 00:00:00'::TIMESTAMP_MS + to_milliseconds(i * 137), 'x' \
         FROM range(20) t(i);");
    // Before the fix this failed: the scoped read downcast the millisecond
    // column as microseconds.
    run("CALL moraine_index_create('lake', 'main', 't', 'by_ts', ['ts'], true);");

    // An inline (single-row) INSERT duplicating row 0's timestamp is
    // rejected: the inline path derives the same millisecond key.
    let dup = run_ducklake_sql_output(
        store.path(),
        data.path(),
        &meta,
        "INSERT INTO lake.main.t VALUES ('2023-01-01 00:00:00'::TIMESTAMP_MS, 'dup');",
    );
    assert!(
        !dup.status.success(),
        "a cross-path millisecond-timestamp duplicate must be rejected; stdout: {}",
        String::from_utf8_lossy(&dup.stdout)
    );
}

/// A `HUGEINT` column is refused at index creation with a clear reason:
/// DuckDB stores it as a lossy double in Parquet, so it cannot be a
/// faithful equality index (a silently wrong one would be worse).
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_rejects_hugeint() {
    let store = TempDir::new("index-hugeint-store");
    let data = TempDir::new("index-hugeint-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &meta,
        "CREATE TABLE lake.main.t(big HUGEINT, name VARCHAR);",
    );
    let out = run_ducklake_sql_output(
        store.path(),
        data.path(),
        &meta,
        "CALL moraine_index_create('lake', 'main', 't', 'by_big', ['big'], true);",
    );
    assert!(
        !out.status.success(),
        "a HUGEINT index must be refused; stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr).to_lowercase();
    assert!(
        stderr.contains("lossy double"),
        "expected the lossy-double reason, got: {stderr}"
    );
}

/// With no data-path store resolvable (a lake attached with neither a
/// recorded nor a supplied data path), an index can still be created on
/// an empty table — but a later bulk INSERT is refused rather than
/// silently leaving the index under-covered.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_bulk_insert_without_a_data_store_is_refused() {
    let store = TempDir::new("index-nostore-store");
    let data = TempDir::new("index-nostore-data");
    // DATA_PATH only (no META_DATA_PATH) on a fresh lake: moraine records
    // and resolves no data store.
    let run = |sql: &str| run_ducklake_sql(store.path(), data.path(), sql);

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    // An index on the still-empty table needs no scoped read, so it is
    // created fine.
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

    // A bulk INSERT registers a Parquet file that would need scoped-reading
    // to maintain the index; with no store, the commit is refused.
    let out = run_ducklake_sql_output(
        store.path(),
        data.path(),
        "",
        "INSERT INTO lake.main.t SELECT i, 'x' FROM range(20) t(i);",
    );
    assert!(
        !out.status.success(),
        "a bulk insert with no data store must be refused; stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr).to_lowercase();
    assert!(
        stderr.contains("no data-path store"),
        "expected the missing-store reason, got: {stderr}"
    );
}

/// UPDATE and both compaction shapes write per-row-id files; the index
/// tracks every move, and a rebuilt index backfills them.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_survives_update_and_compaction() {
    let store = TempDir::new("index-rewrite-store");
    let data = TempDir::new("index-rewrite-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    let count = |sql: &str| csv_rows(&run(sql));
    let lookup_count = |key: i64| {
        count(&format!(
            "SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', {key});"
        ))
    };

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

    // UPDATE writes a per-row-id file (delete plus re-insert under
    // preserved ids); the unchanged key still resolves exactly once.
    run("UPDATE lake.main.t SET b = 'updated' WHERE a = 7;");
    assert_eq!(lookup_count(7), vec![vec!["1".to_string()]]);
    assert_eq!(
        count(
            "SELECT data.b FROM lake.main.t data \
             JOIN moraine_index_lookup('lake', 'main', 't', 'by_a', 7) hits \
               ON data.rowid = hits.row_id;"
        ),
        vec![vec!["updated".to_string()]],
        "the row-id join follows the preserved id into the UPDATE file"
    );

    // Deletes then rewrite: the compacted replacement re-derives its
    // surviving rows' entries as no-ops.
    run("DELETE FROM lake.main.t WHERE a IN (1, 2, 3);");
    run("CALL ducklake_rewrite_data_files('lake', delete_threshold => 0.01);");
    assert_eq!(
        lookup_count(1),
        vec![vec!["0".to_string()]],
        "deleted keys stay gone after the rewrite"
    );
    assert_eq!(
        lookup_count(50),
        vec![vec!["1".to_string()]],
        "survivors stay found after the rewrite"
    );

    // A delete against the rewritten (per-row-id) file.
    run("DELETE FROM lake.main.t WHERE a = 50;");
    assert_eq!(lookup_count(50), vec![vec!["0".to_string()]]);
    assert!(
        count(
            "SELECT data.a FROM lake.main.t data \
             JOIN moraine_index_lookup('lake', 'main', 't', 'by_a', 50) hits \
               ON data.rowid = hits.row_id;"
        )
        .is_empty(),
        "DuckLake's scan applies the delete when reading through the row-id join"
    );

    // Merge-adjacent over the mixed file set.
    run("INSERT INTO lake.main.t SELECT i, 'y' FROM range(100, 200) t(i);");
    run("CALL ducklake_merge_adjacent_files('lake');");
    assert_eq!(lookup_count(150), vec![vec!["1".to_string()]]);
    assert_eq!(lookup_count(7), vec![vec!["1".to_string()]]);

    // Rebuild: backfill must read the per-row-id files.
    run("CALL moraine_index_drop('lake', 'main', 't', 'by_a');");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");
    assert_eq!(lookup_count(150), vec![vec!["1".to_string()]]);
    assert_eq!(
        lookup_count(1),
        vec![vec!["0".to_string()]],
        "the rebuilt index omits rows deleted before compaction"
    );
}

/// The index functions resolve a lake whose metadata catalog was named
/// by `METADATA_CATALOG` rather than DuckLake's default
/// `__ducklake_metadata_<lake>`. Name derivation cannot find such a
/// catalog, so resolution matches attached databases on path — the same
/// resolver the maintenance functions use.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_functions_resolve_a_custom_metadata_catalog() {
    let store = TempDir::new("index-custom-meta-store");
    let data = TempDir::new("index-custom-meta-data");
    let options = format!(
        ", META_DATA_PATH '{}', METADATA_CATALOG 'custom_meta'",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &options, sql);

    run("CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(6) t(i);\
         SELECT * FROM moraine_index_create('lake','main','t','by_a',['a'],true);");

    // Addressed by the lake name, whose metadata catalog shares no name
    // with it.
    assert_eq!(
        csv_rows(&run(
            "SELECT index_name FROM moraine_indexes('lake','main','t');"
        )),
        vec![vec!["by_a".to_string()]]
    );
    assert_eq!(
        csv_rows(&run(
            "SELECT row_id FROM moraine_index_lookup('lake','main','t','by_a', 3);"
        )),
        vec![vec!["3".to_string()]]
    );

    // ...and by the metadata catalog's own name.
    assert_eq!(
        csv_rows(&run(
            "SELECT index_name FROM moraine_indexes('custom_meta','main','t');"
        )),
        vec![vec!["by_a".to_string()]]
    );

    run("SELECT * FROM moraine_index_drop('lake','main','t','by_a');");
    assert!(
        csv_rows(&run(
            "SELECT index_name FROM moraine_indexes('lake','main','t');"
        ))
        .is_empty(),
        "the drop must land through the same resolution"
    );
}

/// `staged := true` builds an index over a table that already holds bulk
/// data, across several commits, and lands ready: lookups serve, the
/// introspection view reports it built, and a later duplicate is refused
/// exactly as a single-commit build's index would refuse it.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_create_staged_builds_over_existing_data() {
    let store = TempDir::new("index-staged-store");
    let data = TempDir::new("index-staged-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(500) t(i);");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true, staged := true);");

    // The build finished: not building, and the backfilled rows resolve.
    assert_eq!(
        csv_rows(&run(
            "SELECT is_building FROM moraine_indexes('lake','main','t');"
        )),
        vec![vec!["false".to_string()]],
        "the staged build flipped ready"
    );
    for value in ["0", "250", "499"] {
        assert_eq!(
            csv_rows(&run(&format!(
                "SELECT count(*) FROM moraine_index_lookup('lake','main','t','by_a', {value});"
            ))),
            vec![vec!["1".to_string()]],
            "backfilled value {value} is indexed"
        );
    }

    // Writers maintain the finished index, and a duplicate is refused with
    // the terminal (non-retryable) constraint message.
    run("INSERT INTO lake.main.t SELECT i, 'y' FROM range(500, 600) t(i);");
    assert_eq!(
        csv_rows(&run(
            "SELECT count(*) FROM moraine_index_lookup('lake','main','t','by_a', 550);"
        )),
        vec![vec!["1".to_string()]],
        "a post-build INSERT is maintained"
    );
    let out = run_ducklake_sql_output(
        store.path(),
        data.path(),
        &meta,
        "INSERT INTO lake.main.t SELECT i, 'dup' FROM range(20) t(i);",
    );
    assert!(!out.status.success(), "the duplicate INSERT must fail");
    let stderr = String::from_utf8_lossy(&out.stderr).to_lowercase();
    assert!(
        stderr.contains("duplicate value violates equality index"),
        "expected the equality-index constraint error, got: {stderr}"
    );
    for retry_word in ["conflict", "concurrent", "unique", "primary key"] {
        assert!(
            !stderr.contains(retry_word),
            "the constraint message must not contain `{retry_word}`: {stderr}"
        );
    }
}

/// Flushing an indexed table whose inlined rows are partly deleted — the
/// shape that carries a delete file targeting a data file the committed
/// head has never seen.
///
/// DuckLake's flush writes *every* inlined row into one Parquet file,
/// tombstoned ones included, then writes a delete file naming the
/// tombstoned rows' positions in that same just-written file. So one
/// commit carries both the `ducklake_data_file` insert and a
/// `ducklake_delete_file` insert against it. The flush preserves row ids and
/// values, and the tombstoned rows' index entries were removed by the earlier
/// delete, so index upkeep must leave the whole output pair alone.
///
/// Only an indexed table reaches any of this: upkeep returns early when
/// the table carries no index, which is why the uninindexed flush tests in
/// `inline.rs` pass either way.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn flushing_an_indexed_table_with_deleted_inlined_rows() {
    let store = TempDir::new("index-flush-store");
    let data = TempDir::new("index-flush-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

    // Under the inlining limit, so these land as inlined rows rather than
    // a Parquet file.
    run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(5) t(i);");
    run("DELETE FROM lake.main.t WHERE a IN (1, 3);");

    // The flush materializes all five rows and deletes two back out.
    run("CALL ducklake_flush_inlined_data('lake');");

    assert_eq!(
        csv_rows(&run("SELECT count(*), sum(a) FROM lake.main.t;")),
        vec![vec!["3".to_string(), "6".to_string()]],
        "rows 1 and 3 stay deleted across the flush"
    );

    // The index must agree with the table: survivors resolve, and the
    // flushed-then-deleted rows must not — a stale entry here is the
    // silent half of the bug, invisible to the count above.
    for survivor in ["0", "2", "4"] {
        assert_eq!(
            csv_rows(&run(&format!(
                "SELECT count(*) FROM moraine_index_lookup('lake','main','t','by_a', {survivor});"
            ))),
            vec![vec!["1".to_string()]],
            "surviving value {survivor} is indexed after the flush"
        );
    }
    for killed in ["1", "3"] {
        assert_eq!(
            csv_rows(&run(&format!(
                "SELECT count(*) FROM moraine_index_lookup('lake','main','t','by_a', {killed});"
            ))),
            vec![vec!["0".to_string()]],
            "deleted value {killed} must not survive in the index"
        );
    }

    // The value a deleted row held is free again: re-inserting it must not
    // trip the unique index against a resurrected entry.
    run("INSERT INTO lake.main.t VALUES (1, 'again');");
    assert_eq!(
        csv_rows(&run(
            "SELECT count(*) FROM moraine_index_lookup('lake','main','t','by_a', 1);"
        )),
        vec![vec!["1".to_string()]],
        "the reinserted value resolves to exactly one row"
    );
}

/// Index entries across the whole data-file lifecycle: a file that is
/// ended by a rewrite and then hard-pruned by expiry and cleanup.
///
/// Nothing removes a live index's entries when a data file's row leaves
/// the catalog — the sweep only reclaims entries of *dropped* indexes. The
/// invariant that keeps that sound is that a file's rows never leave
/// without either a row-grain deletion having already taken their entries
/// (a delete file, an inlined delete) or a replacement re-deriving them
/// under their preserved row ids. This walks the full sequence and holds
/// the index to the table's own answer at each step.
///
/// A stale entry is visible here, not silent: the lookup table function
/// returns row ids directly, so a leaked entry still appears there even
/// though the ordinary DuckLake join would filter it as a missing row.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_entries_survive_the_data_file_lifecycle() {
    let store = TempDir::new("index-lifecycle-store");
    let data = TempDir::new("index-lifecycle-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    let lookup_count = |key: i64| {
        csv_rows(&run(&format!(
            "SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', {key});"
        )))
    };
    let found = |n: &str| vec![vec![n.to_string()]];

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");
    run("DELETE FROM lake.main.t WHERE a IN (1, 2, 3);");

    // The rewrite ends the source into history and writes a replacement
    // holding only the survivors.
    run("CALL ducklake_rewrite_data_files('lake', delete_threshold => 0.01);");
    assert_eq!(lookup_count(1), found("0"), "deleted keys stay gone");
    assert_eq!(lookup_count(50), found("1"), "survivors stay found");

    // Expiry plus cleanup hard-prunes the ended source's row, so the file
    // the original entries were derived from is gone from the catalog
    // entirely.
    run("CALL ducklake_expire_snapshots('lake', older_than => now() + INTERVAL 1 DAY);");
    run("CALL ducklake_cleanup_old_files('lake', cleanup_all => true);");

    assert_eq!(
        csv_rows(&run("SELECT count(*), sum(a) FROM lake.main.t;")),
        vec![vec!["97".to_string(), "4944".to_string()]],
        "the table is unchanged by the pruning"
    );
    assert_eq!(
        lookup_count(1),
        found("0"),
        "a pruned file must not leave its deleted rows' entries behind"
    );
    assert_eq!(lookup_count(50), found("1"), "survivors are still indexed");

    // The sharpest probe: a stale unique entry for a deleted value would
    // refuse this insert as a duplicate.
    run("INSERT INTO lake.main.t VALUES (1, 'again');");
    assert_eq!(
        lookup_count(1),
        found("1"),
        "the freed value is claimable again, exactly once"
    );
}

/// `step_entries` and `step_bytes` size the staged build's commits from
/// SQL. The default step is a million entries or eight mebibytes, which no
/// reachable test table exceeds — so without these the build is always one
/// step, and a link too slow to carry that one step has no recourse.
///
/// Observed through the snapshot count: each step is its own commit.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_create_takes_a_step_size() {
    let store = TempDir::new("index-step-store");
    let data = TempDir::new("index-step-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

    let snapshots = |out: String| -> i64 {
        csv_rows(&out)[0][0]
            .parse()
            .expect("the snapshot count is a number")
    };

    run("CREATE TABLE lake.main.t(a BIGINT);");
    run("INSERT INTO lake.main.t SELECT i FROM range(400) t(i);");
    let before = snapshots(run("SELECT count(*) FROM ducklake_snapshots('lake');"));

    run(
        "CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true, \
         staged := true, step_entries := 100);",
    );
    let after = snapshots(run("SELECT count(*) FROM ducklake_snapshots('lake');"));

    // The definition commit, then 400 entries in steps of 100.
    assert_eq!(after - before, 1 + 4, "one commit per step of 100");
    assert_eq!(
        csv_rows(&run(
            "SELECT is_building FROM moraine_indexes('lake','main','t');"
        )),
        vec![vec!["false".to_string()]],
        "the build still flipped ready"
    );
    assert_eq!(
        csv_rows(&run(
            "SELECT count(*) FROM moraine_index_lookup('lake','main','t','by_a', 399);"
        )),
        vec![vec!["1".to_string()]],
        "every step's entries landed"
    );

    // A byte bound cuts steps the same way, and a bound below one entry
    // still advances one entry at a time rather than stalling.
    run("CREATE TABLE lake.main.u(a BIGINT);");
    run("INSERT INTO lake.main.u SELECT i FROM range(5) t(i);");
    let before = snapshots(run("SELECT count(*) FROM ducklake_snapshots('lake');"));
    run(
        "CALL moraine_index_create('lake', 'main', 'u', 'by_a', ['a'], true, \
         staged := true, step_bytes := 1);",
    );
    let after = snapshots(run("SELECT count(*) FROM ducklake_snapshots('lake');"));
    assert_eq!(after - before, 1 + 5, "one commit per entry");

    // A step admitting nothing is refused at bind, before any commit.
    let out = run_ducklake_sql_output(
        store.path(),
        data.path(),
        &meta,
        "CALL moraine_index_create('lake', 'main', 't', 'by_b', ['a'], true, \
         staged := true, step_entries := 0);",
    );
    assert!(!out.status.success(), "a zero step must fail");
    let stderr = String::from_utf8_lossy(&out.stderr).to_lowercase();
    assert!(
        stderr.contains("step_entries"),
        "the refusal names the parameter, got: {stderr}"
    );
}

/// Deferred maintenance lets a non-unique index move SQL-write upkeep out
/// of the data commit, then catches it up in bounded post-commit steps before
/// returning. The index never serves a partial answer.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn moraine_index_deferred_maintenance_catches_up_after_sql_insert() {
    let store = TempDir::new("index-deferred-store");
    let data = TempDir::new("index-deferred-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

    run("CREATE TABLE lake.main.t(a BIGINT);");
    run(
        "CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], false, \
         maintenance := 'deferred');",
    );
    let schema_rows = csv_rows(&run(
        "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_schema_versions;",
    ));
    run("INSERT INTO lake.main.t SELECT i % 3 FROM range(7) t(i);");

    assert_eq!(
        csv_rows(&run(
            "SELECT count(*) FROM moraine_index_lookup('lake','main','t','by_a', 1);"
        )),
        vec![vec!["2".to_string()]],
        "the post-commit pass made every inserted row visible"
    );
    assert_eq!(
        csv_rows(&run(
            "SELECT is_building FROM moraine_indexes('lake','main','t');"
        )),
        vec![vec!["false".to_string()]],
        "the post-commit pass flipped the index back to ready"
    );

    run("INSERT INTO lake.main.t VALUES (1);");
    run("CALL ducklake_flush_inlined_data('lake');");
    run("CALL lake.set_option('data_inlining_row_limit', 0);");
    run("UPDATE lake.main.t SET a = 2 WHERE a = 0;");
    assert_eq!(
        csv_rows(&run(
            "SELECT count(*) FROM moraine_index_lookup('lake','main','t','by_a', 1);"
        )),
        vec![vec!["3".to_string()]],
        "a later repair starts at the preceding source tail"
    );
    assert_eq!(
        csv_rows(&run(
            "SELECT count(*) FROM moraine_index_lookup('lake','main','t','by_a', 2);"
        )),
        vec![vec!["5".to_string()]],
        "deferred UPDATE additions replace their synchronous removals"
    );
    assert_eq!(
        csv_rows(&run(
            "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_schema_versions;",
        )),
        schema_rows,
        "routine deferred repair does not grow table schema history"
    );

    let stderr = run_ducklake_sql_expect_err_with_options(
        store.path(),
        data.path(),
        &meta,
        "CALL moraine_index_create('lake', 'main', 't', 'unique_by_a', ['a'], true, \
         maintenance := 'deferred');",
    );
    assert!(
        stderr.contains("non-unique"),
        "deferred uniqueness cannot be sound: {stderr}"
    );
}
