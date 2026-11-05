//! Sort specs through the verb surface: setting, replacing, clearing, and
//! reading one back — at head, by time travel, and through the projection
//! DuckLake reads.

use moraine::{Error, SortKeyDef, TableId};

use crate::fixtures::seeded;

fn key(expression: &str, direction: &str) -> SortKeyDef {
    SortKeyDef {
        expression: expression.into(),
        dialect: "duckdb".into(),
        sort_direction: direction.into(),
        null_order: "NULLS_LAST".into(),
    }
}

#[tokio::test]
async fn a_spec_reads_back_with_its_keys_in_declared_order() {
    let (catalog, _, table, _) = seeded().await;
    catalog
        .commit(async move |tx| {
            tx.set_sorting(table, &[key("year(x)", "DESC"), key("x", "ASC")])
                .await?;
            Ok(())
        })
        .await
        .unwrap();

    let spec = catalog
        .read_scope()
        .await
        .unwrap()
        .sorting_of(table)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        spec.keys,
        vec![key("year(x)", "DESC"), key("x", "ASC")],
        "keys keep their declared order and verbatim expressions"
    );
}

#[tokio::test]
async fn an_unsorted_table_has_no_spec() {
    let (catalog, _, table, _) = seeded().await;
    assert!(
        catalog
            .read_scope()
            .await
            .unwrap()
            .sorting_of(table)
            .await
            .unwrap()
            .is_none()
    );
}

/// Re-sorting ends the old spec into history and mints a new one, so a
/// snapshot before the change still reconstructs the spec in force then.
#[tokio::test]
async fn resorting_leaves_the_old_spec_readable_by_time_travel() {
    let (catalog, _, table, _) = seeded().await;
    let first = catalog
        .commit(async move |tx| {
            tx.set_sorting(table, &[key("x", "ASC")]).await?;
            Ok(())
        })
        .await
        .unwrap();
    let second = catalog
        .commit(async move |tx| {
            tx.set_sorting(table, &[key("x", "DESC")]).await?;
            Ok(())
        })
        .await
        .unwrap();

    let spec_then = catalog
        .read_scope_at(first)
        .await
        .unwrap()
        .sorting_of(table)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(spec_then.keys, vec![key("x", "ASC")]);

    let spec_now = catalog
        .read_scope_at(second)
        .await
        .unwrap()
        .sorting_of(table)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(spec_now.keys, vec![key("x", "DESC")]);
    assert_ne!(spec_then.id, spec_now.id, "a new spec gets a fresh id");
}

/// `RESET SORTED BY` is a genuine clear — the old spec ends and nothing
/// takes its place, unlike the set-to-empty a partition reset performs.
#[tokio::test]
async fn clearing_unsorts_the_table_but_keeps_the_past() {
    let (catalog, _, table, _) = seeded().await;
    let sorted = catalog
        .commit(async move |tx| {
            tx.set_sorting(table, &[key("x", "ASC")]).await?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(async move |tx| tx.clear_sorting(table).await)
        .await
        .unwrap();

    assert!(
        catalog
            .read_scope()
            .await
            .unwrap()
            .sorting_of(table)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        catalog
            .read_scope_at(sorted)
            .await
            .unwrap()
            .sorting_of(table)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn clearing_an_unsorted_table_is_not_found() {
    let (catalog, _, table, _) = seeded().await;
    let err = catalog
        .commit(async move |tx| tx.clear_sorting(table).await)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err}");
}

#[tokio::test]
async fn an_empty_spec_is_refused_and_an_absent_table_is_not_found() {
    let (catalog, _, table, _) = seeded().await;

    let empty = catalog
        .commit(async move |tx| {
            tx.set_sorting(table, &[]).await?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(empty, Error::Constraint(_)), "{empty}");

    let absent_table = catalog
        .commit(async |tx| {
            tx.set_sorting(TableId::new(9999), &[key("x", "ASC")])
                .await?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(absent_table, Error::NotFound(_)), "{absent_table}");
}

/// A sort key names its column inside a free-form SQL string rather than
/// by field id, so there is no column to validate and a renamed column is
/// the caller's problem, not a refusal.
#[tokio::test]
async fn an_expression_naming_no_live_column_is_stored_verbatim() {
    let (catalog, _, table, _) = seeded().await;
    catalog
        .commit(async move |tx| {
            tx.set_sorting(table, &[key("nonexistent_column + 1", "ASC")])
                .await?;
            Ok(())
        })
        .await
        .unwrap();

    let spec = catalog
        .read_scope()
        .await
        .unwrap()
        .sorting_of(table)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(spec.keys[0].expression, "nonexistent_column + 1");
}

#[tokio::test]
async fn dropping_the_table_drops_its_spec() {
    let (catalog, _, table, _) = seeded().await;
    catalog
        .commit(async move |tx| {
            tx.set_sorting(table, &[key("x", "ASC")]).await?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(async move |tx| tx.drop_table(table).await)
        .await
        .unwrap();

    assert!(
        catalog
            .read_scope()
            .await
            .unwrap()
            .sorting_of(table)
            .await
            .unwrap()
            .is_none()
    );
}

/// Unlike partitioning, a sort change is not schema-changing: DuckLake
/// marks the table altered without bumping the version, because a sort
/// spec never invalidates a cross-file compaction.
#[tokio::test]
async fn setting_a_spec_leaves_the_schema_version_alone() {
    let (catalog, _, table, _) = seeded().await;
    let before = catalog
        .read_scope()
        .await
        .unwrap()
        .current_snapshot()
        .schema_version;

    catalog
        .commit(async move |tx| {
            tx.set_sorting(table, &[key("x", "ASC")]).await?;
            Ok(())
        })
        .await
        .unwrap();

    let after = catalog.read_scope().await.unwrap();
    assert_eq!(after.current_snapshot().schema_version, before);
    assert!(
        after.sorting_of(table).await.unwrap().is_some(),
        "the spec landed all the same"
    );
}

/// The spec a verb writes is the spec DuckLake reads: it appears in the
/// `ducklake_sort_info` and `ducklake_sort_expression` projections the
/// extension serves, in sort-key order.
#[tokio::test]
async fn a_verb_written_spec_serves_through_the_ducklake_projections() {
    let (catalog, _, table, _) = seeded().await;
    catalog
        .commit(async move |tx| {
            tx.set_sorting(table, &[key("year(x)", "DESC"), key("x", "ASC")])
                .await?;
            Ok(())
        })
        .await
        .unwrap();

    let specs = moraine::ffi_support::dump_sort_info(&catalog)
        .await
        .unwrap();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].table_id, table.get());

    let rows = moraine::ffi_support::dump_sort_expression_rows(&catalog)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter()
            .map(|row| (row.sort_key_index, row.expression.as_str()))
            .collect::<Vec<_>>(),
        vec![(0, "year(x)"), (1, "x")]
    );
    assert_eq!(rows[0].sort_direction, "DESC");
    assert_eq!(rows[0].dialect, "duckdb");
}
