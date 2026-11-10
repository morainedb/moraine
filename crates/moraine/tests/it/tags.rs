//! Tags through the verb surface: setting, superseding, removing, and the
//! begin/end lifecycle each entry carries.

use moraine::{Error, SchemaId, TableId, TagTarget, ViewId};

use crate::fixtures::seeded;

/// The live value for `key` on `object`, if any.
#[allow(clippy::unwrap_used)]
async fn live_value(snapshot: &moraine::ReadScope, object: u64, key: &str) -> Option<String> {
    snapshot
        .tags_of(object)
        .await
        .unwrap()
        .into_iter()
        .find(|entry| entry.key == key && entry.end_snapshot.is_none())
        .map(|entry| entry.value)
}

#[tokio::test]
async fn a_tag_reads_back_and_a_second_value_supersedes_the_first() {
    let (catalog, _, table, _) = seeded().await;
    let target = TagTarget::Table(table);

    let first = catalog
        .commit(async move |tx| tx.set_tag(target, "comment", "the orders table").await)
        .await
        .unwrap();
    assert_eq!(
        live_value(&catalog.read_scope().await.unwrap(), table.get(), "comment")
            .await
            .as_deref(),
        Some("the orders table")
    );

    catalog
        .commit(async move |tx| tx.set_tag(target, "comment", "revised").await)
        .await
        .unwrap();

    let head = catalog.read_scope().await.unwrap();
    assert_eq!(
        live_value(&head, table.get(), "comment").await.as_deref(),
        Some("revised")
    );
    // The superseded entry stays readable, ended at the commit that
    // replaced it — the whole point of the begin/end lifecycle.
    let ended: Vec<_> = head
        .tags_of(table.get())
        .await
        .unwrap()
        .into_iter()
        .filter(|entry| entry.end_snapshot.is_some())
        .collect();
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0].value, "the orders table");
    assert_eq!(ended[0].begin_snapshot, first.get());
}

#[tokio::test]
async fn removing_a_tag_ends_it_without_erasing_it() {
    let (catalog, _, table, _) = seeded().await;
    let target = TagTarget::Table(table);
    catalog
        .commit(async move |tx| tx.set_tag(target, "owner", "sales").await)
        .await
        .unwrap();
    catalog
        .commit(async move |tx| tx.remove_tag(target, "owner").await)
        .await
        .unwrap();

    let head = catalog.read_scope().await.unwrap();
    assert_eq!(live_value(&head, table.get(), "owner").await, None);
    let entries = head.tags_of(table.get()).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert!(entries[0].end_snapshot.is_some());
}

#[tokio::test]
async fn several_keys_coexist_on_one_object() {
    let (catalog, _, table, _) = seeded().await;
    let target = TagTarget::Table(table);
    catalog
        .commit(async move |tx| {
            tx.set_tag(target, "comment", "c").await?;
            tx.set_tag(target, "owner", "o").await
        })
        .await
        .unwrap();

    let head = catalog.read_scope().await.unwrap();
    assert_eq!(
        live_value(&head, table.get(), "comment").await.as_deref(),
        Some("c")
    );
    assert_eq!(
        live_value(&head, table.get(), "owner").await.as_deref(),
        Some("o")
    );
}

#[tokio::test]
async fn schemas_and_views_are_taggable_too() {
    let (catalog, schema, _, _) = seeded().await;
    catalog
        .commit(async move |tx| {
            tx.create_view(schema, "v", "duckdb", "select 1").await?;
            Ok(())
        })
        .await
        .unwrap();
    let view = catalog
        .read_scope()
        .await
        .unwrap()
        .view_by_name(schema, "v")
        .await
        .unwrap()
        .unwrap()
        .id;

    catalog
        .commit(async move |tx| {
            tx.set_tag(TagTarget::Schema(schema), "comment", "sales domain")
                .await?;
            tx.set_tag(TagTarget::View(view), "comment", "a view").await
        })
        .await
        .unwrap();

    let head = catalog.read_scope().await.unwrap();
    assert_eq!(
        live_value(&head, schema.get(), "comment").await.as_deref(),
        Some("sales domain")
    );
    assert_eq!(
        live_value(&head, view.get(), "comment").await.as_deref(),
        Some("a view")
    );
}

#[tokio::test]
async fn tagging_an_absent_object_is_not_found() {
    let (catalog, _, _, _) = seeded().await;
    for target in [
        TagTarget::Schema(SchemaId::new(9999)),
        TagTarget::Table(TableId::new(9999)),
        TagTarget::View(ViewId::new(9999)),
    ] {
        let err = catalog
            .commit(async move |tx| tx.set_tag(target, "k", "v").await)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err}");
    }
}

#[tokio::test]
async fn removing_a_tag_that_was_never_set_is_not_found() {
    let (catalog, _, table, _) = seeded().await;
    let err = catalog
        .commit(async move |tx| tx.remove_tag(TagTarget::Table(table), "absent").await)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err}");
}

#[tokio::test]
async fn an_empty_key_is_refused() {
    let (catalog, _, table, _) = seeded().await;
    let err = catalog
        .commit(async move |tx| tx.set_tag(TagTarget::Table(table), "", "v").await)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");
}

/// Tagging is an alteration: it bumps the catalog schema version, the
/// boundary case the commit protocol pins: comments and tags bump.
#[tokio::test]
async fn a_tag_change_bumps_the_schema_version() {
    let (catalog, _, table, _) = seeded().await;
    let before = catalog
        .read_scope()
        .await
        .unwrap()
        .current_snapshot()
        .schema_version;

    catalog
        .commit(async move |tx| tx.set_tag(TagTarget::Table(table), "comment", "c").await)
        .await
        .unwrap();

    let after = catalog.read_scope().await.unwrap().current_snapshot();
    assert_eq!(after.schema_version, before + 1);
}

/// A tag set before a snapshot and superseded after it reads as its old
/// value at that snapshot — the entries carry their own lifecycle, so time
/// travel filters entries rather than records.
#[tokio::test]
async fn time_travel_reads_the_value_in_force_then() {
    let (catalog, _, table, _) = seeded().await;
    let target = TagTarget::Table(table);
    let original = catalog
        .commit(async move |tx| tx.set_tag(target, "comment", "first").await)
        .await
        .unwrap();
    catalog
        .commit(async move |tx| tx.set_tag(target, "comment", "second").await)
        .await
        .unwrap();

    let past = catalog.read_scope_at(original).await.unwrap();
    let in_force: Vec<_> = past
        .tags_of(table.get())
        .await
        .unwrap()
        .into_iter()
        .filter(|entry| {
            entry.begin_snapshot <= original.get()
                && entry.end_snapshot.is_none_or(|end| end > original.get())
        })
        .collect();
    assert_eq!(in_force.len(), 1);
    assert_eq!(in_force[0].value, "first");
}
