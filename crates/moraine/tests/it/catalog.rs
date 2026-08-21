//! Integration tests: exercise the public API only, against real SlateDB
//! on in-memory object storage.

use std::sync::Arc;

use moraine::{
    Catalog, CatalogOptions, ColumnAlteration, ColumnDef, ColumnId, Error, OptionScope, SchemaId,
    SnapshotId,
};
use object_store::memory::InMemory;

use crate::fixtures::{col, open_memory, seeded};

#[tokio::test]
async fn encrypted_flag_is_fixed_at_bootstrap() {
    // A fresh store bootstraps with the requested flag as the stored
    // `encrypted` global option.
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let mut options = CatalogOptions::default();
    options.encrypted = true;
    let catalog = Catalog::open(store.clone(), options).await.unwrap();
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(
        head.option(OptionScope::Global, "encrypted").as_deref(),
        Some("true")
    );
    catalog.close().await.unwrap();

    // The flag is creation-time only: reopening with a different request
    // does not flip the stored value.
    let catalog = Catalog::open(store, CatalogOptions::default())
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(
        head.option(OptionScope::Global, "encrypted").as_deref(),
        Some("true")
    );
    catalog.close().await.unwrap();

    // The default bootstrap records the flag explicitly as "false".
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(
        head.option(OptionScope::Global, "encrypted").as_deref(),
        Some("false")
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn bootstrap_creates_snapshot_zero() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let snapshot = catalog.snapshot().await.unwrap();
    assert_eq!(snapshot.current_snapshot().id, SnapshotId::new(0));
    assert_eq!(snapshot.current_snapshot().schema_version, 0);
    let schemas = snapshot.schemas();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, "main");

    // `main` consumed catalog id 0; the first user-created schema follows.
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    let sales = head.schema_by_name("sales").unwrap();
    assert_eq!(sales.id, SchemaId::new(1));

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn reopen_finds_the_initialized_store() {
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let first = catalog.snapshot().await.unwrap().current_snapshot();
    catalog.close().await.unwrap();

    let catalog = Catalog::open(store, CatalogOptions::default())
        .await
        .unwrap();
    let second = catalog.snapshot().await.unwrap().current_snapshot();
    // Same snapshot 0, same commit time: opened, not re-initialized.
    assert_eq!(first, second);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn snapshot_at_beyond_head_is_not_found() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let err = catalog.snapshot_at(SnapshotId::new(1)).await.unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn committed_state_survives_reopen() {
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let s = tx.create_schema("durable")?;
            tx.create_table(s, "t", &[col("x")])?;
            Ok(())
        })
        .await
        .unwrap();
    catalog.close().await.unwrap();

    let catalog = Catalog::open(store, CatalogOptions::default())
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.current_snapshot().id, SnapshotId::new(1));
    let s = head.schema_by_name("durable").unwrap();
    assert!(head.table_by_name(s.id, "t").is_some());
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn ddl_commits_are_visible_and_time_travelable() {
    let catalog = open_memory().await;

    let s1 = catalog
        .commit(|tx| {
            let s = tx.create_schema("sales")?;
            tx.create_table(s, "orders", &[col("id"), col("qty")])?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(s1, SnapshotId::new(1));

    let s2 = catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("sales").expect("committed above");
            let table = tx
                .table_by_name(schema.id, "orders")
                .expect("committed above");
            tx.rename_table(table.id, "orders_v2")?;
            tx.add_column(table.id, &col("note"))?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(s2, SnapshotId::new(2));

    // Head sees the final shape.
    let head = catalog.snapshot().await.unwrap();
    let schema = head.schema_by_name("sales").unwrap();
    let table = head.table_by_name(schema.id, "orders_v2").unwrap();
    assert_eq!(head.columns_of(table.id).len(), 3);
    assert!(head.table_by_name(schema.id, "orders").is_none());

    // Snapshot 1 still sees the original shape.
    let past = catalog.snapshot_at(s1).await.unwrap();
    let old = past.table_by_name(schema.id, "orders").unwrap();
    assert_eq!(old.id, table.id);
    assert_eq!(past.columns_of(old.id).len(), 2);

    // Snapshot 0 sees only the bootstrap-minted `main` schema.
    let zero = catalog.snapshot_at(SnapshotId::new(0)).await.unwrap();
    assert_eq!(zero.schemas().len(), 1);
    assert_eq!(zero.schemas()[0].name, "main");

    catalog.close().await.unwrap();
}

/// A widening type promotion is a version transition like any other, so a
/// snapshot taken before it still reports the narrower type. Asserting the
/// promotion at head alone would pass even if the old version were
/// overwritten in place rather than ended into `history`.
#[tokio::test]
async fn type_promotion_is_time_travel_correct() {
    let catalog = open_memory().await;

    let narrow = moraine::ColumnDef {
        name: "amount".into(),
        column_type: "INTEGER".into(),
        nulls_allowed: true,
        default_value: None,
        children: Vec::new(),
    };
    let before = catalog
        .commit(|tx| {
            let schema = tx.create_schema("s")?;
            tx.create_table(schema, "t", std::slice::from_ref(&narrow))?;
            Ok(())
        })
        .await
        .unwrap();

    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("s").expect("committed above");
            let table = tx.table_by_name(schema.id, "t").expect("committed above");
            let column = tx.columns_of(table.id)[0].id;
            tx.alter_column(
                table.id,
                column,
                moraine::ColumnAlteration {
                    column_type: Some("BIGINT".into()),
                    nulls_allowed: None,
                    default_value: None,
                },
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let column_type_at = |view: &moraine::CatalogSnapshot| {
        let schema = view.schema_by_name("s").expect("schema");
        let table = view.table_by_name(schema.id, "t").expect("table");
        view.columns_of(table.id)[0].column_type.clone()
    };

    assert_eq!(column_type_at(&catalog.snapshot().await.unwrap()), "BIGINT");
    assert_eq!(
        column_type_at(&catalog.snapshot_at(before).await.unwrap()),
        "INTEGER",
        "the pre-promotion snapshot must still report the narrower type"
    );

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn drop_ends_versions_and_schema_version_tracks_ddl() {
    let catalog = open_memory().await;
    let s1 = catalog
        .commit(|tx| {
            let s = tx.create_schema("tmp")?;
            tx.create_table(s, "t", &[col("a")])?;
            Ok(())
        })
        .await
        .unwrap();
    let s2 = catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("tmp").expect("committed above");
            let table = tx.table_by_name(schema.id, "t").expect("committed above");
            tx.drop_table(table.id)?;
            tx.drop_schema(schema.id)?;
            Ok(())
        })
        .await
        .unwrap();

    let head = catalog.snapshot().await.unwrap();
    // Only the bootstrap-minted `main` schema remains live.
    assert_eq!(head.schemas().len(), 1);
    assert_eq!(head.schemas()[0].name, "main");
    // Every DDL commit advanced the schema version.
    assert_eq!(head.current_snapshot().schema_version, 2);

    // The dropped entities are still visible at their snapshot.
    let past = catalog.snapshot_at(s1).await.unwrap();
    assert_eq!(past.schemas().len(), 2);
    let _ = s2;
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn empty_commit_mints_no_snapshot() {
    let catalog = open_memory().await;
    let id = catalog.commit(|_tx| Ok(())).await.unwrap();
    assert_eq!(id, SnapshotId::new(0));
    let err = catalog.snapshot_at(SnapshotId::new(1)).await.unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn logical_errors_abort_the_commit() {
    let catalog = open_memory().await;
    catalog
        .commit(|tx| {
            tx.create_schema("sales")?;
            Ok(())
        })
        .await
        .unwrap();
    let err = catalog
        .commit(|tx| {
            tx.create_schema("sales")?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));
    // The failed commit left no snapshot behind.
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.current_snapshot().id, SnapshotId::new(1));
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn dropped_column_field_ids_are_not_reused_across_commits() {
    let catalog = open_memory().await;
    catalog
        .commit(|tx| {
            let s = tx.create_schema("s")?;
            tx.create_table(s, "t", &[col("a"), col("b")])?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("s").expect("committed above");
            let table = tx.table_by_name(schema.id, "t").expect("committed above");
            tx.drop_column(table.id, ColumnId::new(2))?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("s").expect("committed above");
            let table = tx.table_by_name(schema.id, "t").expect("committed above");
            let id = tx.add_column(table.id, &col("c"))?;
            assert_eq!(id, ColumnId::new(3), "field id 2 must not be reused");
            Ok(())
        })
        .await
        .unwrap();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn read_only_reads_the_committed_catalog_but_rejects_writes() {
    let store: Arc<InMemory> = Arc::new(InMemory::new());

    let writer = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    writer
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();

    // A read-only attach sees the committed state.
    let reader = Catalog::open_read_only(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let snap = reader.snapshot().await.unwrap();
    assert!(snap.schema_by_name("sales").is_some());

    // A write through the read-only catalog does not compile at all —
    // `ReadOnlyCatalog` carries no mutator — which the crate root pins with
    // a `compile_fail` doctest. There is no runtime refusal left to assert.

    writer.close().await.unwrap();
    reader.close().await.unwrap();
}

#[tokio::test]
async fn read_only_attach_does_not_fence_the_live_writer() {
    let store: Arc<InMemory> = Arc::new(InMemory::new());

    let writer = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    writer
        .commit(|tx| tx.create_schema("first").map(|_| ()))
        .await
        .unwrap();

    // Attach a reader while the writer is live.
    let reader = Catalog::open_read_only(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    assert!(
        reader
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("first")
            .is_some()
    );

    // The reader did not fence the writer: its next durable commit still lands.
    writer
        .commit(|tx| tx.create_schema("second").map(|_| ()))
        .await
        .unwrap();

    // A freshly attached reader sees the newer state.
    let reader2 = Catalog::open_read_only(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    assert!(
        reader2
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("second")
            .is_some()
    );

    writer.close().await.unwrap();
    reader.close().await.unwrap();
    reader2.close().await.unwrap();
}

#[tokio::test]
async fn read_only_refuses_an_uninitialized_store() {
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    // No writer ever created the catalog: a read-only attach has nothing to
    // read — the store has no manifest to follow, so the open is refused.
    let err = Catalog::open_read_only(store, CatalogOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Store(_)), "got {err:?}");
}

#[tokio::test]
async fn set_table_schema_moves_a_table_between_schemas() {
    let catalog = open_memory().await;
    let ids = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let source = tx.create_schema("source")?;
            let target = tx.create_schema("target")?;
            let table = tx.create_table(source, "orders", &[col("id")])?;
            ids.set(Some((source, target, table)));
            Ok(())
        })
        .await
        .unwrap();
    let (source, target, table) = ids.get().unwrap();
    let created = catalog.snapshot().await.unwrap().current_snapshot().id;

    catalog
        .commit(move |tx| tx.set_table_schema(table, target))
        .await
        .unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert!(head.tables_in(source).is_empty());
    assert_eq!(head.tables_in(target)[0].id, table);
    assert_eq!(head.table_by_name(target, "orders").unwrap().id, table);
    assert!(head.table_by_name(source, "orders").is_none());

    // The move is versioned: time travel sees the table in its old schema.
    let past = catalog.snapshot_at(created).await.unwrap();
    assert_eq!(past.tables_in(source)[0].id, table);
    assert!(past.tables_in(target).is_empty());
    catalog.close().await.unwrap();
}

/// Two read-write attaches of one store coexist: commits ride the slot log
/// rather than a single fenced writer, so a second open never fences the
/// first, both processes commit, and each sees the other's writes through
/// tail replay.
#[tokio::test]
async fn two_read_write_attaches_coexist_without_fencing() {
    let store: Arc<InMemory> = Arc::new(InMemory::new());

    // A zero refresh window makes each read revalidate its cached head, so a
    // peer's latest commit is seen through tail replay rather than after the
    // window elapses.
    let options = || {
        let mut options = CatalogOptions::default();
        options.reader_poll_interval = std::time::Duration::ZERO;
        options
    };

    let first = Catalog::open(store.clone(), options()).await.unwrap();
    first
        .commit(|tx| tx.create_schema("from_first").map(|_| ()))
        .await
        .unwrap();

    // A second read-write open does not fence the first; both keep committing.
    let second = Catalog::open(store.clone(), options()).await.unwrap();
    second
        .commit(|tx| tx.create_schema("from_second").map(|_| ()))
        .await
        .unwrap();
    first
        .commit(|tx| tx.create_schema("first_again").map(|_| ()))
        .await
        .unwrap();

    // Each attach replays the log and sees every committed schema.
    for catalog in [&first, &second] {
        let view = catalog.snapshot().await.unwrap();
        assert!(view.schema_by_name("from_first").is_some());
        assert!(view.schema_by_name("from_second").is_some());
        assert!(view.schema_by_name("first_again").is_some());
    }
    second.close().await.unwrap();
}

/// A type moraine cannot store is refused where the column enters the
/// catalog — at creation, at addition, and at an alteration that would
/// change into it — rather than at the first insert that would need it.
/// Typed `Unsupported`, so a bridge maps it to "not implemented" without
/// reading the message.
#[tokio::test]
async fn a_variant_column_is_refused_as_unsupported_on_every_column_verb() {
    let (catalog, schema, table, _) = seeded().await;
    let variant = ColumnDef {
        name: "v".into(),
        column_type: "VARIANT".into(),
        nulls_allowed: true,
        default_value: None,
        children: Vec::new(),
    };

    let on_create = catalog
        .commit({
            let variant = variant.clone();
            move |tx| {
                tx.create_table(schema, "with_variant", std::slice::from_ref(&variant))?;
                Ok(())
            }
        })
        .await
        .unwrap_err();
    assert!(matches!(on_create, Error::Unsupported(_)), "{on_create}");

    let on_add = catalog
        .commit(move |tx| {
            tx.add_column(table, &variant)?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(on_add, Error::Unsupported(_)), "{on_add}");

    let column = catalog.snapshot().await.unwrap().columns_of(table)[0].id;
    let on_alter = catalog
        .commit(move |tx| {
            tx.alter_column(
                table,
                column,
                ColumnAlteration {
                    column_type: Some("VARIANT".into()),
                    ..ColumnAlteration::default()
                },
            )
        })
        .await
        .unwrap_err();
    assert!(matches!(on_alter, Error::Unsupported(_)), "{on_alter}");

    // Nothing landed: the table still has only the column it was seeded
    // with, and the refused table was never created.
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.columns_of(table).len(), 1);
    assert!(head.table_by_name(schema, "with_variant").is_none());
}
