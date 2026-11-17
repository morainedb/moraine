//! Integration tests: exercise the public API only, against real SlateDB
//! on in-memory object storage.

use std::sync::Arc;

use moraine::{
    Catalog, CatalogOptions, ColumnAlteration, ColumnDef, ColumnId, Error, OptionScope, SchemaId,
    SnapshotId,
};
use object_store::memory::InMemory;

use crate::fixtures::{col, open_memory, seeded};

/// Metadata reads taken at once on a handle that has materialized nothing
/// yet — the shape DuckLake issues once a statement's scans initialize in
/// parallel rather than one pipeline at a time. Every reader sees the same
/// catalog, and none fails against a half-built one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_metadata_reads_agree() {
    const READERS: usize = 16;

    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let seeding = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    seeding
        .commit(async |tx| {
            let schema = tx.schema_by_name("main").await.unwrap().unwrap().id;
            tx.create_table(schema, "t", &[col("a")]).await?;
            Ok(())
        })
        .await
        .unwrap();
    seeding.close().await.unwrap();

    // A fresh handle, read by everyone at once before anything is resident.
    let catalog = Arc::new(
        Catalog::open(store, CatalogOptions::default())
            .await
            .unwrap(),
    );
    let mut readers = Vec::with_capacity(READERS);
    for _ in 0..READERS {
        let catalog = Arc::clone(&catalog);
        readers.push(tokio::spawn(async move {
            let snapshot = catalog.read_scope().await?;
            let table = match snapshot.schema_by_name("main").await? {
                Some(schema) => snapshot
                    .table_by_name(schema.id, "t")
                    .await?
                    .map(|table| table.id),
                None => None,
            };
            Ok::<_, Error>((snapshot.current_snapshot().id, table))
        }));
    }

    let mut seen = Vec::with_capacity(READERS);
    for reader in readers {
        seen.push(reader.await.unwrap().unwrap());
    }
    let first = seen[0];
    assert!(
        first.1.is_some(),
        "the seeded table must be visible to a first read"
    );
    assert!(
        seen.iter().all(|answer| *answer == first),
        "concurrent first reads disagreed: {seen:?}"
    );

    Arc::into_inner(catalog).unwrap().close().await.unwrap();
}

/// Raising moves the recorded version to the newest shape this build
/// serves, reports the move, and is idempotent. A dry run records nothing.
#[tokio::test]
async fn raising_the_catalog_version_records_the_newest_shape() {
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();

    let planned = catalog.raise_catalog_version(true).await.unwrap();
    assert_eq!((planned.from.as_str(), planned.to.as_str()), ("1.0", "1.1"));
    assert_eq!(
        catalog
            .read_scope()
            .await
            .unwrap()
            .option(OptionScope::Global, "version")
            .await
            .unwrap()
            .as_deref(),
        Some("1.0"),
        "a dry run records nothing"
    );

    let raised = catalog.raise_catalog_version(false).await.unwrap();
    assert_eq!((raised.from.as_str(), raised.to.as_str()), ("1.0", "1.1"));
    assert_eq!(
        catalog
            .read_scope()
            .await
            .unwrap()
            .option(OptionScope::Global, "version")
            .await
            .unwrap()
            .as_deref(),
        Some("1.1")
    );

    // Nothing left to raise; the report says so rather than failing.
    let again = catalog.raise_catalog_version(false).await.unwrap();
    assert_eq!(again.from, again.to);
    catalog.close().await.unwrap();
}

/// A catalog records the DuckLake catalog version it serves, so the shape
/// the extension reports can follow it rather than a build-time constant.
#[tokio::test]
async fn catalog_version_is_recorded_at_bootstrap() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let head = catalog.read_scope().await.unwrap();

    assert_eq!(
        head.option(OptionScope::Global, "version")
            .await
            .unwrap()
            .as_deref(),
        Some("1.0")
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn encrypted_flag_is_fixed_at_bootstrap() {
    // A fresh store bootstraps with the requested flag as the stored
    // `encrypted` global option.
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let mut options = CatalogOptions::default();
    options.encrypted = true;
    let catalog = Catalog::open(store.clone(), options).await.unwrap();
    let head = catalog.read_scope().await.unwrap();
    assert_eq!(
        head.option(OptionScope::Global, "encrypted")
            .await
            .unwrap()
            .as_deref(),
        Some("true")
    );
    catalog.close().await.unwrap();

    // The flag is creation-time only: reopening with a different request
    // does not flip the stored value.
    let catalog = Catalog::open(store, CatalogOptions::default())
        .await
        .unwrap();
    let head = catalog.read_scope().await.unwrap();
    assert_eq!(
        head.option(OptionScope::Global, "encrypted")
            .await
            .unwrap()
            .as_deref(),
        Some("true")
    );
    catalog.close().await.unwrap();

    // The default bootstrap records the flag explicitly as "false".
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let head = catalog.read_scope().await.unwrap();
    assert_eq!(
        head.option(OptionScope::Global, "encrypted")
            .await
            .unwrap()
            .as_deref(),
        Some("false")
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn bootstrap_creates_snapshot_zero() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let snapshot = catalog.read_scope().await.unwrap();
    assert_eq!(snapshot.current_snapshot().id, SnapshotId::new(0));
    assert_eq!(snapshot.current_snapshot().schema_version, 0);
    let schemas = snapshot.schemas().await.unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, "main");

    // `main` consumed catalog id 0; the first user-created schema follows.
    catalog
        .commit(async |tx| tx.create_schema("sales").await.map(|_| ()))
        .await
        .unwrap();
    let head = catalog.read_scope().await.unwrap();
    let sales = head.schema_by_name("sales").await.unwrap().unwrap();
    assert_eq!(sales.id, SchemaId::new(1));

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn reopen_finds_the_initialized_store() {
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let first = catalog.read_scope().await.unwrap().current_snapshot();
    catalog.close().await.unwrap();

    let catalog = Catalog::open(store, CatalogOptions::default())
        .await
        .unwrap();
    let second = catalog.read_scope().await.unwrap().current_snapshot();
    // Same snapshot 0, same commit time: opened, not re-initialized.
    assert_eq!(first, second);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn snapshot_at_beyond_head_is_not_found() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let err = catalog.read_scope_at(SnapshotId::new(1)).await.unwrap_err();
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
        .commit(async |tx| {
            let s = tx.create_schema("durable").await?;
            tx.create_table(s, "t", &[col("x")]).await?;
            Ok(())
        })
        .await
        .unwrap();
    catalog.close().await.unwrap();

    let catalog = Catalog::open(store, CatalogOptions::default())
        .await
        .unwrap();
    let head = catalog.read_scope().await.unwrap();
    assert_eq!(head.current_snapshot().id, SnapshotId::new(1));
    let s = head.schema_by_name("durable").await.unwrap().unwrap();
    assert!(head.table_by_name(s.id, "t").await.unwrap().is_some());
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn ddl_commits_are_visible_and_time_travelable() {
    let catalog = open_memory().await;

    let s1 = catalog
        .commit(async |tx| {
            let s = tx.create_schema("sales").await?;
            tx.create_table(s, "orders", &[col("id"), col("qty")])
                .await?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(s1, SnapshotId::new(1));

    let s2 = catalog
        .commit(async |tx| {
            let schema = tx
                .schema_by_name("sales")
                .await
                .unwrap()
                .expect("committed above");
            let table = tx
                .table_by_name(schema.id, "orders")
                .await
                .unwrap()
                .expect("committed above");
            tx.rename_table(table.id, "orders_v2").await?;
            tx.add_column(table.id, &col("note")).await?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(s2, SnapshotId::new(2));

    // Head sees the final shape.
    let head = catalog.read_scope().await.unwrap();
    let schema = head.schema_by_name("sales").await.unwrap().unwrap();
    let table = head
        .table_by_name(schema.id, "orders_v2")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.columns_of(table.id).await.unwrap().len(), 3);
    assert!(
        head.table_by_name(schema.id, "orders")
            .await
            .unwrap()
            .is_none()
    );

    // Snapshot 1 still sees the original shape.
    let past = catalog.read_scope_at(s1).await.unwrap();
    let old = past
        .table_by_name(schema.id, "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.id, table.id);
    assert_eq!(past.columns_of(old.id).await.unwrap().len(), 2);

    // Snapshot 0 sees only the bootstrap-minted `main` schema.
    let zero = catalog.read_scope_at(SnapshotId::new(0)).await.unwrap();
    assert_eq!(zero.schemas().await.unwrap().len(), 1);
    assert_eq!(zero.schemas().await.unwrap()[0].name, "main");

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
        .commit(async |tx| {
            let schema = tx.create_schema("s").await?;
            tx.create_table(schema, "t", std::slice::from_ref(&narrow))
                .await?;
            Ok(())
        })
        .await
        .unwrap();

    catalog
        .commit(async |tx| {
            let schema = tx
                .schema_by_name("s")
                .await
                .unwrap()
                .expect("committed above");
            let table = tx
                .table_by_name(schema.id, "t")
                .await
                .unwrap()
                .expect("committed above");
            let column = tx.columns_of(table.id).await.unwrap()[0].id;
            tx.alter_column(
                table.id,
                column,
                moraine::ColumnAlteration {
                    column_type: Some("BIGINT".into()),
                    nulls_allowed: None,
                    default_value: None,
                },
            )
            .await?;
            Ok(())
        })
        .await
        .unwrap();

    let column_type_at = async |view: &moraine::ReadScope| {
        let schema = view.schema_by_name("s").await.unwrap().expect("schema");
        let table = view
            .table_by_name(schema.id, "t")
            .await
            .unwrap()
            .expect("table");
        view.columns_of(table.id).await.unwrap()[0]
            .column_type
            .clone()
    };

    assert_eq!(
        column_type_at(&catalog.read_scope().await.unwrap()).await,
        "BIGINT"
    );
    assert_eq!(
        column_type_at(&catalog.read_scope_at(before).await.unwrap()).await,
        "INTEGER",
        "the pre-promotion snapshot must still report the narrower type"
    );

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn drop_ends_versions_and_schema_version_tracks_ddl() {
    let catalog = open_memory().await;
    let s1 = catalog
        .commit(async |tx| {
            let s = tx.create_schema("tmp").await?;
            tx.create_table(s, "t", &[col("a")]).await?;
            Ok(())
        })
        .await
        .unwrap();
    let s2 = catalog
        .commit(async |tx| {
            let schema = tx
                .schema_by_name("tmp")
                .await
                .unwrap()
                .expect("committed above");
            let table = tx
                .table_by_name(schema.id, "t")
                .await
                .unwrap()
                .expect("committed above");
            tx.drop_table(table.id).await?;
            tx.drop_schema(schema.id).await?;
            Ok(())
        })
        .await
        .unwrap();

    let head = catalog.read_scope().await.unwrap();
    // Only the bootstrap-minted `main` schema remains live.
    assert_eq!(head.schemas().await.unwrap().len(), 1);
    assert_eq!(head.schemas().await.unwrap()[0].name, "main");
    // Every DDL commit advanced the schema version.
    assert_eq!(head.current_snapshot().schema_version, 2);

    // The dropped entities are still visible at their snapshot.
    let past = catalog.read_scope_at(s1).await.unwrap();
    assert_eq!(past.schemas().await.unwrap().len(), 2);
    let _ = s2;
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn empty_commit_mints_no_snapshot() {
    let catalog = open_memory().await;
    let id = catalog.commit(async |_tx| Ok(())).await.unwrap();
    assert_eq!(id, SnapshotId::new(0));
    let err = catalog.read_scope_at(SnapshotId::new(1)).await.unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn logical_errors_abort_the_commit() {
    let catalog = open_memory().await;
    catalog
        .commit(async |tx| {
            tx.create_schema("sales").await?;
            Ok(())
        })
        .await
        .unwrap();
    let err = catalog
        .commit(async |tx| {
            tx.create_schema("sales").await?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));
    // The failed commit left no snapshot behind.
    let head = catalog.read_scope().await.unwrap();
    assert_eq!(head.current_snapshot().id, SnapshotId::new(1));
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn dropped_column_field_ids_are_not_reused_across_commits() {
    let catalog = open_memory().await;
    catalog
        .commit(async |tx| {
            let s = tx.create_schema("s").await?;
            tx.create_table(s, "t", &[col("a"), col("b")]).await?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(async |tx| {
            let schema = tx
                .schema_by_name("s")
                .await
                .unwrap()
                .expect("committed above");
            let table = tx
                .table_by_name(schema.id, "t")
                .await
                .unwrap()
                .expect("committed above");
            tx.drop_column(table.id, ColumnId::new(2)).await?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(async |tx| {
            let schema = tx
                .schema_by_name("s")
                .await
                .unwrap()
                .expect("committed above");
            let table = tx
                .table_by_name(schema.id, "t")
                .await
                .unwrap()
                .expect("committed above");
            let id = tx.add_column(table.id, &col("c")).await?;
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
        .commit(async |tx| tx.create_schema("sales").await.map(|_| ()))
        .await
        .unwrap();

    // A read-only attach sees the committed state.
    let reader = Catalog::open_read_only(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let snap = reader.read_scope().await.unwrap();
    assert!(snap.schema_by_name("sales").await.unwrap().is_some());

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
        .commit(async |tx| tx.create_schema("first").await.map(|_| ()))
        .await
        .unwrap();

    // Attach a reader while the writer is live.
    let reader = Catalog::open_read_only(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    assert!(
        reader
            .read_scope()
            .await
            .unwrap()
            .schema_by_name("first")
            .await
            .unwrap()
            .is_some()
    );

    // The reader did not fence the writer: its next durable commit still lands.
    writer
        .commit(async |tx| tx.create_schema("second").await.map(|_| ()))
        .await
        .unwrap();

    // A freshly attached reader sees the newer state.
    let reader2 = Catalog::open_read_only(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    assert!(
        reader2
            .read_scope()
            .await
            .unwrap()
            .schema_by_name("second")
            .await
            .unwrap()
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
        .commit(async |tx| {
            let source = tx.create_schema("source").await?;
            let target = tx.create_schema("target").await?;
            let table = tx.create_table(source, "orders", &[col("id")]).await?;
            ids.set(Some((source, target, table)));
            Ok(())
        })
        .await
        .unwrap();
    let (source, target, table) = ids.get().unwrap();
    let created = catalog.read_scope().await.unwrap().current_snapshot().id;

    catalog
        .commit(async move |tx| tx.set_table_schema(table, target).await)
        .await
        .unwrap();

    let head = catalog.read_scope().await.unwrap();
    assert!(head.tables_in(source).await.unwrap().is_empty());
    assert_eq!(head.tables_in(target).await.unwrap()[0].id, table);
    assert_eq!(
        head.table_by_name(target, "orders")
            .await
            .unwrap()
            .unwrap()
            .id,
        table
    );
    assert!(
        head.table_by_name(source, "orders")
            .await
            .unwrap()
            .is_none()
    );

    // The move is versioned: time travel sees the table in its old schema.
    let past = catalog.read_scope_at(created).await.unwrap();
    assert_eq!(past.tables_in(source).await.unwrap()[0].id, table);
    assert!(past.tables_in(target).await.unwrap().is_empty());
    catalog.close().await.unwrap();
}

/// SlateDB's newest-writer-wins fencing, surfaced typed: after a second
/// read-write open of the same store, the first writer's next commit fails
/// as [`Error::Fenced`] — not an opaque store error — and the message
/// carries none of DuckLake's four retry substrings, because a fenced
/// writer is dead for writes and re-running the commit cannot revive it.
#[tokio::test]
async fn second_writer_fences_the_first_with_a_typed_error() {
    let store: Arc<InMemory> = Arc::new(InMemory::new());

    let first = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    first
        .commit(async |tx| tx.create_schema("before").await.map(|_| ()))
        .await
        .unwrap();

    // The second read-write open fences the first (newest writer wins).
    let second = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();

    let err = first
        .commit(async |tx| tx.create_schema("after").await.map(|_| ()))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Fenced(_)), "got {err:?}");
    let text = err.to_string();
    for substring in ["conflict", "concurrent", "unique", "primary key"] {
        assert!(
            !text.contains(substring),
            "{text:?} contains DuckLake's retry substring {substring:?}"
        );
    }

    // The winner is unaffected.
    second
        .commit(async |tx| tx.create_schema("after").await.map(|_| ()))
        .await
        .unwrap();
    second.close().await.unwrap();
}

/// A `VARIANT` column is storable through every column verb — created,
/// added, and altered into — since moraine's inline encoding carries the
/// type. Only an equality index turns it away, and does so where the
/// index is created.
#[tokio::test]
async fn a_variant_column_is_storable_on_every_column_verb_but_never_indexable() {
    let (catalog, schema, table, _) = seeded().await;
    let variant = ColumnDef {
        name: "v".into(),
        column_type: "VARIANT".into(),
        nulls_allowed: true,
        default_value: None,
        children: Vec::new(),
    };

    catalog
        .commit({
            let variant = variant.clone();
            async move |tx| {
                tx.create_table(schema, "with_variant", std::slice::from_ref(&variant))
                    .await?;
                Ok(())
            }
        })
        .await
        .unwrap();

    catalog
        .commit({
            let variant = variant.clone();
            async move |tx| {
                tx.add_column(table, &variant).await?;
                Ok(())
            }
        })
        .await
        .unwrap();

    let column = catalog
        .read_scope()
        .await
        .unwrap()
        .columns_of(table)
        .await
        .unwrap()[0]
        .id;
    catalog
        .commit(async move |tx| {
            tx.alter_column(
                table,
                column,
                ColumnAlteration {
                    column_type: Some("VARIANT".into()),
                    ..ColumnAlteration::default()
                },
            )
            .await
        })
        .await
        .unwrap();

    let head = catalog.read_scope().await.unwrap();
    assert_eq!(head.columns_of(table).await.unwrap().len(), 2);
    assert!(
        head.table_by_name(schema, "with_variant")
            .await
            .unwrap()
            .is_some()
    );

    let added = head
        .columns_of(table)
        .await
        .unwrap()
        .iter()
        .find(|c| c.name == "v")
        .map(|c| c.id)
        .unwrap();
    let refused = catalog
        .commit(async move |tx| {
            tx.create_index(
                table,
                &moraine::IndexDef {
                    name: "by_v".into(),
                    columns: vec![added],
                    unique: false,
                },
                &[],
            )
            .await
            .map(|_| ())
        })
        .await
        .unwrap_err();
    assert!(matches!(refused, Error::Constraint(_)), "{refused}");
    assert!(
        refused.to_string().contains("index key can encode"),
        "{refused}"
    );
}
