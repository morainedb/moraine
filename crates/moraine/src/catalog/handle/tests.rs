use std::sync::Arc;

use object_store::{ObjectStore, memory::InMemory};
use slatedb::IsolationLevel;

use super::*;
use crate::{
    catalog::IndexId,
    store::{
        handle::ReadHandle,
        index_encoding::{
            CanonicalKey, Direction, IndexKeyValue, IntWidth, NullOrder, encode_ordered_values,
        },
        key::{EntityKey, IndexKey, Key, SysKey},
        open::StoreBuilder,
        proto, read, value,
    },
    transaction::commit,
};

/// The stored structural format and fold cursor of a store (fold absent reads
/// as 0), read through a fresh reader.
async fn stored_format_and_fold(object_store: &Arc<dyn ObjectStore>) -> (u64, u64) {
    let reader = StoreBuilder::new("", Arc::clone(object_store))
        .open_reader()
        .await
        .unwrap();
    let format = read::read_format(ReadHandle::Reader(&reader))
        .await
        .unwrap()
        .unwrap()
        .format_version;
    let fold = crate::transaction::folder::reader_cursor(&reader);
    reader.close().await.unwrap();
    (format, fold)
}

/// Hand-seeds a legacy (format 1) store carrying a `legacy` schema, as the
/// pre-flip single-writer binary left it: the folded store holds every record
/// and the slot format and fold cursor are both absent. Written through a raw
/// writer, then closed.
async fn legacy_store_with_schema(object_store: Arc<dyn ObjectStore>) {
    // Bootstrap supplies the base records (snapshot 0, the `main` schema, the
    // encrypted option, head 0); the raw writes below downgrade the stamp to
    // format 1, drop the fold cursor, and land a `legacy` schema at snapshot 1.
    let db = commit::open_initialized(
        StoreBuilder::new("", Arc::clone(&object_store)),
        false,
        None,
    )
    .await
    .unwrap();
    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    tx.put(
        Key::Sys(SysKey::Format).encode(),
        value::encode_value(&proto::FormatValue {
            format_version: commit::FORMAT_VERSION,
            writer_version: "legacy".into(),
        }),
    )
    .unwrap();
    tx.put(
        Key::Snapshot { snapshot_id: 1 }.encode(),
        value::encode_value(&proto::SnapshotValue {
            snapshot_id: 1,
            snapshot_time_micros: 1,
            schema_version: 0,
            next_catalog_id: 2,
            next_file_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
            transaction_id: None,
            deleted_data_file_ids: Vec::new(),
        }),
    )
    .unwrap();
    tx.put(
        Key::current(EntityKey::Schema { schema_id: 1 }).encode(),
        value::encode_value(&proto::SchemaValue {
            schema_id: 1,
            schema_uuid: "uuid-legacy".into(),
            begin_snapshot: 1,
            end_snapshot: None,
            schema_name: "legacy".into(),
            path: "legacy/".into(),
            path_is_relative: true,
        }),
    )
    .unwrap();
    tx.put(
        Key::Sys(SysKey::Head).encode(),
        value::encode_value(&proto::HeadValue {
            snapshot_id: 1,
            batch_seq: 0,
        }),
    )
    .unwrap();
    commit::commit_durably(&db, tx).await.unwrap();
    db.close().await.unwrap();
}

/// A legacy store migrates to the slot-log topology on its first read-write
/// attach, serving its pre-migration data; the new commit lands in a slot a
/// fresh reader replays, and reopening is an idempotent no-op migration.
#[tokio::test]
async fn a_legacy_store_migrates_on_first_write_attach_and_serves_its_data() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    legacy_store_with_schema(Arc::clone(&object_store)).await;

    let catalog = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("legacy")
            .is_some(),
        "the pre-migration schema survives"
    );
    assert_eq!(
        stored_format_and_fold(&object_store).await,
        (commit::FORMAT_MULTI_WRITER, 0),
        "one atomic stamp: format 4 and fold 0"
    );

    catalog
        .commit(|tx| tx.create_schema("post").map(|_| ()))
        .await
        .unwrap();

    // The new commit rode a slot: a fresh reader replays it over the folded,
    // migrated store.
    let fresh = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    let view = fresh.snapshot().await.unwrap();
    assert!(view.schema_by_name("legacy").is_some());
    assert!(view.schema_by_name("post").is_some());

    // Reopening migrates nothing: still format 4, fold 0.
    assert_eq!(
        stored_format_and_fold(&object_store).await,
        (commit::FORMAT_MULTI_WRITER, 0),
    );
}

/// A read-only attach of a legacy store serves it unmigrated: it writes
/// nothing, and its absent fold cursor reads as 0 with an empty tail.
#[tokio::test]
async fn read_only_attach_of_a_legacy_store_serves_it_unmigrated() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    legacy_store_with_schema(Arc::clone(&object_store)).await;

    let reader = Catalog::open_read_only(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    assert!(
        reader
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("legacy")
            .is_some()
    );

    // Nothing was written: the store is still the legacy format.
    assert_eq!(
        stored_format_and_fold(&object_store).await,
        (commit::FORMAT_VERSION, 0),
    );
}

/// Attaches read-write, re-attaching through a transient open error. Racing
/// migrations open and close writers against one shared in-process object
/// store, so a losing open can observe a competitor's writer as a closed
/// handle — a contention artifact a healthy attach absorbs by re-attaching,
/// exactly as the migration loop already re-probes a fenced stamp. Convergence
/// is the invariant; which open wins the race is not.
#[allow(clippy::expect_used)]
async fn attach_through_contention(object_store: &Arc<dyn ObjectStore>) -> Catalog {
    for attempt in 0..64u32 {
        match Catalog::open(Arc::clone(object_store), CatalogOptions::default()).await {
            Ok(catalog) => return catalog,
            Err(err) if attempt + 1 == 64 => {
                panic!("concurrent migration never converged after re-attaching: {err:?}")
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
        }
    }
    unreachable!("the loop returns or panics")
}

/// Two new-binary opens racing the same legacy store converge on exactly one
/// stamp: whichever open migrates, the store ends at format 4 / fold 0 with its
/// data intact, and both attach. A second format write can never commit — the
/// later migration reads the converted format and rolls back, or is fenced — so
/// asserting format 4 / fold 0 is asserting exactly-one-stamp.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_migrations_converge_on_one_stamp() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    legacy_store_with_schema(Arc::clone(&object_store)).await;

    let (a, b) = tokio::join!(
        attach_through_contention(&object_store),
        attach_through_contention(&object_store),
    );

    assert_eq!(
        stored_format_and_fold(&object_store).await,
        (commit::FORMAT_MULTI_WRITER, 0),
        "exactly one stamp: format 4 and fold 0",
    );
    assert!(
        a.snapshot()
            .await
            .unwrap()
            .schema_by_name("legacy")
            .is_some()
    );
    assert!(
        b.snapshot()
            .await
            .unwrap()
            .schema_by_name("legacy")
            .is_some()
    );
}

/// Migrating a legacy store fences an incumbent old-binary writer: the
/// migration opens its own writer, and the live legacy writer's next commit
/// fails typed — never corruption, never a silent success.
#[tokio::test]
async fn migration_fences_a_live_legacy_writer_with_a_typed_error() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    legacy_store_with_schema(Arc::clone(&object_store)).await;

    // An incumbent old-binary writer holds the store open over the legacy data.
    let incumbent = StoreBuilder::new("", Arc::clone(&object_store))
        .open_writer()
        .await
        .unwrap();

    // A new-binary attach migrates the store, opening its own writer — which
    // fences the incumbent.
    let migrated = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    assert!(
        migrated
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("legacy")
            .is_some()
    );

    // The fenced incumbent's next write fails typed — never a silent success.
    // The writer notices a newer epoch at whichever operation it next flushes:
    // the transaction begin, the staging put, or the commit. The invariant is
    // that the first one to surface it is a typed `Fenced`, not which one it is.
    let outcome: Result<()> = async {
        let tx = incumbent.begin(IsolationLevel::Snapshot).await?;
        tx.put(
            Key::Sys(SysKey::Head).encode(),
            value::encode_value(&proto::HeadValue {
                snapshot_id: 99,
                batch_seq: 0,
            }),
        )?;
        commit::commit_durably(&incumbent, tx).await?;
        Ok(())
    }
    .await;
    let err = outcome.expect_err("a fenced writer must not commit");
    assert!(matches!(err, Error::Fenced(_)), "{err:?}");
}

/// A one-column ascending key over `value`.
fn value_key(value: u128) -> CanonicalKey {
    encode_ordered_values(
        &[Some(IndexKeyValue::UInt {
            value,
            width: IntWidth::I64,
        })],
        &[Direction::Ascending],
        &[NullOrder::Last],
    )
    .unwrap()
}

/// Bootstraps a fresh slot-backed store through the writer and closes it, so a
/// later attach opens a reader that already sees everything seeded below.
async fn bootstrap_multi(object_store: &Arc<dyn ObjectStore>) {
    let db = commit::open_initialized(StoreBuilder::new("", Arc::clone(object_store)), false, None)
        .await
        .unwrap();
    db.close().await.unwrap();
}

/// Seeds `rows` non-unique entries of one index directly into the folded
/// store, as a completed fold would have left them. The index id is never made
/// live, so the entries are exactly what a dropped index orphans.
async fn seed_orphaned_entries(object_store: &Arc<dyn ObjectStore>, index_id: u64, rows: u64) {
    let db = StoreBuilder::new("", Arc::clone(object_store))
        .open_writer()
        .await
        .unwrap();
    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    for row in 0..rows {
        let key = Key::Index(IndexKey::Multi {
            index_id,
            key: value_key(u128::from(row)),
            row_id: row,
        })
        .encode();
        tx.put(key, Vec::new()).unwrap();
    }
    commit::commit_durably(&db, tx).await.unwrap();
    db.close().await.unwrap();
}

/// Maintenance on a slot-backed catalog reclaims a dropped index's entries
/// through the folder role — the folder is the one process allowed to write
/// the store directly, and a sweep touches only ids no live index holds.
#[tokio::test]
async fn maintain_sweeps_orphaned_entries_on_a_slot_backed_catalog() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    bootstrap_multi(&object_store).await;
    seed_orphaned_entries(&object_store, 42, 5).await;
    let catalog = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();

    let report = catalog
        .maintain(MaintenanceRequest::default())
        .await
        .unwrap();
    assert_eq!(report.indexes_swept, 1);
    assert_eq!(report.index_entries_reclaimed, 5);

    // A second pass finds the range already empty.
    let again = catalog
        .maintain(MaintenanceRequest::default())
        .await
        .unwrap();
    assert_eq!(again.index_entries_reclaimed, 0);
    assert_eq!(again.indexes_swept, 0);
}

/// `reclaim_index_entries` on a slot-backed catalog deletes a not-live index's
/// entries through the folder role, one bounded batch at a time.
#[tokio::test]
async fn reclaim_index_entries_runs_under_the_folder_role() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    bootstrap_multi(&object_store).await;
    seed_orphaned_entries(&object_store, 7, 5).await;
    let catalog = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();

    let deleted = catalog
        .reclaim_index_entries(IndexId::new(7), 3)
        .await
        .unwrap();
    assert_eq!(deleted, 3);
    let rest = catalog
        .reclaim_index_entries(IndexId::new(7), 10)
        .await
        .unwrap();
    assert_eq!(rest, 2);
    let none = catalog
        .reclaim_index_entries(IndexId::new(7), 10)
        .await
        .unwrap();
    assert_eq!(none, 0);
}

/// The load-bearing property: a verb commit races the slot log while a
/// maintenance sweep holds the fenced folder writer, and lands unimpeded.
/// Commits never touch the folder writer — they race the object-store log and
/// read through the reader — so the folder is availability-optional and a
/// commit never waits on it. A small batch size makes the sweep span many
/// folder transactions, so the commit overlaps a live folder session.
#[tokio::test(flavor = "multi_thread")]
async fn a_commit_lands_unimpeded_during_a_maintenance_sweep() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    bootstrap_multi(&object_store).await;
    seed_orphaned_entries(&object_store, 42, 500).await;
    let catalog = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();

    let (swept, committed) = tokio::join!(
        catalog.maintain(MaintenanceRequest {
            sweep_orphaned_index_entries: true,
            batch_size: 1,
        }),
        catalog.commit(|tx| tx.create_schema("live").map(|_| ())),
    );

    let report = swept.unwrap();
    assert_eq!(report.index_entries_reclaimed, 500);
    let id = committed.unwrap();
    assert_eq!(id, SnapshotId::new(1));

    let snapshot = catalog.snapshot().await.unwrap();
    assert!(snapshot.schema_by_name("live").is_some());
}

/// A staged index build over a slot-backed catalog drives to ready: its
/// definition and final flip ride the log, so a fresh attach replays the
/// finished index. An empty table exercises the plumbing without a backfill.
#[tokio::test]
async fn create_index_staged_lands_ready_over_the_slot_log() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    let table = {
        let created = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").unwrap().id;
                let t = tx.create_table(
                    schema,
                    "orders",
                    &[crate::catalog::ColumnDef {
                        name: "a".into(),
                        column_type: "BIGINT".into(),
                        nulls_allowed: true,
                        default_value: None,
                        children: Vec::new(),
                    }],
                )?;
                created.set(Some(t));
                Ok(())
            })
            .await
            .unwrap();
        created.get().unwrap()
    };

    let index = catalog
        .create_index_staged(
            table,
            &crate::catalog::IndexDef {
                name: "by_a".into(),
                columns: vec![crate::catalog::ColumnId::new(1)],
                unique: false,
            },
            &[],
            None,
            "",
            None,
        )
        .await
        .unwrap();

    // A fresh attach replays the finished index through the log.
    let other = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    let info = other.snapshot().await.unwrap().indexes_of(table).remove(0);
    assert_eq!(info.id, index);
    assert_eq!(info.state, crate::catalog::IndexState::Ready);
}

/// Collects `tracing` events fired on the current thread as `(level, rendered
/// message and fields)`, for asserting the migration diagnostic.
#[derive(Clone, Default)]
struct CapturedEvents(Arc<std::sync::Mutex<Vec<(tracing::Level, String)>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapturedEvents {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Render(String);
        impl tracing::field::Visit for Render {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write as _;
                let _ = write!(self.0, " {}={value:?}", field.name());
            }
        }
        let mut render = Render(String::new());
        event.record(&mut render);
        if let Ok(mut events) = self.0.lock() {
            events.push((*event.metadata().level(), render.0));
        }
    }
}

/// Runs `body` under a subscriber that captures every event fired on this
/// thread, returning what it captured.
fn capture_events(body: impl FnOnce()) -> Vec<(tracing::Level, String)> {
    use tracing_subscriber::layer::SubscriberExt as _;

    let captured = CapturedEvents::default();
    let subscriber = tracing_subscriber::registry().with(captured.clone());
    tracing::subscriber::with_default(subscriber, body);
    let events = captured.0.lock().unwrap();
    events.clone()
}

/// The migration diagnostic: a `warn` naming the slot-log format.
fn migration_warns(events: &[(tracing::Level, String)]) -> Vec<&(tracing::Level, String)> {
    events
        .iter()
        .filter(|(level, message)| {
            *level == tracing::Level::WARN && message.contains("slot-log format")
        })
        .collect()
}

/// Migrating a legacy store emits one `warn` event naming the old and new
/// store formats, so an operator sees the otherwise-silent restamp — the line
/// the extension's log sink surfaces on a migrating attach.
#[test]
fn migration_warns_naming_the_old_and_new_format() {
    let events = capture_events(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            legacy_store_with_schema(Arc::clone(&object_store)).await;
            Catalog::open(object_store, CatalogOptions::default())
                .await
                .unwrap();
        });
    });

    let warns = migration_warns(&events);
    let warn = warns
        .first()
        .unwrap_or_else(|| panic!("no migration warn event in {events:?}"));
    assert_eq!(warns.len(), 1, "exactly one migration warn: {events:?}");
    assert!(
        warn.1
            .contains(&format!("from_format={}", commit::FORMAT_VERSION)),
        "the warn names the old format: {}",
        warn.1
    );
    assert!(
        warn.1
            .contains(&format!("to_format={}", commit::FORMAT_MULTI_WRITER)),
        "the warn names the new format: {}",
        warn.1
    );
}

/// The migration warn is bound to an actual format 1–3 → 4 conversion: a fresh
/// bootstrap and a re-attach of an already-migrated store both restamp nothing
/// and so must stay silent, or every attach would spam the operator.
#[test]
fn bootstrap_and_reattach_do_not_warn() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let bootstrap = capture_events(|| {
        runtime.block_on(async {
            Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
                .await
                .unwrap();
        });
    });
    assert!(
        migration_warns(&bootstrap).is_empty(),
        "a fresh bootstrap migrates nothing and must not warn: {bootstrap:?}"
    );

    let reattach = capture_events(|| {
        runtime.block_on(async {
            Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
                .await
                .unwrap();
        });
    });
    assert!(
        migration_warns(&reattach).is_empty(),
        "re-attaching an already-migrated store must not warn: {reattach:?}"
    );
}
