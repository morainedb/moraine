use std::sync::Arc;

use object_store::memory::InMemory;

use super::{
    synthetic::{SOURCE_SCOPE, TARGET_SCOPE},
    *,
};
use crate::{
    catalog::{Catalog, CatalogOptions, MigrationRequest},
    fault::{SyntheticMigration, inject_crash, install_migration},
    store::{
        key::EntityKey,
        open::StoreBuilder,
        read::{EntityRecord, scan_current_entities},
    },
    transaction::commit::{FORMAT_VERSION, MAX_FORMAT_VERSION, MIN_FORMAT_VERSION, commit_durably},
};

/// The format the rewriting synthetic unit lands on: the newest this binary
/// reads, so a store it migrated still attaches.
const REWRITTEN_FORMAT: u64 = MAX_FORMAT_VERSION;

/// The format the second link lands on: past the newest this binary reads,
/// so the composed jump's end state is distinguishable from one link's.
const CHAINED_FORMAT: u64 = MAX_FORMAT_VERSION + 1;

/// How many option records the fixture plants.
const SEEDED_RECORDS: u64 = 3;

/// A bootstrapped catalog carrying [`SEEDED_RECORDS`] option records under
/// [`SOURCE_SCOPE`], closed so a migrator can take the writer.
async fn seeded_store() -> Arc<InMemory> {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let catalog = Catalog::open(object_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog.close().await.unwrap();

    let db = open_migrator(&object_store).await;
    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    // Bootstrap stamps the newest format; restamp the base so the synthetic
    // units below have an old store to carry forward.
    tx.put(
        Key::Sys(SysKey::Format).encode(),
        value::encode_value(&proto::FormatValue {
            format_version: FORMAT_VERSION,
            writer_version: env!("CARGO_PKG_VERSION").to_string(),
        }),
    )
    .unwrap();
    for scope_id in 1..=SEEDED_RECORDS {
        tx.put(
            Key::current(EntityKey::Option {
                scope_kind: SOURCE_SCOPE,
                scope_id,
            })
            .encode(),
            value::encode_value(&proto::OptionScopeValue {
                options: std::collections::HashMap::from([(
                    "planted".to_string(),
                    scope_id.to_string(),
                )]),
            }),
        )
        .unwrap();
    }
    commit_durably(&db, tx).await.unwrap();
    db.close().await.unwrap();

    object_store
}

/// Opens the store's writer directly, the way the migrate verb does —
/// without the format and marker checks an ordinary attach applies.
async fn open_migrator(object_store: &Arc<InMemory>) -> Db {
    StoreBuilder::new("", object_store.clone())
        .open_writer()
        .await
        .unwrap()
}

/// The two durable facts a reopen reads: the format stamp and the marker.
async fn durable_state(object_store: &Arc<InMemory>) -> (u64, Option<proto::MigrationValue>) {
    let db = open_migrator(object_store).await;
    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let format = read::read_format(ReadHandle::Tx(&tx))
        .await
        .unwrap()
        .unwrap()
        .format_version;
    let marker = read::read_migration(ReadHandle::Tx(&tx)).await.unwrap();
    tx.rollback();
    db.close().await.unwrap();
    (format, marker)
}

/// Every option record as `(scope_kind, scope_id)`, in ascending order.
async fn option_scopes(object_store: &Arc<InMemory>) -> Vec<(u64, u64)> {
    let db = open_migrator(object_store).await;
    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let records = scan_current_entities(ReadHandle::Tx(&tx)).await.unwrap();
    tx.rollback();
    db.close().await.unwrap();

    let mut scopes: Vec<(u64, u64)> = records
        .into_iter()
        .filter_map(|record| match record {
            EntityRecord::Option {
                scope_kind,
                scope_id,
                ..
            } => Some((scope_kind, scope_id)),
            _ => None,
        })
        .collect();
    scopes.sort_unstable();
    scopes
}

/// The scopes a fully migrated store holds: the bootstrap's own global
/// record, untouched, plus every planted record moved to the target scope.
fn migrated_scopes() -> Vec<(u64, u64)> {
    let mut expected = vec![(0, 0)];
    expected.extend((1..=SEEDED_RECORDS).map(|scope_id| (TARGET_SCOPE, scope_id)));
    expected
}

/// Installs `units` and runs the shipped driver against the store — [`run`],
/// reached exactly as [`Catalog::migrate`] reaches it, so what these tests
/// cover is the planner that ships rather than a parallel one.
async fn migrate_with(
    object_store: &Arc<InMemory>,
    units: SyntheticMigration,
) -> Result<MigrationReport> {
    install_migration(units);
    let db = open_migrator(object_store).await;
    let outcome = run(&db).await;
    db.close().await.unwrap();
    outcome
}

/// The whole point of the marker: with one present the store is stamped old
/// and every attach refuses, so no reader sees the half-rewritten middle.
#[tokio::test]
async fn a_crash_mid_rewrite_leaves_the_store_old_and_unreadable() {
    let object_store = seeded_store().await;

    inject_crash(Some(CrashPoint::AfterStep));
    let error = migrate_with(&object_store, SyntheticMigration::MoveOptionScope)
        .await
        .err()
        .unwrap();
    assert!(matches!(error, Error::Migration(_)), "{error:?}");

    let (format, marker) = durable_state(&object_store).await;
    assert_eq!(format, FORMAT_VERSION, "the flip has not happened");
    let marker = marker.expect("a crashed migration leaves its marker");
    assert_eq!(marker.from_format, FORMAT_VERSION);
    assert_eq!(marker.to_format, REWRITTEN_FORMAT);
    assert_eq!(marker.cursor, 1_u64.to_be_bytes().to_vec());

    let error = Catalog::open(object_store.clone(), CatalogOptions::default())
        .await
        .err()
        .unwrap();
    assert!(matches!(error, Error::Corruption(_)), "{error:?}");
}

/// Every seam: crash there, reopen, resume, and land on a coherent store
/// holding exactly the records it started with, each moved exactly once.
#[tokio::test]
async fn a_crash_at_every_seam_resumes_to_a_coherent_store() {
    for point in [
        CrashPoint::AfterStart,
        CrashPoint::AfterStep,
        CrashPoint::BeforeFinish,
        CrashPoint::AfterFinish,
    ] {
        let object_store = seeded_store().await;

        inject_crash(Some(point));
        let crashed = migrate_with(&object_store, SyntheticMigration::MoveOptionScope).await;

        let (format, marker) = durable_state(&object_store).await;

        // Never the one combination the protocol forbids.
        assert!(
            !(format == REWRITTEN_FORMAT && marker.is_some()),
            "new format with the marker still present at {point:?}"
        );

        if matches!(point, CrashPoint::AfterFinish) {
            // The finish batch was already durable, so this seam stops a
            // migration that had in fact completed.
            assert_eq!(format, REWRITTEN_FORMAT, "{point:?}");
            assert!(marker.is_none(), "{point:?}");
        } else {
            assert!(crashed.is_err(), "{point:?}");
            assert_eq!(format, FORMAT_VERSION, "{point:?}");
            assert!(marker.is_some(), "{point:?}");
        }

        inject_crash(None);
        migrate_with(&object_store, SyntheticMigration::MoveOptionScope)
            .await
            .unwrap_or_else(|error| panic!("resume after {point:?}: {error:?}"));

        let (format, marker) = durable_state(&object_store).await;
        assert_eq!(format, REWRITTEN_FORMAT, "{point:?}");
        assert!(marker.is_none(), "{point:?}");
        assert_eq!(
            option_scopes(&object_store).await,
            migrated_scopes(),
            "every record moved exactly once after {point:?}"
        );
    }
}

/// Re-running a completed migration is a no-op, not a second rewrite: the
/// format already names the target, so the plan is empty.
#[tokio::test]
async fn a_completed_migration_reruns_as_a_noop() {
    let object_store = seeded_store().await;
    migrate_with(&object_store, SyntheticMigration::MoveOptionScope)
        .await
        .unwrap();

    migrate_with(&object_store, SyntheticMigration::MoveOptionScope)
        .await
        .unwrap();

    assert_eq!(option_scopes(&object_store).await, migrated_scopes());
    let (format, marker) = durable_state(&object_store).await;
    assert_eq!(format, REWRITTEN_FORMAT);
    assert!(marker.is_none());
}

/// A multi-version jump is the composition of its links, each with its own
/// start, steps, and finish, so the format lands on the last link's target
/// and no marker survives.
#[tokio::test]
async fn a_multi_version_jump_composes_its_links() {
    let object_store = seeded_store().await;

    migrate_with(&object_store, SyntheticMigration::MoveOptionScopeThenLink)
        .await
        .unwrap();

    let (format, marker) = durable_state(&object_store).await;
    assert_eq!(format, CHAINED_FORMAT);
    assert!(marker.is_none());
    assert_eq!(option_scopes(&object_store).await, migrated_scopes());
}

/// Time travel survives the rewrite: a pre-migration snapshot still resolves
/// to the state it named, so the migration preserves history rather than
/// flattening it.
#[tokio::test]
async fn a_migrated_store_still_time_travels() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let catalog = Catalog::open(object_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let first = catalog
        .commit(|tx| {
            tx.create_schema("sales")?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            tx.create_schema("ops")?;
            Ok(())
        })
        .await
        .unwrap();
    catalog.close().await.unwrap();

    migrate_with(&object_store, SyntheticMigration::MoveOptionScope)
        .await
        .unwrap();

    // The rewriting unit lands on the newest format this binary reads, so the
    // historical read goes through an ordinary attach — the way an operator
    // would check that a migration preserved history.
    let catalog = Catalog::open(object_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let past = catalog.snapshot_at(first).await.unwrap();
    catalog.close().await.unwrap();

    let mut names: Vec<String> = past
        .schemas()
        .into_iter()
        .map(|schema| schema.name)
        .collect();
    names.sort();
    assert_eq!(names, vec!["main".to_string(), "sales".to_string()]);
}
/// A marker the format stamp contradicts cannot be produced by any step of
/// the protocol, so meeting one is corruption, not a resume.
#[test]
fn a_marker_the_format_contradicts_is_corruption() {
    install_migration(SyntheticMigration::None);
    let error = plan(
        FORMAT_VERSION,
        Some(&proto::MigrationValue {
            from_format: FORMAT_VERSION + 1,
            to_format: FORMAT_VERSION + 2,
            cursor: Vec::new(),
        }),
    )
    .err()
    .unwrap();

    assert!(matches!(error, Error::Corruption(_)), "{error:?}");
}

/// A migration this binary does not carry is refused by name rather than
/// guessed at — the same discipline as meeting a newer format.
#[test]
fn an_unknown_migration_in_flight_is_refused() {
    install_migration(SyntheticMigration::None);
    let error = plan(
        FORMAT_VERSION,
        Some(&proto::MigrationValue {
            from_format: FORMAT_VERSION,
            to_format: FORMAT_VERSION + 1,
            cursor: Vec::new(),
        }),
    )
    .err()
    .unwrap();

    assert!(matches!(error, Error::Migration(_)), "{error:?}");
}

/// The shipped registry is empty — every format so far is additive — so the
/// verb reports a no-op rather than pretending it rewrote anything.
#[tokio::test]
async fn the_migrate_verb_is_a_noop_against_a_current_store() {
    install_migration(SyntheticMigration::None);
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let catalog = Catalog::open(object_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog.close().await.unwrap();

    let report = Catalog::migrate(
        object_store,
        CatalogOptions::default(),
        MigrationRequest::default(),
    )
    .await
    .unwrap();

    assert_eq!(report.from_format, MAX_FORMAT_VERSION);
    assert_eq!(report.to_format, MAX_FORMAT_VERSION);
    assert!(report.units_run.is_empty());
    assert!(!report.resumed);
}

/// The checkpoint flag takes one and releases it once the run is durable,
/// leaving the store usable — the recovery point is an operator's choice,
/// not a lingering cost.
#[tokio::test]
async fn the_checkpoint_flag_takes_and_releases_one() {
    install_migration(SyntheticMigration::None);
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let catalog = Catalog::open(object_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog.close().await.unwrap();

    Catalog::migrate(
        object_store.clone(),
        CatalogOptions::default(),
        MigrationRequest { checkpoint: true },
    )
    .await
    .unwrap();

    let catalog = Catalog::open(object_store, CatalogOptions::default())
        .await
        .unwrap();
    assert_eq!(catalog.snapshot().await.unwrap().schemas().len(), 1);
    catalog.close().await.unwrap();
}

/// The registry and the readable floor have to agree, and nothing but this
/// test makes them.
///
/// `MIGRATIONS` is empty today, so the floor sits at the base format and
/// every arm of the version check that depends on it is dormant. The moment
/// a real unit is added that stops being true: its `to_format` is where the
/// keys now live, so a store below that is one this binary cannot read
/// directly and must migrate first. Adding a unit without raising
/// `MIN_FORMAT_VERSION` with it would leave the below-floor arm silently
/// unreachable and let an ordinary attach scan for keys the rewrite moved.
///
/// The chain shape matters for the same reason: `chain_from` walks by
/// matching `from_format`, so a gap strands every later unit and a repeat
/// makes which one runs depend on registry order.
#[test]
fn the_registry_and_the_readable_floor_agree() {
    for pair in MIGRATIONS.windows(2) {
        assert_eq!(
            pair[0].to_format, pair[1].from_format,
            "{} and {} leave a gap the chain cannot cross",
            pair[0].name, pair[1].name
        );
    }

    let mut sources: Vec<u64> = MIGRATIONS.iter().map(|unit| unit.from_format).collect();
    let before = sources.len();
    sources.sort_unstable();
    sources.dedup();
    assert_eq!(
        sources.len(),
        before,
        "two units read the same format; which one runs would depend on registry order"
    );

    for unit in MIGRATIONS {
        assert!(
            unit.from_format < unit.to_format,
            "{} does not move the store forward",
            unit.name
        );
        assert!(
            unit.to_format <= MAX_FORMAT_VERSION,
            "{} migrates to {}, past the newest format this binary names ({MAX_FORMAT_VERSION})",
            unit.name,
            unit.to_format
        );
    }

    let expected_floor = MIGRATIONS
        .last()
        .map_or(FORMAT_VERSION, |unit| unit.to_format);
    assert_eq!(
        MIN_FORMAT_VERSION, expected_floor,
        "the floor must sit at the newest rewritten layout: below it the keys are somewhere else"
    );
}
