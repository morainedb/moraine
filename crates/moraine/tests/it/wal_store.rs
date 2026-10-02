//! A write-ahead log on an object store of its own.
//!
//! Commit latency is one WAL PUT, so a deployment may want the log on a
//! faster store than the catalog's — an S3 Express One Zone bucket beside
//! a standard one. The log is then the only thing that moves: the keys
//! keep the catalog's own path, and every later open of that lake must
//! name the same store.

use std::sync::Arc;

use futures::StreamExt;
use moraine::{Catalog, CatalogOptions, CensusRequest, Error, MigrationRequest, WalStore};
use object_store::{ObjectStore, memory::InMemory};

/// Options naming `wal` as the write-ahead log's store, addressed as
/// `name` — what the catalog records and holds every later open to.
fn with_wal_named(name: &str, wal: &Arc<InMemory>) -> CatalogOptions {
    let mut options = CatalogOptions::default();
    options.wal_store = Some(WalStore::new(name, Arc::clone(wal) as Arc<dyn ObjectStore>));
    options
}

fn with_wal(wal: &Arc<InMemory>) -> CatalogOptions {
    with_wal_named("memory://wal", wal)
}

/// How many of the store's objects sit in its write-ahead log directory,
/// how many are sorted-string tables, and how many are neither.
#[allow(clippy::unwrap_used)]
async fn objects(store: &Arc<InMemory>) -> (usize, usize, usize) {
    let mut listing = store.list(None);
    let (mut wal, mut sst, mut rest) = (0, 0, 0);
    while let Some(object) = listing.next().await {
        let object = object.unwrap();
        match object
            .location
            .parts()
            .find(|part| matches!(part.as_ref(), "wal" | "compacted"))
            .as_ref()
            .map(object_store::path::PathPart::as_ref)
        {
            Some("wal") => wal += 1,
            Some("compacted") => sst += 1,
            _ => rest += 1,
        }
    }
    (wal, sst, rest)
}

/// The log is written to the store it names, and the catalog store carries
/// none of it.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn commits_write_their_log_to_the_wal_store() {
    let catalog_store = Arc::new(InMemory::new());
    let wal_store = Arc::new(InMemory::new());

    let catalog = Catalog::open(catalog_store.clone(), with_wal(&wal_store))
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();

    let (wal, sst, beside) = objects(&wal_store).await;
    assert!(wal > 0, "the commit's log must land on the WAL store");
    assert_eq!(
        (sst, beside),
        (0, 0),
        "only the log belongs on the WAL store"
    );
    assert_eq!(
        objects(&catalog_store).await.0,
        0,
        "the catalog store must carry no log objects"
    );
}

/// A commit that never reached a sorted-string table is replayed from the
/// separate store, so a crash loses nothing by moving the log.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_reopen_replays_the_log_from_the_wal_store() {
    let catalog_store = Arc::new(InMemory::new());
    let wal_store = Arc::new(InMemory::new());

    let catalog = Catalog::open(catalog_store.clone(), with_wal(&wal_store))
        .await
        .unwrap();
    // Creating the catalog writes its own state out; what follows is the
    // commit the reopen has to find in the log.
    let written_out = objects(&catalog_store).await.1;
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();
    assert_eq!(
        objects(&catalog_store).await.1,
        written_out,
        "the commit must still be unflushed for the reopen to prove replay"
    );
    // No close: nothing is flushed on the way out, so the commit lives in
    // the log alone — and the log is on the other store.
    drop(catalog);

    let reopened = Catalog::open(catalog_store, with_wal(&wal_store))
        .await
        .unwrap();
    assert!(
        reopened
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("sales")
            .is_some()
    );
}

/// A reader follows the same log.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_read_only_catalog_reads_the_wal_store() {
    let catalog_store = Arc::new(InMemory::new());
    let wal_store = Arc::new(InMemory::new());

    let writer = Catalog::open(catalog_store.clone(), with_wal(&wal_store))
        .await
        .unwrap();
    writer
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();
    drop(writer);

    let reader = Catalog::open_read_only(catalog_store, with_wal(&wal_store))
        .await
        .unwrap();
    assert!(
        reader
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("sales")
            .is_some()
    );
}

/// A lake's log store is fixed when it is created: an open that omits it
/// would replay an empty log over live state, so it is refused by name.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn opening_without_the_wal_store_is_refused() {
    let catalog_store = Arc::new(InMemory::new());
    let wal_store = Arc::new(InMemory::new());

    let catalog = Catalog::open(catalog_store.clone(), with_wal(&wal_store))
        .await
        .unwrap();
    catalog.close().await.unwrap();

    let error = Catalog::open(catalog_store.clone(), CatalogOptions::default())
        .await
        .expect_err("a lake created with a WAL store cannot be opened without one");
    assert!(
        matches!(error, Error::Configuration(message) if message.contains("write-ahead log")),
        "the refusal must name the missing WAL store"
    );

    let error = Catalog::open_read_only(catalog_store, CatalogOptions::default())
        .await
        .expect_err("a reader is refused on the same terms");
    assert!(matches!(error, Error::Configuration(_)));
}

/// A log store other than the recorded one is refused too: it holds none
/// of this catalog's log, so replaying it would drop every commit no
/// sorted-string table carries yet.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn opening_against_another_wal_store_is_refused() {
    let catalog_store = Arc::new(InMemory::new());
    let wal_store = Arc::new(InMemory::new());

    let catalog = Catalog::open(
        catalog_store.clone(),
        with_wal_named("memory://log", &wal_store),
    )
    .await
    .unwrap();
    catalog.close().await.unwrap();

    let elsewhere = Arc::new(InMemory::new());
    let error = Catalog::open(catalog_store, with_wal_named("memory://other", &elsewhere))
        .await
        .expect_err("only the recorded log store may be opened against");
    assert!(
        matches!(error, Error::Configuration(message)
            if message.contains("memory://log") && message.contains("memory://other")),
        "the refusal must name both stores"
    );
}

/// The refusal holds from the moment the catalog exists: a catalog created
/// and then lost without a clean close is refused, not mistaken for an
/// empty store and created a second time over the one in the log.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_catalog_lost_before_any_flush_is_still_refused() {
    let catalog_store = Arc::new(InMemory::new());
    let wal_store = Arc::new(InMemory::new());

    let catalog = Catalog::open(catalog_store.clone(), with_wal(&wal_store))
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();
    drop(catalog);

    let error = Catalog::open(catalog_store.clone(), CatalogOptions::default())
        .await
        .expect_err("the catalog records its log store from the moment it exists");
    assert!(
        matches!(error, Error::Configuration(message) if message.contains("memory://wal")),
        "a second bootstrap would strand the commits the log holds"
    );

    // And the commit itself is still there, through the store it names.
    let reopened = Catalog::open(catalog_store, with_wal(&wal_store))
        .await
        .unwrap();
    assert!(
        reopened
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("sales")
            .is_some()
    );
}

/// The other direction: a lake whose log is in the catalog store cannot
/// adopt a separate one, which would leave its live log unread.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn adopting_a_wal_store_is_refused() {
    let catalog_store = Arc::new(InMemory::new());

    let catalog = Catalog::open(catalog_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog.close().await.unwrap();

    let wal_store = Arc::new(InMemory::new());
    let error = Catalog::open(catalog_store, with_wal(&wal_store))
        .await
        .expect_err("a lake created without a WAL store cannot adopt one");
    assert!(
        matches!(error, Error::Configuration(message) if message.contains("write-ahead log")),
        "the refusal must name the WAL store it was given"
    );
}

/// The census counts the log wherever it lives, so the figures of a lake
/// with a separate log still sum.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn the_census_counts_the_log_on_the_wal_store() {
    let catalog_store = Arc::new(InMemory::new());
    let wal_store = Arc::new(InMemory::new());

    let catalog = Catalog::open(catalog_store, with_wal(&wal_store))
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();

    let census = catalog
        .store_census(CensusRequest::default())
        .await
        .unwrap();
    let objects = census.objects.expect("the store was listed");
    assert!(
        objects.wal_objects > 0,
        "the log on the WAL store must be counted: {objects:?}"
    );
    assert!(objects.wal_bytes > 0);
    assert_eq!(
        objects.total_objects,
        objects.wal_objects
            + objects.manifest_objects
            + objects.sst_objects
            + objects.other_objects,
        "the parts must still sum to the total"
    );
}

/// A catalog whose log is in the catalog store moves it to one of its own,
/// and from then on is held to that store.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_move_adopts_a_log_store_and_then_requires_it() {
    let catalog_store = Arc::new(InMemory::new());
    let catalog = Catalog::open(catalog_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();
    catalog.close().await.unwrap();

    let wal_store = Arc::new(InMemory::new());
    let moved = Catalog::move_wal_store(
        catalog_store.clone(),
        CatalogOptions::default(),
        Some(WalStore::new(
            "memory://log",
            Arc::clone(&wal_store) as Arc<dyn ObjectStore>,
        )),
    )
    .await
    .unwrap();
    assert_eq!(moved.from, None);
    assert_eq!(moved.to.as_deref(), Some("memory://log"));
    assert!(moved.moved());

    let reopened = Catalog::open(
        catalog_store.clone(),
        with_wal_named("memory://log", &wal_store),
    )
    .await
    .unwrap();
    assert!(
        reopened
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("sales")
            .is_some()
    );
    reopened
        .commit(|tx| tx.create_schema("ops").map(|_| ()))
        .await
        .unwrap();
    assert!(
        objects(&wal_store).await.0 > 0,
        "commits after the move write their log to the new store"
    );
    reopened.close().await.unwrap();

    let error = Catalog::open(catalog_store, CatalogOptions::default())
        .await
        .expect_err("the moved-to store is the one the catalog now records");
    assert!(matches!(error, Error::Configuration(message) if message.contains("memory://log")));
}

/// The move drains the log it leaves behind: a commit that no sorted-string
/// table held yet survives it, which is the whole reason the move takes the
/// old store at all.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_move_carries_an_unflushed_commit_across() {
    let catalog_store = Arc::new(InMemory::new());
    let catalog = Catalog::open(catalog_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();
    // No close: the commit is durable in the log and nowhere else.
    drop(catalog);

    let wal_store = Arc::new(InMemory::new());
    Catalog::move_wal_store(
        catalog_store.clone(),
        CatalogOptions::default(),
        Some(WalStore::new(
            "memory://log",
            Arc::clone(&wal_store) as Arc<dyn ObjectStore>,
        )),
    )
    .await
    .unwrap();

    let reopened = Catalog::open(catalog_store, with_wal_named("memory://log", &wal_store))
        .await
        .unwrap();
    assert!(
        reopened
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("sales")
            .is_some(),
        "the commit in the old log must have been written out before the move"
    );
}

/// A log store can move back into the catalog store, and a move to where
/// the log already is changes nothing.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_move_is_reversible_and_idempotent() {
    let catalog_store = Arc::new(InMemory::new());
    let wal_store = Arc::new(InMemory::new());
    let log = || {
        WalStore::new(
            "memory://wal",
            Arc::clone(&wal_store) as Arc<dyn ObjectStore>,
        )
    };
    let catalog = Catalog::open(catalog_store.clone(), with_wal(&wal_store))
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();
    catalog.close().await.unwrap();

    let standing =
        Catalog::move_wal_store(catalog_store.clone(), with_wal(&wal_store), Some(log()))
            .await
            .unwrap();
    assert!(!standing.moved(), "the log is already there: {standing:?}");

    let back = Catalog::move_wal_store(catalog_store.clone(), with_wal(&wal_store), None)
        .await
        .unwrap();
    assert_eq!(back.from.as_deref(), Some("memory://wal"));
    assert_eq!(back.to, None);

    let reopened = Catalog::open(catalog_store, CatalogOptions::default())
        .await
        .unwrap();
    assert!(
        reopened
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("sales")
            .is_some()
    );
}

/// A migration opens the writer itself, and rewrites keys from what the
/// log replayed, so it is held to the recorded log store exactly as an
/// attach is.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_migration_must_name_the_log_store() {
    let catalog_store = Arc::new(InMemory::new());
    let wal_store = Arc::new(InMemory::new());
    let catalog = Catalog::open(catalog_store.clone(), with_wal(&wal_store))
        .await
        .unwrap();
    catalog.close().await.unwrap();

    let error = Catalog::migrate(
        catalog_store.clone(),
        CatalogOptions::default(),
        MigrationRequest::default(),
    )
    .await
    .expect_err("a migration without the recorded log store must be refused");
    assert!(matches!(error, Error::Configuration(message) if message.contains("memory://wal")));

    let report = Catalog::migrate(
        catalog_store,
        with_wal(&wal_store),
        MigrationRequest::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        report.from_format, report.to_format,
        "named, the migration runs and finds nothing to do"
    );
}

/// A log store inside the catalog's data root is refused by the move as it
/// is by an open: the orphaned-file cleanup that lists that root would
/// delete the log objects under it.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_move_into_the_data_path_is_refused() {
    let catalog_store = Arc::new(InMemory::new());
    let mut created = CatalogOptions::default();
    created.data_path = Some("/lake/data".to_string());
    let catalog = Catalog::open(catalog_store.clone(), created).await.unwrap();
    catalog.close().await.unwrap();

    let error = Catalog::move_wal_store(
        catalog_store.clone(),
        CatalogOptions::default(),
        Some(WalStore::new(
            "/lake/data/wal",
            Arc::new(InMemory::new()) as Arc<dyn ObjectStore>,
        )),
    )
    .await
    .expect_err("a log store under the data root must be refused");
    assert!(
        matches!(error, Error::Constraint(message) if message.contains("/lake/data")),
        "the refusal must name the data root it would sit under"
    );

    // Refused before anything moved: the log is still in the catalog store.
    Catalog::open(catalog_store, CatalogOptions::default())
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
}

/// A move has to name the log store as it stands, since that is the log it
/// drains; the refusal names the one the catalog records.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_move_from_the_wrong_store_is_refused() {
    let catalog_store = Arc::new(InMemory::new());
    let wal_store = Arc::new(InMemory::new());
    let catalog = Catalog::open(catalog_store.clone(), with_wal(&wal_store))
        .await
        .unwrap();
    catalog.close().await.unwrap();

    let elsewhere = Arc::new(InMemory::new());
    let error = Catalog::move_wal_store(
        catalog_store,
        with_wal_named("memory://guess", &elsewhere),
        Some(WalStore::new(
            "memory://next",
            Arc::new(InMemory::new()) as Arc<dyn ObjectStore>,
        )),
    )
    .await
    .expect_err("the move must start from the recorded log store");
    assert!(
        matches!(error, Error::Configuration(message) if message.contains("memory://wal")),
        "the refusal must name the recorded store"
    );
}
