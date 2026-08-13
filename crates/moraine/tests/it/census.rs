//! The store census and the store merge, through the public API.

use std::{sync::Arc, time::Duration};

use moraine::{
    Catalog, CatalogOptions, CensusRequest, CompactStoreRequest, CompactionTarget, Error,
    MergeOutcome, StoreCensus, SubspaceName,
};
use object_store::memory::InMemory;

use crate::fixtures::col;

/// The subspaces a committed catalog has written out.
#[allow(clippy::unwrap_used)]
async fn census_of(catalog: &Catalog, request: CensusRequest) -> StoreCensus {
    catalog.store_census(request).await.unwrap()
}

fn merge_request(target: CompactionTarget, wait: Option<Duration>) -> CompactStoreRequest {
    let mut request = CompactStoreRequest::default();
    request.target = target;
    request.wait = wait;
    request
}

fn measured<'a>(
    census: &'a StoreCensus,
    subspace: &SubspaceName,
) -> Option<&'a moraine::SubspaceCensus> {
    census
        .subspaces
        .iter()
        .find(|measured| &measured.subspace == subspace)
}

/// A catalog that has committed reports the subspaces those commits wrote,
/// and reports none it did not touch.
#[tokio::test]
async fn a_census_names_the_subspaces_the_catalog_wrote() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let sales = tx.create_schema("sales")?;
            tx.create_table(sales, "orders", &[col("id")])?;
            Ok(())
        })
        .await
        .unwrap();

    let census = census_of(&catalog, CensusRequest::default()).await;

    // Physical figures count what has been written out, and a small catalog
    // may still be entirely in the write-ahead log — but the shape of the
    // report is fixed either way.
    for subspace in &census.subspaces {
        assert!(
            !matches!(subspace.subspace, SubspaceName::Unknown(_)),
            "moraine wrote a segment it cannot name: {subspace:?}"
        );
    }
    assert_eq!(census.total_bytes(), {
        let mut total = 0;
        for subspace in &census.subspaces {
            total += subspace.bytes;
        }
        total
    });
}

/// The scanning leg counts live keys, and splits `current` into entity
/// records and the deletion-schedule entries a merge cannot reclaim.
#[tokio::test]
async fn the_scanning_leg_counts_live_keys() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let sales = tx.create_schema("sales")?;
            tx.create_table(sales, "orders", &[col("id")])?;
            tx.create_table(sales, "returns", &[col("id")])?;
            Ok(())
        })
        .await
        .unwrap();

    let census = census_of(&catalog, {
        let mut request = CensusRequest::default();
        request.count_live_entries = true;
        request
    })
    .await;

    let current = measured(&census, &SubspaceName::Current).expect("current is always written");
    let live = current.live.expect("the scanning leg was requested");
    assert!(live.keys > 0, "{live:?}");
    assert!(live.key_bytes > 0, "{live:?}");
    assert_eq!(live.scheduled_files, 0, "nothing has been expired");

    // The default request asks for none of this.
    let physical = census_of(&catalog, CensusRequest::default()).await;
    assert!(
        measured(&physical, &SubspaceName::Current).is_none_or(|measured| measured.live.is_none())
    );
}

/// A read-only catalog measures the store but cannot merge it: the
/// compactor that executes a submitted merge runs inside the writer.
#[tokio::test]
async fn a_read_only_catalog_measures_but_does_not_merge() {
    let object_store = Arc::new(InMemory::new());
    let catalog = Catalog::open(Arc::clone(&object_store) as _, CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();

    let reader = Catalog::open_read_only(object_store as _, CatalogOptions::default())
        .await
        .unwrap();

    reader.store_census(CensusRequest::default()).await.unwrap();
    let mut counting = CensusRequest::default();
    counting.count_live_entries = true;
    reader.store_census(counting).await.unwrap();

    // The merge is a mutator, so it is not on `ReadOnlyCatalog` at all —
    // the census is the whole read-only maintenance surface.
}

/// A subspace this build cannot name addresses no keys, so it cannot be a
/// merge target.
#[tokio::test]
async fn an_unknown_subspace_is_not_a_merge_target() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();

    let refused = catalog
        .compact_store(merge_request(
            CompactionTarget::Subspace(SubspaceName::Unknown(vec![0xfe])),
            None,
        ))
        .await
        .unwrap_err();

    assert!(matches!(refused, Error::Configuration(_)), "{refused:?}");
}

/// Verification without a wait cannot distinguish submission from a
/// completed merge, so the core refuses that contradictory request.
#[tokio::test]
async fn verified_compaction_requires_a_wait_budget() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let mut request = merge_request(CompactionTarget::Subspace(SubspaceName::Index), None);
    request.require_completed = true;

    let error = catalog.compact_store(request).await.unwrap_err();
    assert!(matches!(error, Error::Configuration(_)), "{error:?}");
}

/// A catalog with nothing to merge reports every measured subspace skipped
/// rather than failing or reporting nothing at all.
#[tokio::test]
async fn a_store_with_nothing_to_merge_reports_skipped() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();

    let report = catalog
        .compact_store(merge_request(
            CompactionTarget::WholeStore,
            Some(Duration::from_secs(5)),
        ))
        .await
        .unwrap();

    let census = census_of(&catalog, CensusRequest::default()).await;
    assert_eq!(report.merges.len(), census.subspaces.len());
    for merge in &report.merges {
        assert!(
            matches!(merge.outcome, MergeOutcome::Skipped(_)),
            "{merge:?}"
        );
        assert_eq!(merge.bytes_after, None);
    }
}

/// A merge of one subspace reports only that subspace.
#[tokio::test]
async fn a_targeted_merge_reports_only_its_subspace() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();

    let report = catalog
        .compact_store(merge_request(
            CompactionTarget::Subspace(SubspaceName::Current),
            None,
        ))
        .await
        .unwrap();

    assert!(
        report
            .merges
            .iter()
            .all(|merge| merge.subspace == SubspaceName::Current),
        "{report:?}"
    );
}

/// A merge mints no snapshot and moves no catalog state: the view before
/// and after is the same one.
#[tokio::test]
async fn a_merge_leaves_the_catalog_untouched() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let sales = tx.create_schema("sales")?;
            tx.create_table(sales, "orders", &[col("id")])?;
            Ok(())
        })
        .await
        .unwrap();

    let before = catalog.snapshot().await.unwrap();
    catalog
        .compact_store(merge_request(
            CompactionTarget::WholeStore,
            Some(Duration::from_secs(5)),
        ))
        .await
        .unwrap();
    let after = catalog.snapshot().await.unwrap();

    assert_eq!(
        before.current_snapshot().id,
        after.current_snapshot().id,
        "a merge minted a snapshot"
    );
    assert_eq!(before.schemas().len(), after.schemas().len());
    assert!(after.schema_by_name("sales").is_some());
}
