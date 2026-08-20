//! The read cache through the public API: repeated reads must serve the
//! same catalog a cold read would build, whatever the cache did in between.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, SnapshotId};
use object_store::memory::InMemory;

use crate::{
    counting_store::CountingStore,
    fixtures::{col, datafile, seeded},
};

/// A read-only handle materializes the catalog **once** and serves every
/// later read from the cache.
///
/// This is the incident's shape as a test. A handle that rebuilds per read
/// returns the right answer every time and differs only in traffic, so the
/// assertion counts object-store reads rather than timing anything: after
/// the first view, repeated reads must cost a bounded handful of reads —
/// the head point read and its neighbours — not another scan of `current`.
#[tokio::test]
async fn a_read_only_handle_materializes_once() {
    let object_store = Arc::new(InMemory::new());
    let writer = Catalog::open(
        Arc::clone(&object_store) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();
    writer
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            let table = tx.create_table(schema, "t", &[col("a")])?;
            for _ in 0..64 {
                tx.register_data_file(table, datafile(100), &[])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    let counting = Arc::new(CountingStore::new(object_store));
    let reader = Catalog::open_read_only(
        Arc::clone(&counting) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();

    // The first view is the materialization, and is allowed to read.
    let first = reader.snapshot().await.unwrap();
    let cold = counting.take_reads();
    assert!(cold > 0, "a cold view read nothing");

    // Every later view serves the same catalog for a small, constant cost.
    for _ in 0..8 {
        let view = reader.snapshot().await.unwrap();
        assert_eq!(view.schemas().len(), first.schemas().len());
    }
    let warm = counting.take_reads();

    assert!(
        warm < cold,
        "warm reads cost as much as the cold one ({warm} vs {cold}): the handle is \
         rebuilding the catalog per read"
    );
}

/// A whole-subspace scan reads ahead rather than paying a round trip per
/// block.
///
/// SlateDB's scan default is one block, fetched serially — invisible on
/// local storage and ruinous on remote, where a 12.8 MB subspace measured
/// 276 s at ~46 KB/s, which is 3 200 sequential fetches and nothing else.
/// The assertion counts reads rather than timing them, because on an
/// in-memory store the defect costs nothing observable.
#[tokio::test]
async fn a_materialization_reads_ahead_rather_than_block_by_block() {
    let object_store = Arc::new(InMemory::new());
    let writer = Catalog::open(
        Arc::clone(&object_store) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();

    // Enough live rows that `current` spans many blocks: a scan that
    // fetches one at a time issues an order more reads than one that does
    // not, whatever the block size turns out to be.
    writer
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            let table = tx.create_table(schema, "t", &[col("a")])?;
            for _ in 0..4_000 {
                tx.register_data_file(table, datafile(100), &[])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    let counting = Arc::new(CountingStore::new(object_store));
    let reader = Catalog::open_read_only(
        Arc::clone(&counting) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();
    counting.take_reads();

    let view = reader.snapshot().await.unwrap();
    let reads = counting.take_reads();
    let files = view
        .tables_in(view.schemas()[0].id)
        .first()
        .map(|table| view.data_files_of(table.id).len())
        .unwrap_or_default();

    assert!(files >= 4_000, "seed did not land: {files} files");
    // One round trip per 4 KiB block would be in the thousands here. The
    // bound is deliberately loose: it catches the defect's order of
    // magnitude without pinning SlateDB's block size or layout.
    // Measured: 5 reads with read-ahead, 89 without, on this seed. The
    // bound sits between them with headroom, so it catches the defect's
    // order of magnitude without pinning SlateDB's block size or layout.
    assert!(
        reads < 20,
        "materialization issued {reads} reads for {files} files — scanning block by block"
    );
}

/// A read-only handle scans once for a whole population of DuckLake's
/// metadata tables, not once per `dump_*` call.
///
/// DuckLake issues roughly two dozen dumps to populate its metadata, and
/// each one used to rescan `current` *and* `history` on a reader — the
/// entity projection was gated on holding the writer. That is the cost a
/// query pays on every execution, not just at attach.
#[tokio::test]
async fn a_read_only_handle_scans_once_for_many_dumps() {
    let object_store = Arc::new(InMemory::new());
    let writer = Catalog::open(
        Arc::clone(&object_store) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();
    writer
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            let table = tx.create_table(schema, "t", &[col("a")])?;
            for _ in 0..2_000 {
                tx.register_data_file(table, datafile(100), &[])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    let counting = Arc::new(CountingStore::new(object_store));
    let reader = Catalog::open_read_only(
        Arc::clone(&counting) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();

    // The first dump scans; it is the one that installs the projection.
    let first = moraine::ffi_support::dump_data_files(&reader)
        .await
        .unwrap();
    let cold = counting.take_reads();
    assert!(!first.is_empty(), "seed did not land");
    assert!(cold > 0, "a cold dump read nothing");

    // A population's worth of further dumps must not rescan.
    for _ in 0..12 {
        let again = moraine::ffi_support::dump_data_files(&reader)
            .await
            .unwrap();
        assert_eq!(again.len(), first.len());
    }
    let warm = counting.take_reads();

    assert!(
        warm < cold,
        "twelve further dumps cost {warm} reads against {cold} for one: the reader is \
         rescanning per dump"
    );
}

/// A second read is served from the cache after a commit moved head. It
/// must show the commit — a cache that serves a stale head is worse than
/// no cache.
#[tokio::test]
async fn a_read_after_a_commit_sees_the_commit() {
    let (catalog, schema, table_a, _) = seeded().await;

    let first = catalog.snapshot().await.unwrap();
    assert!(first.table_by_name(schema, "late").is_none());

    catalog
        .commit(|tx| {
            tx.create_table(schema, "late", &[col("x")])?;
            tx.register_data_file(table_a, datafile(5), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    let second = catalog.snapshot().await.unwrap();
    let late = second
        .table_by_name(schema, "late")
        .expect("a cached read must see the new table");
    assert_eq!(second.columns_of(late.id).len(), 1);
    assert_eq!(second.data_files_of(table_a).len(), 1);

    catalog.close().await.unwrap();
}

/// Reading twice with no commit in between must not drift: the second
/// read serves the cache, and the cache must equal what it replaced.
#[tokio::test]
async fn repeated_reads_agree() {
    let (catalog, schema, table_a, _) = seeded().await;
    catalog
        .commit(|tx| {
            tx.register_data_file(table_a, datafile(3), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    let first = catalog.snapshot().await.unwrap();
    let second = catalog.snapshot().await.unwrap();

    assert_eq!(
        first.current_snapshot().id.get(),
        second.current_snapshot().id.get()
    );
    assert_eq!(
        first.tables_in(schema).len(),
        second.tables_in(schema).len()
    );
    assert_eq!(
        first.data_files_of(table_a).len(),
        second.data_files_of(table_a).len()
    );

    catalog.close().await.unwrap();
}

/// Every entity kind a commit can touch must survive the fold: the view a
/// warm handle serves answers exactly as a cold reopen does.
#[tokio::test]
async fn a_cached_read_matches_a_cold_reopen() {
    let (catalog, schema, table_a, table_b) = seeded().await;
    let warm = catalog.snapshot().await.unwrap();
    assert_eq!(warm.tables_in(schema).len(), 2);

    let wide = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            tx.register_data_file(table_a, datafile(7), &[])?;
            tx.rename_table(table_b, "renamed")?;
            tx.create_view(schema, "v", "duckdb", "SELECT 1")?;
            wide.set(Some(tx.create_table(
                schema,
                "wide",
                &[col("x"), col("y")],
            )?));
            Ok(())
        })
        .await
        .unwrap();
    let wide = wide.get().unwrap();

    // Dropping a column ends one child of a table the same commit did not
    // create, which is the fold's over-cascade trap.
    catalog
        .commit(|tx| {
            let second = tx.columns_of(wide)[1].id;
            tx.drop_column(wide, second)
        })
        .await
        .unwrap();

    // The live handle answers from its folded-forward cache.
    let cached = catalog.snapshot().await.unwrap();
    let head = cached.current_snapshot().id;

    // A cold handle over the same store has no cache and must scan.
    let cold = catalog
        .snapshot_at(SnapshotId::new(head.get()))
        .await
        .unwrap();

    assert_eq!(cached.tables_in(schema).len(), cold.tables_in(schema).len());
    assert!(cached.table_by_name(schema, "renamed").is_some());
    assert!(cached.view_by_name(schema, "v").is_some());
    assert_eq!(
        cached.data_files_of(table_a).len(),
        cold.data_files_of(table_a).len()
    );
    // The drop must end exactly one column, not cascade to the table.
    assert_eq!(cached.columns_of(wide).len(), cold.columns_of(wide).len());
    assert_eq!(cached.columns_of(wide).len(), 1);

    catalog.close().await.unwrap();
}

/// A held view is a value, not a cursor: commits after it was built must
/// leave it exactly as it was, however many land and whatever they touch.
#[tokio::test]
async fn a_held_view_is_unmoved_by_later_commits() {
    let (catalog, schema, table_a, table_b) = seeded().await;

    let held = catalog.snapshot().await.unwrap();
    let at = held.current_snapshot().id.get();
    let tables_before = held.tables_in(schema).len();
    let files_before = held.data_files_of(table_a).len();

    for round in 0..4u64 {
        catalog
            .commit(|tx| {
                tx.register_data_file(table_a, datafile(round), &[])?;
                tx.create_table(schema, &format!("later{round}"), &[col("x")])?;
                Ok(())
            })
            .await
            .unwrap();
    }
    catalog.commit(|tx| tx.drop_table(table_b)).await.unwrap();

    assert_eq!(held.current_snapshot().id.get(), at);
    assert_eq!(held.tables_in(schema).len(), tables_before);
    assert_eq!(held.data_files_of(table_a).len(), files_before);
    assert!(held.table_by_name(schema, "later0").is_none());
    assert!(held.table_by_name(schema, "b").is_some());

    // The same view, rebuilt from `history` at the same snapshot, agrees —
    // so what the held value shows is the catalog at `at`, not a stale
    // accident of how it was built.
    let travelled = catalog.snapshot_at(SnapshotId::new(at)).await.unwrap();
    assert_eq!(travelled.tables_in(schema).len(), tables_before);
    assert_eq!(travelled.data_files_of(table_a).len(), files_before);

    catalog.close().await.unwrap();
}

/// A read-only catalog caches its view as a writer does. It has no commits
/// of its own to fold, so what the cache must never do is drift: a second
/// read has to answer exactly what the first one did and exactly what the
/// store holds.
#[tokio::test]
async fn a_read_only_catalog_serves_a_cached_view_that_matches_the_store() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let writer = Catalog::open(object_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    writer
        .commit(|tx| {
            let schema = tx.create_schema("s")?;
            tx.create_table(schema, "t", &[col("x")])?;
            Ok(())
        })
        .await
        .unwrap();

    // A reader opened after `commit` returns resolves that commit.
    let reader = Catalog::open_read_only(object_store, CatalogOptions::default())
        .await
        .unwrap();
    let first = reader.snapshot().await.unwrap();
    let schema = first.schema_by_name("s").unwrap().id;
    assert!(first.table_by_name(schema, "t").is_some());

    // The second read is served from the cache and must not drift.
    let second = reader.snapshot().await.unwrap();
    assert_eq!(
        first.current_snapshot().id.get(),
        second.current_snapshot().id.get()
    );
    assert_eq!(
        second.tables_in(schema).len(),
        first.tables_in(schema).len()
    );
    assert!(second.table_by_name(schema, "t").is_some());

    // And it matches what a rebuild from the store at the same snapshot
    // shows, so the cache is not the only thing that believes it.
    let scanned = reader
        .snapshot_at(SnapshotId::new(first.current_snapshot().id.get()))
        .await
        .unwrap();
    assert_eq!(
        scanned.tables_in(schema).len(),
        first.tables_in(schema).len()
    );

    writer.close().await.unwrap();
}

/// A cache size bounds the cache without disabling it: the catalog is
/// served through a capped cache on the writer's side and the reader's
/// alike.
///
/// That a directory becomes a device is `store::cache`'s to assert, not
/// this test's: one cache serves the whole process, so whichever store
/// opens first in this binary decides whether there is a device at all.
#[tokio::test]
async fn a_bounded_disk_cache_serves_a_writer_and_a_reader() {
    let object_store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
    let cache = std::env::temp_dir().join(format!(
        "moraine-bounded-cache-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&cache);

    let mut options = CatalogOptions::default();
    options.cache_dir = Some(cache.clone());
    options.cache_size = Some(64 * 1024 * 1024);

    let writer = Catalog::open(Arc::clone(&object_store), options.clone())
        .await
        .unwrap();
    writer
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            tx.create_table(schema, "t", &[col("a")])?;
            Ok(())
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    let reader = Catalog::open_read_only(object_store, options)
        .await
        .unwrap();
    let view = reader.snapshot().await.unwrap();
    let schema = view.schema_by_name("main").expect("bootstrap").id;
    assert!(view.table_by_name(schema, "t").is_some());
    reader.close().await.unwrap();

    let _ = std::fs::remove_dir_all(&cache);
}

/// The cache reports what it served. Reads run through the process's one
/// instance, so the counters move as soon as anything reads a store —
/// which is what makes the budget sizable from measurement instead of
/// from the defaults.
///
/// Asserted as a delta, not an absolute: every other test in this binary
/// shares the same process-wide cache, so only this test's own reads are
/// its to claim.
#[tokio::test]
async fn the_cache_reports_what_it_served() {
    let object_store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;

    let before = moraine::cache_tally();

    let writer = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    writer
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            tx.create_table(schema, "t", &[col("a")])?;
            Ok(())
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    // A cold reader must fetch and decode the store's SSTs to answer, so
    // the metadata slot is consulted whatever it can serve.
    let reader = Catalog::open_read_only(object_store, CatalogOptions::default())
        .await
        .unwrap();
    let view = reader.snapshot().await.unwrap();
    assert!(
        view.table_by_name(view.schema_by_name("main").expect("bootstrap").id, "t")
            .is_some()
    );
    reader.close().await.unwrap();

    let after = moraine::cache_tally();
    let metadata_lookups = (after.metadata_hits + after.metadata_misses)
        - (before.metadata_hits + before.metadata_misses);
    assert!(
        metadata_lookups > 0,
        "reads did not reach the cache: {before:?} then {after:?}"
    );
    assert!(
        after.metadata_hit_rate().is_some(),
        "a cache that has served has a rate to report"
    );
}

/// Two catalogs in one process share the cache but not the tally: what one
/// reads lands in its own counts and not the other's, and the process's
/// counts take both.
///
/// The point is attribution. A host with several catalogs on one budget
/// can read the process's numbers today; what it cannot do without this is
/// tell which attach is spending them.
#[tokio::test]
async fn each_catalog_tallies_its_own_reads() {
    let quiet = Catalog::open(
        Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();

    let busy_store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
    let writer = Catalog::open(Arc::clone(&busy_store), CatalogOptions::default())
        .await
        .unwrap();
    writer
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            tx.create_table(schema, "t", &[col("a")])?;
            Ok(())
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    let quiet_before = quiet.cache_tally();
    let process_before = moraine::cache_tally();

    // A cold reader fetches and decodes the store's SSTs, so its own
    // metadata counters must move.
    let busy = Catalog::open_read_only(busy_store, CatalogOptions::default())
        .await
        .unwrap();
    let view = busy.snapshot().await.unwrap();
    assert!(
        view.table_by_name(view.schema_by_name("main").expect("bootstrap").id, "t")
            .is_some()
    );

    let busy_tally = busy.cache_tally();
    let busy_lookups = busy_tally.metadata_hits + busy_tally.metadata_misses;
    assert!(
        busy_lookups > 0,
        "the reading catalog's own tally must move: {busy_tally:?}"
    );

    assert_eq!(
        quiet.cache_tally(),
        quiet_before,
        "an idle catalog must not be charged for another's reads"
    );

    let process_after = moraine::cache_tally();
    assert!(
        (process_after.metadata_hits + process_after.metadata_misses)
            - (process_before.metadata_hits + process_before.metadata_misses)
            >= busy_lookups,
        "the process's counts still cover every attach's reads"
    );

    busy.close().await.unwrap();
    quiet.close().await.unwrap();
}

/// A bulk scan must not cost the probe path its residency. The meta slot
/// holds SST indexes and filters and data blocks cannot compete for it,
/// so a whole-subspace scan between two probes leaves the second probe
/// served exactly as the first was.
///
/// Read on the metadata counters rather than on timing: an in-memory
/// store makes a fetch nearly free, so a regression here would be
/// invisible in milliseconds and obvious in misses.
#[tokio::test]
async fn a_scan_does_not_evict_what_probes_need() {
    let object_store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
    let writer = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();

    // Enough tables that a `current` scan walks real blocks rather than
    // one, so the scan is a genuine eviction opportunity.
    writer
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            for n in 0..64 {
                tx.create_table(schema, &format!("t{n}"), &[col("a")])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    let reader = Catalog::open_read_only(object_store, CatalogOptions::default())
        .await
        .unwrap();

    // Warm: whatever this costs, it is what a repeat should cost.
    let _ = reader.snapshot().await.unwrap();
    let before_repeat = moraine::cache_tally();
    let _ = reader.snapshot().await.unwrap();
    let warm_misses = moraine::cache_tally().metadata_misses - before_repeat.metadata_misses;

    // A full scan of every subspace, then the same read again.
    let census = reader
        .store_census({
            let mut request = moraine::CensusRequest::default();
            request.count_live_entries = true;
            request
        })
        .await
        .unwrap();
    assert!(!census.subspaces.is_empty());

    let before_after_scan = moraine::cache_tally();
    let _ = reader.snapshot().await.unwrap();
    let after_scan_misses =
        moraine::cache_tally().metadata_misses - before_after_scan.metadata_misses;
    reader.close().await.unwrap();

    assert!(
        after_scan_misses <= warm_misses,
        "a scan cost the probe path its residency: {warm_misses} misses warm, \
         {after_scan_misses} after a scan"
    );
}

/// Several catalogs in one process share one cache and one budget, so a
/// second attach neither builds its own nor resets the first's tally.
#[tokio::test]
async fn attached_catalogs_share_one_cache() {
    let first_store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
    let second_store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;

    for store in [&first_store, &second_store] {
        let writer = Catalog::open(Arc::clone(store), CatalogOptions::default())
            .await
            .unwrap();
        writer
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                tx.create_table(schema, "t", &[col("a")])?;
                Ok(())
            })
            .await
            .unwrap();
        writer.close().await.unwrap();
    }

    let first = Catalog::open_read_only(first_store, CatalogOptions::default())
        .await
        .unwrap();
    let _ = first.snapshot().await.unwrap();
    let after_first = moraine::cache_tally();

    let second = Catalog::open_read_only(second_store, CatalogOptions::default())
        .await
        .unwrap();
    let _ = second.snapshot().await.unwrap();
    let after_second = moraine::cache_tally();

    first.close().await.unwrap();
    second.close().await.unwrap();

    // One tally across both: the second catalog's reads add to it rather
    // than starting their own.
    let first_lookups = after_first.metadata_hits + after_first.metadata_misses;
    let second_lookups = after_second.metadata_hits + after_second.metadata_misses;
    assert!(
        second_lookups > first_lookups,
        "the second catalog's reads did not reach the shared cache: \
         {first_lookups} then {second_lookups}"
    );
}

/// A preload warms the cache before anything reads through it, so the
/// first read after an attach finds more resident than it would have.
#[tokio::test]
async fn a_preload_warms_before_the_first_read() {
    let object_store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
    let writer = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    writer
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            for n in 0..32 {
                tx.create_table(schema, &format!("t{n}"), &[col("a")])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    let mut options = CatalogOptions::default();
    options.cache_preload = Some(moraine::CachePreload::All);

    let reader = Catalog::open_read_only(object_store, options)
        .await
        .unwrap();
    let after_open = reader.cache_tally();

    // The warm ran during the open, before any read was issued.
    let open_lookups = after_open.metadata_hits
        + after_open.metadata_misses
        + after_open.block_hits
        + after_open.block_misses;
    assert!(
        open_lookups > 0,
        "a preload consulted the cache not at all: {after_open:?}"
    );
    let attributed = after_open.preload_metadata_hits
        + after_open.preload_metadata_misses
        + after_open.preload_block_hits
        + after_open.preload_block_misses;
    assert!(
        attributed > 0 && attributed <= open_lookups,
        "preload traffic was not attributed separately: {after_open:?}"
    );

    let view = reader.snapshot().await.unwrap();
    assert!(view.schema_by_name("main").is_some());
    reader.close().await.unwrap();
}

/// One shared cache still reports SST metadata apart from data blocks.
///
/// Nothing outside SlateDB can classify a cached entry, so the split comes
/// from what each typed admission recorded. If that broke, metadata occupancy
/// would read zero however full the cache was, and the attach-time sizing
/// warning would never fire.
#[tokio::test]
async fn the_shared_cache_reports_metadata_apart_from_data_blocks() {
    let object_store = Arc::new(InMemory::new());
    let writer = Catalog::open(
        Arc::clone(&object_store) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();
    writer
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            let table = tx.create_table(schema, "t", &[col("a")])?;
            for _ in 0..256 {
                tx.register_data_file(table, datafile(100), &[])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    // A cold reader walks every SST's filter and index out of object storage.
    let reader = Catalog::open_read_only(
        object_store as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();
    reader.snapshot().await.unwrap();

    let status = moraine::cache_status();
    assert!(status.metadata_occupancy_bytes > 0, "{status:?}");

    // Metadata is protected up to a share of the one capacity rather than
    // partitioned into its own, so its ceiling is under the whole.
    assert!(status.metadata_capacity_bytes > 0, "{status:?}");
    assert!(
        status.metadata_capacity_bytes < status.block_capacity_bytes,
        "{status:?}"
    );
}

/// The handle's decoded catalog is reported, so a host sizing a process can
/// see the one cache no byte budget covers. It grows with the catalog and is
/// replaced when the head moves, never evicted under pressure.
#[tokio::test]
async fn a_handle_reports_what_its_decoded_catalog_holds() {
    let object_store = Arc::new(InMemory::new());
    let writer = Catalog::open(
        Arc::clone(&object_store) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();

    // Nothing materialized yet.
    assert_eq!(writer.projection_bytes(), 0);

    writer
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            let table = tx.create_table(schema, "t", &[col("a")])?;
            for _ in 0..64 {
                tx.register_data_file(table, datafile(100), &[])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    writer.snapshot().await.unwrap();
    let small = writer.projection_bytes();
    assert!(small > 0, "a materialized catalog reports nothing");

    writer
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            let table = tx.create_table(schema, "u", &[col("b")])?;
            for _ in 0..512 {
                tx.register_data_file(table, datafile(100), &[])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    writer.snapshot().await.unwrap();

    assert!(
        writer.projection_bytes() > small,
        "a larger catalog must report more: {small} then {}",
        writer.projection_bytes()
    );
}

/// Warming a table reads its index and inline probe ranges, and an index
/// lookup on the warmed handle still answers. Whether those blocks stay
/// resident is a property of the shared cache under load, measured by the
/// object-storage benchmark rather than pinned here.
#[tokio::test]
async fn warming_a_table_reads_its_probe_ranges() {
    use moraine::{IndexDef, IndexEntry, IndexKeyValue, IntWidth};

    let key = |value: i128| IndexKeyValue::Int {
        value,
        width: IntWidth::I64,
    };
    let seed = || async {
        let object_store = Arc::new(InMemory::new());
        let writer = Catalog::open(
            Arc::clone(&object_store) as Arc<dyn object_store::ObjectStore>,
            CatalogOptions::default(),
        )
        .await
        .unwrap();
        let created = std::cell::Cell::new(None);
        writer
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                let table = tx.create_table(schema, "items", &[col("value")])?;
                let entries = (0..2_000_u64)
                    .map(|row_id| IndexEntry {
                        row_id,
                        values: vec![Some(key(i128::from(row_id)))],
                    })
                    .collect::<Vec<_>>();
                let index = tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_value".to_owned(),
                        columns: vec![moraine::ColumnId::new(1)],
                        unique: false,
                    },
                    &entries,
                )?;
                created.set(Some((table, index)));
                Ok(())
            })
            .await
            .unwrap();
        writer.close().await.unwrap();
        let (table, index) = created.get().unwrap();
        (object_store, table, index)
    };

    let (object_store, table, index) = seed().await;
    let counting = Arc::new(CountingStore::new(object_store));
    let reader = Catalog::open_read_only(
        Arc::clone(&counting) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();
    reader.snapshot().await.unwrap();
    counting.take_reads();

    reader.warm_tables(&[table]).await.unwrap();
    let warm_reads = counting.take_reads();
    assert!(warm_reads > 0, "warming a cold table read nothing");

    let found = reader
        .index_lookup(table, index, &[key(1_500)])
        .await
        .unwrap();
    assert_eq!(found, vec![1_500]);
    reader.close().await.unwrap();
}
