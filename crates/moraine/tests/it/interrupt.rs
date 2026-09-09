//! What a host interrupt leaves behind.
//!
//! The bridge races an interrupt against the operation and drops the
//! loser, so an interrupt *is* a dropped future here: every case below
//! drops the caller's future at a chosen point and asserts what the
//! catalog is left holding. The point is a parked durable write rather
//! than a hopeful sleep — see [`gated_store`]. A parked write does not
//! hold the store: the batch behind it submits and awaits a flush of its
//! own, which is what the first case below pins.
//!
//! The commit is split at its point of no return. Everything before the
//! durable write is staged in memory, so dropping it changes nothing;
//! the write itself runs on a task of its own, so dropping the caller
//! cannot abort it mid-batch. That split is what makes the middle case
//! below *ambiguous but never torn*: the interrupt returns promptly and
//! the write still lands or fails whole.

pub mod gated_store;

use std::{sync::Arc, time::Duration};

use moraine::{Catalog, CatalogOptions, SnapshotId};
use object_store::memory::InMemory;

use crate::interrupt::gated_store::GatedStore;

/// How long a test waits for a shielded write to finish once the gate is
/// open. Generous: it bounds a hang, it does not time anything.
const SETTLE: Duration = Duration::from_secs(10);

/// A catalog over a gated store, seeded with one schema so head is past
/// bootstrap and the commit under test is not the store's first.
///
/// Seeded through a handle of its own and closed before the handle under
/// test opens, so the handle returned here starts with an empty projection
/// cache and stages its first commit against what it reads rather than
/// against what it wrote.
#[allow(clippy::unwrap_used)]
async fn gated() -> (Catalog, Arc<GatedStore>) {
    let store = GatedStore::open(Arc::new(InMemory::new()));
    let seeder = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    seeder
        .commit(|tx| tx.create_schema("seed").map(|_| ()))
        .await
        .unwrap();
    seeder.close().await.unwrap();

    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    (catalog, store)
}

/// The head this handle can see right now.
#[allow(clippy::unwrap_used)]
async fn head(catalog: &Catalog) -> u64 {
    catalog
        .snapshot()
        .await
        .unwrap()
        .current_snapshot()
        .id
        .get()
}

/// Waits for head to reach `target`, failing rather than hanging if the
/// shielded write never lands.
#[allow(clippy::unwrap_used)]
async fn await_head(catalog: &Catalog, target: u64) {
    let reached = tokio::time::timeout(SETTLE, async {
        loop {
            if head(catalog).await == target {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "the shielded write never landed: head never reached {target}"
    );
}

/// A commit issued while another batch's flush is parked does not wait
/// behind it: its batch submits and, once its caller is dropped, still
/// lands whole when the gate opens, after the parked one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::unwrap_used)]
async fn a_commit_behind_a_parked_flush_submits_and_lands_after_it() {
    let (catalog, store) = gated().await;
    let head_before = head(&catalog).await;

    store.gate_wal_writes();
    // One batch in flight, parked at the gate. Its own caller is dropped;
    // the write survives that (the case below is about exactly this).
    {
        let holder = catalog.commit(|tx| tx.create_schema("holder").map(|_| ()));
        tokio::pin!(holder);
        tokio::select! {
            () = store.arrival() => {}
            _ = &mut holder => panic!("the holding commit finished before its write parked"),
        }
    }

    // The commit behind it submits at once and waits only on a flush; its
    // caller is dropped while that flush is still owed.
    {
        let behind = catalog.commit(|tx| tx.create_schema("behind").map(|_| ()));
        tokio::pin!(behind);
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
            _ = &mut behind => panic!("a commit landed while every flush was parked"),
        }
        // `behind` is dropped here — the interrupt.
    }

    store.open_gate();
    await_head(&catalog, head_before + 2).await;

    let view = catalog.snapshot().await.unwrap();
    assert!(
        view.schema_by_name("holder").is_some(),
        "the batch in flight was supposed to land"
    );
    assert!(
        view.schema_by_name("behind").is_some(),
        "the batch submitted behind it was supposed to land"
    );
    let first = catalog
        .snapshot_at(SnapshotId::new(head_before + 1))
        .await
        .unwrap();
    assert!(
        first.schema_by_name("holder").is_some() && first.schema_by_name("behind").is_none(),
        "the parked batch must land first and whole"
    );
}

/// Interrupted *during* the durable write, a commit returns promptly and
/// the write still lands — whole.
///
/// This is the ambiguous case the split buys: the caller is told nothing
/// about which side of the write it landed on, but the write is never
/// abandoned mid-batch, so head ends at exactly `N` or exactly `N + 1`
/// and the catalog is coherent either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::unwrap_used)]
async fn an_interrupt_during_the_write_returns_at_once_and_the_write_still_lands() {
    let (catalog, store) = gated().await;
    let head_before = head(&catalog).await;
    let wal_before = store.wal_writes();

    store.gate_wal_writes();
    let interrupted = {
        let commit = catalog.commit(|tx| tx.create_schema("shielded").map(|_| ()));
        tokio::pin!(commit);
        tokio::select! {
            () = store.arrival() => true,
            _ = &mut commit => false,
        }
        // `commit` is dropped with its batch already in the air.
    };
    assert!(
        interrupted,
        "the commit finished before its write reached the gate"
    );
    assert_eq!(
        store.wal_writes(),
        wal_before,
        "the interrupt was supposed to return while the write was still parked"
    );

    store.open_gate();
    await_head(&catalog, head_before + 1).await;

    let view = catalog.snapshot().await.unwrap();
    assert!(
        view.schema_by_name("shielded").is_some(),
        "the shielded write landed without its schema"
    );
    let minted = catalog
        .snapshot_at(SnapshotId::new(head_before + 1))
        .await
        .unwrap();
    assert!(
        minted.schema_by_name("shielded").is_some(),
        "the minted snapshot is torn: it advanced head without its own writes"
    );
}

/// The same shield on the staged-row path, which lands its batch without
/// the group coalescer the verb path rides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::unwrap_used)]
async fn a_staged_commit_interrupted_during_its_write_still_lands() {
    use moraine::ffi_support::staged::{Cell, RowOperation, TableKind, staged_begin};

    let (catalog, store) = gated().await;
    let head_before = head(&catalog).await;
    let next = head_before + 1;
    let view = catalog.snapshot().await.unwrap();
    let next_catalog_id = view.schemas().last().map_or(1, |s| s.id.get() + 1);

    let mut tx = staged_begin(&catalog, None, String::new()).await.unwrap();
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: vec![
            Cell::U64(next),
            Cell::I64(1),
            Cell::U64(0),
            Cell::U64(next_catalog_id + 1),
            Cell::U64(0),
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: vec![
            Cell::U64(next),
            Cell::Str("created_schema:staged".to_string()),
            Cell::Null,
            Cell::Null,
            Cell::Null,
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Schema,
        cells: vec![
            Cell::U64(next_catalog_id),
            Cell::Str("uuid-staged".to_string()),
            Cell::U64(next),
            Cell::Null,
            Cell::Str("staged".to_string()),
            Cell::Str("staged/".to_string()),
            Cell::Bool(true),
        ],
    });

    store.gate_wal_writes();
    let interrupted = {
        let commit = tx.commit();
        tokio::pin!(commit);
        tokio::select! {
            () = store.arrival() => true,
            _ = &mut commit => false,
        }
    };
    assert!(
        interrupted,
        "the staged commit finished before its write reached the gate"
    );

    store.open_gate();
    await_head(&catalog, next).await;
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("staged")
            .is_some(),
        "the staged path's shielded write landed without its schema"
    );
}

/// An interrupted read leaves nothing behind and nothing broken.
///
/// A read holds only a read point, so cancelling it is the trivial case —
/// but "trivial" is the claim under test: the handle must still serve the
/// next read and the next commit, and the store must be untouched. The
/// read is caught mid-flight the same way as above, parked behind the
/// batch holding the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::unwrap_used)]
async fn an_interrupted_read_releases_its_snapshot_and_writes_nothing() {
    let (catalog, store) = gated().await;
    let head_before = head(&catalog).await;
    let wal_before = store.wal_writes();

    store.gate_wal_writes();
    {
        let holder = catalog.commit(|tx| tx.create_schema("holder").map(|_| ()));
        tokio::pin!(holder);
        tokio::select! {
            () = store.arrival() => {}
            _ = &mut holder => panic!("the holding commit finished before its write parked"),
        }
    }

    // Dropped without ever being awaited to completion: reads take no
    // part in the batch holding the store, so this one is cancelled
    // mid-materialization or not at all — either way it must leave no
    // trace.
    {
        let read = catalog.snapshot_at(SnapshotId::new(head_before));
        tokio::pin!(read);
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
            result = &mut read => {
                result.unwrap();
            }
        }
    }

    assert_eq!(
        store.wal_writes(),
        wal_before,
        "a cancelled read must write nothing"
    );

    store.open_gate();
    await_head(&catalog, head_before + 1).await;

    catalog
        .commit(|tx| tx.create_schema("after").map(|_| ()))
        .await
        .unwrap();
    let view = catalog.snapshot().await.unwrap();
    assert!(
        view.schema_by_name("after").is_some(),
        "the handle was left unusable by a cancelled read"
    );
    assert_eq!(
        view.current_snapshot().id.get(),
        head_before + 2,
        "a cancelled read cost the catalog a snapshot"
    );
}
