//! What a host interrupt leaves behind.
//!
//! The bridge races an interrupt against the operation and drops the
//! loser, so an interrupt *is* a dropped future here: every case below
//! drops the caller's future at a chosen point and asserts what the
//! catalog is left holding. The point is a parked durable write rather
//! than a hopeful sleep — see [`gated_store`], and note that a parked
//! write also holds the leader, which is what puts a *second* commit
//! squarely before a write of its own.
//!
//! A commit's durable write is the leader's inline conditional put into
//! the commit-slot log; there is no task of its own between the caller
//! and that put. A caller dropped during the put returns promptly and
//! never wedges the handle, and the put is atomic — it lands the whole
//! commit or none of it, never a torn slot. What it does *not* buy is a
//! deterministic landing: when the drop coincides with the put, whether
//! the commit lands is ambiguous. The guarantee under interrupt is the
//! shape of the outcome — never torn, never wedged — not which of the
//! two coherent outcomes it is.

pub mod gated_store;

use std::{sync::Arc, time::Duration};

use moraine::{Catalog, CatalogOptions, SnapshotId};
use object_store::memory::InMemory;

use crate::interrupt::gated_store::GatedStore;

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

/// Interrupted before its slot put, a commit contributes nothing.
///
/// The window is staged with a real leader rather than a timer: one batch
/// drives the slot at a time, so a leader parked at the gate keeps the
/// commit under test waiting its turn behind it — it has read nothing,
/// staged nothing, and issued no slot put. Dropping it there must leave
/// the catalog holding exactly what the batch in flight put there, and
/// the handle must still commit afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::unwrap_used)]
async fn an_interrupt_before_the_write_leaves_the_catalog_untouched() {
    let (catalog, store) = gated().await;
    let head_before = head(&catalog).await;

    store.gate_slot_writes();
    // One batch leading, parked mid slot put; kept alive so its put lands
    // once the gate opens. It holds the slot while a second commit queues
    // behind it, which is what puts that second commit before a put of its
    // own.
    let holder = catalog.commit(|tx| tx.create_schema("holder").map(|_| ()));
    tokio::pin!(holder);
    tokio::select! {
        () = store.arrival() => {}
        _ = &mut holder => panic!("the holding commit finished before its write parked"),
    }

    // The commit under test cannot reach the slot while the leader holds it,
    // so it can only be here — queued behind the leader, before its put.
    {
        let cancelled = catalog.commit(|tx| tx.create_schema("cancelled").map(|_| ()));
        tokio::pin!(cancelled);
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
            _ = &mut cancelled => panic!("a second batch landed while one held the slot"),
        }
        // `cancelled` is dropped here — the interrupt.
    }

    store.open_gate();
    holder.await.unwrap();
    // The cancelled commit could only ever land after the holder's, so a
    // settle past that is what makes its absence meaningful.
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        head(&catalog).await,
        head_before + 1,
        "a commit cancelled before its put must not advance head past the batch in flight"
    );
    let view = catalog.snapshot().await.unwrap();
    assert!(
        view.schema_by_name("holder").is_some(),
        "the batch in flight was supposed to land"
    );
    assert!(
        view.schema_by_name("cancelled").is_none(),
        "a cancelled commit left a partial record behind"
    );
    assert!(
        catalog
            .snapshot_at(SnapshotId::new(head_before + 2))
            .await
            .is_err(),
        "a cancelled commit minted a snapshot"
    );

    // No wedge: the handle commits again and that commit lands whole.
    catalog
        .commit(|tx| tx.create_schema("after").map(|_| ()))
        .await
        .unwrap();
    let after = catalog.snapshot().await.unwrap();
    assert!(
        after.schema_by_name("after").is_some(),
        "the handle was left unusable by a cancelled commit"
    );
    assert_eq!(
        after.current_snapshot().id.get(),
        head_before + 2,
        "the next commit advances the coherent head by exactly one"
    );
}

/// Interrupted *during* its slot put, a commit returns promptly and never
/// tears — its landing is ambiguous, its shape is not.
///
/// Cancelling the leader on the one await where the put's fate is unknown
/// treats the batch as abandoned. The put is atomic, so the outcome is one
/// of exactly two coherent states — the commit landed whole at `N + 1`, or
/// it did not land and head is still `N` — never a slot torn between them,
/// and never a wedged handle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::unwrap_used)]
async fn an_interrupt_during_the_write_returns_at_once_and_never_tears() {
    let (catalog, store) = gated().await;
    let head_before = head(&catalog).await;
    let slot_before = store.slot_writes();

    store.gate_slot_writes();
    let interrupted = {
        let commit = catalog.commit(|tx| tx.create_schema("shielded").map(|_| ()));
        tokio::pin!(commit);
        tokio::select! {
            () = store.arrival() => true,
            _ = &mut commit => false,
        }
        // `commit` is dropped with its slot put already parked at the gate.
    };
    assert!(
        interrupted,
        "the commit finished before its write reached the gate"
    );
    assert_eq!(
        store.slot_writes(),
        slot_before,
        "the interrupt was supposed to return while the put was still parked"
    );

    // Let the abandoned put settle out either way.
    store.open_gate();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let view = catalog.snapshot().await.unwrap();
    let head_now = view.current_snapshot().id.get();
    if view.schema_by_name("shielded").is_some() {
        assert_eq!(
            head_now,
            head_before + 1,
            "a landed shielded commit advances head by exactly its own commit"
        );
        let minted = catalog
            .snapshot_at(SnapshotId::new(head_before + 1))
            .await
            .unwrap();
        assert!(
            minted.schema_by_name("shielded").is_some(),
            "a landed commit's own snapshot carries its own writes"
        );
    } else {
        assert_eq!(
            head_now, head_before,
            "an abandoned commit left head where it was, never advanced without its writes"
        );
    }

    // No wedge, either way: the handle commits again and lands whole one
    // step past whichever coherent head the interrupt left.
    catalog
        .commit(|tx| tx.create_schema("after").map(|_| ()))
        .await
        .unwrap();
    let after = catalog.snapshot().await.unwrap();
    assert!(after.schema_by_name("after").is_some());
    assert_eq!(
        after.current_snapshot().id.get(),
        head_now + 1,
        "the next commit advances the coherent head by exactly one"
    );
}

/// The same never-torn shape on the staged-row path, whose slot put is the
/// leader's just as the verb path's is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::unwrap_used)]
async fn a_staged_commit_interrupted_during_its_write_never_tears() {
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

    store.gate_slot_writes();
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

    // Let the abandoned put settle out either way.
    store.open_gate();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let view = catalog.snapshot().await.unwrap();
    let head_now = view.current_snapshot().id.get();
    if view.schema_by_name("staged").is_some() {
        assert_eq!(
            head_now, next,
            "a landed staged commit advances head by exactly its own commit"
        );
    } else {
        assert_eq!(
            head_now, head_before,
            "an abandoned staged commit left head where it was, never advanced without its writes"
        );
    }

    // No wedge: the handle commits again and lands whole one step on.
    catalog
        .commit(|tx| tx.create_schema("after").map(|_| ()))
        .await
        .unwrap();
    let after = catalog.snapshot().await.unwrap();
    assert!(after.schema_by_name("after").is_some());
    assert_eq!(
        after.current_snapshot().id.get(),
        head_now + 1,
        "the next commit advances the coherent head by exactly one"
    );
}

/// An interrupted read leaves nothing behind and nothing broken.
///
/// A read is a pure materialization — it takes no part in the slot log, so
/// it never reaches the put gate; dropping its future is the whole
/// interrupt. "Trivial" is the claim under test: the read writes nothing,
/// and the handle must still serve the next read and the next commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::unwrap_used)]
async fn an_interrupted_read_releases_its_snapshot_and_writes_nothing() {
    let (catalog, store) = gated().await;
    let head_before = head(&catalog).await;
    let slot_before = store.slot_writes();

    // Dropped without ever being awaited to completion: a read never puts a
    // slot, so it is cancelled mid-materialization or not at all — either
    // way it must leave no trace.
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
        store.slot_writes(),
        slot_before,
        "a cancelled read must write nothing to the log"
    );

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
        head_before + 1,
        "a cancelled read cost the catalog a snapshot"
    );
}
