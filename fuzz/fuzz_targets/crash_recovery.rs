//! Crash at an arbitrary write offset, reopen, and assert the atomicity
//! guarantee: one commit is one batch, so a reopen sees the state either
//! wholly before that commit or wholly after it, never torn.
//!
//! The suite already drives every *named* crash case. What the fuzzer adds
//! is the offsets nobody named: it picks where the store freezes, so the
//! commit dies at write boundaries a hand-written case would have to
//! enumerate. A commit that fails is not a finding — dying is the point —
//! and the finding is a reopen that shows half of one.
//!
//! Each iteration builds and reopens a real store, so this runs at a tiny
//! fraction of the codec targets' rate — single-digit executions per
//! second against the codecs' hundreds of thousands. That is inherent: the
//! subject is a durable-state transition, not a decoder.
//!
//! Run it with `ASAN_OPTIONS=detect_leaks=0`. A catalog whose store froze
//! mid-commit cannot be closed — that is what being crashed means — so the
//! target abandons it, and the leak checker would report every one of those
//! as a finding on top of the one it is actually looking for.

#![no_main]

use std::sync::Arc;

use libfuzzer_sys::fuzz_target;
use moraine::{Catalog, CatalogOptions, ColumnDef};
use object_store::memory::InMemory;

#[path = "freezing.rs"]
mod freezing;

use freezing::FreezingStore;

fn column(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
        children: Vec::new(),
    }
}

/// How long to let a doomed operation run before calling it a crash. A
/// commit whose WAL flush can never land does not fail — it waits, the
/// durable wait being unbounded by design — so the deadline *is* the crash.
const UNTIL_CRASH: std::time::Duration = std::time::Duration::from_millis(300);

/// Opens against `store` with a fast flush, so a commit's durable wait does
/// not dominate every iteration.
async fn open(store: Arc<dyn object_store::ObjectStore>) -> Result<Catalog, moraine::Error> {
    let mut options = CatalogOptions::default();
    options.flush_interval = std::time::Duration::from_millis(1);
    Catalog::open(store, options).await
}

fuzz_target!(|data: &[u8]| {
    // The first two bytes choose the write offset to die at. Small values
    // dominate on purpose: the interesting boundaries are early, and the
    // fuzzer still reaches the long tail by growing the input.
    let Some((&low, rest)) = data.split_first() else {
        return;
    };
    let high = rest.first().copied().unwrap_or(0);
    let allowance = i64::from(u16::from_le_bytes([low, high]));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("building a current-thread runtime cannot fail");

    runtime.block_on(async move {
        let backing = Arc::new(InMemory::new());

        // The pre-crash state, built on a store that never freezes.
        let seed = Arc::new(FreezingStore::thawed(Arc::clone(&backing)));
        let catalog = open(seed).await.expect("seeding a fresh store must open");
        catalog
            .commit(|tx| tx.create_schema("before").map(|_| ()))
            .await
            .expect("the seed commit runs on a thawed store");
        catalog.close().await.expect("closing a thawed store");

        // The crash: one commit creating a schema and a table inside it,
        // against a store that stops writing partway through. Every
        // outcome here is legal, the process being dead either way.
        let frozen = Arc::new(FreezingStore::freeze_after(Arc::clone(&backing), allowance));
        let opened = tokio::time::timeout(UNTIL_CRASH, open(frozen)).await;
        if let Ok(Ok(catalog)) = opened {
            let _ = tokio::time::timeout(
                UNTIL_CRASH,
                catalog.commit(|tx| {
                    let schema = tx.create_schema("after")?;
                    tx.create_table(schema, "t", &[column("a")])?;
                    Ok(())
                }),
            )
            .await;
            // Both bounded and both discarded: a frozen store's close fails
            // or hangs by construction, and either is a crash.
            let _ = tokio::time::timeout(UNTIL_CRASH, catalog.close()).await;
        }

        // Reopen on a thawed store and read what survived.
        let recovered = Arc::new(FreezingStore::thawed(Arc::clone(&backing)));
        let catalog = open(recovered)
            .await
            .expect("a store crashed mid-commit must still open");
        let view = catalog.snapshot().await.expect("and must still be read");

        // The guarantee. The commit either landed whole or not at all, so
        // `after` and its table stand or fall together — a schema with no
        // table is the torn state atomicity forbids.
        let schema = view.schema_by_name("after");
        let landed = schema.is_some();
        if landed {
            let id = schema.expect("just checked").id;
            assert!(
                view.table_by_name(id, "t").is_some(),
                "torn commit at write offset {allowance}: schema `after` landed without its table"
            );
        }
        // `before` predates the crash and is durable either way.
        assert!(
            view.schema_by_name("before").is_some(),
            "a committed schema vanished after a crash at write offset {allowance}"
        );

        catalog.close().await.expect("closing the recovered store");
    });
});
