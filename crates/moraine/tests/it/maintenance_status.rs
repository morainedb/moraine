use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, MaintenanceStatusPass, MaintenanceStatusStep, Timestamp};
use object_store::{ObjectStore, memory::InMemory};

fn pass(number: u32) -> MaintenanceStatusPass {
    MaintenanceStatusPass::new(
        Timestamp::from_micros(i64::from(number) * 1_000_000),
        if number.is_multiple_of(2) {
            "scheduled"
        } else {
            "manual"
        },
        vec![MaintenanceStatusStep::new(
            "sweep_indexes",
            "ran",
            format!("pass {number}"),
        )],
    )
}

/// Status is a bounded durable diagnostic, not catalog history: recording it
/// neither mints a snapshot nor disappears with the writer that ran the pass.
#[tokio::test]
async fn maintenance_status_is_bounded_and_survives_a_read_only_reopen() {
    let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let writer = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    let head = writer.snapshot().await.unwrap().current_snapshot().id;

    for number in 0..18 {
        writer.record_maintenance_pass(pass(number)).await.unwrap();
    }

    let status = writer.maintenance_status().await.unwrap();
    assert_eq!(status.len(), 16);
    assert_eq!(status.first().unwrap().steps[0].detail, "pass 17");
    assert_eq!(status.last().unwrap().steps[0].detail, "pass 2");
    assert_eq!(
        writer.snapshot().await.unwrap().current_snapshot().id,
        head,
        "status must not mint a DuckLake snapshot"
    );
    writer.close().await.unwrap();

    let reader = Catalog::open_read_only(object_store, CatalogOptions::default())
        .await
        .unwrap();
    assert_eq!(reader.maintenance_status().await.unwrap(), status);
    assert_eq!(reader.snapshot().await.unwrap().current_snapshot().id, head);
    reader.close().await.unwrap();
}
