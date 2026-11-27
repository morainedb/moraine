use object_store::memory::InMemory;

use super::*;
use crate::{
    CatalogOptions,
    store::{
        key::{CurrentKey, Key, SysKey},
        proto::GcFileValue,
    },
};

#[tokio::test]
async fn scheduled_files_stay_protected_and_plans_preserve_the_snapshot() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog.adopt_data_path("s3://bucket/data/").await.unwrap();
    catalog
        .commit_own_rows_durably(vec![(
            Key::Current(CurrentKey::GcFile { data_file_id: 9 }).encode(),
            Some(value::encode_value(&GcFileValue {
                data_file_id: 9,
                path: "retired.parquet".into(),
                path_is_relative: true,
                schedule_start_micros: 0,
            })),
        )])
        .await
        .unwrap();
    let session = catalog.begin_read().await.unwrap();
    let before = commit::read_head_value(session.handle()).await.unwrap();
    session.finish();
    assert_eq!(
        catalog
            .plan_orphan_deletion(&[
                "s3://bucket/data/retired.parquet".into(),
                "s3://bucket/data/orphan.parquet".into()
            ])
            .await
            .unwrap(),
        vec!["s3://bucket/data/orphan.parquet"]
    );
    let session = catalog.begin_read().await.unwrap();
    let after = commit::read_head_value(session.handle()).await.unwrap();
    assert_eq!(before.snapshot_id, after.snapshot_id);
    assert_eq!(before.batch_seq + 1, after.batch_seq);
    session.finish();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn older_formats_cannot_authorize_deletion() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let session = catalog.begin_read().await.unwrap();
    let mut format = read::read_format(session.handle()).await.unwrap().unwrap();
    session.finish();
    format.format_version = 12;
    catalog
        .commit_own_rows_durably(vec![(
            Key::Sys(SysKey::Format).encode(),
            Some(value::encode_value(&format)),
        )])
        .await
        .unwrap();
    assert!(matches!(
        catalog
            .plan_orphan_deletion(&["s3://bucket/orphan.parquet".into()])
            .await,
        Err(Error::Migration(_))
    ));
    let session = catalog.begin_read().await.unwrap();
    assert!(
        session
            .handle()
            .get(paths::key(""))
            .await
            .unwrap()
            .is_none()
    );
    session.finish();
    catalog.close().await.unwrap();
}
