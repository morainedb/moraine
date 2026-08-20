//! `Catalog::create_index_staged`: driving a multi-commit index build to
//! `ready` over a table whose rows live in registered Parquet files.

// Test bodies await whole catalog operations in sequence; boxing each
// call would say nothing about the code under test.
#![allow(clippy::large_futures)]

use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use moraine::{
    BuildStep, Catalog, ColumnId, DataStore, Error, IndexDef, IndexKeyValue, IndexState, IntWidth,
    TableId,
};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use parquet::arrow::ArrowWriter;
use tracing::{Event, Subscriber, field::Visit};
use tracing_subscriber::{layer::Context, prelude::*};

use crate::fixtures::{col, datafile, open_memory};

#[derive(Clone, Default)]
struct CapturedEvents(Arc<std::sync::Mutex<Vec<BTreeMap<String, String>>>>);

impl<S> tracing_subscriber::Layer<S> for CapturedEvents
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        #[derive(Default)]
        struct Fields(BTreeMap<String, String>);

        impl Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.insert(field.name().to_owned(), format!("{value:?}"));
            }

            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                self.0.insert(field.name().to_owned(), value.to_string());
            }

            fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                self.0.insert(field.name().to_owned(), value.to_string());
            }

            fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
                self.0.insert(field.name().to_owned(), value.to_string());
            }
        }

        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.0.get("message").is_some_and(|message| {
            message.contains("staged index") || message == "index lookup resolved"
        }) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(fields.0);
        }
    }
}

impl CapturedEvents {
    fn named_for(&self, message: &str, index_name: &str) -> Vec<BTreeMap<String, String>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|fields| {
                fields.get("message").is_some_and(|value| value == message)
                    && fields
                        .get("index_name")
                        .is_some_and(|value| value == index_name)
            })
            .cloned()
            .collect()
    }

    fn lookup_with_requested_keys(&self, requested: u64) -> BTreeMap<String, String> {
        let matching = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|fields| {
                fields
                    .get("message")
                    .is_some_and(|value| value == "index lookup resolved")
                    && fields
                        .get("lookup_keys")
                        .is_some_and(|value| value == &requested.to_string())
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1, "lookup events: {matching:?}");
        matching[0].clone()
    }
}

fn captured_events() -> &'static CapturedEvents {
    static EVENTS: OnceLock<CapturedEvents> = OnceLock::new();
    EVENTS.get_or_init(|| {
        let events = CapturedEvents::default();
        let subscriber = tracing_subscriber::registry().with(events.clone());
        assert!(
            tracing::subscriber::set_global_default(subscriber).is_ok(),
            "the integration test process installs only this tracing subscriber"
        );
        events
    })
}

/// A table holding one registered file of `values`, plus the data store
/// that file lives in.
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn table_with_file(values: Vec<i64>) -> (Catalog, TableId, DataStore) {
    let catalog = open_memory().await;
    let rows = u64::try_from(values.len()).expect("row count fits");
    let data = Arc::new(InMemory::new());
    let arrow_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![Arc::new(Int64Array::from(values.clone()))],
    )
    .unwrap();
    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    let footer_offset = buffer.len() - 8;
    let footer_size = u64::from(u32::from_le_bytes(
        buffer[footer_offset..footer_offset + 4].try_into().unwrap(),
    ));
    let file_size_bytes = u64::try_from(buffer.len()).unwrap();
    data.put(
        &Path::from(format!("main/orders/data-{rows}.parquet")),
        buffer.into(),
    )
    .await
    .unwrap();

    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
            let table = tx.create_table(schema, "orders", &[col("a")])?;
            tx.register_data_file(
                table,
                moraine::DataFile {
                    file_size_bytes,
                    footer_size,
                    ..datafile(rows)
                },
                &[],
            )?;
            created.set(Some(table));
            Ok(())
        })
        .await
        .unwrap();

    (catalog, created.get().unwrap(), DataStore::new(data))
}

/// A large explicit lookup uses one continuously refilled 512-probe window,
/// deduplicates keys before reads, and exposes the cache/store accounting
/// needed to distinguish latency from read amplification.
#[tokio::test]
async fn explicit_lookup_reports_its_bounded_probe_window() {
    let events = captured_events();
    let catalog = open_memory().await;
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            let table = tx.create_table(schema, "lookup_telemetry", &[col("a")])?;
            let index = tx.create_index(
                table,
                &IndexDef {
                    name: "lookup_telemetry_index".to_owned(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            created.set(Some((table, index)));
            Ok(())
        })
        .await
        .unwrap();
    let (table, index) = created.get().expect("ids");
    let mut keys = (0..600)
        .map(|value| vec![int(i128::from(value))])
        .collect::<Vec<_>>();
    keys.push(vec![int(10)]);

    assert!(
        catalog
            .index_lookup_many(table, index, &keys)
            .await
            .unwrap()
            .is_empty()
    );

    let event = events.lookup_with_requested_keys(601);
    assert_eq!(
        event.get("lookup_unique_keys").map(String::as_str),
        Some("600")
    );
    assert_eq!(
        event.get("lookup_peak_in_flight").map(String::as_str),
        Some("512")
    );
    assert_eq!(event.get("lookup_hits").map(String::as_str), Some("0"));
    assert_eq!(event.get("lookup_misses").map(String::as_str), Some("600"));
    for field in [
        "lookup_ms",
        "lookup_head_ms",
        "lookup_probe_window_ms",
        "lookup_probe_service_ms",
        "lookup_metadata_hits",
        "lookup_metadata_misses",
        "lookup_block_hits",
        "lookup_block_misses",
        "lookup_gets",
        "lookup_get_ms",
    ] {
        assert!(
            event
                .get(field)
                .is_some_and(|value| value.parse::<u64>().is_ok()),
            "missing integer {field}: {event:?}"
        );
    }
}

fn def(unique: bool) -> IndexDef {
    IndexDef {
        name: "by_a".into(),
        columns: vec![ColumnId::new(1)],
        unique,
    }
}

/// A step bounded only by its entry count, so a test that means to pin
/// step boundaries is not also measuring entry width.
fn by_entries(entries: usize) -> BuildStep {
    BuildStep {
        entries,
        bytes: u64::MAX,
    }
}

/// The head snapshot id — one per commit, so its movement counts the
/// commits a build spent.
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

/// The current global schema version.
#[allow(clippy::unwrap_used)]
async fn schema_version(catalog: &Catalog) -> u64 {
    catalog
        .snapshot()
        .await
        .unwrap()
        .current_snapshot()
        .schema_version
}

/// The durable schema-history rows for one table.
#[allow(clippy::unwrap_used)]
async fn schema_history_len(catalog: &Catalog, table: TableId) -> usize {
    moraine::ffi_support::dump_schema_versions(catalog)
        .await
        .unwrap()
        .into_iter()
        .filter(|row| row.table_id == table.get())
        .count()
}

fn int(value: i128) -> IndexKeyValue {
    IndexKeyValue::Int {
        value,
        width: IntWidth::I64,
    }
}

/// The driver builds across several bounded steps and flips the index
/// ready; every backfilled row is then resolvable.
#[tokio::test]
async fn staged_build_spans_steps_and_finishes_ready() {
    let (catalog, table, data) = table_with_file((0..7).collect()).await;

    let index = catalog
        .create_index_staged(table, &def(true), &[], Some(data), "", Some(by_entries(2)))
        .await
        .unwrap();

    let info = catalog
        .snapshot()
        .await
        .unwrap()
        .indexes_of(table)
        .remove(0);
    assert_eq!(info.state, IndexState::Ready, "the build flipped ready");
    assert_eq!(info.id, index);

    for value in 0..7 {
        let hits = catalog
            .index_lookup(table, index, &[int(i128::from(value))])
            .await
            .unwrap();
        assert_eq!(
            hits,
            vec![u64::try_from(value).unwrap()],
            "value {value} is indexed"
        );
    }
    catalog.close().await.unwrap();
}

/// Intermediate cursor advances mint snapshots but not schema versions;
/// only publishing the definition and flipping it ready change schema.
#[tokio::test]
async fn intermediate_steps_do_not_grow_table_schema_history() {
    let (catalog, table, data) = table_with_file((0..7).collect()).await;
    let snapshot_before = head(&catalog).await;
    let schema_version_before = schema_version(&catalog).await;
    let schema_history_before = schema_history_len(&catalog, table).await;

    catalog
        .create_index_staged(table, &def(true), &[], Some(data), "", Some(by_entries(2)))
        .await
        .unwrap();

    assert_eq!(head(&catalog).await - snapshot_before, 5);
    assert_eq!(schema_version(&catalog).await - schema_version_before, 2);
    assert_eq!(
        schema_history_len(&catalog, table).await - schema_history_before,
        2
    );
    let changes: Vec<_> = moraine::ffi_support::dump_snapshots(&catalog)
        .await
        .unwrap()
        .into_iter()
        .filter(|snapshot| snapshot.snapshot_id > snapshot_before)
        .map(|snapshot| snapshot.changes_made)
        .collect();
    let altered = format!("altered_table:{}", table.get());
    let inserted = format!("inserted_into_table:{}", table.get());
    assert_eq!(
        changes,
        vec![
            altered.clone(),
            inserted.clone(),
            inserted.clone(),
            inserted,
            altered,
        ],
        "only intermediate cursor advances loosen to append classification"
    );
    catalog.close().await.unwrap();
}

/// The driver names its long derivation phase, records the derived
/// denominator, and reports each durable step against that denominator.
#[tokio::test]
async fn staged_build_reports_explicit_progress() {
    const INDEX_NAME: &str = "telemetry_by_a";
    let events = captured_events();
    let (catalog, table, data) = table_with_file((0..7).collect()).await;
    let telemetry_def = IndexDef {
        name: INDEX_NAME.to_owned(),
        ..def(true)
    };

    let index = catalog
        .create_index_staged(
            table,
            &telemetry_def,
            &[],
            Some(data),
            "",
            Some(by_entries(2)),
        )
        .await
        .unwrap();
    catalog.close().await.unwrap();

    let started = events.named_for("staged index backfill derivation started", INDEX_NAME);
    assert_eq!(started.len(), 1);
    assert_eq!(started[0].get("index"), Some(&index.get().to_string()));
    assert_eq!(started[0].get("derivation_attempt"), Some(&"1".to_owned()));

    let derived = events.named_for("staged index backfill derived", INDEX_NAME);
    assert_eq!(derived.len(), 1);
    assert_eq!(derived[0].get("total_entries"), Some(&"7".to_owned()));
    assert_eq!(
        derived[0].get("peak_buffered_entries"),
        Some(&"2".to_owned()),
        "derivation retains no more entries than one build step"
    );
    assert!(derived[0].contains_key("derive_ms"));
    assert!(derived[0].contains_key("sort_ms"));

    let progress = events.named_for("staged index build step committed", INDEX_NAME);
    assert_eq!(progress.len(), 4);
    assert_eq!(
        progress
            .iter()
            .map(|event| event["completed_entries"].as_str())
            .collect::<Vec<_>>(),
        ["2", "4", "6", "7"]
    );
    assert!(progress.iter().all(|event| event["total_entries"] == "7"));
    assert_eq!(progress.last().unwrap()["progress_percent"], "100");
    assert_eq!(progress.last().unwrap()["is_final"], "true");
    assert!(progress.iter().all(|event| event.contains_key("commit_ms")));
}

/// A build interrupted partway resumes from its persisted cursor: the
/// re-derived entries below the watermark land as idempotent puts, and the
/// finished index covers every row exactly once.
#[tokio::test]
async fn interrupted_staged_build_resumes_from_its_cursor() {
    let (catalog, table, data) = table_with_file((0..7).collect()).await;

    // Begin the definition and advance one step by hand, leaving it
    // building — the state a cancelled or crashed driver leaves behind.
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index_staged(table, &def(true))?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();
    catalog
        .commit(|tx| {
            tx.build_index_step(
                index,
                &[
                    moraine::IndexEntry {
                        row_id: 0,
                        values: vec![Some(int(0))],
                    },
                    moraine::IndexEntry {
                        row_id: 1,
                        values: vec![Some(int(1))],
                    },
                ],
                false,
            )
            .map(|_| ())
        })
        .await
        .unwrap();
    let building = catalog
        .snapshot()
        .await
        .unwrap()
        .indexes_of(table)
        .remove(0);
    assert_eq!(building.state, IndexState::Building);
    assert_eq!(building.build_cursor, Some(1), "the cursor persisted");

    // Re-issuing the create resumes rather than starting over or colliding
    // on the name.
    let resumed = catalog
        .create_index_staged(table, &def(true), &[], Some(data), "", Some(by_entries(2)))
        .await
        .unwrap();
    assert_eq!(resumed, index, "resumed the same definition");

    let info = catalog
        .snapshot()
        .await
        .unwrap()
        .indexes_of(table)
        .remove(0);
    assert_eq!(info.state, IndexState::Ready);
    for value in 0..7 {
        assert_eq!(
            catalog
                .index_lookup(table, index, &[int(i128::from(value))])
                .await
                .unwrap()
                .len(),
            1,
            "value {value} is indexed exactly once"
        );
    }
    catalog.close().await.unwrap();
}

/// A duplicate among the backfilled rows fails the build: the caller gets
/// `Constraint` and the definition is gone, exactly as a single-commit
/// create over the same rows would leave things.
#[tokio::test]
async fn duplicate_in_backfill_fails_the_build_and_drops_the_definition() {
    // The duplicate (4) sits past the first step, so it is only discovered
    // once the build is already several commits in.
    let (catalog, table, data) = table_with_file(vec![0, 1, 2, 3, 4, 4, 6]).await;

    let err = catalog
        .create_index_staged(table, &def(true), &[], Some(data), "", Some(by_entries(2)))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .indexes_of(table)
            .is_empty(),
        "the failed build left no definition behind"
    );
    catalog.close().await.unwrap();
}

/// A non-unique staged build admits the duplicates a unique one refuses.
#[tokio::test]
async fn non_unique_staged_build_admits_duplicates() {
    let (catalog, table, data) = table_with_file(vec![0, 1, 2, 3, 4, 4, 6]).await;

    let index = catalog
        .create_index_staged(table, &def(false), &[], Some(data), "", Some(by_entries(2)))
        .await
        .unwrap();

    let hits = catalog.index_lookup(table, index, &[int(4)]).await.unwrap();
    assert_eq!(hits.len(), 2, "both rows holding 4 are indexed");
    catalog.close().await.unwrap();
}

/// Resuming adopts the definition already on disk, so a create describing a
/// different index is refused rather than silently handed the stored one —
/// its entries are already encoded under the stored orders.
#[tokio::test]
async fn resuming_with_a_different_definition_is_refused() {
    use moraine::{ColumnOrder, Direction, NullOrder};
    let (catalog, table, data) = table_with_file((0..7).collect()).await;
    catalog
        .commit(|tx| tx.create_index_staged(table, &def(true)).map(|_| ()))
        .await
        .unwrap();

    // Same name and columns, opposite uniqueness.
    let unique_differs = catalog
        .create_index_staged(
            table,
            &def(false),
            &[],
            Some(data.clone()),
            "",
            Some(by_entries(2)),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(unique_differs, Error::Constraint(_)),
        "{unique_differs}"
    );

    // Same name, columns and uniqueness, different declared order.
    let orders_differ = catalog
        .create_index_staged(
            table,
            &def(true),
            &[ColumnOrder {
                direction: Direction::Descending,
                nulls: NullOrder::First,
            }],
            Some(data),
            "",
            Some(by_entries(2)),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(orders_differ, Error::Constraint(_)),
        "{orders_differ}"
    );
    catalog.close().await.unwrap();
}

/// A staged create naming an index that is already ready is refused, like
/// any duplicate name.
#[tokio::test]
async fn staged_create_over_a_ready_index_is_refused() {
    let (catalog, table, data) = table_with_file((0..3).collect()).await;
    catalog
        .create_index_staged(
            table,
            &def(true),
            &[],
            Some(data.clone()),
            "",
            Some(by_entries(2)),
        )
        .await
        .unwrap();

    let err = catalog
        .create_index_staged(table, &def(true), &[], Some(data), "", Some(by_entries(2)))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)), "{err}");
    catalog.close().await.unwrap();
}

/// A step ends at its byte bound as well as its entry bound. Under a bound
/// no single entry fits, every step carries exactly one — the build still
/// advances rather than stalling on a step it can never fill.
#[tokio::test]
async fn steps_end_at_the_byte_bound() {
    let (catalog, table, data) = table_with_file((0..7).collect()).await;
    let before = head(&catalog).await;

    catalog
        .create_index_staged(
            table,
            &def(true),
            &[],
            Some(data),
            "",
            Some(BuildStep {
                entries: 1_000,
                bytes: 1,
            }),
        )
        .await
        .unwrap();

    // The definition commit, then one per entry.
    assert_eq!(head(&catalog).await - before, 1 + 7);
    catalog.close().await.unwrap();
}

/// The entry bound still binds when the byte bound is out of reach: seven
/// entries two at a time is four steps, not seven.
#[tokio::test]
async fn steps_end_at_the_entry_bound() {
    let (catalog, table, data) = table_with_file((0..7).collect()).await;
    let before = head(&catalog).await;

    catalog
        .create_index_staged(table, &def(true), &[], Some(data), "", Some(by_entries(2)))
        .await
        .unwrap();

    assert_eq!(head(&catalog).await - before, 1 + 4);
    catalog.close().await.unwrap();
}

/// A step admitting no entry at all is refused rather than looping.
#[tokio::test]
async fn a_zero_step_bound_is_refused() {
    let (catalog, table, data) = table_with_file((0..3).collect()).await;

    for bound in [
        BuildStep {
            entries: 0,
            bytes: 1_024,
        },
        BuildStep {
            entries: 8,
            bytes: 0,
        },
    ] {
        let err = catalog
            .create_index_staged(table, &def(true), &[], Some(data.clone()), "", Some(bound))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Constraint(_)), "{err}");
    }
    catalog.close().await.unwrap();
}
