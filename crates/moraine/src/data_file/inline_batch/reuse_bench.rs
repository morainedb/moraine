//! Isolates inline body copying, decoding, and original-array reuse.

use std::{hint::black_box, sync::Arc, time::Instant};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions},
};

use super::*;
use crate::store::index_encoding::{Direction, NullOrder};

fn batch(columns: usize) -> RecordBatch {
    let mut arrays: Vec<_> = (0..columns - 1)
        .map(|column| {
            let values = Int64Array::from(
                (0..2048)
                    .map(|row| {
                        (row % 19 != 0).then_some(i64::try_from(row * columns + column).unwrap())
                    })
                    .collect::<Vec<_>>(),
            );
            (format!("column{column}"), Arc::new(values) as ArrayRef)
        })
        .collect();
    let value = "x".repeat(1024);
    arrays.push((
        "payload".into(),
        Arc::new(StringArray::from(vec![value.as_str(); 2048])) as ArrayRef,
    ));
    RecordBatch::try_from_iter(arrays).unwrap()
}

fn body(batch: &RecordBatch) -> Bytes {
    let mut tracker = DictionaryTracker::new(false);
    let mut context = IpcWriteContext::default();
    let (dictionaries, encoded) = IpcDataGenerator::default()
        .encode(
            batch,
            &mut tracker,
            &IpcWriteOptions::default(),
            &mut context,
        )
        .unwrap();
    assert!(dictionaries.is_empty());
    let mut body = u32::try_from(encoded.ipc_message.len())
        .unwrap()
        .to_le_bytes()
        .to_vec();
    body.extend_from_slice(&encoded.ipc_message);
    body.extend_from_slice(&encoded.arrow_data);
    body.into()
}

fn plans() -> Vec<IndexProjection> {
    [vec![0], vec![1, 2]]
        .into_iter()
        .enumerate()
        .map(|(index, positions)| IndexProjection {
            index_id: u64::try_from(index + 1).unwrap(),
            unique: false,
            directions: vec![Direction::Ascending; positions.len()],
            nulls: vec![NullOrder::Last; positions.len()],
            positions,
        })
        .collect()
}

fn derive(
    batch: &RecordBatch,
    body: &Bytes,
    plans: &[IndexProjection],
    mode: &str,
) -> Vec<ScopedIndexEntry> {
    let input = match mode {
        "copy_decode" => {
            decode_inline_batch(batch.schema(), &Bytes::copy_from_slice(body)).unwrap()
        }
        "shared_decode" => decode_inline_batch(batch.schema(), &body.clone()).unwrap(),
        "retained" => batch.clone(),
        _ => unreachable!(),
    };
    record_batch_index_entries(&input, plans, None, 0, Ordinals::Dense, 0, None, None).unwrap()
}

#[test]
fn retained_arrays_and_ipc_derive_identical_keys() {
    let batch = batch(8);
    let body = body(&batch);
    let plans = plans();
    let keys = |entries: Vec<ScopedIndexEntry>| {
        entries
            .into_iter()
            .map(|entry| {
                (
                    entry.index,
                    entry.ordinal,
                    entry.row_id,
                    entry.key,
                    entry.unique,
                )
            })
            .collect::<Vec<_>>()
    };
    let expected = keys(derive(&batch, &body, &plans, "copy_decode"));
    assert_eq!(
        keys(derive(&batch, &body, &plans, "shared_decode")),
        expected
    );
    assert_eq!(keys(derive(&batch, &body, &plans, "retained")), expected);
}

#[test]
#[ignore = "manual inline-array reuse benchmark"]
fn inline_array_reuse_benchmark() {
    println!("columns,body_bytes,retained_array_bytes,mode,sample,microseconds_per_batch");
    for columns in [8, 64] {
        let batch = batch(columns);
        let body = body(&batch);
        let plans = plans();
        let modes = ["copy_decode", "shared_decode", "retained"];
        for sample in 0..5 {
            for index in 0..modes.len() {
                let mode = modes[(index + sample) % modes.len()];
                let start = Instant::now();
                for _ in 0..200 {
                    black_box(derive(&batch, &body, &plans, mode));
                }
                let per_batch = start.elapsed().as_secs_f64() * 1_000_000.0 / 200.0;
                println!(
                    "{columns},{},{},{mode},{sample},{per_batch:.3}",
                    body.len(),
                    batch.get_array_memory_size()
                );
            }
        }
    }
}
