use std::sync::Arc;

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    ffi::from_ffi,
};

use super::*;

#[test]
fn located_row_abi_exposes_only_arrow_results() {
    let header = include_str!("../../../cpp/moraine_abi.h");
    for removed in [
        "MoraineRowBatch",
        "moraine_rows_at(",
        "moraine_rows_at_free(",
        "moraine_row_scan_next(",
    ] {
        assert!(!header.contains(removed), "legacy IPC export: {removed}");
    }
    for retained in [
        "moraine_rows_at_open(",
        "moraine_row_scan_next_arrow(",
        "moraine_row_scan_free(",
    ] {
        assert!(
            header.contains(retained),
            "missing Arrow cursor export: {retained}"
        );
    }
}

#[test]
fn exported_batch_retains_original_buffers() {
    let values = Arc::new(Int64Array::from(vec![Some(7), None, Some(9)]));
    let pointer = values.values().as_ptr();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)])),
        vec![values],
    )
    .unwrap();
    let (array, schema) = export_batch(batch).unwrap();
    // SAFETY: array and schema were exported together and ownership is transferred
    // once.
    let data = unsafe { from_ffi(array, &schema) }.unwrap();
    let batch = RecordBatch::from(StructArray::from(data));
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values().as_ptr(), pointer);
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 9);
}
