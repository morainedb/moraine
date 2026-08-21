//! Generated protobuf wire messages (see `proto/remote.proto` and `build.rs`).

#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::doc_markdown,
    clippy::module_name_repetitions
)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/moraine.remote.rs"));
}

pub(crate) use generated::{
    CellValue, CommitValue, CommittedValue, ErrorKindValue, ErrorValue, HelloValue,
    InlineDropValue, InlineFileDeleteRemoveValue, InlineFileDeleteValue, InlineFlushDeleteValue,
    InlineInlineDeleteValue, InlineInsertValue, InlineSchemaDropValue, InlineSchemaValue,
    RequestMessage, ResponseMessage, RowCellsValue, RowOperationValue, SnapshotRowsValue, Unit,
    cell_value, request_message, response_message, row_operation_value,
};
