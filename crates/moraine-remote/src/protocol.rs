//! The forwarded-session messages, their protobuf codec, and the typed
//! failure kinds a client renders to text.
//!
//! [`Request`] and [`Response`] mirror the staged-row ABI: there is no SQL
//! and no dialect on the wire, because the leader and the client own both
//! ends. [`WireRowOperation`] and [`WireCell`] mirror moraine's own
//! `RowOperation`/`Cell` shape without naming those types — the dependency
//! points from moraine to here, so moraine converts at each end.

use prost::Message;

use crate::{
    error::{Error, Result},
    frame,
    proto::{
        CellValue, CommitValue, CommittedValue, ErrorKindValue, ErrorValue, HelloValue,
        InlineDropValue, InlineFileDeleteRemoveValue, InlineFileDeleteValue,
        InlineFlushDeleteValue, InlineInlineDeleteValue, InlineInsertValue, InlineSchemaDropValue,
        InlineSchemaValue, RequestMessage, ResponseMessage, RowCellsValue, RowOperationValue,
        SnapshotRowsValue, Unit, cell_value, request_message, response_message,
        row_operation_value,
    },
};

/// The width of a session token: a shared bucket secret.
pub const TOKEN_LEN: usize = 32;

/// The width of a transaction id a client stamps a forwarded commit with.
pub const TXID_LEN: usize = 16;

/// One request in a forwarded catalog session. One connection carries one
/// session, opened by [`Request::Hello`] and driven through to
/// [`Request::Commit`] or [`Request::Rollback`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// The session handshake: a shared bucket secret and the protocol
    /// version the client speaks.
    Hello {
        /// The shared bucket secret authorizing the session.
        token: [u8; TOKEN_LEN],
        /// The protocol version the client speaks.
        protocol_version: u32,
    },
    /// Open a staged transaction at the leader's current head.
    Begin,
    /// Read the transaction's own snapshot record.
    Snapshot,
    /// Read the snapshot records visible to the transaction.
    VisibleSnapshots,
    /// Read the files scheduled for deletion.
    ScheduledDeletions,
    /// Stage one row mutation.
    Stage(WireRowOperation),
    /// Commit the staged transaction, stamped with the client's transaction
    /// id. An all-zero id lets the leader mint one.
    Commit {
        /// The id the client resolves an ambiguous outcome by.
        transaction_id: [u8; TXID_LEN],
    },
    /// Discard the staged transaction.
    Rollback,
}

/// One response in a forwarded catalog session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// The request succeeded and carries no value.
    Ok,
    /// A read's result: opaque per-row bytes the client decodes into its own
    /// row type.
    Snapshot(Vec<Vec<u8>>),
    /// A commit landed, minting this snapshot.
    Committed {
        /// The minted snapshot id.
        snapshot_id: u64,
    },
    /// The request failed. [`ErrorKind`] is rendered to text client-side.
    Error {
        /// The failure class.
        kind: ErrorKind,
        /// The failure's detail, the text following the kind's prefix.
        message: String,
    },
}

/// A typed failure a forwarded session surfaces. The client renders each to
/// text byte-for-byte matching moraine's core error `Display`, so a forwarded
/// failure composes with a SQL retry loop's substring dispatch exactly as a
/// direct one does: [`ErrorKind::Conflict`] renders text carrying `conflict`;
/// the terminal kinds [`ErrorKind::RetryBudgetExhausted`] and
/// [`ErrorKind::SlotLog`] carry none of the retry substrings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// A concurrent commit changed state this transaction read or wrote.
    Conflict,
    /// A commit spent its whole retry budget on benign races without
    /// settling.
    RetryBudgetExhausted,
    /// Stored bytes failed to decode.
    Corruption,
    /// An operation referenced a missing entity.
    NotFound,
    /// An operation would violate name uniqueness.
    AlreadyExists,
    /// An operation would violate a structural constraint.
    Constraint,
    /// A lookup targeted an index whose backfill has not completed.
    IndexBuilding,
    /// An environment or option value could not be parsed.
    Configuration,
    /// This writer has been fenced by a newer one.
    Fenced,
    /// The commit-slot log could not be reached.
    SlotLog,
    /// The underlying store failed.
    Store,
    /// A DuckLake feature moraine does not implement.
    Unsupported,
    /// A held or requested snapshot fell below the retention horizon.
    SnapshotExpired,
    /// A host interrupt cancelled the operation, or a durable write past its
    /// point of no return never reported its outcome.
    Interrupted,
    /// The store requires, is undergoing, or was written by an unsupported
    /// structural format.
    Migration,
    /// Another process created this store while this open was creating it.
    OpenRaced,
}

impl ErrorKind {
    /// Renders this kind and a detail message to the full error text moraine's
    /// core produces for the same failure. The rendering is a wire contract:
    /// it must match moraine's error `Display` byte-for-byte.
    #[must_use]
    pub fn render(self, message: &str) -> String {
        match self {
            Self::Conflict => format!("commit conflict: {message}"),
            Self::RetryBudgetExhausted => format!("retry budget exhausted: {message}"),
            Self::Corruption => format!("corruption: {message}"),
            Self::NotFound => format!("not found: {message}"),
            Self::AlreadyExists => format!("already exists: {message}"),
            Self::Constraint => format!("constraint violation: {message}"),
            Self::IndexBuilding => format!("index building: {message}"),
            Self::Configuration => format!("configuration: {message}"),
            Self::Fenced => format!("writer fenced: {message}"),
            Self::SlotLog => format!("commit-slot log unavailable: {message}"),
            Self::Store => "store error".to_string(),
            Self::Unsupported => format!("unsupported: {message}"),
            Self::SnapshotExpired => format!("snapshot expired: {message}"),
            Self::Interrupted => format!("interrupted: {message}"),
            Self::Migration => format!("migration required: {message}"),
            Self::OpenRaced => format!("open raced: {message}"),
        }
    }

    fn to_wire(self) -> ErrorKindValue {
        match self {
            Self::Conflict => ErrorKindValue::Conflict,
            Self::RetryBudgetExhausted => ErrorKindValue::RetryBudgetExhausted,
            Self::Corruption => ErrorKindValue::Corruption,
            Self::NotFound => ErrorKindValue::NotFound,
            Self::AlreadyExists => ErrorKindValue::AlreadyExists,
            Self::Constraint => ErrorKindValue::Constraint,
            Self::IndexBuilding => ErrorKindValue::IndexBuilding,
            Self::Configuration => ErrorKindValue::Configuration,
            Self::Fenced => ErrorKindValue::Fenced,
            Self::SlotLog => ErrorKindValue::SlotLog,
            Self::Store => ErrorKindValue::Store,
            Self::Unsupported => ErrorKindValue::Unsupported,
            Self::SnapshotExpired => ErrorKindValue::SnapshotExpired,
            Self::Interrupted => ErrorKindValue::Interrupted,
            Self::Migration => ErrorKindValue::Migration,
            Self::OpenRaced => ErrorKindValue::OpenRaced,
        }
    }

    fn from_wire(wire: ErrorKindValue) -> Result<Self> {
        match wire {
            ErrorKindValue::Conflict => Ok(Self::Conflict),
            ErrorKindValue::RetryBudgetExhausted => Ok(Self::RetryBudgetExhausted),
            ErrorKindValue::Corruption => Ok(Self::Corruption),
            ErrorKindValue::NotFound => Ok(Self::NotFound),
            ErrorKindValue::AlreadyExists => Ok(Self::AlreadyExists),
            ErrorKindValue::Constraint => Ok(Self::Constraint),
            ErrorKindValue::IndexBuilding => Ok(Self::IndexBuilding),
            ErrorKindValue::Configuration => Ok(Self::Configuration),
            ErrorKindValue::Fenced => Ok(Self::Fenced),
            ErrorKindValue::SlotLog => Ok(Self::SlotLog),
            ErrorKindValue::Store => Ok(Self::Store),
            ErrorKindValue::Unsupported => Ok(Self::Unsupported),
            ErrorKindValue::SnapshotExpired => Ok(Self::SnapshotExpired),
            ErrorKindValue::Interrupted => Ok(Self::Interrupted),
            ErrorKindValue::Migration => Ok(Self::Migration),
            ErrorKindValue::OpenRaced => Ok(Self::OpenRaced),
            ErrorKindValue::Unspecified => Err(Error::Protocol(
                "error carries no kind discriminant".to_string(),
            )),
        }
    }
}

/// One column value in a staged row. Mirrors moraine's `Cell`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireCell {
    /// SQL `NULL`.
    Null,
    /// An unsigned integer column.
    U64(u64),
    /// A signed integer column.
    I64(i64),
    /// A boolean column.
    Bool(bool),
    /// A text column.
    Str(String),
}

/// One staged row mutation. Mirrors moraine's `RowOperation`; the table is
/// carried as its opaque ABI discriminant, and the client and leader convert
/// to and from moraine's `TableKind` at each end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireRowOperation {
    /// A new row.
    Insert {
        /// The row's table discriminant.
        table: u32,
        /// The row's column values, in table order.
        cells: Vec<WireCell>,
    },
    /// A row removed with no history mirror.
    Delete {
        /// The row's table discriminant.
        table: u32,
        /// The removed row's key columns, in table order.
        cells: Vec<WireCell>,
    },
    /// An `UPDATE ... SET end_snapshot` row.
    UpdateSetEnd {
        /// The row's table discriminant.
        table: u32,
        /// The ended row's key columns followed by the new `end_snapshot`.
        cells: Vec<WireCell>,
    },
    /// An `UPDATE ... SET begin_snapshot` row.
    UpdateSetBegin {
        /// The row's table discriminant.
        table: u32,
        /// The rebased row's key columns followed by the new `begin_snapshot`.
        cells: Vec<WireCell>,
    },
    /// `inline/schema`: an Arrow IPC schema for one `(table_id,
    /// schema_version)`.
    InlineSchema {
        /// Owning table.
        table_id: u64,
        /// Schema version the layout is pinned to.
        schema_version: u64,
        /// The Arrow IPC schema message, verbatim.
        arrow_schema: Vec<u8>,
    },
    /// `inline/insert`: one Arrow record-batch chunk of inlined rows.
    InlineInsert {
        /// Owning table.
        table_id: u64,
        /// Schema version the chunk was written under.
        schema_version: u64,
        /// Commit snapshot of the insert.
        begin_snapshot: u64,
        /// The chunk's first row id.
        row_id_start: u64,
        /// Row count carried by `arrow_body`.
        row_count: u64,
        /// The user-column cells, an Arrow IPC record-batch body.
        arrow_body: Vec<u8>,
    },
    /// `inline/inline_delete`: tombstones one inlined-insert row.
    InlineInlineDelete {
        /// Owning table.
        table_id: u64,
        /// The tombstoned row.
        row_id: u64,
        /// The commit snapshot the row ends at.
        end_snapshot: u64,
    },
    /// `inline/file_delete`: an inlined delete against a Parquet-file row.
    InlineFileDelete {
        /// Owning table.
        table_id: u64,
        /// Targeted data file.
        data_file_id: u64,
        /// Deleted row.
        row_id: u64,
        /// The commit snapshot the delete takes effect at.
        begin_snapshot: u64,
    },
    /// Removes one live `inline/file_delete` record.
    InlineFileDeleteRemove {
        /// Owning table.
        table_id: u64,
        /// The data file the removed deletion targeted.
        data_file_id: u64,
        /// The row the removed deletion killed.
        row_id: u64,
    },
    /// `inline/*` flush: removes chunks begun at or before `flush_snapshot`.
    InlineFlushDelete {
        /// Owning table.
        table_id: u64,
        /// Schema version being flushed.
        schema_version: u64,
        /// Chunks begun at or before this snapshot are flushed.
        flush_snapshot: u64,
    },
    /// Removes every `inline/*` record for `table_id`.
    InlineDrop {
        /// The dropped table.
        table_id: u64,
    },
    /// Removes only the `inline/schema` record for one `(table_id,
    /// schema_version)`.
    InlineSchemaDrop {
        /// Owning table.
        table_id: u64,
        /// The schema version deregistered.
        schema_version: u64,
    },
}

impl Request {
    /// Encodes this request to its framed wire bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        frame::frame(&encode_message(&self.to_wire()))
    }

    /// Decodes a framed request. Any framing, protobuf, or structural failure
    /// is [`Error::Protocol`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Protocol`] if the bytes are not a valid framed request.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let payload = frame::unframe(bytes)?;
        Self::from_wire(decode_message::<RequestMessage>(payload)?)
    }

    fn to_wire(&self) -> RequestMessage {
        use request_message::Request as Wire;
        let request = match self {
            Self::Hello {
                token,
                protocol_version,
            } => Wire::Hello(HelloValue {
                token: token.to_vec(),
                protocol_version: *protocol_version,
            }),
            Self::Begin => Wire::Begin(Unit {}),
            Self::Snapshot => Wire::Snapshot(Unit {}),
            Self::VisibleSnapshots => Wire::VisibleSnapshots(Unit {}),
            Self::ScheduledDeletions => Wire::ScheduledDeletions(Unit {}),
            Self::Stage(operation) => Wire::Stage(operation.to_wire()),
            Self::Commit { transaction_id } => Wire::Commit(CommitValue {
                transaction_id: transaction_id.to_vec(),
            }),
            Self::Rollback => Wire::Rollback(Unit {}),
        };
        RequestMessage {
            request: Some(request),
        }
    }

    fn from_wire(wire: RequestMessage) -> Result<Self> {
        use request_message::Request as Wire;
        let request = wire
            .request
            .ok_or_else(|| Error::Protocol("request carries no variant".to_string()))?;
        Ok(match request {
            Wire::Hello(hello) => Self::Hello {
                token: token_from_wire(&hello.token)?,
                protocol_version: hello.protocol_version,
            },
            Wire::Begin(_) => Self::Begin,
            Wire::Snapshot(_) => Self::Snapshot,
            Wire::VisibleSnapshots(_) => Self::VisibleSnapshots,
            Wire::ScheduledDeletions(_) => Self::ScheduledDeletions,
            Wire::Stage(operation) => Self::Stage(WireRowOperation::from_wire(operation)?),
            Wire::Commit(commit) => Self::Commit {
                transaction_id: txid_from_wire(&commit.transaction_id)?,
            },
            Wire::Rollback(_) => Self::Rollback,
        })
    }
}

impl Response {
    /// Encodes this response to its framed wire bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        frame::frame(&encode_message(&self.to_wire()))
    }

    /// Decodes a framed response. Any framing, protobuf, or structural failure
    /// is [`Error::Protocol`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Protocol`] if the bytes are not a valid framed
    /// response.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let payload = frame::unframe(bytes)?;
        Self::from_wire(decode_message::<ResponseMessage>(payload)?)
    }

    fn to_wire(&self) -> ResponseMessage {
        use response_message::Response as Wire;
        let response = match self {
            Self::Ok => Wire::Ok(Unit {}),
            Self::Snapshot(rows) => Wire::Snapshot(SnapshotRowsValue { rows: rows.clone() }),
            Self::Committed { snapshot_id } => Wire::Committed(CommittedValue {
                snapshot_id: *snapshot_id,
            }),
            Self::Error { kind, message } => Wire::Error(ErrorValue {
                kind: kind.to_wire() as i32,
                message: message.clone(),
            }),
        };
        ResponseMessage {
            response: Some(response),
        }
    }

    fn from_wire(wire: ResponseMessage) -> Result<Self> {
        use response_message::Response as Wire;
        let response = wire
            .response
            .ok_or_else(|| Error::Protocol("response carries no variant".to_string()))?;
        Ok(match response {
            Wire::Ok(_) => Self::Ok,
            Wire::Snapshot(rows) => Self::Snapshot(rows.rows),
            Wire::Committed(committed) => Self::Committed {
                snapshot_id: committed.snapshot_id,
            },
            Wire::Error(error) => {
                let kind = ErrorKindValue::try_from(error.kind)
                    .map_err(|_| Error::Protocol(format!("unknown error kind {}", error.kind)))?;
                Self::Error {
                    kind: ErrorKind::from_wire(kind)?,
                    message: error.message,
                }
            }
        })
    }
}

impl WireCell {
    fn to_wire(&self) -> CellValue {
        use cell_value::Cell as Wire;
        let cell = match self {
            Self::Null => Wire::Null(Unit {}),
            Self::U64(value) => Wire::Unsigned(*value),
            Self::I64(value) => Wire::Signed(*value),
            Self::Bool(value) => Wire::Boolean(*value),
            Self::Str(value) => Wire::Text(value.clone()),
        };
        CellValue { cell: Some(cell) }
    }

    fn from_wire(wire: CellValue) -> Result<Self> {
        use cell_value::Cell as Wire;
        let cell = wire
            .cell
            .ok_or_else(|| Error::Protocol("cell carries no variant".to_string()))?;
        Ok(match cell {
            Wire::Null(_) => Self::Null,
            Wire::Unsigned(value) => Self::U64(value),
            Wire::Signed(value) => Self::I64(value),
            Wire::Boolean(value) => Self::Bool(value),
            Wire::Text(value) => Self::Str(value),
        })
    }
}

fn cells_to_wire(cells: &[WireCell]) -> Vec<CellValue> {
    cells.iter().map(WireCell::to_wire).collect()
}

fn cells_from_wire(cells: Vec<CellValue>) -> Result<Vec<WireCell>> {
    cells.into_iter().map(WireCell::from_wire).collect()
}

impl WireRowOperation {
    fn to_wire(&self) -> RowOperationValue {
        use row_operation_value::Operation as Wire;
        let operation = match self {
            Self::Insert { table, cells } => Wire::Insert(RowCellsValue {
                table: *table,
                cells: cells_to_wire(cells),
            }),
            Self::Delete { table, cells } => Wire::Delete(RowCellsValue {
                table: *table,
                cells: cells_to_wire(cells),
            }),
            Self::UpdateSetEnd { table, cells } => Wire::UpdateSetEnd(RowCellsValue {
                table: *table,
                cells: cells_to_wire(cells),
            }),
            Self::UpdateSetBegin { table, cells } => Wire::UpdateSetBegin(RowCellsValue {
                table: *table,
                cells: cells_to_wire(cells),
            }),
            Self::InlineSchema {
                table_id,
                schema_version,
                arrow_schema,
            } => Wire::InlineSchema(InlineSchemaValue {
                table_id: *table_id,
                schema_version: *schema_version,
                arrow_schema: arrow_schema.clone(),
            }),
            Self::InlineInsert {
                table_id,
                schema_version,
                begin_snapshot,
                row_id_start,
                row_count,
                arrow_body,
            } => Wire::InlineInsert(InlineInsertValue {
                table_id: *table_id,
                schema_version: *schema_version,
                begin_snapshot: *begin_snapshot,
                row_id_start: *row_id_start,
                row_count: *row_count,
                arrow_body: arrow_body.clone(),
            }),
            Self::InlineInlineDelete {
                table_id,
                row_id,
                end_snapshot,
            } => Wire::InlineInlineDelete(InlineInlineDeleteValue {
                table_id: *table_id,
                row_id: *row_id,
                end_snapshot: *end_snapshot,
            }),
            Self::InlineFileDelete {
                table_id,
                data_file_id,
                row_id,
                begin_snapshot,
            } => Wire::InlineFileDelete(InlineFileDeleteValue {
                table_id: *table_id,
                data_file_id: *data_file_id,
                row_id: *row_id,
                begin_snapshot: *begin_snapshot,
            }),
            Self::InlineFileDeleteRemove {
                table_id,
                data_file_id,
                row_id,
            } => Wire::InlineFileDeleteRemove(InlineFileDeleteRemoveValue {
                table_id: *table_id,
                data_file_id: *data_file_id,
                row_id: *row_id,
            }),
            Self::InlineFlushDelete {
                table_id,
                schema_version,
                flush_snapshot,
            } => Wire::InlineFlushDelete(InlineFlushDeleteValue {
                table_id: *table_id,
                schema_version: *schema_version,
                flush_snapshot: *flush_snapshot,
            }),
            Self::InlineDrop { table_id } => Wire::InlineDrop(InlineDropValue {
                table_id: *table_id,
            }),
            Self::InlineSchemaDrop {
                table_id,
                schema_version,
            } => Wire::InlineSchemaDrop(InlineSchemaDropValue {
                table_id: *table_id,
                schema_version: *schema_version,
            }),
        };
        RowOperationValue {
            operation: Some(operation),
        }
    }

    fn from_wire(wire: RowOperationValue) -> Result<Self> {
        use row_operation_value::Operation as Wire;
        let operation = wire
            .operation
            .ok_or_else(|| Error::Protocol("row operation carries no variant".to_string()))?;
        Ok(match operation {
            Wire::Insert(row) => Self::Insert {
                table: row.table,
                cells: cells_from_wire(row.cells)?,
            },
            Wire::Delete(row) => Self::Delete {
                table: row.table,
                cells: cells_from_wire(row.cells)?,
            },
            Wire::UpdateSetEnd(row) => Self::UpdateSetEnd {
                table: row.table,
                cells: cells_from_wire(row.cells)?,
            },
            Wire::UpdateSetBegin(row) => Self::UpdateSetBegin {
                table: row.table,
                cells: cells_from_wire(row.cells)?,
            },
            Wire::InlineSchema(inline) => Self::InlineSchema {
                table_id: inline.table_id,
                schema_version: inline.schema_version,
                arrow_schema: inline.arrow_schema,
            },
            Wire::InlineInsert(inline) => Self::InlineInsert {
                table_id: inline.table_id,
                schema_version: inline.schema_version,
                begin_snapshot: inline.begin_snapshot,
                row_id_start: inline.row_id_start,
                row_count: inline.row_count,
                arrow_body: inline.arrow_body,
            },
            Wire::InlineInlineDelete(inline) => Self::InlineInlineDelete {
                table_id: inline.table_id,
                row_id: inline.row_id,
                end_snapshot: inline.end_snapshot,
            },
            Wire::InlineFileDelete(inline) => Self::InlineFileDelete {
                table_id: inline.table_id,
                data_file_id: inline.data_file_id,
                row_id: inline.row_id,
                begin_snapshot: inline.begin_snapshot,
            },
            Wire::InlineFileDeleteRemove(inline) => Self::InlineFileDeleteRemove {
                table_id: inline.table_id,
                data_file_id: inline.data_file_id,
                row_id: inline.row_id,
            },
            Wire::InlineFlushDelete(inline) => Self::InlineFlushDelete {
                table_id: inline.table_id,
                schema_version: inline.schema_version,
                flush_snapshot: inline.flush_snapshot,
            },
            Wire::InlineDrop(inline) => Self::InlineDrop {
                table_id: inline.table_id,
            },
            Wire::InlineSchemaDrop(inline) => Self::InlineSchemaDrop {
                table_id: inline.table_id,
                schema_version: inline.schema_version,
            },
        })
    }
}

fn token_from_wire(bytes: &[u8]) -> Result<[u8; TOKEN_LEN]> {
    <[u8; TOKEN_LEN]>::try_from(bytes).map_err(|_| {
        Error::Protocol(format!(
            "token is {} bytes, expected {TOKEN_LEN}",
            bytes.len()
        ))
    })
}

/// A commit's transaction id: the wire width, or all-zero when the client sent
/// an empty id and left minting to the leader.
fn txid_from_wire(bytes: &[u8]) -> Result<[u8; TXID_LEN]> {
    if bytes.is_empty() {
        return Ok([0; TXID_LEN]);
    }
    <[u8; TXID_LEN]>::try_from(bytes).map_err(|_| {
        Error::Protocol(format!(
            "transaction id is {} bytes, expected {TXID_LEN}",
            bytes.len()
        ))
    })
}

fn encode_message<M: Message>(message: &M) -> Vec<u8> {
    let mut buf = Vec::with_capacity(message.encoded_len());
    // Infallible by construction: insufficient capacity is the only error
    // `encode` returns, and a `Vec`'s `BufMut` capacity is unbounded.
    #[allow(clippy::expect_used)]
    message
        .encode(&mut buf)
        .expect("prost encode into a Vec cannot fail");

    buf
}

fn decode_message<M: Message + Default>(payload: &[u8]) -> Result<M> {
    M::decode(payload).map_err(|err| Error::Protocol(err.to_string()))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn any_cell() -> impl Strategy<Value = WireCell> {
        prop_oneof![
            Just(WireCell::Null),
            any::<u64>().prop_map(WireCell::U64),
            any::<i64>().prop_map(WireCell::I64),
            any::<bool>().prop_map(WireCell::Bool),
            ".*".prop_map(WireCell::Str),
        ]
    }

    fn any_cells() -> impl Strategy<Value = Vec<WireCell>> {
        proptest::collection::vec(any_cell(), 0..6)
    }

    fn any_row_operation() -> impl Strategy<Value = WireRowOperation> {
        prop_oneof![
            (any::<u32>(), any_cells())
                .prop_map(|(table, cells)| WireRowOperation::Insert { table, cells }),
            (any::<u32>(), any_cells())
                .prop_map(|(table, cells)| WireRowOperation::Delete { table, cells }),
            (any::<u32>(), any_cells())
                .prop_map(|(table, cells)| WireRowOperation::UpdateSetEnd { table, cells }),
            (any::<u32>(), any_cells())
                .prop_map(|(table, cells)| WireRowOperation::UpdateSetBegin { table, cells }),
            (
                any::<u64>(),
                any::<u64>(),
                proptest::collection::vec(any::<u8>(), 0..16)
            )
                .prop_map(|(table_id, schema_version, arrow_schema)| {
                    WireRowOperation::InlineSchema {
                        table_id,
                        schema_version,
                        arrow_schema,
                    }
                }),
            (
                any::<u64>(),
                any::<u64>(),
                any::<u64>(),
                any::<u64>(),
                any::<u64>(),
                proptest::collection::vec(any::<u8>(), 0..16),
            )
                .prop_map(
                    |(
                        table_id,
                        schema_version,
                        begin_snapshot,
                        row_id_start,
                        row_count,
                        arrow_body,
                    )| WireRowOperation::InlineInsert {
                        table_id,
                        schema_version,
                        begin_snapshot,
                        row_id_start,
                        row_count,
                        arrow_body,
                    }
                ),
            (any::<u64>(), any::<u64>(), any::<u64>()).prop_map(
                |(table_id, row_id, end_snapshot)| WireRowOperation::InlineInlineDelete {
                    table_id,
                    row_id,
                    end_snapshot,
                }
            ),
            (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>()).prop_map(
                |(table_id, data_file_id, row_id, begin_snapshot)| {
                    WireRowOperation::InlineFileDelete {
                        table_id,
                        data_file_id,
                        row_id,
                        begin_snapshot,
                    }
                }
            ),
            (any::<u64>(), any::<u64>(), any::<u64>()).prop_map(
                |(table_id, data_file_id, row_id)| WireRowOperation::InlineFileDeleteRemove {
                    table_id,
                    data_file_id,
                    row_id,
                }
            ),
            (any::<u64>(), any::<u64>(), any::<u64>()).prop_map(
                |(table_id, schema_version, flush_snapshot)| WireRowOperation::InlineFlushDelete {
                    table_id,
                    schema_version,
                    flush_snapshot,
                }
            ),
            any::<u64>().prop_map(|table_id| WireRowOperation::InlineDrop { table_id }),
            (any::<u64>(), any::<u64>()).prop_map(|(table_id, schema_version)| {
                WireRowOperation::InlineSchemaDrop {
                    table_id,
                    schema_version,
                }
            }),
        ]
    }

    fn any_request() -> impl Strategy<Value = Request> {
        prop_oneof![
            (any::<[u8; TOKEN_LEN]>(), any::<u32>()).prop_map(|(token, protocol_version)| {
                Request::Hello {
                    token,
                    protocol_version,
                }
            }),
            Just(Request::Begin),
            Just(Request::Snapshot),
            Just(Request::VisibleSnapshots),
            Just(Request::ScheduledDeletions),
            any_row_operation().prop_map(Request::Stage),
            any::<[u8; TXID_LEN]>().prop_map(|transaction_id| Request::Commit { transaction_id }),
            Just(Request::Rollback),
        ]
    }

    fn any_error_kind() -> impl Strategy<Value = ErrorKind> {
        prop_oneof![
            Just(ErrorKind::Conflict),
            Just(ErrorKind::RetryBudgetExhausted),
            Just(ErrorKind::Corruption),
            Just(ErrorKind::NotFound),
            Just(ErrorKind::AlreadyExists),
            Just(ErrorKind::Constraint),
            Just(ErrorKind::IndexBuilding),
            Just(ErrorKind::Configuration),
            Just(ErrorKind::Fenced),
            Just(ErrorKind::SlotLog),
            Just(ErrorKind::Store),
            Just(ErrorKind::Unsupported),
            Just(ErrorKind::SnapshotExpired),
            Just(ErrorKind::Interrupted),
            Just(ErrorKind::Migration),
            Just(ErrorKind::OpenRaced),
        ]
    }

    fn any_response() -> impl Strategy<Value = Response> {
        prop_oneof![
            Just(Response::Ok),
            proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..24), 0..6)
                .prop_map(Response::Snapshot),
            any::<u64>().prop_map(|snapshot_id| Response::Committed { snapshot_id }),
            (any_error_kind(), ".*").prop_map(|(kind, message)| Response::Error { kind, message }),
        ]
    }

    proptest! {
        #[test]
        fn requests_roundtrip(request in any_request()) {
            prop_assert_eq!(Request::decode(&request.encode()).unwrap(), request);
        }

        #[test]
        fn responses_roundtrip(response in any_response()) {
            prop_assert_eq!(Response::decode(&response.encode()).unwrap(), response);
        }

        // Decode is total: arbitrary bytes decode as some message or fail as
        // `Protocol`, never panic.
        #[test]
        fn decode_arbitrary_bytes_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..128),
        ) {
            let _ = Request::decode(&bytes);
            let _ = Response::decode(&bytes);
        }

        // Framed garbage is the same obligation one layer in.
        #[test]
        fn decode_framed_garbage_never_panics(
            payload in proptest::collection::vec(any::<u8>(), 0..128),
        ) {
            let _ = Request::decode(&frame::frame(&payload));
            let _ = Response::decode(&frame::frame(&payload));
        }
    }

    #[test]
    fn a_conflict_kind_renders_the_retry_substring() {
        assert!(ErrorKind::Conflict.render("boom").contains("conflict"));
    }

    // The terminal kinds carry none of the substrings a SQL retry loop
    // dispatches on, so an exhausted budget or an unreachable log surfaces at
    // once instead of being re-driven against a premise that already failed.
    #[test]
    fn terminal_kinds_render_no_retry_substring() {
        let substrings = ["conflict", "concurrent", "unique", "primary key"];
        for kind in [ErrorKind::RetryBudgetExhausted, ErrorKind::SlotLog] {
            let text = kind.render("some detail");
            for substring in substrings {
                assert!(
                    !text.contains(substring),
                    "{text:?} from {kind:?} contains {substring:?}"
                );
            }
        }
    }

    // The rendered text matches moraine's core error `Display` byte-for-byte,
    // so a forwarded failure composes with a SQL retry loop exactly as a
    // direct one does. These literals mirror moraine's `error.rs`.
    #[test]
    fn rendering_matches_the_core_error_texts() {
        assert_eq!(ErrorKind::Conflict.render("x"), "commit conflict: x");
        assert_eq!(
            ErrorKind::RetryBudgetExhausted.render("x"),
            "retry budget exhausted: x"
        );
        assert_eq!(ErrorKind::Corruption.render("x"), "corruption: x");
        assert_eq!(ErrorKind::NotFound.render("x"), "not found: x");
        assert_eq!(ErrorKind::AlreadyExists.render("x"), "already exists: x");
        assert_eq!(ErrorKind::Constraint.render("x"), "constraint violation: x");
        assert_eq!(ErrorKind::IndexBuilding.render("x"), "index building: x");
        assert_eq!(ErrorKind::Configuration.render("x"), "configuration: x");
        assert_eq!(ErrorKind::Fenced.render("x"), "writer fenced: x");
        assert_eq!(
            ErrorKind::SlotLog.render("x"),
            "commit-slot log unavailable: x"
        );
        assert_eq!(ErrorKind::Store.render("ignored"), "store error");
        assert_eq!(ErrorKind::Unsupported.render("x"), "unsupported: x");
        assert_eq!(
            ErrorKind::SnapshotExpired.render("x"),
            "snapshot expired: x"
        );
        assert_eq!(ErrorKind::Interrupted.render("x"), "interrupted: x");
        assert_eq!(ErrorKind::Migration.render("x"), "migration required: x");
        assert_eq!(ErrorKind::OpenRaced.render("x"), "open raced: x");
    }
}
