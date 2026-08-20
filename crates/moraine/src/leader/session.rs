//! One forwarded catalog session over a TCP connection: the request loop, the
//! constant-time handshake, the reads served from the pinned head, and the
//! commit routed through the funnel.
//!
//! A `Hello` is answered by a constant-time token compare before any catalog
//! work — a wrong token is refused at once. A dropped connection ends the loop
//! with the session's staged rows discarded and no commit submitted, so it
//! rolls back leaving no trace.

use std::sync::Arc;

use moraine_remote::{ErrorKind, Request, Response, TOKEN_LEN, WireCell, WireRowOperation};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    catalog::Catalog,
    error::{Error, Result},
    leader::funnel::{CommitFunnel, PinnedHead},
    store::{handle::ReadHandle, read, value},
    transaction::{
        slot_commit::SlotHead,
        staged::{self, Cell, RowOperation, StagedBacking, TableKind},
    },
};

/// A ceiling on one framed message, mirroring the client's transport guard.
const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

/// The shared context every session serves against.
pub(crate) struct SessionContext {
    pub(crate) secret: [u8; TOKEN_LEN],
    pub(crate) funnel: Arc<CommitFunnel>,
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) stats: Arc<crate::leader::LeaderStats>,
}

/// Serves one forwarded session to completion. A transport error (a dropped
/// connection) ends it quietly; a protocol or catalog error is answered on the
/// wire and the loop continues, except a refused handshake, which ends it.
pub(crate) async fn serve<S>(mut stream: S, context: &SessionContext) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _session_gauge = crate::leader::SessionGuard::open(Arc::clone(&context.stats));
    let mut session = Session {
        context,
        authenticated: false,
        head: None,
        ops: Vec::new(),
    };

    loop {
        let framed = match read_frame(&mut stream).await {
            Ok(framed) => framed,
            // A client that closed its half ends the session with no trace.
            Err(Error::SlotLog(_)) => return Ok(()),
            Err(err) => return Err(err),
        };
        let Ok(request) = Request::decode(&framed) else {
            return Ok(());
        };

        let (response, keep_open) = session.handle(request).await;
        write_frame(&mut stream, &response.encode()).await?;
        if !keep_open {
            return Ok(());
        }
    }
}

/// One connection's evolving state.
struct Session<'a> {
    context: &'a SessionContext,
    authenticated: bool,
    head: Option<PinnedHead>,
    ops: Vec<RowOperation>,
}

impl Session<'_> {
    /// Handles one request, returning its response and whether the session
    /// stays open.
    async fn handle(&mut self, request: Request) -> (Response, bool) {
        // Every request before a successful handshake is refused, and the
        // handshake itself does no catalog work until the token matches.
        if let Request::Hello { token, .. } = &request {
            if constant_time_eq::constant_time_eq(token, &self.context.secret) {
                self.authenticated = true;
                return (Response::Ok, true);
            }
            return (
                Response::Error {
                    kind: ErrorKind::Constraint,
                    message: "forwarding token rejected".to_string(),
                },
                false,
            );
        }
        if !self.authenticated {
            return (
                Response::Error {
                    kind: ErrorKind::Constraint,
                    message: "session is not authenticated".to_string(),
                },
                false,
            );
        }

        match self.serve(request).await {
            Ok(response) => (response, true),
            Err(err) => (error_response(&err), true),
        }
    }

    /// Serves one authenticated request.
    async fn serve(&mut self, request: Request) -> Result<Response> {
        match request {
            Request::Hello { .. } => Ok(Response::Ok),
            Request::Begin => {
                self.head = Some(self.context.funnel.pin_head().await?);
                self.ops.clear();
                Ok(Response::Ok)
            }
            Request::Snapshot => {
                let head = self.pinned()?;
                Ok(Response::Snapshot(vec![value::encode_value(
                    &head.view.snapshot,
                )]))
            }
            Request::VisibleSnapshots => {
                let head = self.pinned()?;
                let rows = read::scan_snapshots_overlaid(
                    ReadHandle::Reader(&head.reader),
                    Some(&head.overlay),
                )
                .await?;
                Ok(Response::Snapshot(
                    rows.iter().map(value::encode_value).collect(),
                ))
            }
            Request::ScheduledDeletions => {
                let head = self.pinned()?;
                Ok(Response::Snapshot(
                    head.view
                        .gc_files
                        .values()
                        .map(value::encode_value)
                        .collect(),
                ))
            }
            Request::Stage(operation) => {
                self.pinned()?;
                self.ops.push(wire_operation(operation)?);
                Ok(Response::Ok)
            }
            Request::Commit { transaction_id } => {
                let snapshot = self.commit(transaction_id).await?;
                Ok(Response::Committed {
                    snapshot_id: snapshot.get(),
                })
            }
            Request::Rollback => {
                self.head = None;
                self.ops.clear();
                Ok(Response::Ok)
            }
        }
    }

    /// The pinned head, or a constraint error if the client staged or read
    /// before `Begin`.
    fn pinned(&self) -> Result<&PinnedHead> {
        self.head
            .as_ref()
            .ok_or_else(|| Error::Constraint("no transaction is open; send Begin first".into()))
    }

    /// Assembles the staged rows against the pinned head and routes the commit
    /// through the funnel, returning only after the slot PUT is durable. The
    /// client's `transaction_id` stamps the commit so the client can resolve an
    /// ambiguous outcome by identity; an all-zero id lets the leader mint one.
    async fn commit(&mut self, transaction_id: [u8; 16]) -> Result<crate::catalog::SnapshotId> {
        let head = self
            .head
            .take()
            .ok_or_else(|| Error::Constraint("no transaction is open; send Begin first".into()))?;
        let ops = std::mem::take(&mut self.ops);

        let slots = self.context.catalog.slot_store().slots.clone();
        let backing = StagedBacking::slots(
            Box::new(SlotHead {
                view: head.view.clone(),
                overlay: head.overlay.clone(),
                next_sequence: head.next_sequence,
                reader: None,
            }),
            Arc::clone(&head.reader),
            slots,
        );

        let client_id = (transaction_id != [0; 16]).then_some(transaction_id);
        // Cloned into a local so the assembly's borrow is of this frame, not
        // of the shared `SessionContext` the spawned session holds.
        let projections = Arc::clone(self.context.catalog.projections());
        let assembly = staged::assemble(
            &backing,
            &ops,
            None,
            "",
            &projections,
            Arc::new(crate::data_file::ScopedReadMetrics::default()),
            client_id,
        )
        .await?;
        let validated_head = head.view.snapshot.snapshot_id;
        self.context.funnel.submit(validated_head, assembly).await
    }
}

/// Maps a moraine error to the wire's typed kind and its detail — the string
/// following the kind's prefix, which the client renders back to moraine's
/// `Display` byte-for-byte.
fn error_response(err: &Error) -> Response {
    let (kind, message) = match err {
        Error::CommitConflict(message) => (ErrorKind::Conflict, message.clone()),
        Error::RetryBudgetExhausted(message) => (ErrorKind::RetryBudgetExhausted, message.clone()),
        Error::Corruption(message) => (ErrorKind::Corruption, message.clone()),
        Error::ConcurrentModification => (ErrorKind::Conflict, String::new()),
        Error::NotFound(message) => (ErrorKind::NotFound, message.clone()),
        Error::AlreadyExists(message) => (ErrorKind::AlreadyExists, message.clone()),
        Error::Constraint(message) => (ErrorKind::Constraint, message.clone()),
        Error::IndexBuilding(message) => (ErrorKind::IndexBuilding, message.clone()),
        Error::Configuration(message) => (ErrorKind::Configuration, message.clone()),
        Error::Fenced(message) => (ErrorKind::Fenced, message.clone()),
        Error::SlotLog(message) => (ErrorKind::SlotLog, message.clone()),
        Error::Unsupported(message) => (ErrorKind::Unsupported, message.clone()),
        Error::SnapshotExpired(message) => (ErrorKind::SnapshotExpired, message.clone()),
        Error::Interrupted(message) => (ErrorKind::Interrupted, message.clone()),
        Error::Migration(message) => (ErrorKind::Migration, message.clone()),
        Error::OpenRaced(message) => (ErrorKind::OpenRaced, message.clone()),
        Error::Store(source) => (ErrorKind::Store, source.to_string()),
    };
    Response::Error { kind, message }
}

/// Converts a wire row operation to moraine's own, resolving the opaque table
/// discriminant to a [`TableKind`].
fn wire_operation(operation: WireRowOperation) -> Result<RowOperation> {
    let table = |discriminant: u32| -> Result<TableKind> {
        i32::try_from(discriminant)
            .ok()
            .and_then(|value| TableKind::try_from(value).ok())
            .ok_or_else(|| Error::Corruption(format!("unknown table discriminant {discriminant}")))
    };
    Ok(match operation {
        WireRowOperation::Insert { table: t, cells } => RowOperation::Insert {
            table: table(t)?,
            cells: wire_cells(cells),
        },
        WireRowOperation::Delete { table: t, cells } => RowOperation::Delete {
            table: table(t)?,
            cells: wire_cells(cells),
        },
        WireRowOperation::UpdateSetEnd { table: t, cells } => RowOperation::UpdateSetEnd {
            table: table(t)?,
            cells: wire_cells(cells),
        },
        WireRowOperation::UpdateSetBegin { table: t, cells } => RowOperation::UpdateSetBegin {
            table: table(t)?,
            cells: wire_cells(cells),
        },
        WireRowOperation::InlineSchema {
            table_id,
            schema_version,
            arrow_schema,
        } => RowOperation::InlineSchema {
            table_id,
            schema_version,
            arrow_schema,
        },
        WireRowOperation::InlineInsert {
            table_id,
            schema_version,
            begin_snapshot,
            row_id_start,
            row_count,
            arrow_body,
        } => RowOperation::InlineInsert {
            table_id,
            schema_version,
            begin_snapshot,
            row_id_start,
            row_count,
            arrow_body,
        },
        WireRowOperation::InlineInlineDelete {
            table_id,
            row_id,
            end_snapshot,
        } => RowOperation::InlineInlineDelete {
            table_id,
            row_id,
            end_snapshot,
        },
        WireRowOperation::InlineFileDelete {
            table_id,
            data_file_id,
            row_id,
            begin_snapshot,
        } => RowOperation::InlineFileDelete {
            table_id,
            data_file_id,
            row_id,
            begin_snapshot,
        },
        WireRowOperation::InlineFileDeleteRemove {
            table_id,
            data_file_id,
            row_id,
        } => RowOperation::InlineFileDeleteRemove {
            table_id,
            data_file_id,
            row_id,
        },
        WireRowOperation::InlineFlushDelete {
            table_id,
            schema_version,
            flush_snapshot,
        } => RowOperation::InlineFlushDelete {
            table_id,
            schema_version,
            flush_snapshot,
        },
        WireRowOperation::InlineDrop { table_id } => RowOperation::InlineDrop { table_id },
        WireRowOperation::InlineSchemaDrop {
            table_id,
            schema_version,
        } => RowOperation::InlineSchemaDrop {
            table_id,
            schema_version,
        },
    })
}

fn wire_cells(cells: Vec<WireCell>) -> Vec<Cell> {
    cells.into_iter().map(wire_cell).collect()
}

fn wire_cell(cell: WireCell) -> Cell {
    match cell {
        WireCell::Null => Cell::Null,
        WireCell::U64(value) => Cell::U64(value),
        WireCell::I64(value) => Cell::I64(value),
        WireCell::Bool(value) => Cell::Bool(value),
        WireCell::Str(value) => Cell::Str(value),
    }
}

/// Reads one length-prefixed framed message: a 4-byte big-endian length, then
/// that many framed bytes. A closed connection surfaces as [`Error::SlotLog`],
/// which the loop treats as a clean end.
async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    let mut length = [0u8; 4];
    reader
        .read_exact(&mut length)
        .await
        .map_err(|err| Error::SlotLog(format!("forwarded session read: {err}")))?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME_LEN {
        return Err(Error::Corruption(format!(
            "forwarded frame length {length} exceeds the ceiling"
        )));
    }
    let mut framed = vec![0u8; length];
    reader
        .read_exact(&mut framed)
        .await
        .map_err(|err| Error::SlotLog(format!("forwarded session read: {err}")))?;
    Ok(framed)
}

/// Writes one length-prefixed framed message.
async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, framed: &[u8]) -> Result<()> {
    let length = u32::try_from(framed.len())
        .map_err(|_| Error::Corruption("forwarded frame exceeds the length prefix".into()))?;
    writer
        .write_all(&length.to_be_bytes())
        .await
        .map_err(|err| Error::SlotLog(format!("forwarded session write: {err}")))?;
    writer
        .write_all(framed)
        .await
        .map_err(|err| Error::SlotLog(format!("forwarded session write: {err}")))?;
    writer
        .flush()
        .await
        .map_err(|err| Error::SlotLog(format!("forwarded session write: {err}")))?;
    Ok(())
}
