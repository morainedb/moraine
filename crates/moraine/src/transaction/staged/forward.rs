//! Contention-triggered forwarding: a staged transaction that opened forwarded
//! drives its commit through a leader over one connection, then falls back to a
//! fresh direct race only if the leader is unreachable.
//!
//! The fallback never rides the forwarded transaction's id: the forwarded
//! attempt stamps its own id, and a retreat re-assembles a fresh transaction
//! with a fresh id, so no id ever reaches two committers. An ambiguous
//! outcome — the connection dropped after the commit was sent, so the slot may
//! have landed — is resolved by that id through the log before any retreat, so
//! a landed commit is reported, never re-applied.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use moraine_remote::{Client, ErrorKind, Request, Response, WireCell, WireRowOperation};
use moraine_wal::SlotLog;
use object_store::ObjectStore;
use tracing::{debug, warn};

use super::{Cell, RowOperation};
use crate::{
    catalog::SnapshotId,
    error::Error,
    transaction::{commit, slot_commit},
};

/// The protocol version a forwarding client announces. The leader authenticates
/// on the token, not the version, so this is informational.
const PROTOCOL_VERSION: u32 = 1;

/// How long a forwarding client waits to reach a leader before treating the
/// endpoint as unreachable, aging it, and committing directly. One bounded
/// probe per session, never a stall against a crashed leader's advert.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(250);

/// A leader a contended staged transaction forwards to, plus the store parts a
/// retreat to direct and an ambiguous-outcome scan need.
pub(crate) struct Forward {
    endpoint: String,
    secret: [u8; commit::SECRET_LEN],
    forwarding: Arc<slot_commit::Forwarding>,
    object_store: Arc<dyn ObjectStore>,
    path: String,
    cache: slot_commit::CacheOptions,
    slots: SlotLog,
}

impl Forward {
    pub(crate) fn new(
        endpoint: String,
        secret: [u8; commit::SECRET_LEN],
        forwarding: Arc<slot_commit::Forwarding>,
        object_store: Arc<dyn ObjectStore>,
        path: String,
        cache: slot_commit::CacheOptions,
        slots: SlotLog,
    ) -> Self {
        Self {
            endpoint,
            secret,
            forwarding,
            object_store,
            path,
            cache,
            slots,
        }
    }
}

/// What a forwarded commit resolved to.
pub(crate) enum Forwarded {
    /// The leader committed, minting this snapshot.
    Committed(SnapshotId),
    /// Return this error verbatim: a conflict DuckLake re-drives, or a content
    /// error a direct attempt would only repeat.
    Surface(Error),
    /// The leader is unreachable or its log momentarily unavailable, and the
    /// commit did not land: re-assemble a fresh direct attempt.
    FallBack,
}

/// Drives one forwarded commit to a decision. The transaction is stamped with a
/// fresh id the client owns, so an ambiguous drop is resolved by that id rather
/// than re-driven blind.
pub(crate) async fn forward_commit(
    forward: &Forward,
    ops: &[RowOperation],
    floor: u64,
) -> Forwarded {
    let transaction_id = uuid::Uuid::new_v4().into_bytes();

    match run_session(forward, ops, transaction_id).await {
        Session::Committed(snapshot_id) => Forwarded::Committed(SnapshotId::new(snapshot_id)),
        Session::Surface(err) => Forwarded::Surface(err),
        // The leader answered that its log was momentarily unavailable: the
        // commit did not land, so retreat to direct without aging a reachable
        // leader.
        Session::LogUnavailable => Forwarded::FallBack,
        // The endpoint could not be reached before the commit was sent: age it
        // and retreat. Nothing landed.
        Session::Unreachable => {
            forward.forwarding.age(&forward.endpoint);
            Forwarded::FallBack
        }
        // The connection dropped after the commit was sent: the slot may have
        // landed. Resolve by identity before any retreat, so a landed commit is
        // reported and never re-applied.
        Session::Ambiguous => {
            forward.forwarding.age(&forward.endpoint);
            resolve_ambiguous(forward, transaction_id, floor).await
        }
    }
}

/// Resolves an ambiguous forwarded commit by its id: committed at a snapshot,
/// or absent (safe to retreat to a fresh direct attempt).
async fn resolve_ambiguous(forward: &Forward, transaction_id: [u8; 16], floor: u64) -> Forwarded {
    match slot_commit::transaction_outcome_from(
        &forward.object_store,
        &forward.path,
        &forward.cache,
        &forward.slots,
        transaction_id,
        floor,
    )
    .await
    {
        Ok(Some(snapshot_id)) => {
            debug!(
                snapshot = snapshot_id,
                "ambiguous forwarded commit had landed"
            );
            Forwarded::Committed(SnapshotId::new(snapshot_id))
        }
        Ok(None) => Forwarded::FallBack,
        Err(err) => Forwarded::Surface(err),
    }
}

/// The response classes a forwarded session settles to.
enum Session {
    Committed(u64),
    Surface(Error),
    LogUnavailable,
    Unreachable,
    Ambiguous,
}

/// Runs one forwarded session: connect, handshake, begin, stage, commit. A
/// failure before the commit is sent is `Unreachable`; a drop after it is
/// `Ambiguous`; a leader-answered error is `Surface` or `LogUnavailable`.
async fn run_session(forward: &Forward, ops: &[RowOperation], transaction_id: [u8; 16]) -> Session {
    // An `IP:port` endpoint only; a hostname advert parses as unreachable and is
    // aged. The operator surface may want hostname support later.
    let Ok(address) = forward.endpoint.parse::<SocketAddr>() else {
        warn!(
            endpoint = forward.endpoint,
            "leader advert endpoint unparseable"
        );
        return Session::Unreachable;
    };

    let mut client = match Client::connect(address, CONNECT_TIMEOUT).await {
        Ok(client) => client,
        Err(err) => {
            debug!(endpoint = forward.endpoint, error = %err, "leader unreachable; committing direct");
            return Session::Unreachable;
        }
    };

    let hello = Request::Hello {
        token: forward.secret,
        protocol_version: PROTOCOL_VERSION,
    };
    // A refused or dropped handshake both mean this endpoint cannot serve us
    // now: age it and commit direct.
    match client.request(&hello).await {
        Ok(Response::Ok) => {}
        _ => return Session::Unreachable,
    }

    match client.request(&Request::Begin).await {
        Ok(Response::Ok) => {}
        Ok(Response::Error { kind, message }) => return Session::Surface(map_error(kind, message)),
        Ok(_) => return Session::Surface(unexpected("Begin")),
        Err(_) => return Session::Unreachable,
    }

    for op in ops {
        match client.request(&Request::Stage(wire_operation(op))).await {
            Ok(Response::Ok) => {}
            Ok(Response::Error { kind, message }) => {
                return Session::Surface(map_error(kind, message));
            }
            Ok(_) => return Session::Surface(unexpected("Stage")),
            Err(_) => return Session::Unreachable,
        }
    }

    match client.request(&Request::Commit { transaction_id }).await {
        Ok(Response::Committed { snapshot_id }) => Session::Committed(snapshot_id),
        // The leader reached its log and it was momentarily unavailable: the
        // commit did not land, so retreat to direct.
        Ok(Response::Error {
            kind: ErrorKind::SlotLog,
            ..
        }) => Session::LogUnavailable,
        Ok(Response::Error { kind, message }) => Session::Surface(map_error(kind, message)),
        Ok(_) => Session::Surface(unexpected("Commit")),
        // The commit was sent but no answer arrived: the slot may have landed.
        Err(_) => Session::Ambiguous,
    }
}

/// Maps a forwarded error kind and detail back to moraine's own error, so a
/// surfaced conflict carries the retry substring and a terminal one does not.
fn map_error(kind: ErrorKind, message: String) -> Error {
    match kind {
        ErrorKind::Conflict => Error::CommitConflict(message),
        ErrorKind::RetryBudgetExhausted => Error::RetryBudgetExhausted(message),
        ErrorKind::Corruption => Error::Corruption(message),
        ErrorKind::NotFound => Error::NotFound(message),
        ErrorKind::AlreadyExists => Error::AlreadyExists(message),
        ErrorKind::Constraint => Error::Constraint(message),
        ErrorKind::IndexBuilding => Error::IndexBuilding(message),
        ErrorKind::Configuration => Error::Configuration(message),
        ErrorKind::Fenced => Error::Fenced(message),
        ErrorKind::SlotLog => Error::SlotLog(message),
        ErrorKind::Unsupported => Error::Unsupported(message),
        ErrorKind::SnapshotExpired => Error::SnapshotExpired(message),
        ErrorKind::Interrupted => Error::Interrupted(message),
        ErrorKind::Migration => Error::Migration(message),
        ErrorKind::OpenRaced => Error::OpenRaced(message),
        ErrorKind::Store => Error::Corruption(format!("forwarded store error: {message}")),
    }
}

/// A leader that answered a request with the wrong response shape is a protocol
/// break, terminal by design (no retry substring).
fn unexpected(request: &str) -> Error {
    Error::Corruption(format!(
        "leader answered {request} with an unexpected response"
    ))
}

/// Converts a staged row operation to its wire form. The table rides as its
/// opaque ABI discriminant, which the leader resolves back at its end.
fn wire_operation(operation: &RowOperation) -> WireRowOperation {
    let discriminant = |table: super::TableKind| table as u32;
    match operation {
        RowOperation::Insert { table, cells } => WireRowOperation::Insert {
            table: discriminant(*table),
            cells: wire_cells(cells),
        },
        RowOperation::Delete { table, cells } => WireRowOperation::Delete {
            table: discriminant(*table),
            cells: wire_cells(cells),
        },
        RowOperation::UpdateSetEnd { table, cells } => WireRowOperation::UpdateSetEnd {
            table: discriminant(*table),
            cells: wire_cells(cells),
        },
        RowOperation::UpdateSetBegin { table, cells } => WireRowOperation::UpdateSetBegin {
            table: discriminant(*table),
            cells: wire_cells(cells),
        },
        RowOperation::InlineSchema {
            table_id,
            schema_version,
            arrow_schema,
        } => WireRowOperation::InlineSchema {
            table_id: *table_id,
            schema_version: *schema_version,
            arrow_schema: arrow_schema.clone(),
        },
        RowOperation::InlineInsert {
            table_id,
            schema_version,
            begin_snapshot,
            row_id_start,
            row_count,
            arrow_body,
        } => WireRowOperation::InlineInsert {
            table_id: *table_id,
            schema_version: *schema_version,
            begin_snapshot: *begin_snapshot,
            row_id_start: *row_id_start,
            row_count: *row_count,
            arrow_body: arrow_body.clone(),
        },
        RowOperation::InlineInlineDelete {
            table_id,
            row_id,
            end_snapshot,
        } => WireRowOperation::InlineInlineDelete {
            table_id: *table_id,
            row_id: *row_id,
            end_snapshot: *end_snapshot,
        },
        RowOperation::InlineFileDelete {
            table_id,
            data_file_id,
            row_id,
            begin_snapshot,
        } => WireRowOperation::InlineFileDelete {
            table_id: *table_id,
            data_file_id: *data_file_id,
            row_id: *row_id,
            begin_snapshot: *begin_snapshot,
        },
        RowOperation::InlineFileDeleteRemove {
            table_id,
            data_file_id,
            row_id,
        } => WireRowOperation::InlineFileDeleteRemove {
            table_id: *table_id,
            data_file_id: *data_file_id,
            row_id: *row_id,
        },
        RowOperation::InlineFlushDelete {
            table_id,
            schema_version,
            flush_snapshot,
        } => WireRowOperation::InlineFlushDelete {
            table_id: *table_id,
            schema_version: *schema_version,
            flush_snapshot: *flush_snapshot,
        },
        RowOperation::InlineDrop { table_id } => WireRowOperation::InlineDrop {
            table_id: *table_id,
        },
        RowOperation::InlineSchemaDrop {
            table_id,
            schema_version,
        } => WireRowOperation::InlineSchemaDrop {
            table_id: *table_id,
            schema_version: *schema_version,
        },
    }
}

fn wire_cells(cells: &[Cell]) -> Vec<WireCell> {
    cells.iter().map(wire_cell).collect()
}

fn wire_cell(cell: &Cell) -> WireCell {
    match cell {
        Cell::Null => WireCell::Null,
        Cell::U64(value) => WireCell::U64(*value),
        Cell::I64(value) => WireCell::I64(*value),
        Cell::Bool(value) => WireCell::Bool(*value),
        Cell::Str(value) => WireCell::Str(value.clone()),
    }
}
