//! The wire protocol for one forwarded catalog session on a leader: the
//! messages, their framing and codec, and a [`Client`] that carries one
//! session over a TCP connection.
//!
//! The protocol mirrors moraine's staged-row ABI — [`Request`] and
//! [`Response`] carry rows and reads, never SQL — because the leader and the
//! client own both ends. This crate names nothing in moraine: the dependency
//! points the other way, so [`WireRowOperation`] and [`WireCell`] mirror
//! moraine's own `RowOperation`/`Cell` shape and moraine converts at each end.
//!
//! # A forwarded failure composes with a SQL retry loop
//!
//! DuckLake's commit loop dispatches its retry decision on the *text* of an
//! error, keying on the substrings `conflict`, `concurrent`, `unique`, and
//! `primary key`. So the protocol carries a typed [`ErrorKind`] and renders it
//! to text *client-side*, byte-for-byte matching moraine's core error
//! `Display`. A forwarded conflict then reads to the retry loop exactly as a
//! direct one does, and a terminal failure — an exhausted retry budget, an
//! unreachable log — carries none of the substrings, so it surfaces at once.
//!
//! ```
//! use moraine_remote::{ErrorKind, Request, Response, WireCell, WireRowOperation};
//!
//! // A staged insert forwarded to a leader survives a codec round trip.
//! let staged = Request::Stage(WireRowOperation::Insert {
//!     table: 3,
//!     cells: vec![WireCell::U64(1), WireCell::Str("orders".to_string())],
//! });
//! assert_eq!(Request::decode(&staged.encode()).unwrap(), staged);
//!
//! // A forwarded conflict renders text a SQL retry loop retries on; a
//! // terminal failure renders text it does not.
//! let conflict = Response::Error {
//!     kind: ErrorKind::Conflict,
//!     message: "raced".to_string(),
//! };
//! let Response::Error { kind, message } = Response::decode(&conflict.encode()).unwrap() else {
//!     unreachable!()
//! };
//! assert!(kind.render(&message).contains("conflict"));
//! assert!(
//!     !ErrorKind::SlotLog
//!         .render("bucket unreachable")
//!         .contains("conflict")
//! );
//! ```
//!
//! # Transport
//!
//! Length-prefixed protobuf over TCP: a 4-byte big-endian length, then a
//! framed message (a 4-byte magic, a 1-byte encoding version, the protobuf
//! payload). One connection is one session — a forwarded `BEGIN…COMMIT` pins
//! to its leader with no affinity rules, and a dropped connection ends the
//! session so a retry reconnects as a fresh transaction.
//!
//! # Layering
//!
//! - `protocol` — the messages, their codec, and the typed error kinds.
//! - `frame` — the magic and encoding version every framed message opens with.
//! - `client` — the session client and the length-prefixed transport.

#![forbid(unsafe_code)]

mod client;
mod error;
mod frame;
mod proto;
mod protocol;

pub use client::Client;
pub use error::Error;
pub use protocol::{ErrorKind, Request, Response, TOKEN_LEN, TXID_LEN, WireCell, WireRowOperation};
