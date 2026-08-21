//! The crate's error type: two failure domains, wire protocol and transport.

/// Errors returned by the forwarded-session protocol and its client.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A message could not be framed or decoded: bad magic, an unsupported
    /// encoding version, a truncated or malformed protobuf, or an unknown
    /// enum discriminant.
    #[error("remote protocol: {0}")]
    Protocol(String),

    /// The connection failed: an I/O error reading or writing the stream.
    #[error("remote transport: {0}")]
    Transport(#[source] std::io::Error),

    /// A connection attempt did not complete within its deadline.
    #[error("remote connect timed out")]
    ConnectTimeout,
}

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;
