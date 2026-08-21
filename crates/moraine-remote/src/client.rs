//! The forwarding client: one TCP connection carrying one session. Each
//! [`Client::request`] writes one length-prefixed framed [`Request`] and reads
//! one framed [`Response`]. A dropped connection ends the session; a caller
//! reconnects with a fresh [`Client::connect`].

use std::{net::SocketAddr, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

use crate::{
    error::{Error, Result},
    frame,
    protocol::{Request, Response},
};

/// One forwarded catalog session over a TCP connection.
#[derive(Debug)]
pub struct Client {
    stream: TcpStream,
}

impl Client {
    /// Connects to a leader, failing with [`Error::ConnectTimeout`] if the
    /// connection does not establish within `connect_timeout`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ConnectTimeout`] on deadline, or [`Error::Transport`]
    /// if the connection is refused or otherwise fails.
    pub async fn connect(addr: SocketAddr, connect_timeout: Duration) -> Result<Self> {
        let stream = timeout(connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| Error::ConnectTimeout)?
            .map_err(Error::Transport)?;

        Ok(Self { stream })
    }

    /// Sends one request and reads its response on this session's connection.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the connection fails mid-exchange, or
    /// [`Error::Protocol`] if the response cannot be framed or decoded.
    pub async fn request(&mut self, request: &Request) -> Result<Response> {
        write_message(&mut self.stream, &request.encode()).await?;
        let framed = read_message(&mut self.stream).await?;
        Response::decode(&framed)
    }
}

/// Writes a length-prefixed framed message: a 4-byte big-endian length, then
/// the framed bytes.
pub(crate) async fn write_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    framed: &[u8],
) -> Result<()> {
    let len = u32::try_from(framed.len())
        .map_err(|_| Error::Protocol("message exceeds the 4-byte length prefix".to_string()))?;
    writer
        .write_all(&len.to_be_bytes())
        .await
        .map_err(Error::Transport)?;
    writer.write_all(framed).await.map_err(Error::Transport)?;
    writer.flush().await.map_err(Error::Transport)?;

    Ok(())
}

/// Reads one length-prefixed framed message, returning its framed bytes.
pub(crate) async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    reader
        .read_exact(&mut len_bytes)
        .await
        .map_err(Error::Transport)?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > frame::MAX_FRAME_LEN {
        return Err(Error::Protocol(format!(
            "framed message length {len} exceeds the {} byte ceiling",
            frame::MAX_FRAME_LEN
        )));
    }

    let mut framed = vec![0u8; len];
    reader
        .read_exact(&mut framed)
        .await
        .map_err(Error::Transport)?;

    Ok(framed)
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use super::*;
    use crate::protocol::{ErrorKind, WireCell, WireRowOperation};

    /// A stub leader that reads framed requests and replies with the canned
    /// response its scripted policy dictates, one connection per session.
    async fn serve_session(mut stream: TcpStream) -> Result<()> {
        loop {
            let framed = match read_message(&mut stream).await {
                Ok(framed) => framed,
                // A client that closed its half ends the session.
                Err(Error::Transport(_)) => return Ok(()),
                Err(err) => return Err(err),
            };
            let request = Request::decode(&framed)?;
            let response = match request {
                Request::Hello { .. } | Request::Begin | Request::Stage(_) => Response::Ok,
                Request::Commit { .. } => Response::Committed { snapshot_id: 42 },
                Request::Snapshot | Request::VisibleSnapshots | Request::ScheduledDeletions => {
                    Response::Snapshot(vec![b"row".to_vec()])
                }
                Request::Rollback => Response::Error {
                    kind: ErrorKind::Conflict,
                    message: "raced".to_string(),
                },
            };
            write_message(&mut stream, &response.encode()).await?;
        }
    }

    async fn spawn_stub() -> SocketAddr {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let _ = serve_session(stream).await;
                });
            }
        });

        addr
    }

    #[tokio::test]
    async fn a_full_session_against_the_stub() {
        let addr = spawn_stub().await;
        let mut client = Client::connect(addr, Duration::from_secs(5)).await.unwrap();

        assert_eq!(
            client
                .request(&Request::Hello {
                    token: [7; 32],
                    protocol_version: 1,
                })
                .await
                .unwrap(),
            Response::Ok
        );
        assert_eq!(client.request(&Request::Begin).await.unwrap(), Response::Ok);
        assert_eq!(
            client
                .request(&Request::Stage(WireRowOperation::Insert {
                    table: 3,
                    cells: vec![WireCell::U64(1), WireCell::Str("orders".to_string())],
                }))
                .await
                .unwrap(),
            Response::Ok
        );
        assert_eq!(
            client
                .request(&Request::Commit {
                    transaction_id: [0; 16],
                })
                .await
                .unwrap(),
            Response::Committed { snapshot_id: 42 }
        );
    }

    #[tokio::test]
    async fn a_read_returns_snapshot_rows() {
        let addr = spawn_stub().await;
        let mut client = Client::connect(addr, Duration::from_secs(5)).await.unwrap();
        assert_eq!(
            client.request(&Request::VisibleSnapshots).await.unwrap(),
            Response::Snapshot(vec![b"row".to_vec()])
        );
    }

    #[tokio::test]
    async fn connect_times_out_against_an_unreachable_address() {
        // A TEST-NET-1 address (RFC 5737) that never answers: connect must
        // surface the deadline rather than hang.
        let addr: SocketAddr = "192.0.2.1:9".parse().unwrap();
        let err = Client::connect(addr, Duration::from_millis(50))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ConnectTimeout), "{err:?}");
    }

    #[tokio::test]
    async fn a_dropped_session_reconnects() {
        let addr = spawn_stub().await;

        let mut first = Client::connect(addr, Duration::from_secs(5)).await.unwrap();
        assert_eq!(first.request(&Request::Begin).await.unwrap(), Response::Ok);
        drop(first);

        let mut second = Client::connect(addr, Duration::from_secs(5)).await.unwrap();
        assert_eq!(
            second
                .request(&Request::Commit {
                    transaction_id: [0; 16],
                })
                .await
                .unwrap(),
            Response::Committed { snapshot_id: 42 }
        );
    }

    #[tokio::test]
    async fn a_forwarded_conflict_carries_the_retry_substring() {
        let addr = spawn_stub().await;
        let mut client = Client::connect(addr, Duration::from_secs(5)).await.unwrap();
        let Response::Error { kind, message } = client.request(&Request::Rollback).await.unwrap()
        else {
            panic!("stub replies to Rollback with an error");
        };
        assert!(kind.render(&message).contains("conflict"));
    }
}
