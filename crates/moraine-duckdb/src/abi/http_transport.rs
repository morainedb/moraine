//! The HTTP transport a host supplies where the process has no socket
//! stack. The browser build's S3 client sends every request through it; a
//! native build records a registration and never consults it.

use std::{
    ffi::{CStr, CString, c_char, c_void},
    sync::RwLock,
};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{
    ClientOptions,
    client::{
        HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse,
        HttpResponseBody, HttpService,
    },
};

use super::guard;
use crate::error::{AbiError, MoraineError, codes};

/// Mirrors the C `MoraineHttpRequest`: one request for the host to perform.
/// Strings are NUL-terminated and, with the body, valid until `send`
/// returns.
#[repr(C)]
pub struct MoraineHttpRequest {
    /// The HTTP method, upper-case.
    pub method: *const c_char,
    /// The absolute, percent-encoded URL.
    pub url: *const c_char,
    /// Request header names, one per `header_values` entry.
    pub header_names: *const *const c_char,
    /// Request header values, in `header_names` order.
    pub header_values: *const *const c_char,
    /// The number of headers.
    pub header_count: usize,
    /// The request body; may be null when `body_len` is zero.
    pub body: *const u8,
    /// The request body length in bytes.
    pub body_len: usize,
}

/// Receives the response to one request. Strings are NUL-terminated and,
/// with the body, valid until this call returns.
pub type MoraineHttpReceiver = unsafe extern "C" fn(
    output: *mut c_void,
    status: u16,
    header_names: *const *const c_char,
    header_values: *const *const c_char,
    header_count: usize,
    body: *const u8,
    body_len: usize,
);

/// Performs one request to completion. Calls `receiver` exactly once with
/// the response before returning true; returns false when no response
/// arrived. Must not unwind. Callable from any thread for the process's
/// lifetime.
pub type MoraineHttpSend = Option<
    unsafe extern "C" fn(
        context: *mut c_void,
        request: *const MoraineHttpRequest,
        receiver: MoraineHttpReceiver,
        output: *mut c_void,
    ) -> bool,
>;

/// A host transport, registered once per process before the first attach.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MoraineHttpTransport {
    /// Opaque state passed to `send`.
    pub context: *mut c_void,
    /// The request function; null is refused at registration.
    pub send: MoraineHttpSend,
}

// SAFETY: the transport contract makes `send` callable from any thread.
unsafe impl Send for MoraineHttpTransport {}
// SAFETY: the transport contract permits concurrent `send` calls.
unsafe impl Sync for MoraineHttpTransport {}

fn registry() -> &'static RwLock<Option<MoraineHttpTransport>> {
    static REGISTRY: RwLock<Option<MoraineHttpTransport>> = RwLock::new(None);
    &REGISTRY
}

/// Registers `transport` as the process's HTTP transport, replacing any
/// earlier one. A null `send` is `INVALID_ARGUMENT`.
///
/// # Safety
///
/// `transport` must obey the contract on [`MoraineHttpSend`] for the
/// process's lifetime. `err`, if non-null, must be a valid, writable
/// [`MoraineError`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_register_http_transport(
    transport: MoraineHttpTransport,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if transport.send.is_none() {
            return Err(AbiError::invalid_argument(
                "moraine_register_http_transport: send must not be null",
            ));
        }
        let mut slot = registry()
            .write()
            .map_err(|_| AbiError::new(codes::INTERNAL, "HTTP transport registry poisoned"))?;
        *slot = Some(transport);
        Ok(())
    };
    // SAFETY: error-slot validity is the caller's contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// The connector over the registered transport, or `None` before any
/// registration.
#[must_use]
pub fn registered() -> Option<HostHttpConnector> {
    let transport = (*registry().read().ok()?)?;
    Some(HostHttpConnector { transport })
}

/// An `object_store` connector whose clients perform requests through a
/// host transport.
#[derive(Debug, Clone, Copy)]
pub struct HostHttpConnector {
    transport: MoraineHttpTransport,
}

impl std::fmt::Debug for MoraineHttpTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MoraineHttpTransport")
    }
}

impl HostHttpConnector {
    /// A connector over `transport`, for a host that does not register.
    #[must_use]
    pub fn new(transport: MoraineHttpTransport) -> Self {
        Self { transport }
    }
}

impl HttpConnector for HostHttpConnector {
    fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(*self))
    }
}

/// What the receiver collected from one response.
#[derive(Default)]
struct Reply {
    status: u16,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    body: Vec<u8>,
}

#[derive(Debug)]
enum TransportError {
    NoResponse,
    InvalidHeader(String),
    NonContiguousBody,
    InvalidStatus(u16),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoResponse => formatter.write_str("the host transport returned no response"),
            Self::InvalidHeader(name) => write!(formatter, "header `{name}` cannot cross the ABI"),
            Self::NonContiguousBody => {
                formatter.write_str("a multi-part request body cannot cross the ABI")
            }
            Self::InvalidStatus(status) => write!(formatter, "invalid HTTP status {status}"),
        }
    }
}

impl std::error::Error for TransportError {}

fn request_error(error: TransportError) -> HttpError {
    HttpError::new(HttpErrorKind::Request, error)
}

/// Lowers `request` to C strings. Each header name and value must be free
/// of interior NULs, which HTTP header syntax already forbids.
struct LoweredRequest {
    method: CString,
    url: CString,
    header_names: Vec<CString>,
    header_values: Vec<CString>,
    body: Bytes,
}

impl LoweredRequest {
    fn new(request: HttpRequest) -> Result<Self, HttpError> {
        let (parts, body) = request.into_parts();
        let method = CString::new(parts.method.as_str())
            .map_err(|_| request_error(TransportError::InvalidHeader("method".into())))?;
        let url = CString::new(parts.uri.to_string())
            .map_err(|_| request_error(TransportError::InvalidHeader("url".into())))?;
        let mut header_names = Vec::with_capacity(parts.headers.len());
        let mut header_values = Vec::with_capacity(parts.headers.len());
        for (name, value) in &parts.headers {
            let invalid = || request_error(TransportError::InvalidHeader(name.to_string()));
            header_names.push(CString::new(name.as_str()).map_err(|_| invalid())?);
            header_values.push(CString::new(value.as_bytes()).map_err(|_| invalid())?);
        }
        let body = if body.is_empty() {
            Bytes::new()
        } else {
            body.as_bytes()
                .cloned()
                .ok_or_else(|| request_error(TransportError::NonContiguousBody))?
        };
        Ok(Self {
            method,
            url,
            header_names,
            header_values,
            body,
        })
    }
}

#[async_trait]
impl HttpService for HostHttpConnector {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let lowered = LoweredRequest::new(request)?;
        let names: Vec<*const c_char> = lowered.header_names.iter().map(|s| s.as_ptr()).collect();
        let values: Vec<*const c_char> = lowered.header_values.iter().map(|s| s.as_ptr()).collect();
        let c_request = MoraineHttpRequest {
            method: lowered.method.as_ptr(),
            url: lowered.url.as_ptr(),
            header_names: names.as_ptr(),
            header_values: values.as_ptr(),
            header_count: names.len(),
            body: lowered.body.as_ptr(),
            body_len: lowered.body.len(),
        };

        let Some(send) = self.transport.send else {
            return Err(request_error(TransportError::NoResponse));
        };
        let mut reply: Option<Reply> = None;
        // SAFETY: the transport contract; every pointer in `c_request` and
        // the output slot outlive this call, and `receive` reads them only
        // while it runs.
        let answered = unsafe {
            send(
                self.transport.context,
                &raw const c_request,
                receive,
                (&raw mut reply).cast(),
            )
        };
        let (true, Some(reply)) = (answered, reply) else {
            return Err(HttpError::new(
                HttpErrorKind::Connect,
                TransportError::NoResponse,
            ));
        };

        let mut response = http::Response::builder()
            .status(reply.status)
            .body(HttpResponseBody::from(Bytes::from(reply.body)))
            .map_err(|_| request_error(TransportError::InvalidStatus(reply.status)))?;
        let headers = response.headers_mut();
        for (name, value) in reply.headers {
            let (Ok(name), Ok(value)) = (
                http::HeaderName::from_bytes(&name),
                http::HeaderValue::from_bytes(&value),
            ) else {
                // A header the client cannot represent is one it never asked for.
                continue;
            };
            headers.append(name, value);
        }
        Ok(response)
    }
}

/// Collects one response into the `Option<Reply>` behind `output`.
unsafe extern "C" fn receive(
    output: *mut c_void,
    status: u16,
    header_names: *const *const c_char,
    header_values: *const *const c_char,
    header_count: usize,
    body: *const u8,
    body_len: usize,
) {
    let mut reply = Reply {
        status,
        ..Reply::default()
    };
    if header_count > 0 && !header_names.is_null() && !header_values.is_null() {
        // SAFETY: the receiver contract; both arrays hold `header_count`
        // NUL-terminated strings valid for this call.
        let (names, values) = unsafe {
            (
                std::slice::from_raw_parts(header_names, header_count),
                std::slice::from_raw_parts(header_values, header_count),
            )
        };
        for (&name, &value) in names.iter().zip(values) {
            if name.is_null() || value.is_null() {
                continue;
            }
            // SAFETY: as above.
            let (name, value) = unsafe { (CStr::from_ptr(name), CStr::from_ptr(value)) };
            reply
                .headers
                .push((name.to_bytes().to_vec(), value.to_bytes().to_vec()));
        }
    }
    if body_len > 0 && !body.is_null() {
        // SAFETY: the receiver contract; `body` holds `body_len` bytes for
        // this call.
        reply.body = unsafe { std::slice::from_raw_parts(body, body_len) }.to_vec();
    }
    // SAFETY: `call` passes the address of its `Option<Reply>` as `output`.
    unsafe { *output.cast::<Option<Reply>>() = Some(reply) };
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::{CStr, c_char, c_void},
        sync::Mutex,
    };

    use object_store::client::HttpErrorKind;

    use super::*;

    /// What the fake transport saw of the last request.
    #[derive(Default, Debug)]
    struct Seen {
        method: String,
        url: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    fn seen() -> &'static Mutex<Seen> {
        static SEEN: Mutex<Seen> = Mutex::new(Seen {
            method: String::new(),
            url: String::new(),
            headers: Vec::new(),
            body: Vec::new(),
        });
        &SEEN
    }

    unsafe fn text(pointer: *const c_char) -> String {
        // SAFETY: test strings are NUL-terminated and live for the call.
        unsafe { CStr::from_ptr(pointer) }
            .to_string_lossy()
            .into_owned()
    }

    unsafe extern "C" fn answer_partial_content(
        _context: *mut c_void,
        request: *const MoraineHttpRequest,
        receiver: MoraineHttpReceiver,
        output: *mut c_void,
    ) -> bool {
        // SAFETY: the connector passes a live request for this call.
        let request = unsafe { &*request };
        let mut record = Seen {
            // SAFETY: as above.
            method: unsafe { text(request.method) },
            // SAFETY: as above.
            url: unsafe { text(request.url) },
            ..Seen::default()
        };
        for index in 0..request.header_count {
            // SAFETY: `header_count` entries are live for this call.
            let (name, value) = unsafe {
                (
                    text(*request.header_names.add(index)),
                    text(*request.header_values.add(index)),
                )
            };
            record.headers.push((name, value));
        }
        if request.body_len > 0 {
            // SAFETY: `body_len` bytes are live for this call.
            record.body =
                unsafe { std::slice::from_raw_parts(request.body, request.body_len) }.to_vec();
        }
        *seen().lock().unwrap() = record;

        let names = [c"ETag".as_ptr(), c"Content-Range".as_ptr()];
        let values = [c"\"abc\"".as_ptr(), c"bytes 0-4/100".as_ptr()];
        let body = b"hello";
        // SAFETY: every pointer outlives the receiver call.
        unsafe {
            receiver(
                output,
                206,
                names.as_ptr(),
                values.as_ptr(),
                2,
                body.as_ptr(),
                body.len(),
            );
        }
        true
    }

    unsafe extern "C" fn answer_nothing(
        _context: *mut c_void,
        _request: *const MoraineHttpRequest,
        _receiver: MoraineHttpReceiver,
        _output: *mut c_void,
    ) -> bool {
        false
    }

    fn transport(send: MoraineHttpSend) -> MoraineHttpTransport {
        MoraineHttpTransport {
            context: std::ptr::null_mut(),
            send,
        }
    }

    /// Method, URL, headers, and body reach the transport; status, headers,
    /// and body come back as the response.
    #[tokio::test]
    async fn a_request_round_trips_through_the_transport() {
        let connector = HostHttpConnector::new(transport(Some(answer_partial_content)));
        let request = http::Request::builder()
            .method("GET")
            .uri("https://bucket.s3.example/key?versionId=1")
            .header("Range", "bytes=0-4")
            .body(object_store::client::HttpRequestBody::from(
                Bytes::from_static(b"payload"),
            ))
            .unwrap();

        let response = connector.call(request).await.unwrap();

        {
            let seen = seen().lock().unwrap();
            assert_eq!(seen.method, "GET");
            assert_eq!(seen.url, "https://bucket.s3.example/key?versionId=1");
            assert_eq!(
                seen.headers,
                vec![("range".to_owned(), "bytes=0-4".to_owned())]
            );
            assert_eq!(seen.body, b"payload");
        }
        assert_eq!(response.status(), 206);
        assert_eq!(response.headers()["etag"], "\"abc\"");
        assert_eq!(response.headers()["content-range"], "bytes 0-4/100");
        let body = response.into_body().bytes().await.unwrap();
        assert_eq!(&body[..], b"hello");
    }

    /// A transport that produced no response is a connect error, which the
    /// client retries.
    #[tokio::test]
    async fn no_response_is_a_connect_error() {
        let connector = HostHttpConnector::new(transport(Some(answer_nothing)));
        let request = http::Request::builder()
            .method("GET")
            .uri("https://bucket.s3.example/key")
            .body(object_store::client::HttpRequestBody::empty())
            .unwrap();

        let error = connector.call(request).await.unwrap_err();

        assert_eq!(error.kind(), HttpErrorKind::Connect);
    }

    /// Registration refuses a null `send` and otherwise makes the transport
    /// available to the connector.
    #[test]
    fn registration_requires_a_send_function() {
        let mut error = MoraineError::default();
        let code =
            // SAFETY: a valid error slot; the transport is never called.
            unsafe { moraine_register_http_transport(transport(None), &raw mut error) };
        assert_eq!(code, codes::INVALID_ARGUMENT);

        // SAFETY: a null error slot is permitted; the transport is never called.
        let code = unsafe {
            moraine_register_http_transport(transport(Some(answer_nothing)), std::ptr::null_mut())
        };
        assert_eq!(code, codes::OK);
        assert!(registered().is_some());
    }
}
