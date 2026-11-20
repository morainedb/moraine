# RFC 0013: Browser build

## Summary

The extension builds for DuckDB-Wasm as a read-only catalog. The same core
and shim compile for `wasm32-unknown-emscripten`; what differs is the HTTP
transport (the host's synchronous request instead of a socket stack), the
runtime (one thread, advancing only inside a call), and the attach surface
(read-only, over `s3://` or `memory://`). Nothing stored changes: a lake
written natively is read in the browser from the same bucket.

## Scope

The build targets the `wasm_mvp` and `wasm_eh` platforms of the extension
distribution. `wasm_threads` stays excluded: Rust's shipped standard library
for emscripten carries no atomics, so a threaded runtime cannot link.

A browser process never writes to the lake. `StoreOptions::open_writer` in
`crates/moraine/src/store/open.rs` returns `Error::Unsupported` on
emscripten, naming the platform and `READ_ONLY`. Every writer path opens
through it: a read-write attach, migration, moving the WAL store, rewriting
the data path, compacting the store, and checkpoint management. A reader over
a commit log keeps a managed checkpoint, which is a write, so
`open_reader_mode` refuses a reader session on emscripten too; the browser
reader follows the latest manifest or a pinned checkpoint and issues only
`GET` and `LIST`.

`StoreKind::from_path` in `crates/moraine-duckdb/src/abi/attach.rs` accepts
`s3://` and `memory://` on emscripten and refuses a local path with
`INVALID_ARGUMENT`: the emscripten filesystem is neither persistent nor the
one DuckDB-Wasm serves its own files from.

## Dependencies

The dependency graph compiles for emscripten through target tables rather
than a feature:

| Crate | Native | Emscripten |
|---|---|---|
| `object_store` in `moraine-duckdb` | `aws` (reqwest, hyper, rustls) | `aws-base`: the S3 request logic alone |
| `tokio` in `moraine-duckdb` | `rt-multi-thread`, `sync`, `macros` | `rt`, `sync`, `macros` |
| `slatedb` (workspace) | `default-features = false`, `foyer` | same |

`slatedb` must not enable tokio `signal` or `rt-multi-thread`, which tokio
refuses on emscripten, and `slatedb` and `slatedb-mirror` must take `backon`
without its `gloo-timers-sleep` default, whose `wasm-bindgen` glue has no
emscripten runtime. A slatedb release lacking either cannot be pinned for
this build.

## HTTP transport

A browser has no sockets and performs its own TLS. The S3 client is therefore
built on `object_store`'s `HttpConnector` seam with a connector that hands
each request to the host.

`crates/moraine-duckdb/src/abi/http_transport.rs` defines the ABI and the
connector. The host registers once per process, before the first attach:

```c
typedef struct MoraineHttpRequest {
  const char *method;                 // "GET", "HEAD", ...
  const char *url;                    // absolute, percent-encoded
  const char *const *header_names;    // request headers, NUL-terminated
  const char *const *header_values;
  uintptr_t header_count;
  const uint8_t *body;                // may be null when body_len is 0
  uintptr_t body_len;
} MoraineHttpRequest;

typedef void (*MoraineHttpReceiver)(void *output, uint16_t status,
    const char *const *header_names, const char *const *header_values,
    uintptr_t header_count, const uint8_t *body, uintptr_t body_len);

typedef struct MoraineHttpTransport {
  void *context;
  // Performs one request to completion. Calls `receiver` exactly once with
  // the response before returning true; returns false when no response
  // arrived (network failure, blocked by CORS). Must not unwind.
  bool (*send)(void *context, const MoraineHttpRequest *request,
      MoraineHttpReceiver receiver, void *output);
} MoraineHttpTransport;

int32_t moraine_register_http_transport(MoraineHttpTransport transport,
    MoraineError *err);
```

`HostHttpConnector` implements `HttpConnector`; its `HttpService` lowers an
`HttpRequest` to one `send`, buffering the body, and raises the reply to an
`HttpResponse` with the status and headers as received. A `false` from `send`
is `HttpErrorKind::Connect`. Status codes pass through untouched, so
`object_store`'s retry policy, error classification, and range handling are
the native ones. The connector is compiled on every platform, so its tests
run natively; only the emscripten S3 builder in `attach.rs` installs it, with
`with_http_connector`. Registering on another platform records the transport
and nothing consults it. Attaching an `s3://` store on emscripten before a
transport is registered fails with `INVALID_ARGUMENT`.

The shim's transport is `cpp/http_transport.cpp`, compiled only under
`__EMSCRIPTEN__`. `send` is a synchronous `XMLHttpRequest` with an
`arraybuffer` response, issued through `EM_JS`. A synchronous request is legal
in the Web Worker DuckDB-Wasm runs in and nowhere else. It is registered from
`LoadInternal` in `cpp/moraine_extension.cpp` before any attach.

Response headers reach the client only if the bucket's CORS policy exposes
them. A bucket serving a browser reader allows `GET` and `HEAD` from the page's
origin and exposes `ETag`, `Content-Length`, `Content-Range`, and
`Last-Modified`. Credentials come from a DuckDB secret, as natively; the
environment credential chain is empty in a browser.

## Runtime

`new_runtime` in `crates/moraine-duckdb/src/runtime.rs` builds a
`current_thread` runtime on emscripten, timers enabled; the host thread count
is ignored. Entry points still `block_on` it from the calling thread. Because
the transport completes each request before returning, every store future
makes progress on that thread, and SlateDB's manifest poll and the cache's
tasks run only while some call is inside `block_on`. The interrupt probe loop
and the statement deadline are timers and work unchanged, with one coarser
grain: a request in flight cannot be interrupted, so a cancel takes effect
after the current response.

`crates/moraine/src/blocking.rs` provides `offload`, which runs CPU work on
tokio's blocking pool natively and inline on emscripten. It is the one way the
core leaves the async context, and the read path's three offloads use it:
row-summary encoding and positioned row sets in `data_file/row_location.rs`
and index encoding in `data_file/metrics.rs`. The S3 credential refresh in
`abi/s3_credentials.rs` makes the same choice at its own site.

Logging starts no delivery thread on emscripten. `flush_delivery` in
`crates/moraine-duckdb/src/logging.rs` delivers the queue inline on that
platform, so the shim's drain at each operation boundary carries every record
and nothing waits on a thread that does not exist.

## Build

`CMakeLists.txt` sets `Rust_CARGO_TARGET` to `wasm32-unknown-emscripten`
before Corrosion imports the crate when `EMSCRIPTEN` is set, since Corrosion
infers no target for that toolchain, and links no platform libraries there.
`http_transport.cpp` joins the shim sources under `EMSCRIPTEN`. The
distribution lists in `description.yml`, `.github/workflows/extension.yml`,
and `.github/workflows/release.yml` admit `wasm_mvp` and `wasm_eh` once the
pinned slatedb satisfies the dependency constraints above, and exclude
`wasm_threads` and the Windows platforms.

## Tests

- `HostHttpConnector` round-trips method, URL, headers, and body through a
  transport implemented in Rust, and a `false` from `send` is a connect error.
- An S3 attach on emscripten without a registered transport is refused before
  any request.
- `offload` returns the closure's value on both platforms.
- `flush_delivery` with no delivery thread delivers queued records to the
  registered sink.

## Alternatives

- **DuckDB's own filesystem as the object store.** It would reuse DuckDB-Wasm's
  HTTP layer, but SlateDB needs `LIST` with sizes and `object_store`'s error
  classification, and the catalog would then depend on which filesystem the
  host loaded. The connector keeps `object_store`'s S3 client whole.
- **Asyncify.** Lets a blocking call yield to the event loop, but multiplies
  code size and does not apply to a side module loaded into DuckDB-Wasm's
  already-built main module.
- **`wasm_threads` first.** A threaded build would keep the native runtime,
  but needs a standard library built with atomics and the less common
  cross-origin-isolated DuckDB-Wasm bundle. It can follow once the shipped
  toolchain supports it.
- **Write support.** Conditional puts work over the same transport, but
  flushing and compaction would then run only inside calls, which changes
  commit latency and needs its own design.
