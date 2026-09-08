//! Catalog attachment, configuration, migration, and teardown.

use std::{
    ffi::{CStr, c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
    sync::Arc,
};

use moraine::CatalogOptions;
use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory};
use tracing::warn;

use super::{free_c_string, guard, to_c_string};
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::{
        AttachedCatalog, CANCELLED_ATTACH_SHUTDOWN, MoraineCatalogHandle, MoraineInterruptProbe,
        block_on_cancellable_in, new_runtime,
    },
};

/// Mirrors the C `MoraineS3Config`: S3 credentials for an `s3://` store,
/// sourced from a DuckDB secret. Null/empty fields fall back to the AWS_*
/// environment; `use_ssl` is -1 unset, 0 false, 1 true.
#[repr(C)]
pub struct MoraineS3Config {
    /// AWS access key id.
    pub key_id: *const c_char,
    /// AWS secret access key.
    pub secret: *const c_char,
    /// AWS region.
    pub region: *const c_char,
    /// AWS session token, for temporary credentials.
    pub session_token: *const c_char,
    /// Endpoint URL for S3-compatible stores (e.g. MinIO).
    pub endpoint: *const c_char,
    /// Addressing style: `"path"` or `"vhost"`.
    pub url_style: *const c_char,
    /// TLS toggle: -1 unset, 0 plain HTTP, 1 HTTPS.
    pub use_ssl: i32,
}

/// S3 credentials borrowed from a [`MoraineS3Config`]. Every field is
/// optional; an absent field defers to the AWS_* environment.
pub(crate) struct S3Creds<'a> {
    key_id: Option<&'a str>,
    secret: Option<&'a str>,
    region: Option<&'a str>,
    session_token: Option<&'a str>,
    endpoint: Option<&'a str>,
    url_style: Option<&'a str>,
    use_ssl: Option<bool>,
}

/// Borrows the credentials out of a nullable [`MoraineS3Config`]. Null
/// means "no secret — the environment supplies credentials".
///
/// # Safety
///
/// `s3`, if non-null, must point to a valid [`MoraineS3Config`] whose
/// non-null string fields are NUL-terminated C strings, all valid for
/// reads for the duration of the borrow.
pub(crate) unsafe fn borrow_s3_creds<'a>(s3: *const MoraineS3Config) -> Option<S3Creds<'a>> {
    // SAFETY: caller contract above.
    let config = unsafe { s3.as_ref() }?;
    // SAFETY: each string field is null or a NUL-terminated C string valid
    // for the borrow, per the same contract.
    Some(unsafe {
        S3Creds {
            key_id: opt_str(config.key_id),
            secret: opt_str(config.secret),
            region: opt_str(config.region),
            session_token: opt_str(config.session_token),
            endpoint: opt_str(config.endpoint),
            url_style: opt_str(config.url_style),
            use_ssl: match config.use_ssl {
                0 => Some(false),
                1 => Some(true),
                _ => None,
            },
        }
    })
}

/// Borrows a nullable C string as `Some(&str)`, mapping null, empty, and
/// non-UTF-8 to `None`. For S3 secret fields; paths use
/// [`opt_borrow_str`], which errors on bad UTF-8.
///
/// # Safety
///
/// `ptr`, if non-null, must point to a NUL-terminated C string valid for
/// reads for the duration of this call.
unsafe fn opt_str<'a>(ptr: *const c_char) -> Option<&'a str> {
    if ptr.is_null() {
        return None;
    }
    // SAFETY: caller contract; non-null checked above.
    let s = unsafe { CStr::from_ptr(ptr) }.to_str().ok()?;
    (!s.is_empty()).then_some(s)
}

/// Borrows a nullable C string as `Some(&str)`: null and empty mean "not
/// given", but invalid UTF-8 fails the call.
///
/// # Safety
///
/// `ptr`, if non-null, must point to a NUL-terminated C string valid for
/// reads for the duration of this call.
pub(crate) unsafe fn opt_borrow_str<'a>(
    ptr: *const c_char,
    arg_name: &str,
) -> Result<Option<&'a str>, AbiError> {
    if ptr.is_null() {
        return Ok(None);
    }
    // SAFETY: caller contract above.
    let s = unsafe { borrow_str(ptr, arg_name) }?;
    Ok((!s.is_empty()).then_some(s))
}

/// The object store an attach path resolves to.
pub(crate) enum StoreKind {
    /// A directory on the local filesystem, created if absent.
    LocalFile,
    /// A fresh, empty in-memory store.
    Memory,
    /// An S3 (or S3-compatible) bucket.
    S3 { bucket: String },
}

impl StoreKind {
    /// Classifies an attach path by scheme, returning the store kind and the
    /// bucket-relative key prefix (empty for local and in-memory stores).
    pub(crate) fn from_path(path: &str) -> Result<(Self, String), AbiError> {
        if let Some(rest) = path.strip_prefix("s3://") {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            if bucket.is_empty() {
                return Err(AbiError::invalid_argument(
                    "moraine_attach: s3:// URL is missing a bucket",
                ));
            }
            return Ok((
                Self::S3 {
                    bucket: bucket.to_string(),
                },
                prefix.to_string(),
            ));
        }
        for scheme in [
            "gs://", "gcs://", "azure://", "az://", "http://", "https://",
        ] {
            if path.starts_with(scheme) {
                return Err(AbiError::invalid_argument(format!(
                    "moraine_attach: unsupported store scheme in `{path}` \
                     (supported: a local path, `memory://`, or `s3://`)"
                )));
            }
        }
        if path == "memory://" || path == "memory:" {
            return Ok((Self::Memory, String::new()));
        }
        Ok((Self::LocalFile, String::new()))
    }

    pub(crate) fn open(
        &self,
        path: &str,
        s3: Option<&S3Creds>,
    ) -> Result<(Arc<dyn ObjectStore>, moraine::CacheIdentity), AbiError> {
        match self {
            Self::LocalFile => {
                std::fs::create_dir_all(path).map_err(|e| {
                    AbiError::invalid_argument(format!(
                        "moraine_attach: cannot create directory `{path}`: {e}"
                    ))
                })?;
                let fs = LocalFileSystem::new_with_prefix(path).map_err(|e| {
                    AbiError::invalid_argument(format!(
                        "moraine_attach: cannot open `{path}` as a store root: {e}"
                    ))
                })?;
                let identity = moraine::CacheIdentity::local(&fs).map_err(|error| {
                    AbiError::invalid_argument(format!("cannot identify local store: {error}"))
                })?;
                Ok((Arc::new(fs), identity))
            }
            Self::Memory => Ok((Arc::new(InMemory::new()), moraine::CacheIdentity::default())),
            Self::S3 { bucket } => {
                // With a secret, only the secret's values apply; without one,
                // the environment credential chain does.
                let base = if s3.is_some() {
                    AmazonS3Builder::new()
                } else {
                    AmazonS3Builder::from_env()
                };
                let mut builder = base.with_bucket_name(bucket);
                if let Some(c) = s3 {
                    if let Some(v) = c.key_id {
                        builder = builder.with_access_key_id(v);
                    }
                    if let Some(v) = c.secret {
                        builder = builder.with_secret_access_key(v);
                    }
                    if let Some(v) = c.region {
                        builder = builder.with_region(v);
                    }
                    if let Some(v) = c.session_token {
                        builder = builder.with_token(v);
                    }
                    // DuckDB's secret defaults `endpoint` to `s3.amazonaws.com`;
                    // forwarding that would override the region-derived endpoint.
                    if let Some(v) = c.endpoint
                        && !v.is_empty()
                        && !v.contains("amazonaws.com")
                    {
                        builder = builder.with_endpoint(v);
                    }
                    if c.url_style == Some("path") {
                        builder = builder.with_virtual_hosted_style_request(false);
                    }
                    if c.use_ssl == Some(false) {
                        builder = builder.with_allow_http(true);
                    }
                }
                let identity = s3_cache_identity(&builder);
                let store = builder.build().map_err(|e| {
                    AbiError::invalid_argument(format!(
                        "moraine_attach: cannot open s3 bucket `{bucket}`: {e} \
                         (check the s3 secret or the AWS_* environment)"
                    ))
                })?;
                Ok((Arc::new(store), identity))
            }
        }
    }
}

/// Identifies the configured S3 object namespace without retaining credentials.
pub(super) fn s3_cache_identity(builder: &AmazonS3Builder) -> moraine::CacheIdentity {
    use object_store::aws::AmazonS3ConfigKey;

    let namespace = [
        AmazonS3ConfigKey::Endpoint,
        AmazonS3ConfigKey::S3Endpoint,
        AmazonS3ConfigKey::Region,
        AmazonS3ConfigKey::Bucket,
        AmazonS3ConfigKey::VirtualHostedStyleRequest,
        AmazonS3ConfigKey::S3Express,
    ]
    .map(|key| builder.get_config_value(&key));
    moraine::CacheIdentity::new(&format!("s3:{namespace:?}"))
}

/// Borrows a raw pointer argument as a `&str`, checking it for null and
/// UTF-8 validity.
///
/// # Safety
///
/// `ptr`, if non-null, must point to a NUL-terminated C string valid for
/// reads for the duration of this call.
pub(crate) unsafe fn borrow_str<'a>(
    ptr: *const c_char,
    arg_name: &str,
) -> Result<&'a str, AbiError> {
    if ptr.is_null() {
        return Err(AbiError::invalid_argument(format!("`{arg_name}` is null")));
    }
    // SAFETY: caller contract above.
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|_| AbiError::invalid_argument(format!("`{arg_name}` is not valid UTF-8")))
}

/// Borrows a raw byte-buffer argument as a `&[u8]`. A null `ptr` is valid
/// only when `len` is `0`.
///
/// # Safety
///
/// `ptr`, if non-null, must point to `len` valid, readable bytes for the
/// duration of this call.
pub(crate) unsafe fn borrow_bytes<'a>(
    ptr: *const u8,
    len: usize,
    arg_name: &str,
) -> Result<&'a [u8], AbiError> {
    if ptr.is_null() {
        if len == 0 {
            return Ok(&[]);
        }
        return Err(AbiError::invalid_argument(format!(
            "`{arg_name}` is null but its length is nonzero"
        )));
    }
    // SAFETY: caller contract above.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

/// Refuses an attach whose catalog store and data root sit on the same
/// object store with one containing the other: DuckLake's orphan cleanup
/// would delete the catalog's own objects. Containment is compared
/// lexically by path component; symlinks and `..` are not resolved.
pub(super) fn refuse_overlapping_data_path(
    store_path: &str,
    data_path: &str,
) -> Result<(), AbiError> {
    let (store_kind, store_prefix) = StoreKind::from_path(store_path)?;
    let (data_kind, data_prefix) = StoreKind::from_path(data_path)?;

    let overlaps = |a: &str, b: &str| {
        let (a, b) = (std::path::Path::new(a), std::path::Path::new(b));
        a.starts_with(b) || b.starts_with(a)
    };

    let nested = match (&store_kind, &data_kind) {
        // An empty prefix is the bucket root, which contains everything.
        (StoreKind::S3 { bucket: store }, StoreKind::S3 { bucket: data }) => {
            store == data && overlaps(&store_prefix, &data_prefix)
        }
        (StoreKind::LocalFile, StoreKind::LocalFile) => overlaps(store_path, data_path),
        _ => false,
    };

    if nested {
        return Err(AbiError::new(
            codes::CONSTRAINT,
            format!(
                "the catalog store `{store_path}` and DATA_PATH `{data_path}` are nested on the \
                 same object store; DuckLake's orphaned-file cleanup lists DATA_PATH and would \
                 delete the catalog's own objects. Put them in sibling locations."
            ),
        ));
    }
    Ok(())
}

/// Resolves the `DATA_PATH` object store and its bucket-relative key
/// prefix. The recorded data root is authoritative: a differing
/// `data_path_arg` is refused, and a lake with none recorded adopts the
/// given value (recording it unless read-only). `None`/`None` yields no
/// store.
fn resolve_data_store(
    runtime: &tokio::runtime::Runtime,
    catalog: &AttachedCatalog,
    store_path: &str,
    data_path_arg: Option<String>,
    read_only: bool,
    s3_creds: Option<&S3Creds>,
) -> Result<(Option<moraine::DataStore>, String), AbiError> {
    let recorded = runtime
        .block_on(catalog.reads().snapshot())
        .map_err(AbiError::from)?
        .data_path();
    // Recorded only after the overlap check below, so a refused attach
    // leaves nothing behind.
    let mut adopting = false;
    let data_root = match (data_path_arg, recorded) {
        (Some(given), Some(recorded)) => {
            if given.trim_end_matches('/') != recorded.trim_end_matches('/') {
                return Err(AbiError::invalid_argument(format!(
                    "META_DATA_PATH `{given}` does not match the data path recorded for this \
                     lake (`{recorded}`); a lake's data path is fixed when it is created"
                )));
            }
            Some(recorded)
        }
        (Some(given), None) => {
            adopting = !read_only;
            Some(given)
        }
        (None, recorded) => recorded,
    };

    if let Some(root) = data_root.as_deref() {
        refuse_overlapping_data_path(store_path, root)?;
    }

    if adopting {
        let to_record = data_root.clone().unwrap_or_default();
        runtime
            .block_on(catalog.writer()?.commit(move |tx| {
                tx.set_option(moraine::OptionScope::Global, "data_path", &to_record)?;
                Ok(())
            }))
            .map_err(AbiError::from)?;
    }

    match data_root {
        Some(path) => {
            let (kind, prefix) = StoreKind::from_path(&path)?;
            if matches!(kind, StoreKind::LocalFile) {
                std::fs::create_dir_all(&path).map_err(|error| {
                    AbiError::invalid_argument(format!(
                        "cannot create data directory `{path}`: {error}"
                    ))
                })?;
                let prefix =
                    object_store::path::Path::from_filesystem_path(&path).map_err(|error| {
                        AbiError::invalid_argument(format!(
                            "cannot resolve data directory `{path}`: {error}"
                        ))
                    })?;
                // A foreign file can carry an absolute path outside DATA_PATH.
                let store = LocalFileSystem::new();
                let identity = moraine::CacheIdentity::local(&store).map_err(|error| {
                    AbiError::invalid_argument(format!("cannot identify local data store: {error}"))
                })?;
                return Ok((
                    Some(moraine::DataStore::with_cache_identity(
                        Arc::new(store),
                        identity,
                    )),
                    prefix.to_string(),
                ));
            }
            let (store, identity) = kind.open(&path, s3_creds)?;
            Ok((
                Some(moraine::DataStore::with_cache_identity(store, identity)),
                prefix,
            ))
        }
        None => Ok((None, String::new())),
    }
}

/// Winds down the runtime of an attach that will not produce a handle,
/// with a deadline, and returns the error that ended it.
fn cancel_attach(runtime: tokio::runtime::Runtime, error: AbiError) -> AbiError {
    runtime.shutdown_timeout(CANCELLED_ATTACH_SHUTDOWN);
    error
}

/// The object-cache cap an ABI byte count names; zero means "not given".
pub(super) fn cache_size_option(cache_size_bytes: u64) -> Option<u64> {
    (cache_size_bytes != 0).then_some(cache_size_bytes)
}

/// The WAL flush cadence an ABI millisecond count names. Zero means "not
/// given"; `u64::MAX` is the shim's sentinel for an explicit zero interval,
/// which flushes continuously.
fn flush_interval_option(flush_interval_ms: u64) -> Option<std::time::Duration> {
    match flush_interval_ms {
        0 => None,
        u64::MAX => Some(std::time::Duration::ZERO),
        ms => Some(std::time::Duration::from_millis(ms)),
    }
}

/// The preload level an ABI code names: `0` loads nothing, `1` the
/// newest objects, `2` every object the manifest references. Any other
/// value is an error.
pub(super) fn cache_preload_option(
    cache_preload: u8,
) -> Result<Option<moraine::CachePreload>, AbiError> {
    match cache_preload {
        0 => Ok(None),
        1 => Ok(Some(moraine::CachePreload::L0)),
        2 => Ok(Some(moraine::CachePreload::All)),
        other => Err(AbiError::invalid_argument(format!(
            "cache_preload {other} names no preload level: 0 loads nothing, 1 the newest \
             objects, 2 every object"
        ))),
    }
}

/// Attaches a moraine catalog: creates the runtime this handle owns for
/// its lifetime, opens (creating and initializing if empty) the catalog,
/// and writes the resulting handle to `*out`.
///
/// `path`'s scheme selects the store: a local filesystem directory
/// (created if absent) by default, `memory://` for an in-memory store, or
/// `s3://<bucket>[/<prefix>]` for S3. For an `s3://` path, `s3` supplies
/// credentials (any field unset falls back to the AWS_* environment); it
/// may be null to use the environment alone and is ignored otherwise.
///
/// `encrypted` requests DuckLake data-file encryption. Recorded when a
/// fresh store bootstraps; ignored on an already-initialized store, whose
/// stored flag ([`moraine_catalog_encrypted`]) is authoritative.
///
/// `cache_dir`, `cache_size_bytes`, and `cache_memory_bytes` are settled
/// process-wide by the first attach; a later attach naming different values
/// logs them as ignored. Settled process-wide is not the same as counted
/// process-wide, and the three differ on that: `cache_dir` is where each
/// store's disk tier lives and is recovered from (null keeps the caches in
/// memory); `cache_size_bytes` caps **each store's** device, so peak disk is
/// that figure times the attached stores; `cache_memory_bytes` is the
/// process-shared cache budget across every store's cache and the
/// parsed-footer cache. It is not a process RSS limit: SlateDB write buffers,
/// catalog projections, commit staging, DuckDB, and allocator retention are
/// outside it. `0` means "not given" for either byte count.
///
/// `cache_preload` warms this store into that cache before the attach
/// returns: `0` loads nothing, `1` each subspace's SST metadata, `2` the
/// scan-shaped subspaces whole. Any other value is
/// [`codes::INVALID_ARGUMENT`]; the ABI has no default, the caller always
/// names a level (the extension's `ATTACH` passes `1` unless told
/// otherwise). A non-zero level also warms every table's probe ranges in
/// the background after the open. `cache_puts` admits SST
/// metadata (including compaction output) into the cache as it is written;
/// `false` leaves the cache filled by reads alone.
///
/// `checkpoint` pins a read-only attach to an existing SlateDB checkpoint
/// (see [`super::moraine_create_checkpoint`]); the open writes nothing and
/// serves a fixed cut. Null or empty follows the latest manifest; a non-null
/// value with `read_only` false is [`codes::INVALID_ARGUMENT`].
///
/// `host_threads` is how many execution threads the calling host runs;
/// the handle's worker pool is that count clamped to `[2, 8]`, with `0`
/// taking the floor. The size is fixed for the handle's life.
///
/// Cancellable via `probe`/`probe_ctx`, exactly as the read entry points
/// are. A cancelled attach returns [`codes::INTERRUPTED`], writes no
/// handle, and leaves nothing attached; it may still have fenced a writer
/// attached before it, which must re-attach as after any failed attach.
///
/// Returns [`codes::OK`] on success. On failure, `*out` is left
/// unwritten and, if `err` is non-null, `*err` carries the code and a
/// message.
///
/// # Safety
///
/// `path` must be a valid NUL-terminated C string. `s3`, if non-null,
/// must point to a valid [`MoraineS3Config`] whose non-null fields are
/// valid NUL-terminated C strings. `cache_dir`, `data_path`, and
/// `checkpoint`, if non-null, must be valid NUL-terminated C strings.
/// `cache_size_bytes`, `cache_memory_bytes`, `cache_preload`, `cache_puts`,
/// `flush_on_commit`, and `host_threads` are unconstrained.
/// `probe`, if non-null, must be safe to call with `probe_ctx` from any
/// thread. `out` must be a valid, writable `*mut *mut
/// MoraineCatalogHandle`. `err`, if non-null, must be a valid, writable
/// [`MoraineError`]. All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_attach(
    path: *const c_char,
    s3: *const MoraineS3Config,
    read_only: bool,
    encrypted: bool,
    flush_interval_ms: u64,
    flush_on_commit: bool,
    cache_dir: *const c_char,
    cache_size_bytes: u64,
    cache_memory_bytes: u64,
    cache_preload: u8,
    cache_puts: bool,
    data_path: *const c_char,
    checkpoint: *const c_char,
    host_threads: u64,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    out: *mut *mut MoraineCatalogHandle,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Box<MoraineCatalogHandle>, AbiError> {
        // Before anything that could emit an event.
        crate::logging::install();
        if out.is_null() {
            return Err(AbiError::invalid_argument("`out` is null"));
        }
        // SAFETY: `path` validity is this function's own safety contract.
        let path_str = unsafe { borrow_str(path, "path") }?;
        // SAFETY: `cache_dir` validity is this function's own safety contract.
        let cache_dir = unsafe { opt_borrow_str(cache_dir, "cache_dir") }?;
        // SAFETY: `checkpoint` validity is this function's own safety
        // contract.
        let checkpoint = unsafe { opt_borrow_str(checkpoint, "checkpoint") }?;
        if checkpoint.is_some() && !read_only {
            return Err(AbiError::invalid_argument(
                "moraine_attach: a checkpoint pins a fixed past cut, so it applies to a \
                 read-only attach only — add READ_ONLY, or drop the checkpoint",
            ));
        }

        let (store_kind, prefix) = StoreKind::from_path(path_str)?;

        // SAFETY: `s3` validity is this function's own safety contract.
        let s3_creds = unsafe { borrow_s3_creds(s3) };

        let (object_store, cache_identity) = store_kind.open(path_str, s3_creds.as_ref())?;
        // Allocated before the runtime so its worker threads are tagged
        // from their first instant.
        let log_id = crate::logging::allocate_handle_id();
        let _log_guard = crate::logging::enter_handle(log_id);
        let runtime = new_runtime(log_id, usize::try_from(host_threads).unwrap_or(usize::MAX))
            .map_err(|e| {
                AbiError::new(
                    codes::INTERNAL,
                    format!("failed to start tokio runtime: {e}"),
                )
            })?;

        // SAFETY: `data_path` validity is this function's own safety contract.
        let data_path_arg = unsafe { opt_borrow_str(data_path, "data_path") }?.map(str::to_owned);

        // Checked before the open, which records `data_path` when it
        // bootstraps a fresh store. `resolve_data_store` checks the
        // recorded value again.
        if let Some(given) = data_path_arg.as_deref() {
            refuse_overlapping_data_path(path_str, given)?;
        }

        let mut options = CatalogOptions::default();
        options.path = prefix;
        options.cache_identity = Some(cache_identity);
        options.encrypted = encrypted;
        if let Some(interval) = flush_interval_option(flush_interval_ms) {
            options.flush_interval = interval;
        }
        options.flush_on_commit = flush_on_commit;
        options.cache_dir = cache_dir.map(std::path::PathBuf::from);
        options.cache_size = cache_size_option(cache_size_bytes);
        options.cache_memory = cache_size_option(cache_memory_bytes);
        options.cache_preload = cache_preload_option(cache_preload)?;
        let preload = options.cache_preload.is_some();
        options.cache_puts = cache_puts;
        options.checkpoint = checkpoint.map(str::to_owned);
        options.data_path.clone_from(&data_path_arg);
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let opened = unsafe {
            if read_only {
                block_on_cancellable_in(
                    &runtime,
                    probe,
                    probe_ctx,
                    moraine::Catalog::open_read_only(object_store, options),
                )
                .map(AttachedCatalog::Reader)
                // A read-only attach never bootstraps, so a fresh store fails
                // here; the hint names the fix.
                .map_err(AbiError::with_read_only_attach_hint)
            } else {
                block_on_cancellable_in(
                    &runtime,
                    probe,
                    probe_ctx,
                    moraine::Catalog::open(object_store, options),
                )
                .map(AttachedCatalog::Writer)
            }
        };
        let catalog = match opened {
            Ok(catalog) => catalog,
            Err(error) => return Err(cancel_attach(runtime, error)),
        };

        // The DATA_PATH store reuses the catalog store's S3 secret.
        let resolved = resolve_data_store(
            &runtime,
            &catalog,
            path_str,
            data_path_arg,
            read_only,
            s3_creds.as_ref(),
        );
        let (data_store, data_prefix) = match resolved {
            Ok(parts) => parts,
            Err(error) => {
                // Flush and release the open catalog before failing the attach.
                let _ = runtime.block_on(catalog.reads().close());
                return Err(error);
            }
        };

        let mut handle = MoraineCatalogHandle::new(runtime, catalog, log_id);
        handle.data_store = data_store;
        handle.data_prefix = data_prefix;
        handle.spawn_warm_at_attach(preload);
        Ok(Box::new(handle))
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(handle) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe {
                *out = Box::into_raw(handle);
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Writes the lake's recorded data root (the stored global `data_path`
/// option) to `*out` as an owned C string, or null when none was recorded.
/// Free a non-null result exactly once with [`moraine_string_free`].
///
/// Cancellable via `probe`/`probe_ctx`, exactly as
/// [`super::moraine_snapshot`].
///
/// # Safety
///
/// `handle` must be a live handle from [`moraine_attach`]. `out` must be a
/// valid, writable `*mut *mut c_char`. `probe`/`probe_ctx` follow the ABI
/// cancellation contract. `err`, if non-null, must be writable. All for the
/// duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_data_path(
    handle: *mut MoraineCatalogHandle,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    out: *mut *mut c_char,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out.is_null() {
            return Err(AbiError::invalid_argument("`out` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.reads().snapshot())
        }?;
        let path_ptr = match snapshot.data_path() {
            Some(path) => to_c_string(path)?.into_raw(),
            None => ptr::null_mut(),
        };
        // SAFETY: `out` is non-null and writable per the caller contract.
        unsafe { *out = path_ptr };
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// What one [`moraine_migrate`] call did.
#[repr(C)]
pub struct MoraineMigrationReport {
    /// The structural format the store carried when the call began.
    pub from_format: u64,
    /// The format it carries now. Equal to `from_format` when there was
    /// nothing to run.
    pub to_format: u64,
    /// Whether the call resumed a migration a previous run left partly
    /// applied, rather than starting from a settled store.
    pub resumed: bool,
    /// Comma-separated names of the units that ran, in order, or null when
    /// none did. Free with [`moraine_string_free`].
    pub units_run: *mut c_char,
}

/// Applies every structural format migration this binary carries that the
/// store at `path` still needs. Opens the store itself; a store carrying a
/// migration marker is one an attach refuses.
///
/// `checkpoint` takes a whole-store checkpoint before the first rewrite and
/// releases it once the run is durable, leaving a manual recovery point if
/// the migration fails partway.
///
/// Returns [`codes::OK`] on success, having written `*out`. On failure
/// `*out` is left unwritten and, if `err` is non-null, `*err` carries the
/// code and a message.
///
/// # Safety
///
/// `path` must be a valid NUL-terminated C string. `s3`, if non-null, must
/// point to a valid [`MoraineS3Config`] whose non-null fields are valid
/// NUL-terminated C strings. `cache_dir`, if non-null, must be a valid
/// NUL-terminated C string. `cache_size_bytes`, `cache_preload`, and
/// `cache_puts` are unconstrained. `out`
/// must be a valid, writable [`MoraineMigrationReport`]. `err`, if non-null,
/// must be a valid, writable [`MoraineError`]. All for the duration of this
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_migrate(
    path: *const c_char,
    s3: *const MoraineS3Config,
    flush_interval_ms: u64,
    cache_dir: *const c_char,
    cache_size_bytes: u64,
    cache_preload: u8,
    cache_puts: bool,
    checkpoint: bool,
    out: *mut MoraineMigrationReport,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        // Before anything that could emit an event.
        crate::logging::install();
        if out.is_null() {
            return Err(AbiError::invalid_argument("`out` is null"));
        }
        // SAFETY: `path` validity is this function's own safety contract.
        let path_str = unsafe { borrow_str(path, "path") }?;
        // SAFETY: `cache_dir` validity is this function's own safety contract.
        let cache_dir = unsafe { opt_borrow_str(cache_dir, "cache_dir") }?;
        let (store_kind, prefix) = StoreKind::from_path(path_str)?;

        // SAFETY: `s3` validity is this function's own safety contract.
        let s3_creds = unsafe { borrow_s3_creds(s3) };

        let (object_store, cache_identity) = store_kind.open(path_str, s3_creds.as_ref())?;
        let log_id = crate::logging::allocate_handle_id();
        let _log_guard = crate::logging::enter_handle(log_id);
        let runtime = new_runtime(log_id, 0).map_err(|e| {
            AbiError::new(
                codes::INTERNAL,
                format!("failed to start tokio runtime: {e}"),
            )
        })?;

        let mut options = moraine::CatalogOptions::default();
        options.path = prefix;
        options.cache_identity = Some(cache_identity);
        if let Some(interval) = flush_interval_option(flush_interval_ms) {
            options.flush_interval = interval;
        }
        options.cache_dir = cache_dir.map(std::path::PathBuf::from);
        options.cache_size = cache_size_option(cache_size_bytes);
        options.cache_preload = cache_preload_option(cache_preload)?;
        options.cache_puts = cache_puts;

        let mut request = moraine::MigrationRequest::default();
        request.checkpoint = checkpoint;

        let report = runtime
            .block_on(moraine::Catalog::migrate(object_store, options, request))
            .map_err(AbiError::from)?;

        let units_run = if report.units_run.is_empty() {
            ptr::null_mut()
        } else {
            to_c_string(report.units_run.join(","))?.into_raw()
        };
        // SAFETY: `out` is non-null and writable per the caller contract.
        unsafe {
            *out = MoraineMigrationReport {
                from_format: report.from_format,
                to_format: report.to_format,
                resumed: report.resumed,
                units_run,
            };
        }
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Raises the store `handle` names to the newest purely additive format
/// this binary writes, opening the record shapes gated behind it, and
/// reports the formats either side of the move.
///
/// A one-way door, and the only thing that opens it: the shapes it
/// admits are ones an older binary misreads rather than refuses, so the
/// store can no longer be opened by a binary that predates the format.
/// A reader already attached on an older binary is not refused by this —
/// raise the format only once every reader understands it. `dry_run`
/// reports the move it would make and stamps nothing, which is the only
/// way to read a store's format without changing it.
///
/// # Safety
///
/// `handle` must be a pointer previously returned by [`moraine_attach`]
/// and not yet detached.
/// `out_from`/`out_to` must be valid, writable pointers, and `err`, if
/// non-null, a valid, writable [`MoraineError`], for the duration of the
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_raise_format(
    handle: *mut MoraineCatalogHandle,
    dry_run: bool,
    out_from: *mut u64,
    out_to: *mut u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_from.is_null() || out_to.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        let catalog = handle_ref.catalog.writer().map_err(AbiError::from)?;
        let raised = handle_ref
            .block_on(catalog.raise_format(dry_run))
            .map_err(AbiError::from)?;
        // SAFETY: checked non-null above; caller contract for validity.
        unsafe {
            *out_from = raised.from_format;
            *out_to = raised.to_format;
        }
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Frees an owned string a `moraine_*` call returned (such as
/// [`moraine_data_path`]'s `out`). A null pointer is ignored.
///
/// # Safety
///
/// `ptr` must be an owned string a `moraine_*` call returned and not yet
/// freed, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_string_free(ptr: *mut c_char) {
    // SAFETY: caller contract — an owned `moraine_*` string or null.
    unsafe { free_c_string(ptr) };
}

/// Whether the catalog encrypts its data files: the stored global
/// `encrypted` option, fixed when the store was created. A store created
/// before the flag existed reads as not encrypted.
///
/// Cancellable via `probe`/`probe_ctx`, exactly as
/// [`super::moraine_snapshot`].
///
/// # Safety
///
/// `handle` must be a live handle from [`moraine_attach`].
/// `out_encrypted` must be a valid, writable `*mut bool`. `probe`, if
/// non-null, must be safe to call with `probe_ctx` from any thread.
/// `err`, if non-null, must be a valid, writable [`MoraineError`]. All
/// for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_catalog_encrypted(
    handle: *mut MoraineCatalogHandle,
    out_encrypted: *mut bool,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<bool, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_encrypted.is_null() {
            return Err(AbiError::invalid_argument("`out_encrypted` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.reads().snapshot())
        }?;

        Ok(snapshot
            .option(moraine::OptionScope::Global, "encrypted")
            .as_deref()
            == Some("true"))
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(encrypted) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { *out_encrypted = encrypted };
            codes::OK
        }
        Err(code) => code,
    }
}

/// Closes the catalog (flushing background work) and drops the runtime,
/// consuming `handle`. A close failure is logged, not returned; a null
/// `handle` is a no-op. Takes no probe: an interrupted teardown would leak
/// the handle or leave the store half-closed.
///
/// # Safety
///
/// `handle`, if non-null, must be a pointer previously returned by
/// [`moraine_attach`] and not yet passed to `moraine_detach`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_detach(handle: *mut MoraineCatalogHandle) {
    if handle.is_null() {
        return;
    }
    let attempt = || {
        // SAFETY: caller contract above; dropped exactly once.
        let boxed = unsafe { Box::from_raw(handle) };
        boxed.finish_warming();
        if let Err(err) = boxed.block_on(boxed.catalog.reads().close()) {
            warn!(error = %err, "catalog close failed during detach");
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}
