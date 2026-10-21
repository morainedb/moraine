//! Refreshable S3 credentials retained across the C ABI.

use std::{
    ffi::{CStr, c_char, c_void},
    fmt,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use object_store::{CredentialProvider, aws::AwsCredential};
use tokio::sync::Mutex;

/// Receives credentials; expiry is Unix epoch milliseconds, or zero if unknown.
/// Strings are null or NUL-terminated UTF-8, valid until this call returns.
pub type MoraineS3CredentialReceiver = unsafe extern "C" fn(
    output: *mut c_void,
    key_id: *const c_char,
    secret: *const c_char,
    token: *const c_char,
    expiration_epoch_ms: i64,
);

/// Retained credential provider. Callbacks may run on any thread and must
/// not unwind. `load` calls its receiver exactly once on success, before
/// returning; it returns false on failure. Calls may overlap across stores.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MoraineS3CredentialSource {
    /// Callback state, kept alive by balanced retain/release calls.
    pub context: *mut c_void,
    /// Adds one ownership reference; cannot fail.
    pub retain: unsafe extern "C" fn(*mut c_void),
    /// Releases one ownership reference.
    pub release: unsafe extern "C" fn(*mut c_void),
    /// Resolves credentials without touching the attaching connection.
    pub load: unsafe extern "C" fn(*mut c_void, MoraineS3CredentialReceiver, *mut c_void) -> bool,
}

struct RetainedSource(MoraineS3CredentialSource);

// SAFETY: the source contract requires thread-safe callbacks and state.
unsafe impl Send for RetainedSource {}
// SAFETY: concurrent calls are permitted by the source contract.
unsafe impl Sync for RetainedSource {}

impl Drop for RetainedSource {
    fn drop(&mut self) {
        // SAFETY: new retained this reference; all outstanding loads own an Arc.
        unsafe { (self.0.release)(self.0.context) };
    }
}

impl RetainedSource {
    fn load(&self) -> object_store::Result<CachedCredential> {
        let mut result: Option<object_store::Result<CachedCredential>> = None;
        // SAFETY: the source is retained; the receiver borrows result only during load.
        let success = unsafe { (self.0.load)(self.0.context, receive, (&raw mut result).cast()) };
        if !success {
            return Err(credential_error("DuckDB S3 credential refresh failed"));
        }
        result.unwrap_or_else(|| Err(credential_error("S3 provider returned no credentials")))
    }
}

struct CachedCredential {
    credential: Arc<AwsCredential>,
    refresh_at: Instant,
}

pub(crate) struct RefreshingCredentials {
    source: Arc<RetainedSource>,
    cache: Arc<Mutex<Option<CachedCredential>>>,
}

impl fmt::Debug for RefreshingCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DuckDB S3 credential provider")
    }
}

impl RefreshingCredentials {
    /// Retains a source obeying the callback and lifetime contract.
    pub(crate) unsafe fn new(source: &MoraineS3CredentialSource) -> Self {
        // SAFETY: caller guarantees the source is live and its retain cannot fail.
        unsafe { (source.retain)(source.context) };
        Self {
            source: Arc::new(RetainedSource(*source)),
            cache: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait::async_trait]
impl CredentialProvider for RefreshingCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let mut cache = Arc::clone(&self.cache).lock_owned().await;
        if let Some(cached) = cache.as_ref()
            && Instant::now() < cached.refresh_at
        {
            return Ok(Arc::clone(&cached.credential));
        }
        let source = Arc::clone(&self.source);
        // The blocking job owns the lock and source even if its caller is cancelled.
        tokio::task::spawn_blocking(move || {
            let refreshed = source.load()?;
            let credential = Arc::clone(&refreshed.credential);
            *cache = Some(refreshed);
            Ok(credential)
        })
        .await
        .map_err(|_| credential_error("S3 credential refresh worker failed"))?
    }
}

unsafe extern "C" fn receive(
    output: *mut c_void,
    key_id: *const c_char,
    secret: *const c_char,
    token: *const c_char,
    expiration_epoch_ms: i64,
) {
    // SAFETY: load passes a live output slot and strings valid for this call.
    unsafe {
        let output = &mut *output.cast::<Option<object_store::Result<CachedCredential>>>();
        *output = Some((|| {
            let key_id = credential_string(key_id)?;
            let secret_key = credential_string(secret)?;
            if key_id.is_empty() || secret_key.is_empty() {
                return Err(credential_error(
                    "S3 provider returned incomplete credentials",
                ));
            }
            let token = credential_string(token)?;
            let delay = refresh_delay(expiration_epoch_ms, SystemTime::now())?;
            Ok(CachedCredential {
                credential: Arc::new(AwsCredential {
                    key_id,
                    secret_key,
                    token: (!token.is_empty()).then_some(token),
                }),
                refresh_at: Instant::now() + delay,
            })
        })());
    }
}

unsafe fn credential_string(value: *const c_char) -> object_store::Result<String> {
    if value.is_null() {
        return Ok(String::new());
    }
    // SAFETY: the source contract guarantees a NUL-terminated string.
    unsafe { CStr::from_ptr(value) }
        .to_str()
        .map(str::to_owned)
        .map_err(|_| credential_error("S3 provider returned invalid UTF-8"))
}

fn refresh_delay(expiration_epoch_ms: i64, now: SystemTime) -> object_store::Result<Duration> {
    if expiration_epoch_ms == 0 {
        return Ok(Duration::from_mins(1));
    }
    let milliseconds = u64::try_from(expiration_epoch_ms)
        .map_err(|_| credential_error("S3 provider returned an invalid expiry"))?;
    let expiry = SystemTime::UNIX_EPOCH + Duration::from_millis(milliseconds);
    let remaining = expiry
        .duration_since(now)
        .ok()
        .filter(|duration| !duration.is_zero())
        .ok_or_else(|| credential_error("S3 provider returned expired credentials"))?;
    Ok(remaining.mul_f64(0.8).min(Duration::from_mins(5)))
}

fn credential_error(message: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "DuckDB S3 credentials",
        source: std::io::Error::other(message).into(),
    }
}

#[cfg(test)]
#[path = "s3_credentials_tests.rs"]
mod tests;
