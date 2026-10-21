use std::{
    ffi::c_void,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use object_store::{CredentialProvider, aws::AmazonS3Builder};

use super::*;

#[derive(Default)]
struct Source {
    calls: AtomicUsize,
    fail: AtomicBool,
    expired: AtomicBool,
    blocked: AtomicBool,
}

unsafe extern "C" fn retain(context: *mut c_void) {
    // SAFETY: the fixture passes a live Arc<Source> allocation.
    unsafe { Arc::increment_strong_count(context.cast::<Source>()) };
}

unsafe extern "C" fn release(context: *mut c_void) {
    // SAFETY: each release balances one retain.
    unsafe { Arc::decrement_strong_count(context.cast::<Source>()) };
}

unsafe extern "C" fn load(
    context: *mut c_void,
    receive: MoraineS3CredentialReceiver,
    output: *mut c_void,
) -> bool {
    // SAFETY: retained by the provider for this call.
    let source = unsafe { &*context.cast::<Source>() };
    let call = source.calls.fetch_add(1, Ordering::SeqCst);
    while source.blocked.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(1));
    }
    if source.fail.load(Ordering::SeqCst) {
        return false;
    }
    let key = if call == 0 { c"first" } else { c"rotated" };
    let expiry = i64::from(source.expired.load(Ordering::SeqCst));
    // SAFETY: pointers are static; receive and output belong to this load call.
    unsafe {
        receive(
            output,
            key.as_ptr(),
            c"secret".as_ptr(),
            c"token".as_ptr(),
            expiry,
        );
    };
    true
}

fn provider(source: &Arc<Source>) -> Arc<RefreshingCredentials> {
    let callbacks = MoraineS3CredentialSource {
        context: Arc::as_ptr(source).cast_mut().cast(),
        retain,
        release,
        load,
    };
    // SAFETY: callbacks retain the live fixture and obey the source contract.
    Arc::new(unsafe { RefreshingCredentials::new(&callbacks) })
}

#[tokio::test]
async fn an_attached_store_retains_and_uses_the_callback_instead_of_copied_keys() {
    use super::super::attach::{MoraineS3Config, borrow_s3_creds, s3_builder};

    let source = Arc::new(Source::default());
    let store = {
        let callbacks = MoraineS3CredentialSource {
            context: Arc::as_ptr(&source).cast_mut().cast(),
            retain,
            release,
            load,
        };
        let config = MoraineS3Config {
            key_id: c"stale-key".as_ptr(),
            secret: c"stale-secret".as_ptr(),
            session_token: c"stale-token".as_ptr(),
            region: c"us-east-1".as_ptr(),
            endpoint: std::ptr::null(),
            url_style: std::ptr::null(),
            use_ssl: -1,
            credential_source: &raw const callbacks,
        };
        // SAFETY: config and callbacks are live; retain keeps source alive.
        let borrowed = unsafe { borrow_s3_creds(&raw const config) };
        s3_builder("lake", borrowed.as_ref()).build().unwrap()
    };
    assert_eq!(Arc::strong_count(&source), 2);
    let credentials = store.credentials().get_credential().await.unwrap();
    assert_eq!(credentials.key_id, "first");
    assert_eq!(credentials.token.as_deref(), Some("token"));
    drop(store);
    assert_eq!(Arc::strong_count(&source), 1);
}

#[tokio::test]
async fn signing_credentials_refresh_without_reopening_the_store() {
    let source = Arc::new(Source::default());
    let provider = provider(&source);
    let store = AmazonS3Builder::new()
        .with_bucket_name("lake")
        .with_region("us-east-1")
        .with_credentials(provider.clone())
        .build()
        .unwrap();
    assert_eq!(
        store.credentials().get_credential().await.unwrap().key_id,
        "first"
    );
    assert_eq!(
        store.credentials().get_credential().await.unwrap().key_id,
        "first"
    );
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);

    provider.cache.lock().await.as_mut().unwrap().refresh_at = Instant::now();
    let refreshed = store.credentials().get_credential().await.unwrap();
    assert_eq!(refreshed.key_id, "rotated");
    assert_eq!(refreshed.token.as_deref(), Some("token"));
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn refresh_failure_and_expired_results_never_reuse_stale_credentials() {
    let source = Arc::new(Source::default());
    let provider = provider(&source);
    provider.get_credential().await.unwrap();
    provider.cache.lock().await.as_mut().unwrap().refresh_at = Instant::now();
    source.fail.store(true, Ordering::SeqCst);
    assert!(provider.get_credential().await.is_err());
    source.fail.store(false, Ordering::SeqCst);
    source.expired.store(true, Ordering::SeqCst);
    assert!(provider.get_credential().await.is_err());
    source.expired.store(false, Ordering::SeqCst);
    assert_eq!(provider.get_credential().await.unwrap().key_id, "rotated");
}

#[tokio::test]
async fn concurrent_requests_share_one_refresh_and_release_the_source() {
    let source = Arc::new(Source::default());
    let provider = provider(&source);
    assert_eq!(Arc::strong_count(&source), 2);
    let tasks = (0..32).map(|_| {
        let provider = provider.clone();
        tokio::spawn(async move { provider.get_credential().await.unwrap() })
    });
    for result in futures::future::join_all(tasks).await {
        assert_eq!(result.unwrap().key_id, "first");
    }
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    drop(provider);
    assert_eq!(Arc::strong_count(&source), 1);
}

#[test]
fn refresh_deadlines_precede_expiry_and_bound_unknown_expiry() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
    assert_eq!(refresh_delay(0, now).unwrap(), Duration::from_mins(1));
    assert_eq!(
        refresh_delay(1_100_000, now).unwrap(),
        Duration::from_secs(80)
    );
    assert!(refresh_delay(1_000_000, now).is_err());
    assert!(refresh_delay(-1, now).is_err());
}

#[tokio::test]
async fn cancelling_a_refresh_keeps_its_source_and_single_flight_lock_alive() {
    struct Unblock(Arc<Source>);
    impl Drop for Unblock {
        fn drop(&mut self) {
            self.0.blocked.store(false, Ordering::SeqCst);
        }
    }
    let source = Arc::new(Source::default());
    let unblock = Unblock(source.clone());
    source.blocked.store(true, Ordering::SeqCst);
    let provider = provider(&source);
    let first = {
        let provider = provider.clone();
        tokio::spawn(async move { provider.get_credential().await })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while source.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(provider.cache.try_lock().is_err());
    let second = tokio::spawn(async move { provider.get_credential().await });
    drop(unblock);
    assert_eq!(second.await.unwrap().unwrap().key_id, "first");
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&source), 1);
}
