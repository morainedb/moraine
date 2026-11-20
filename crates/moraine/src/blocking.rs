//! CPU work that leaves the async context.

use tokio::task::JoinError;

/// Runs `work` on tokio's blocking pool and returns its value. A browser has
/// no second thread, so there `work` runs inline on the calling task.
pub(crate) async fn offload<T, F>(work: F) -> Result<T, JoinError>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    #[cfg(not(target_os = "emscripten"))]
    {
        tokio::task::spawn_blocking(work).await
    }
    #[cfg(target_os = "emscripten")]
    {
        Ok(work())
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn offload_returns_the_closure_value() {
        let value = super::offload(|| 6 * 7).await.unwrap();
        assert_eq!(value, 42);
    }
}
