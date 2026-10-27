//! The process's memory-release hook: what the core calls after a phase
//! has dropped its working allocations.

/// Installs the release hook, once per process.
///
/// glibc keeps freed pages in its arenas, and the core's phases free in
/// bursts; `malloc_trim` is what returns those pages to the kernel. Other
/// platforms have nothing equivalent to call from here, so the hook stays
/// uninstalled and the core skips the release.
pub(crate) fn install() {
    static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INSTALLED.get_or_init(|| {
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        moraine::set_memory_release(trim);
    });
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
unsafe extern "C" {
    /// glibc's heap trimmer: releases the free pages at the top of each
    /// arena, keeping `pad` bytes.
    fn malloc_trim(pad: usize) -> i32;
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn trim() {
    // SAFETY: `malloc_trim` takes a byte count by value and touches only
    // pages the allocator owns. It has no requirement on the caller beyond
    // linking the allocator this process allocates from, which a DuckDB
    // extension does by construction.
    unsafe { malloc_trim(0) };
}
