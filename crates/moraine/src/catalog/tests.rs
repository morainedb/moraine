use super::resolve_data_path;

#[test]
fn relative_data_paths_accept_trailing_root_separators() {
    for prefix in ["org-123", "org-123/", "org-123///"] {
        for file in ["data.parquet", "deletes.parquet"] {
            let path = resolve_data_path(prefix, "main/probe/", file, true).unwrap();
            assert_eq!(path.as_ref(), format!("org-123/main/probe/{file}"));
        }
    }
    for prefix in ["", "/", "///"] {
        let path = resolve_data_path(prefix, "main/probe/", "data.parquet", true).unwrap();
        assert_eq!(path.as_ref(), "main/probe/data.parquet");
    }
}

#[test]
fn absolute_data_paths_ignore_the_root_and_table_prefixes() {
    let path = resolve_data_path("org-123/", "main/probe/", "other/data.parquet", false).unwrap();
    assert_eq!(path.as_ref(), "other/data.parquet");
}

#[test]
fn data_paths_still_reject_internal_empty_segments() {
    for (prefix, table, file) in [
        ("org//123/", "main/probe/", "data.parquet"),
        ("org-123/", "main//probe/", "data.parquet"),
        ("org-123/", "main/probe/", "sub//data.parquet"),
    ] {
        assert!(resolve_data_path(prefix, table, file, true).is_err());
    }
    assert!(resolve_data_path("org-123/", "main/probe/", "other//data.parquet", false).is_err());
}

/// A cache directory silences the slow-open report however slow the open
/// was, and a fast open without one says nothing.
#[test]
fn only_a_slow_open_with_nowhere_to_cache_is_reported() {
    use std::{path::Path, time::Duration};

    use super::handle::slow_open_without_cache;

    let slow = Duration::from_millis(400);
    let quick = Duration::from_millis(20);
    let dir = Some(Path::new("/var/cache/lake"));

    assert!(slow_open_without_cache(None, slow));
    assert!(!slow_open_without_cache(dir, slow));
    assert!(!slow_open_without_cache(None, quick));
    assert!(!slow_open_without_cache(dir, quick));
}

/// The advice is to set `CACHE_DIR`, which an operator does once, so only
/// the first slow open reports it however many follow.
#[test]
fn only_the_first_slow_open_reports() {
    use std::sync::atomic::AtomicBool;

    use super::handle::first_slow_open;

    let warned = AtomicBool::new(false);
    assert!(first_slow_open(&warned));
    assert!(!first_slow_open(&warned));
    assert!(!first_slow_open(&warned));
}
