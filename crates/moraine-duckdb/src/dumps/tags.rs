//! Dumps for the tag tables: `ducklake_tag` and `ducklake_column_tag`.

use std::ffi::{c_char, c_void};

use super::{conversion::dump_row, dump_rows, free_rows};
use crate::{
    error::MoraineError,
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One `ducklake_tag` row, as returned by [`moraine_dump_tags`] —
/// flattened from the object's container record; ended entries included,
/// lifecycle carried verbatim.
#[repr(C)]
pub struct MoraineTagRow {
    /// `object_id`.
    pub object_id: u64,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `key`, owned.
    pub key: *mut c_char,
    /// `value`, owned.
    pub value: *mut c_char,
}

dump_row! {
    tag_rows(moraine::ffi_support::TagRow) -> MoraineTagRow {
        values { object_id, begin_snapshot }
        optional { end_snapshot => has_end_snapshot }
        strings { key, value }
        optional_strings {}
    }
}

/// Dumps every `ducklake_tag` row into `*out_items`/`*out_len`.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach_with_wal_store`](crate::abi::moraine_attach_with_wal_store), valid writable
/// `out_items`/`out_len`, a `probe` callable with `probe_ctx` from any
/// thread, and a null-or-writable `err`, all for the duration of the
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_tags(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineTagRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        dump_rows(
            handle,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            moraine::ffi_support::dump_tags,
            tag_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_tags`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching [`moraine_dump_tags`] call
/// wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_tags_free(items: *mut MoraineTagRow, len: usize) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}

/// One `ducklake_column_tag` row, as returned by
/// [`moraine_dump_column_tags`] — flattened from the column's latest
/// record (a version transition carries entries forward, so only the
/// latest record's set is emitted).
#[repr(C)]
pub struct MoraineColumnTagRow {
    /// `table_id`.
    pub table_id: u64,
    /// `column_id`.
    pub column_id: u64,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `key`, owned.
    pub key: *mut c_char,
    /// `value`, owned.
    pub value: *mut c_char,
}

dump_row! {
    column_tag_rows(moraine::ffi_support::ColumnTagRow) -> MoraineColumnTagRow {
        values { table_id, column_id, begin_snapshot }
        optional { end_snapshot => has_end_snapshot }
        strings { key, value }
        optional_strings {}
    }
}

/// Dumps every `ducklake_column_tag` row into `*out_items`/`*out_len`.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach_with_wal_store`](crate::abi::moraine_attach_with_wal_store), valid writable
/// `out_items`/`out_len`, a `probe` callable with `probe_ctx` from any
/// thread, and a null-or-writable `err`, all for the duration of the
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_column_tags(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineColumnTagRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        dump_rows(
            handle,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            moraine::ffi_support::dump_column_tags,
            column_tag_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_column_tags`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_column_tags`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_column_tags_free(
    items: *mut MoraineColumnTagRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}

/// One `ducklake_view_column_tag` row, as returned by
/// [`moraine_dump_view_column_tags`] — flattened from the view's latest
/// record, as column tags are from their column's.
#[repr(C)]
pub struct MoraineViewColumnTagRow {
    /// `view_id`.
    pub view_id: u64,
    /// `column_name`, owned. A view's columns are named rather than given
    /// ids, so the name is the key within the view.
    pub column_name: *mut c_char,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `key`, owned.
    pub key: *mut c_char,
    /// `value`, owned.
    pub value: *mut c_char,
}

dump_row! {
    view_column_tag_rows(moraine::ffi_support::ViewColumnTagRow) -> MoraineViewColumnTagRow {
        values { view_id, begin_snapshot }
        optional { end_snapshot => has_end_snapshot }
        strings { column_name, key, value }
        optional_strings {}
    }
}

/// Dumps every `ducklake_view_column_tag` row into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach_with_wal_store`](crate::abi::moraine_attach_with_wal_store), valid writable
/// `out_items`/`out_len`, a `probe` callable with `probe_ctx` from any
/// thread, and a null-or-writable `err`, all for the duration of the
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_view_column_tags(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineViewColumnTagRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        dump_rows(
            handle,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            moraine::ffi_support::dump_view_column_tags,
            view_column_tag_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_view_column_tags`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_view_column_tags`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_view_column_tags_free(
    items: *mut MoraineViewColumnTagRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::CStr, ptr};

    use super::*;
    use crate::{
        abi::moraine_detach,
        dumps::test_support::{TempDir, attach_ok, seed_with_tags},
        error::codes,
    };

    #[test]
    fn dump_tags_and_column_tags_carry_exact_values() {
        let dir = TempDir::new("tags");
        seed_with_tags(dir.path());
        let handle = attach_ok(dir.path());

        let mut items: *mut MoraineTagRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; out/err slots are valid.
        let code = unsafe {
            moraine_dump_tags(
                handle,
                &raw mut items,
                &raw mut len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(len, 1);
        // SAFETY: `items` points to `len` rows written by the call above.
        let row = unsafe { &*items };
        assert_eq!(row.object_id, 2);
        assert_eq!(row.begin_snapshot, 2);
        assert!(!row.has_end_snapshot);
        // SAFETY: owned, NUL-terminated strings written by the dump.
        unsafe {
            assert_eq!(CStr::from_ptr(row.key).to_str().unwrap(), "comment");
            assert_eq!(CStr::from_ptr(row.value).to_str().unwrap(), "our table");
        }

        let mut column_items: *mut MoraineColumnTagRow = ptr::null_mut();
        let mut column_len: usize = 0;
        let mut column_err = MoraineError::default();
        // SAFETY: same contracts as above.
        let column_code = unsafe {
            moraine_dump_column_tags(
                handle,
                &raw mut column_items,
                &raw mut column_len,
                None,
                ptr::null_mut(),
                &raw mut column_err,
            )
        };
        assert_eq!(column_code, codes::OK);
        assert_eq!(column_len, 1);
        // SAFETY: `column_items` points to `column_len` rows written above.
        let column_row = unsafe { &*column_items };
        assert_eq!(column_row.table_id, 2);
        assert_eq!(column_row.column_id, 1);
        assert_eq!(column_row.begin_snapshot, 2);
        assert!(!column_row.has_end_snapshot);
        // SAFETY: owned, NUL-terminated strings written by the dump.
        unsafe {
            assert_eq!(CStr::from_ptr(column_row.key).to_str().unwrap(), "comment");
            assert_eq!(
                CStr::from_ptr(column_row.value).to_str().unwrap(),
                "our column"
            );
        }

        // SAFETY: freed exactly once each.
        unsafe {
            moraine_dump_tags_free(items, len);
            moraine_dump_column_tags_free(column_items, column_len);
            moraine_detach(handle);
        }
    }
}
