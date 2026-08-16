//! Shared scaffolding for the crate's FFI unit tests: temp directories,
//! attach/transaction drivers, and staged-row cell constructors.

use std::{
    ffi::CString,
    path::{Path, PathBuf},
    ptr,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{
    abi::moraine_attach,
    error::{MoraineError, codes},
    runtime::MoraineCatalogHandle,
    staged::{MoraineCell, MoraineTxHandle, moraine_tx_begin, moraine_tx_commit, moraine_tx_stage},
};

/// A directory under the OS temp dir, unique per call, removed on drop.
pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("moraine-duckdb-{tag}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("test setup: create temp dir");
        Self(dir)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }

    pub(crate) fn c_path(&self) -> CString {
        CString::new(self.0.to_str().expect("test path is UTF-8")).expect("no NUL in path")
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Attaches read-write to `dir` with defaults and asserts success.
pub(crate) fn attach_ok(dir: &Path) -> *mut MoraineCatalogHandle {
    let c_path = CString::new(dir.to_str().expect("test path is UTF-8")).expect("no NUL in path");
    let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: `c_path` is a valid C string; outputs are valid local slots.
    let code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            ptr::null(),
            0,
            0,
            0,
            false,
            ptr::null(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut handle,
            &raw mut err,
        )
    };
    // SAFETY: `err.message` is null or was just written by the call above.
    let err_message = unsafe { err.message.as_ref() };
    assert_eq!(code, codes::OK, "attach failed: {err_message:?}");
    assert!(!handle.is_null());
    handle
}

/// Attaches read-write to `dir` with `data_dir` as `META_DATA_PATH`, so
/// the handle carries the store a scoped read needs, and asserts success.
pub(crate) fn attach_with_data_path(dir: &Path, data_dir: &Path) -> *mut MoraineCatalogHandle {
    let c_path = CString::new(dir.to_str().expect("test path is UTF-8")).expect("no NUL in path");
    let c_data =
        CString::new(data_dir.to_str().expect("test path is UTF-8")).expect("no NUL in path");
    let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: both C strings are valid; outputs are valid local slots.
    let code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            ptr::null(),
            0,
            0,
            0,
            false,
            c_data.as_ptr(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut handle,
            &raw mut err,
        )
    };
    // SAFETY: `err.message` is null or was just written by the call above.
    let err_message = unsafe { err.message.as_ref() };
    assert_eq!(code, codes::OK, "attach failed: {err_message:?}");
    assert!(!handle.is_null());
    handle
}

/// Opens a staged-row transaction and asserts success.
pub(crate) fn begin(handle: *mut MoraineCatalogHandle) -> *mut MoraineTxHandle {
    let mut tx: *mut MoraineTxHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: `handle` is attached; outputs are valid local slots.
    let code =
        unsafe { moraine_tx_begin(handle, &raw mut tx, None, ptr::null_mut(), &raw mut err) };
    // SAFETY: `err.message` is null or was just written by `moraine_tx_begin`.
    assert_eq!(code, codes::OK, "begin failed: {:?}", unsafe {
        err.message.as_ref()
    });
    assert!(!tx.is_null());
    tx
}

/// Stages one row and asserts success.
pub(crate) fn stage(
    tx: *mut MoraineTxHandle,
    table_kind: i32,
    operation_kind: i32,
    cells: &[MoraineCell],
) {
    let mut err = MoraineError::default();
    // SAFETY: `tx` is a live handle from `moraine_tx_begin`; `cells` is a
    // valid slice for the duration of this call.
    let code = unsafe {
        moraine_tx_stage(
            tx,
            table_kind,
            operation_kind,
            cells.as_ptr(),
            cells.len(),
            &raw mut err,
        )
    };
    // SAFETY: `err.message` is null or was just written by `moraine_tx_stage`.
    assert_eq!(code, codes::OK, "stage failed: {:?}", unsafe {
        err.message.as_ref()
    });
}

/// Commits a staged transaction, asserting success and returning the
/// minted snapshot id.
pub(crate) fn commit(tx: *mut MoraineTxHandle) -> u64 {
    let mut id: u64 = 0;
    let mut err = MoraineError::default();
    // SAFETY: `tx` is live; outputs are valid local slots.
    let code = unsafe { moraine_tx_commit(tx, &raw mut id, None, ptr::null_mut(), &raw mut err) };
    // SAFETY: `err.message` is null or was just written by `moraine_tx_commit`.
    assert_eq!(code, codes::OK, "commit failed: {:?}", unsafe {
        err.message.as_ref()
    });
    id
}

pub(crate) fn u64_cell(v: u64) -> MoraineCell {
    MoraineCell {
        kind: 1,
        u64_value: v,
        i64_value: 0,
        bool_value: false,
        str_value: ptr::null(),
    }
}

pub(crate) fn i64_cell(v: i64) -> MoraineCell {
    MoraineCell {
        kind: 2,
        u64_value: 0,
        i64_value: v,
        bool_value: false,
        str_value: ptr::null(),
    }
}

pub(crate) fn bool_cell(v: bool) -> MoraineCell {
    MoraineCell {
        kind: 3,
        u64_value: 0,
        i64_value: 0,
        bool_value: v,
        str_value: ptr::null(),
    }
}

pub(crate) fn null_cell() -> MoraineCell {
    MoraineCell {
        kind: 0,
        u64_value: 0,
        i64_value: 0,
        bool_value: false,
        str_value: ptr::null(),
    }
}

/// Owns the `CString`s a string cell borrows from, so the cells stay
/// valid for the `moraine_tx_stage` call that reads them.
pub(crate) struct StrArena(Vec<CString>);

impl StrArena {
    pub(crate) fn new() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn cell(&mut self, s: &str) -> MoraineCell {
        let c = CString::new(s).expect("test string has no NUL");
        let ptr = c.as_ptr();
        self.0.push(c);
        MoraineCell {
            kind: 4,
            u64_value: 0,
            i64_value: 0,
            bool_value: false,
            str_value: ptr,
        }
    }
}
