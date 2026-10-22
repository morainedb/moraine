use std::{cell::Cell, panic::AssertUnwindSafe, rc::Rc};

use super::{AbiError, DumpRow, convert_rows};

enum Input {
    Row(Rc<Cell<usize>>),
    Error,
    Panic,
}

struct Row(Rc<Cell<usize>>);

// SAFETY: each successful conversion owns one release counter reference;
// release updates that counter without unwinding.
unsafe impl DumpRow for Row {
    type Record = Input;

    fn memory_bytes(_: &Self::Record) -> u64 {
        64
    }

    fn convert(record: Input) -> Result<Self, AbiError> {
        match record {
            Input::Row(counter) => Ok(Self(counter)),
            Input::Error => Err(AbiError::invalid_argument("invalid row")),
            Input::Panic => panic!("conversion panicked"),
        }
    }

    unsafe fn release(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[test]
fn conversion_failure_releases_every_completed_row() {
    for completed in [0, 1, 3] {
        let releases = Rc::new(Cell::new(0));
        let mut records: Vec<_> = (0..completed)
            .map(|_| Input::Row(releases.clone()))
            .collect();
        records.push(Input::Error);
        records.push(Input::Row(releases.clone()));
        assert!(convert_rows::<Row>(records).is_err());
        assert_eq!(releases.get(), completed);
    }
}

#[test]
fn conversion_panic_releases_every_completed_row() {
    let releases = Rc::new(Cell::new(0));
    let records = vec![Input::Row(releases.clone()), Input::Panic];
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| convert_rows::<Row>(records)));
    assert!(result.is_err());
    assert_eq!(releases.get(), 1);
}

#[test]
fn successful_conversion_transfers_ownership_to_the_caller() {
    let releases = Rc::new(Cell::new(0));
    let records = vec![Input::Row(releases.clone()), Input::Row(releases.clone())];
    let (mut rows, _memory) = convert_rows::<Row>(records).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(releases.get(), 0);
    for row in &mut rows {
        // SAFETY: each row was converted successfully and is released once.
        unsafe { row.release() };
    }
    assert_eq!(releases.get(), 2);
    assert!(convert_rows::<Row>(Vec::new()).unwrap().0.is_empty());
}

#[test]
fn dump_failure_keeps_outputs_untouched_and_releases_converted_rows() {
    use std::{ptr, sync::Arc};

    use crate::{
        abi::moraine_error_free,
        error::{MoraineError, codes},
        runtime::{AttachedCatalog, MoraineCatalogHandle},
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let catalog = runtime
        .block_on(moraine::Catalog::open(
            Arc::new(object_store::memory::InMemory::new()),
            moraine::CatalogOptions::default(),
        ))
        .unwrap();
    let mut handle = MoraineCatalogHandle::new(runtime, AttachedCatalog::Writer(catalog), 0);
    for (failure, expected_code) in [
        (Input::Error, codes::INVALID_ARGUMENT),
        (Input::Panic, codes::INTERNAL),
    ] {
        let releases = Rc::new(Cell::new(0));
        let completed = Input::Row(Rc::clone(&releases));
        let mut output: *mut Row = ptr::null_mut();
        let mut len = 99;
        let mut error = MoraineError::default();
        // SAFETY: the handle is open and all outputs are writable local slots.
        let code = unsafe {
            crate::dumps::dump_rows(
                &raw mut handle,
                &raw mut output,
                &raw mut len,
                None,
                ptr::null_mut(),
                &raw mut error,
                async move |catalog: &moraine::ReadOnlyCatalog| {
                    catalog.snapshot().await?;
                    Ok(vec![completed, failure])
                },
            )
        };
        assert_eq!(code, expected_code);
        assert_eq!(error.code, expected_code);
        assert!(output.is_null());
        assert_eq!(len, 99);
        assert_eq!(releases.get(), 1);
        // SAFETY: the error was just populated and has not been freed.
        unsafe { moraine_error_free(error.message) };
    }
    handle
        .runtime
        .block_on(handle.catalog.writer().unwrap().close())
        .unwrap();
}
