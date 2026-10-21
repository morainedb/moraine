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
    let mut rows = convert_rows::<Row>(records).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(releases.get(), 0);
    for row in &mut rows {
        // SAFETY: each row was converted successfully and is released once.
        unsafe { row.release() };
    }
    assert_eq!(releases.get(), 2);
    assert!(convert_rows::<Row>(Vec::new()).unwrap().is_empty());
}
