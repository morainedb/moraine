//! Reservations for allocations whose ownership crosses the C ABI.

use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex},
};

use moraine::MemoryReservation;

static EXPORTED: LazyLock<Mutex<HashMap<usize, MemoryReservation>>> = LazyLock::new(Mutex::default);

pub(crate) fn retain(pointer: usize, memory: MemoryReservation) {
    EXPORTED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(pointer, memory);
}

pub(crate) fn release(pointer: usize) {
    EXPORTED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&pointer);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exported_allocation_retains_its_allowance_until_release() {
        let memory = MemoryReservation::reserve(8192).unwrap();
        let pointer = Box::into_raw(Box::new(42_u64));
        retain(pointer as usize, memory);
        assert_eq!(
            EXPORTED
                .lock()
                .unwrap()
                .get(&(pointer as usize))
                .unwrap()
                .bytes(),
            8192
        );
        release(pointer as usize);
        assert!(!EXPORTED.lock().unwrap().contains_key(&(pointer as usize)));
        // SAFETY: the pointer comes from Box::into_raw above and is freed once.
        drop(unsafe { Box::from_raw(pointer) });
    }
}
