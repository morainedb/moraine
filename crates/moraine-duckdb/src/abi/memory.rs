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

pub(crate) fn take(pointer: usize) -> Option<MemoryReservation> {
    EXPORTED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&pointer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_cleanup_unregisters_before_releasing_the_allocation() {
        let mut pointer = std::ptr::null_mut();
        let mut length = 0;
        // SAFETY: output slots are writable and the array is freed once below.
        unsafe {
            crate::abi::write_reserved_array(
                vec![42_u64],
                MemoryReservation::reserve(8192).unwrap(),
                &raw mut pointer,
                &raw mut length,
            );
        }
        let mut registered_during_cleanup = false;
        // SAFETY: pointer and length are the unconsumed outputs above.
        unsafe {
            crate::abi::free_array(pointer, length, |_| {
                registered_during_cleanup =
                    EXPORTED.lock().unwrap().contains_key(&(pointer as usize));
            });
        }
        assert!(!registered_during_cleanup);
    }

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
        let _memory = take(pointer as usize);
        assert!(!EXPORTED.lock().unwrap().contains_key(&(pointer as usize)));
        // SAFETY: the pointer comes from Box::into_raw above and is freed once.
        drop(unsafe { Box::from_raw(pointer) });
    }
}
