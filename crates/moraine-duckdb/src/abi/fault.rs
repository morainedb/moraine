//! Transfers a session's armed read stall from its interrupt probe to the
//! cancellable call, before that call enters the async runtime.

use std::{cell::Cell, time::Duration};

thread_local! {
    static STALL_MILLISECONDS: Cell<u64> = const { Cell::new(0) };
}

/// Arms a stall for the cancellable call entering on this thread.
/// The shim consumes its session-owned arm before making this handoff.
#[unsafe(no_mangle)]
pub extern "C" fn moraine_fault_stall_next_read(milliseconds: u64) {
    STALL_MILLISECONDS.set(milliseconds);
}

/// Consumes the handoff before entering the async runtime.
pub(crate) fn take_read_stall() -> Duration {
    Duration::from_millis(STALL_MILLISECONDS.replace(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_stalls_are_consumed_once_and_isolated_between_threads() {
        moraine_fault_stall_next_read(40);
        std::thread::spawn(|| {
            assert_eq!(take_read_stall(), Duration::ZERO);
            moraine_fault_stall_next_read(80);
            assert_eq!(take_read_stall(), Duration::from_millis(80));
        })
        .join()
        .unwrap();
        assert_eq!(take_read_stall(), Duration::from_millis(40));
        assert_eq!(take_read_stall(), Duration::ZERO);
        moraine_fault_stall_next_read(60_000);
        moraine_fault_stall_next_read(0);
        assert_eq!(take_read_stall(), Duration::ZERO);
    }
}
