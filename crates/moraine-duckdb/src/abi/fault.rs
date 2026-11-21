//! Transfers a session's armed faults from the shim to the call they
//! target, before that call enters the async runtime: a read stall for the
//! next cancellable read, and a dropped acknowledgement for the next commit.

use std::{cell::Cell, time::Duration};

thread_local! {
    static STALL_MILLISECONDS: Cell<u64> = const { Cell::new(0) };
    static DROP_COMMIT_ACK: Cell<bool> = const { Cell::new(false) };
}

/// Arms the next commit on this thread to land and then report its outcome
/// as unknown, as a commit whose acknowledgement was lost does.
#[unsafe(no_mangle)]
pub extern "C" fn moraine_fault_drop_next_commit_ack() {
    DROP_COMMIT_ACK.set(true);
}

/// Consumes the armed acknowledgement drop.
pub(crate) fn take_dropped_commit_ack() -> bool {
    DROP_COMMIT_ACK.replace(false)
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

    #[test]
    fn a_dropped_acknowledgement_is_consumed_once_and_isolated_between_threads() {
        assert!(!take_dropped_commit_ack());
        moraine_fault_drop_next_commit_ack();
        std::thread::spawn(|| assert!(!take_dropped_commit_ack()))
            .join()
            .unwrap();
        assert!(take_dropped_commit_ack());
        assert!(!take_dropped_commit_ack());
    }
}
