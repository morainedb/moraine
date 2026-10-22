//! Shared admission for retained lookup values and cache containers.

use std::{
    ops::Deref,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Debug)]
pub(crate) struct Budget {
    capacity: u64,
    used: AtomicU64,
}

impl Budget {
    pub(crate) fn new(capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            used: AtomicU64::new(0),
        })
    }

    pub(crate) fn capacity(&self) -> u64 {
        self.capacity
    }

    pub(crate) fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }
}

/// A reservation released with its allocation, including spare container space.
#[derive(Debug)]
pub(crate) struct Charge {
    budget: Arc<Budget>,
    bytes: u64,
}

impl Charge {
    pub(crate) fn budget(&self) -> Arc<Budget> {
        Arc::clone(&self.budget)
    }

    pub(crate) fn new(budget: Arc<Budget>) -> Self {
        Self { budget, bytes: 0 }
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) fn resize(&mut self, bytes: u64) -> bool {
        if bytes > self.bytes {
            let additional = bytes - self.bytes;
            if self
                .budget
                .used
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                    used.checked_add(additional)
                        .filter(|&total| total <= self.budget.capacity)
                })
                .is_err()
            {
                return false;
            }
        } else {
            self.budget
                .used
                .fetch_sub(self.bytes - bytes, Ordering::Relaxed);
        }
        self.bytes = bytes;
        true
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct Value<T> {
    value: T,
    charge: OnceLock<Charge>,
}

/// An immutable value whose cache charge follows active readers after eviction.
#[derive(Debug)]
pub(crate) struct Shared<T>(Arc<Value<T>>);

impl<T> Shared<T> {
    pub(crate) fn charged(value: T, charge: Charge) -> Self {
        Self(Arc::new(Value {
            value,
            charge: OnceLock::from(charge),
        }))
    }

    pub(crate) fn new(value: T) -> Self {
        Self(Arc::new(Value {
            value,
            charge: OnceLock::new(),
        }))
    }

    /// Allocation bookkeeping beyond `T`, including the Arc reference counts.
    pub(crate) fn overhead() -> usize {
        size_of::<Value<T>>() - size_of::<T>() + 2 * size_of::<usize>()
    }

    /// Charges a value once. Reusing it requires the same budget and weight.
    pub(crate) fn try_charge(&self, budget: &Arc<Budget>, bytes: u64) -> bool {
        if let Some(charge) = self.0.charge.get() {
            return Arc::ptr_eq(&charge.budget, budget) && charge.bytes == bytes;
        }
        let mut charge = Charge::new(Arc::clone(budget));
        if !charge.resize(bytes) {
            return false;
        }
        let _ = self.0.charge.set(charge);
        self.0
            .charge
            .get()
            .is_some_and(|charge| Arc::ptr_eq(&charge.budget, budget) && charge.bytes == bytes)
    }
}

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> Deref for Shared<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0.value
    }
}

impl<T> AsRef<T> for Shared<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T: PartialEq> PartialEq for Shared<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

/// Estimated hash-table storage, including control bytes and unused buckets.
pub(crate) fn hash_bytes<T>(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    capacity
        .checked_next_power_of_two()
        .and_then(|buckets| buckets.checked_mul(size_of::<T>() + 1))
        .and_then(|bytes| bytes.checked_add(16))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
