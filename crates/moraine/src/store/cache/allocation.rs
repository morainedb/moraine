//! Elastic per-store capacities under the process's shared memory ceiling.

use super::{CACHE_SHARDS, Ordering, StoreCache, as_bytes, caches};

const ALLOCATION_QUANTUM: u64 = 1024 * 1024;

impl StoreCache {
    fn capacity(&self) -> u64 {
        self.capacity.load(Ordering::Relaxed)
    }

    /// Records an upper bound even if a resize only changes some shards.
    fn resize(&self, capacity: u64) -> bool {
        let previous = self.capacity();
        if previous == capacity {
            return true;
        }
        let resized = self.tier.resize(capacity);
        self.capacity.store(
            if resized {
                capacity
            } else {
                previous.max(capacity)
            },
            Ordering::Relaxed,
        );
        resized
    }

    pub(super) fn release(&self) {
        if self.budget.is_none() {
            return;
        }
        let _caches = caches();
        if self.attached.load(Ordering::Acquire) == 0 {
            self.resize(0);
        }
    }

    /// Grows before admission, amortizing pressure checks over admitted bytes.
    pub(super) fn reserve(&self, bytes: u64) {
        let Some(budget) = self.budget else {
            return;
        };
        let capacity = self.capacity();
        if capacity >= budget || bytes > budget {
            return;
        }
        let entry_capacity = bytes.saturating_mul(as_bytes(CACHE_SHARDS * 2)).min(budget);
        let usage = as_bytes(self.tier.usage());
        if capacity >= entry_capacity
            && usage.saturating_add(bytes) <= capacity.saturating_mul(3) / 4
        {
            return;
        }
        let pressure = self.pressure.fetch_add(bytes, Ordering::Relaxed);
        if pressure.saturating_add(bytes) < (capacity / 16).min(ALLOCATION_QUANTUM) {
            return;
        }
        self.pressure.store(0, Ordering::Relaxed);

        let caches = caches();
        if self.attached.load(Ordering::Acquire) == 0 {
            return;
        }
        let capacity = self.capacity();
        if capacity >= entry_capacity
            && as_bytes(self.tier.usage()).saturating_add(bytes) <= capacity.saturating_mul(3) / 4
        {
            return;
        }
        let mut available = budget.saturating_sub(
            caches
                .stores
                .values()
                .map(|store| store.capacity())
                .sum::<u64>(),
        );
        let mut donors: Vec<_> = caches
            .stores
            .values()
            .filter(|store| store.capacity() > 0 && !std::ptr::eq(self, store.as_ref()))
            .collect();
        donors.sort_by_key(|store| store.tier.usage());
        let busy = 1 + donors
            .iter()
            .filter(|store| store.attached.load(Ordering::Acquire) > 0 && store.tier.usage() > 0)
            .count();
        let fair = budget / as_bytes(busy);
        let growth = capacity
            .saturating_mul(2)
            .max(ALLOCATION_QUANTUM.min((budget / 8).max(1)))
            .max(entry_capacity)
            .min(budget);
        let target = growth.max(fair);
        if target <= capacity {
            return;
        }

        // Reclaim unused allowance before evicting any other working set.
        for donor in &donors {
            if available >= target - capacity {
                break;
            }
            let occupied = as_bytes(donor.tier.usage());
            let protected = occupied.saturating_add(occupied / 3);
            reclaim(donor, protected, target - capacity, &mut available);
        }

        // A newly busy store can reclaim its fair share from earlier borrowers.
        let fair_target = growth.min(fair);
        if capacity + available < fair_target {
            for donor in &donors {
                reclaim(donor, fair, fair_target - capacity, &mut available);
            }
        }
        self.resize(target.min(capacity + available));
    }
}

fn reclaim(donor: &StoreCache, floor: u64, wanted: u64, available: &mut u64) {
    let capacity = donor.capacity();
    let release = capacity
        .saturating_sub(floor)
        .min(wanted.saturating_sub(*available));
    if release > 0 && donor.resize(capacity - release) {
        *available += release;
    }
}
