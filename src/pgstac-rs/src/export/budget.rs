//! Global memory budget + spill coordination for the buffered-widening
//! geoparquet path (0.9.11).
//!
//! Only the buffered-widening path consumes the budget; the 0.10 stream-write
//! path does not buffer. The budget defaults to ~25% of the available memory,
//! read **cgroup-aware** so it behaves in containers (A7). A partition that would
//! exceed its share spills its buffered items to a temp file before final encode.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Default fraction of detected memory to use as the global buffer budget.
pub const DEFAULT_BUDGET_FRACTION: f64 = 0.25;

/// A global, shared memory budget across all partition workers.
///
/// Workers reserve an estimated number of bytes before buffering a partition; if
/// the reservation would exceed the budget the worker spills to disk instead.
#[derive(Debug, Clone)]
pub struct MemoryBudget {
    total: u64,
    used: Arc<AtomicU64>,
}

impl MemoryBudget {
    /// Creates a budget with an explicit byte cap.
    pub fn with_bytes(total: u64) -> Self {
        MemoryBudget {
            total,
            used: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Creates a budget sized to a fraction of detected (cgroup-aware) memory.
    pub fn from_fraction(fraction: f64) -> Self {
        let detected = detect_available_memory_bytes();
        let total = ((detected as f64) * fraction).max(1.0) as u64;
        Self::with_bytes(total)
    }

    /// Creates the default budget (~25% of detected memory).
    pub fn default_budget() -> Self {
        Self::from_fraction(DEFAULT_BUDGET_FRACTION)
    }

    /// Total budget in bytes.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Bytes currently reserved.
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    /// Tries to reserve `bytes`. Returns a [`BudgetGuard`] that releases on drop,
    /// or `None` if the reservation would exceed the budget (caller should spill).
    pub fn try_reserve(&self, bytes: u64) -> Option<BudgetGuard> {
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_add(bytes);
            if next > self.total {
                return None;
            }
            match self.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(BudgetGuard {
                        used: self.used.clone(),
                        bytes,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }
}

/// A reservation against a [`MemoryBudget`]; releases its bytes on drop.
#[derive(Debug)]
pub struct BudgetGuard {
    used: Arc<AtomicU64>,
    bytes: u64,
}

impl BudgetGuard {
    /// Reserved bytes.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for BudgetGuard {
    fn drop(&mut self) {
        let _ = self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// Detects available memory in bytes, preferring cgroup v2/v1 limits over the
/// raw machine total so the budget stays correct inside containers (A7).
///
/// Falls back to a conservative 2 GiB if nothing can be read.
pub fn detect_available_memory_bytes() -> u64 {
    const FALLBACK: u64 = 2 * 1024 * 1024 * 1024;

    if let Some(limit) = cgroup_memory_limit() {
        return limit;
    }
    if let Some(total) = meminfo_total() {
        return total;
    }
    FALLBACK
}

/// cgroup v2 `memory.max`, then v1 `memory.limit_in_bytes`. A "max" / very large
/// value (unlimited) is treated as no cgroup limit.
fn cgroup_memory_limit() -> Option<u64> {
    // cgroup v2
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let s = s.trim();
        if s != "max"
            && let Ok(v) = s.parse::<u64>()
            && is_real_limit(v)
        {
            return Some(v);
        }
    }
    // cgroup v1
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        && let Ok(v) = s.trim().parse::<u64>()
        && is_real_limit(v)
    {
        return Some(v);
    }
    None
}

/// A cgroup limit at/near the unsigned max (or page-aligned PAGE_COUNTER_MAX)
/// means "unlimited"; ignore it.
fn is_real_limit(v: u64) -> bool {
    // Common "unlimited" sentinels are within a small factor of u64::MAX.
    v < (u64::MAX / 2)
}

/// `MemTotal` from `/proc/meminfo`, in bytes.
fn meminfo_total() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// Creates a named temp file for spilling a partition's buffered items.
///
/// The file is removed when the returned handle is dropped.
pub fn spill_file() -> std::io::Result<tempfile::NamedTempFile> {
    tempfile::Builder::new()
        .prefix("pgstac-dump-spill-")
        .tempfile()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_and_release() {
        let budget = MemoryBudget::with_bytes(100);
        let g1 = budget.try_reserve(60).expect("first fits");
        assert_eq!(budget.used(), 60);
        assert!(budget.try_reserve(50).is_none(), "would exceed -> spill");
        let g2 = budget.try_reserve(40).expect("exact remainder fits");
        assert_eq!(budget.used(), 100);
        drop(g1);
        assert_eq!(budget.used(), 40);
        drop(g2);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn detection_is_positive() {
        assert!(detect_available_memory_bytes() > 0);
    }

    #[test]
    fn spill_file_roundtrip() {
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut f = spill_file().unwrap();
        f.write_all(b"hello").unwrap();
        let _ = f.as_file_mut().seek(SeekFrom::Start(0)).unwrap();
        let mut s = String::new();
        let _ = f.as_file_mut().read_to_string(&mut s).unwrap();
        assert_eq!(s, "hello");
    }
}
