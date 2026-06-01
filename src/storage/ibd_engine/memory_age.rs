//! `MemoryAge`: one tier of the age-tiered UTXO index.
//!
//! Wraps a list of `MemoryRun`s with snapshot-read semantics: readers take an `Arc` clone under
//! a short write-lock, then iterate without holding any lock. Writes replace the `Arc<Vec<…>>`
//! atomically. Uses `parking_lot::RwLock` (already a dep) instead of `arc-swap` to avoid a new
//! crate dependency. Profiler can revisit if read-lock contention shows up.

use super::memory_run::{MemoryRun, QueryResult};
use super::types::{OutputId, OutputKV};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;

/// RAII guard that pins a height in a `MemoryAge`, preventing the compacter from merging it away.
pub struct Pin {
    age_pins: Arc<parking_lot::Mutex<BTreeSet<i32>>>,
    height: i32,
}

impl Pin {
    fn new(age_pins: Arc<parking_lot::Mutex<BTreeSet<i32>>>, height: i32) -> Self {
        age_pins.lock().insert(height);
        Self { age_pins, height }
    }
}

impl Drop for Pin {
    fn drop(&mut self) {
        self.age_pins.lock().remove(&self.height);
    }
}

/// One tier of the 7-age UTXO index.
///
/// Snapshot pattern:
/// - Readers: `runs.read()` → `Arc::clone` → release → iterate snapshot.
/// - Appends: `runs.write()` → build new `Arc<Vec<…>>` → replace → release.
///
/// The write lock is held only during the Arc swap, not during run builds.
pub struct MemoryAge {
    pub(super) is_mutable: bool,
    /// Fan-in threshold: trigger merge when `runs.len() >= merge_fan_in`.
    pub(super) merge_fan_in: usize,
    /// Snapshot of `Arc<MemoryRun>` list. Readers clone the outer Arc, iterate without lock.
    pub(super) runs: parking_lot::RwLock<Arc<Vec<Arc<MemoryRun>>>>,
    /// Heights pinned by in-flight blocks. Compacter must not merge below `min(pins)`.
    pins: Arc<parking_lot::Mutex<BTreeSet<i32>>>,
    /// Highest height fully merged out of this age (into the next older age).
    pub(super) merged_to: AtomicI32,
    /// CAS guard: only one compacter thread may merge this age at a time.
    pub(super) is_merging: AtomicBool,
    /// Callback to notify the compacter that this age is ready for merge.
    enqueue: Option<Box<dyn Fn() + Send + Sync>>,
}

impl std::fmt::Debug for MemoryAge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryAge")
            .field("is_mutable", &self.is_mutable)
            .field("merge_fan_in", &self.merge_fan_in)
            .field("merged_to", &self.merged_to.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl MemoryAge {
    pub fn new(is_mutable: bool, merge_fan_in: usize) -> Self {
        Self::new_with_enqueue(is_mutable, merge_fan_in, None)
    }

    pub fn new_with_enqueue(
        is_mutable: bool,
        merge_fan_in: usize,
        enqueue: Option<Box<dyn Fn() + Send + Sync>>,
    ) -> Self {
        Self {
            is_mutable,
            merge_fan_in,
            runs: parking_lot::RwLock::new(Arc::new(Vec::new())),
            pins: Arc::new(parking_lot::Mutex::new(BTreeSet::new())),
            merged_to: AtomicI32::new(i32::MIN),
            is_merging: AtomicBool::new(false),
            enqueue,
        }
    }

    /// Pin `height` to prevent the compacter from merging it away.
    /// Returns a RAII `Pin` guard that unpins on drop.
    pub fn pin_height(&self, height: i32) -> Pin {
        Pin::new(Arc::clone(&self.pins), height)
    }

    /// Snapshot the current run list (cheap: one Arc clone under a short read-lock).
    pub fn snapshot_runs(&self) -> Arc<Vec<Arc<MemoryRun>>> {
        Arc::clone(&*self.runs.read())
    }

    /// Append a new batch of entries at `height`.
    ///
    /// For the mutable tip: extends the last run (if mutable) or creates a new one.
    /// For frozen ages: creates a new run (called by compacter after merge).
    pub fn append(&self, entries: Vec<OutputKV>, height: i32) {
        debug_assert!(!entries.is_empty());
        let mut lock = self.runs.write();
        let old = Arc::clone(&*lock);

        // If the last run is mutable (tip only), extend it in place.
        // Otherwise create a new immutable run.
        let new_arc: Arc<Vec<Arc<MemoryRun>>> = if self.is_mutable {
            if let Some(last) = old.last() {
                if last.is_mutable {
                    let mut new_runs = (*old).clone();
                    let mut run = (**last).clone();
                    run.append_and_rebuild(&entries);
                    *new_runs.last_mut().unwrap() = Arc::new(run);
                    Arc::new(new_runs)
                } else {
                    let mut new_run = MemoryRun::new_mutable();
                    new_run.append_and_rebuild(&entries);
                    let mut new_runs = (*old).clone();
                    new_runs.push(Arc::new(new_run));
                    Arc::new(new_runs)
                }
            } else {
                let mut new_run = MemoryRun::new_mutable();
                new_run.append_and_rebuild(&entries);
                Arc::new(vec![Arc::new(new_run)])
            }
        } else {
            // Frozen age (receiving from compacter merge): always a new immutable run.
            let run = MemoryRun::build(entries);
            let mut new_runs = (*old).clone();
            new_runs.push(Arc::new(run));
            Arc::new(new_runs)
        };

        *lock = new_arc;
        drop(lock);

        // Notify compacter if fan-in threshold reached.
        let run_count = self.runs.read().len();
        if run_count >= self.merge_fan_in {
            if let Some(ref eq) = self.enqueue {
                eq();
            }
        }
    }

    /// Freeze the mutable tip run (if any). Called before pushing to next older age.
    pub fn freeze_tip(&self) {
        let mut lock = self.runs.write();
        let old = Arc::clone(&*lock);
        if let Some(last) = old.last() {
            if last.is_mutable {
                let mut new_runs = (*old).clone();
                let mut frozen = (**last).clone();
                frozen.freeze();
                *new_runs.last_mut().unwrap() = Arc::new(frozen);
                *lock = Arc::new(new_runs);
            }
        }
    }

    /// Query this age for `key` within the `[since, before)` height window.
    ///
    /// Iterates runs newest-to-oldest (last-to-first in the Vec), stopping on first resolved hit.
    pub fn lookup_key(&self, key: &[u8; 36], since: i32, before: i32) -> Option<OutputId> {
        let snapshot = self.snapshot_runs();
        for run in snapshot.iter().rev() {
            if let Some(id) = run.lookup_key(key, since, before) {
                return Some(id);
            }
        }
        None
    }

    /// Batch query across all runs in this age.
    ///
    /// For each key in `keys`, fills `ids[i]` with the found `OutputId` (or leaves it as
    /// `OutputId::MAX` if not found here). Callers chain multiple ages newest-to-oldest.
    pub fn batch_query(
        &self,
        keys: &[[u8; 36]],
        ids: &mut [OutputId],
        since: i32,
        before: i32,
    ) -> QueryResult {
        let snapshot = self.snapshot_runs();
        let mut result = QueryResult::default();

        // Newest-to-oldest: last run first.
        for run in snapshot.iter().rev() {
            run.batch_lookup(keys, ids, since, before);
        }

        // Tally result from id sentinels.
        for id in ids.iter() {
            if *id == OutputId::MAX {
                result.absent += 1;
            } else {
                result.resolved += 1;
            }
        }
        result
    }

    /// Returns `true` if this age has enough runs to trigger a merge.
    pub fn merge_ready(&self) -> bool {
        let run_count = self.runs.read().len();
        if run_count < self.merge_fan_in {
            return false;
        }
        // Do not merge if any pinned height falls within the oldest `fan_in` runs.
        let snapshot = self.snapshot_runs();
        let merge_candidates = &snapshot[..self.merge_fan_in.min(snapshot.len())];
        if merge_candidates.is_empty() {
            return false;
        }
        let merge_min = merge_candidates.iter().map(|r| r.height_range.0).min().unwrap_or(i32::MAX);
        let merge_max = merge_candidates.iter().map(|r| r.height_range.1).max().unwrap_or(i32::MIN);
        let pins = self.pins.lock();
        !pins.range(merge_min..=merge_max).next().is_some()
    }

    /// Take the oldest `merge_fan_in` runs for merging. Returns them (or None if not ready).
    ///
    /// Marks `is_merging = true` via CAS. Caller must call `complete_merge` when done.
    pub fn take_for_merge(&self) -> Option<Vec<Arc<MemoryRun>>> {
        if self.is_merging.compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed).is_err() {
            return None; // another thread is already merging
        }
        if !self.merge_ready() {
            self.is_merging.store(false, Ordering::Release);
            return None;
        }
        let snapshot = self.snapshot_runs();
        let take_n = self.merge_fan_in.min(snapshot.len());
        // Take the oldest (first in Vec) runs.
        let taken: Vec<Arc<MemoryRun>> = snapshot[..take_n].to_vec();
        // Remove them from the age.
        let remaining: Vec<Arc<MemoryRun>> = snapshot[take_n..].to_vec();
        *self.runs.write() = Arc::new(remaining);
        Some(taken)
    }

    /// Called after merge is complete to clear the merge guard and update watermark.
    pub fn complete_merge(&self, merged_height: i32) {
        self.merged_to.fetch_max(merged_height, Ordering::Relaxed);
        self.is_merging.store(false, Ordering::Release);
    }

    /// Remove all entries with `height >= since` from mutable runs (reorg recovery).
    pub fn erase_since(&self, since: i32) {
        debug_assert!(self.is_mutable, "erase_since on frozen age");
        let mut lock = self.runs.write();
        let old = Arc::clone(&*lock);
        let mut new_runs: Vec<Arc<MemoryRun>> = Vec::with_capacity(old.len());
        for run in old.iter() {
            if run.height_range.0 >= since {
                // Entire run is at or after `since` — drop it.
                continue;
            }
            if run.height_range.1 < since {
                // Entire run is before `since` — keep as-is.
                new_runs.push(Arc::clone(run));
            } else {
                // Partial overlap — erase in place (requires clone since runs are Arc).
                let mut r = (**run).clone();
                r.erase_since(since);
                if !r.is_empty() {
                    new_runs.push(Arc::new(r));
                }
            }
        }
        *lock = Arc::new(new_runs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::types::OutputKV;

    fn make_key(n: u8) -> [u8; 36] {
        let mut k = [0u8; 36];
        k[0] = n;
        k
    }

    #[test]
    fn test_age_append_and_lookup() {
        let age = MemoryAge::new(true, 8);
        let k = make_key(1);
        age.append(vec![OutputKV::new_add(k, 100, 42)], 100);
        assert_eq!(age.lookup_key(&k, 0, i32::MAX), Some(42));
        assert_eq!(age.lookup_key(&make_key(2), 0, i32::MAX), None);
    }

    #[test]
    fn test_pin_prevents_merge() {
        let age = MemoryAge::new(false, 2);
        // Add 2 runs to trigger fan-in.
        age.append(vec![OutputKV::new_add(make_key(1), 10, 1)], 10);
        age.freeze_tip();
        age.append(vec![OutputKV::new_add(make_key(2), 20, 2)], 20);
        // Pin height 10 — in the merge range.
        let _pin = age.pin_height(10);
        assert!(!age.merge_ready(), "merge_ready should be false while height is pinned");
    }

    #[test]
    fn test_erase_since_mutable() {
        let age = MemoryAge::new(true, 8);
        let k1 = make_key(1);
        let k2 = make_key(2);
        age.append(vec![OutputKV::new_add(k1, 50, 1)], 50);
        age.append(vec![OutputKV::new_add(k2, 100, 2)], 100);
        age.erase_since(75);
        assert_eq!(age.lookup_key(&k1, 0, i32::MAX), Some(1));
        assert_eq!(age.lookup_key(&k2, 0, i32::MAX), None);
    }
}
