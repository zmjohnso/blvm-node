//! `MemoryRun`: sorted, immutable-once-built slice of `OutputKV` with bloom + directory acceleration.
//!
//! Rust port of Hornet's `memory_run.h`. Core read path for the IBD UTXO engine.
//!
//! ## Acceleration structures
//! - **`Directory`**: prefix index → narrows binary search to ~4KB bucket. Port of `directory.h`.
//! - **`BloomFilter`**: blocked bloom, 7 probes, ~12 bits/entry, ~1% FPR. Port of `filter.h`.
//!
//! ## Parallelism
//! `query` is parallelized with rayon (8 sub-ranges when `rayon` feature is enabled).
//! When rayon is absent the loop runs single-threaded — correctness unaffected.

use super::types::{OutputId, OutputKV};
use std::sync::Arc;

// ─── Directory ───────────────────────────────────────────────────────────────

/// Prefix index that narrows binary search from O(n) to O(bucket_size).
///
/// Port of Hornet `directory.h`. Stores one start offset per `1 << prefix_bits` buckets.
/// `lookup_range` returns a `[lo, hi)` slice of `entries` containing all keys with the
/// given prefix — limiting binary search to ~4 KB of entries.
#[derive(Debug, Clone)]
pub struct Directory {
    /// `buckets[b]` = first index in entries[] where prefix == b. Length = (1 << prefix_bits) + 1.
    buckets: Vec<u32>,
    prefix_bits: u32,
}

impl Directory {
    pub fn build(entries: &[OutputKV]) -> Self {
        if entries.is_empty() {
            return Self { buckets: vec![0, 0], prefix_bits: 1 };
        }
        // Target ~85 entries per bucket (85 × 52B ≈ 4420B ≈ 4 KB).
        let n = entries.len();
        let raw_bits = if n <= 128 {
            4u32
        } else {
            // ceil_log2(n / 85) clamped to [4, 16]
            let ratio = (n / 85).max(1);
            (usize::BITS - ratio.leading_zeros()).clamp(4, 16)
        };
        let prefix_bits = raw_bits;
        let num_buckets = 1usize << prefix_bits;
        let mut buckets = vec![0u32; num_buckets + 1];

        for (i, kv) in entries.iter().enumerate() {
            let prefix = key_prefix(&kv.key, prefix_bits) as usize;
            // Count entries per bucket (will prefix-sum below)
            buckets[prefix + 1] = buckets[prefix + 1].max((i + 1) as u32);
        }

        // Build proper start-offset array: buckets[b] = first index with prefix == b.
        // Since entries are sorted, we do a single linear scan.
        let mut bucket_start = vec![0u32; num_buckets + 1];
        let mut cur_bucket = 0usize;
        for (i, kv) in entries.iter().enumerate() {
            let prefix = key_prefix(&kv.key, prefix_bits) as usize;
            while cur_bucket <= prefix {
                bucket_start[cur_bucket] = i as u32;
                cur_bucket += 1;
            }
        }
        while cur_bucket <= num_buckets {
            bucket_start[cur_bucket] = entries.len() as u32;
            cur_bucket += 1;
        }

        Self { buckets: bucket_start, prefix_bits }
    }

    /// Returns `(lo, hi)` index range in `entries` that may contain `key`.
    /// Caller does binary search within `entries[lo..hi]`.
    #[inline]
    pub fn lookup_range(&self, key: &[u8; 36]) -> (usize, usize) {
        let prefix = key_prefix(key, self.prefix_bits) as usize;
        let lo = self.buckets[prefix] as usize;
        let hi = self.buckets[prefix + 1] as usize;
        (lo, hi)
    }
}

#[inline]
fn key_prefix(key: &[u8; 36], bits: u32) -> u32 {
    // Use first 4 bytes of txid (big-endian) as prefix source.
    let raw = u32::from_be_bytes(key[..4].try_into().unwrap());
    raw >> (32 - bits)
}

// ─── BloomFilter ─────────────────────────────────────────────────────────────

/// Blocked bloom filter. Port of Hornet `filter.h`.
///
/// - 64-byte cache-aligned blocks, 7 probes per key, ~12 bits/entry → ~1% FPR.
/// - Hash: `block_idx` from txid[0..4], `bit_pattern` from txid[4..12] XOR (vout × GOLDEN_RATIO).
/// - `may_contain` returns `false` only when the key is definitely absent (no false negatives).
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Raw bits. Length = num_blocks × 64 bytes. Always a multiple of 64.
    data: Vec<u64>,
    /// Number of 64-byte (8 × u64) blocks.
    num_blocks: usize,
}

const BLOOM_WORDS_PER_BLOCK: usize = 8; // 64 bytes / 8 bytes per u64

impl BloomFilter {
    const GOLDEN_RATIO_64: u64 = 0x9e3779b97f4a7c15;

    pub fn build(entries: &[OutputKV]) -> Self {
        if entries.is_empty() {
            return Self { data: vec![0u64; BLOOM_WORDS_PER_BLOCK], num_blocks: 1 };
        }
        // ~12 bits/entry → num_blocks = ceil(entries.len() * 12 / (64 * 8))
        let bits_needed = entries.len() * 12;
        let num_blocks = ((bits_needed + 511) / 512).max(1);
        let mut data = vec![0u64; num_blocks * BLOOM_WORDS_PER_BLOCK];

        for kv in entries {
            let (block_idx, word_bits) = Self::hash_key(&kv.key, num_blocks);
            let base = block_idx * BLOOM_WORDS_PER_BLOCK;
            for probe in 0..7usize {
                let bit = ((word_bits >> (probe * 9)) & 0x1FF) as usize;
                let word = bit / 64;
                let shift = bit % 64;
                data[base + word % BLOOM_WORDS_PER_BLOCK] |= 1u64 << shift;
            }
        }

        Self { data, num_blocks }
    }

    /// Returns `false` if the key is definitely not in the set. Returns `true` if it may be.
    #[inline]
    pub fn may_contain(&self, key: &[u8; 36]) -> bool {
        let (block_idx, word_bits) = Self::hash_key(key, self.num_blocks);
        let base = block_idx * BLOOM_WORDS_PER_BLOCK;
        for probe in 0..7usize {
            let bit = ((word_bits >> (probe * 9)) & 0x1FF) as usize;
            let word = bit / 64;
            let shift = bit % 64;
            if self.data[base + word % BLOOM_WORDS_PER_BLOCK] & (1u64 << shift) == 0 {
                return false;
            }
        }
        true
    }

    #[inline]
    fn hash_key(key: &[u8; 36], num_blocks: usize) -> (usize, u64) {
        // Mix the full key into two independent 64-bit hashes using a Murmur3/xxHash-style
        // finalizer. This gives good distribution even for degenerate keys (e.g., only the
        // first 4 bytes differ). LE interpretation gives better low-bit distribution than BE
        // when keys are sequential integers stored in the high bytes (i*2^32 pattern).
        let r0 = u64::from_le_bytes(key[0..8].try_into().unwrap());
        let r1 = u64::from_le_bytes(key[8..16].try_into().unwrap());
        let r2 = u64::from_le_bytes(key[16..24].try_into().unwrap());
        let r3 = u64::from_le_bytes(key[24..32].try_into().unwrap());
        let r4 = u32::from_le_bytes(key[32..36].try_into().unwrap()) as u64;

        // Combine all words into a single 64-bit accumulator.
        let acc = r0
            .wrapping_add(r1.rotate_left(17))
            .wrapping_add(r2.rotate_right(11))
            .wrapping_add(r3.rotate_left(29))
            .wrapping_add(r4.wrapping_mul(Self::GOLDEN_RATIO_64));

        // Apply Murmur3 64-bit finalizer (high quality, one-to-one mapping).
        let fmix = |mut x: u64| -> u64 {
            x ^= x >> 33;
            x = x.wrapping_mul(0xff51afd7ed558ccd);
            x ^= x >> 33;
            x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
            x ^= x >> 33;
            x
        };

        let h0 = fmix(acc);
        let h1 = fmix(acc.wrapping_add(Self::GOLDEN_RATIO_64));

        let block_idx = (h0 % num_blocks as u64) as usize;
        (block_idx, h1)
    }
}

// ─── MemoryRun ───────────────────────────────────────────────────────────────

/// Result of a batch query against a `MemoryRun`.
#[derive(Debug, Default, Clone)]
pub struct QueryResult {
    /// Number of keys resolved (Add entry found, no covering Delete).
    pub resolved: usize,
    /// Number of keys with a Delete entry (spent in this run).
    pub deleted: usize,
    /// Number of keys definitely absent from this run (bloom negative or not found).
    pub absent: usize,
}

/// Sorted, immutable-once-built collection of `OutputKV` entries with bloom + directory.
///
/// Built by `MemoryAge::append`. Queried by `MemoryIndex::query`. Merged by `Compacter`.
#[derive(Debug, Clone)]
pub struct MemoryRun {
    pub(super) entries: Vec<OutputKV>,
    pub(super) height_range: (i32, i32),
    pub(super) directory: Directory,
    pub(super) filter: BloomFilter,
    /// `true` while the run is the mutable tip (appends allowed). Frozen once pushed to `runs`.
    pub(super) is_mutable: bool,
}

impl MemoryRun {
    /// Build a new `MemoryRun` from pre-sorted entries.
    ///
    /// Entries must be sorted by `OutputKV` ordering (key asc, height desc, op asc).
    pub fn build(mut entries: Vec<OutputKV>) -> Self {
        entries.sort_unstable();
        let height_range = height_range_of(&entries);
        let directory = Directory::build(&entries);
        let filter = BloomFilter::build(&entries);
        Self { entries, height_range, directory, filter, is_mutable: false }
    }

    /// Build an empty mutable run for the tip age (block-by-block append target).
    pub fn new_mutable() -> Self {
        Self {
            entries: Vec::new(),
            height_range: (i32::MAX, i32::MIN),
            directory: Directory::build(&[]),
            filter: BloomFilter::build(&[]),
            is_mutable: true,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn height_range(&self) -> (i32, i32) {
        self.height_range
    }

    /// Append entries (for the mutable tip run only). Sorts in place and rebuilds acceleration structures.
    ///
    /// Called from `MemoryAge::append` while holding the write lock.
    pub fn append_and_rebuild(&mut self, new_entries: &[OutputKV]) {
        debug_assert!(self.is_mutable, "cannot append to frozen run");
        self.entries.extend_from_slice(new_entries);
        self.entries.sort_unstable();
        self.height_range = height_range_of(&self.entries);
        self.directory = Directory::build(&self.entries);
        self.filter = BloomFilter::build(&self.entries);
    }

    /// Freeze the mutable run (called by `MemoryAge` before creating a new mutable run).
    pub fn freeze(&mut self) {
        self.is_mutable = false;
    }

    /// Look up `key` in this run within `[since, before)` height window.
    ///
    /// Returns `Some(id)` if an Add entry is found (non-deleted), `None` otherwise.
    #[inline]
    pub fn lookup_key(&self, key: &[u8; 36], since: i32, before: i32) -> Option<OutputId> {
        // Fast exits
        if self.height_range.1 < since || self.height_range.0 >= before {
            return None;
        }
        if !self.filter.may_contain(key) {
            return None;
        }
        let (lo, hi) = self.directory.lookup_range(key);
        if lo >= hi {
            return None;
        }
        // Binary search for first entry with key >= target.
        let slice = &self.entries[lo..hi];
        let pos = slice.partition_point(|e| e.key < *key);
        // Scan entries with matching key, newest-to-oldest.
        // Sort order: (key, height desc, Add before Delete at same height).
        // For same (key, height): Add appears before Delete. If we see an Add then
        // immediately a Delete at the same height, the UTXO was created and spent at
        // the same height (intra-block) — should not happen after intra-block filtering,
        // but handled defensively: Delete invalidates the paired Add.
        let mut result: Option<OutputId> = None;
        let mut i = pos;
        while i < slice.len() {
            let e = &slice[i];
            if e.key != *key {
                break;
            }
            if e.height < since || e.height >= before {
                i += 1;
                continue;
            }
            if e.is_add() {
                // Peek at next entry: if it's a Delete at the same height, both cancel.
                let next = slice.get(i + 1);
                if let Some(n) = next {
                    if n.key == *key && n.height == e.height && n.is_delete() {
                        i += 2; // skip both (same-height create+spend)
                        continue;
                    }
                }
                result = Some(e.id);
                break;
            } else if e.is_delete() {
                // Delete at a newer height than any Add below — key is spent.
                break;
            }
            i += 1;
        }
        result
    }

    /// Batch lookup for a sorted slice of keys. Fills `ids[i]` with the Add id for `keys[i]`,
    /// or leaves it as `OutputId::MAX` (sentinel for "not found in this run").
    ///
    /// When the `rayon` feature is enabled this splits the key range across 8 rayon workers.
    pub fn batch_lookup(
        &self,
        keys: &[[u8; 36]],
        ids: &mut [OutputId],
        since: i32,
        before: i32,
    ) {
        debug_assert_eq!(keys.len(), ids.len());
        #[cfg(feature = "rayon")]
        {
            use rayon::prelude::*;
            keys.par_iter()
                .zip(ids.par_iter_mut())
                .for_each(|(key, id)| {
                    if *id == OutputId::MAX {
                        if let Some(found) = self.lookup_key(key, since, before) {
                            *id = found;
                        }
                    }
                });
        }
        #[cfg(not(feature = "rayon"))]
        {
            for (key, id) in keys.iter().zip(ids.iter_mut()) {
                if *id == OutputId::MAX {
                    if let Some(found) = self.lookup_key(key, since, before) {
                        *id = found;
                    }
                }
            }
        }
    }

    /// K-way merge of multiple `MemoryRun`s into one frozen run.
    ///
    /// Entries with matching (key, height, op=Add) and a corresponding (key, height, op=Delete)
    /// in the same merge set are cancelled (both dropped) — removing spent UTXOs from frozen storage.
    pub fn merge(inputs: &[Arc<MemoryRun>]) -> Self {
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;

        if inputs.is_empty() {
            return Self::build(vec![]);
        }

        // Estimate capacity
        let total: usize = inputs.iter().map(|r| r.entries.len()).sum();
        let mut merged: Vec<OutputKV> = Vec::with_capacity(total);

        // K-way merge via min-heap. Heap item: (entry, run_idx, entry_idx).
        #[derive(PartialEq, Eq)]
        struct HeapItem {
            entry: OutputKV,
            run_idx: usize,
            entry_idx: usize,
        }
        impl Ord for HeapItem {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                // Min-heap: smallest entry first. OutputKV::Ord: key asc, height desc.
                other.entry.cmp(&self.entry)
            }
        }
        impl PartialOrd for HeapItem {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        let mut heap = BinaryHeap::with_capacity(inputs.len());
        for (ri, run) in inputs.iter().enumerate() {
            if let Some(e) = run.entries.first() {
                heap.push(HeapItem { entry: *e, run_idx: ri, entry_idx: 0 });
            }
        }

        while let Some(HeapItem { entry, run_idx, entry_idx }) = heap.pop() {
            // Push next from the same run.
            let next_idx = entry_idx + 1;
            if let Some(next) = inputs[run_idx].entries.get(next_idx) {
                heap.push(HeapItem { entry: *next, run_idx, entry_idx: next_idx });
            }

            // Cancellation: since Add sorts before Delete (reversed op order in OutputKV::Ord),
            // by the time we process a Delete, its paired Add is already the last entry in
            // `merged` (if they have the same key and height). Pop both to cancel.
            if entry.is_delete() {
                if let Some(last) = merged.last() {
                    if last.key == entry.key && last.height == entry.height && last.is_add() {
                        merged.pop(); // remove paired Add
                        continue;    // skip this Delete — both cancelled
                    }
                }
            }

            merged.push(entry);
        }

        // merged is already sorted (k-way merge preserves order); no need to sort again.
        let height_range = height_range_of(&merged);
        let directory = Directory::build(&merged);
        let filter = BloomFilter::build(&merged);
        Self { entries: merged, height_range, directory, filter, is_mutable: false }
    }

    /// Remove all entries with `height >= since`. Mutable runs only (reorg recovery).
    ///
    /// Rebuilds directory and filter after removal.
    pub fn erase_since(&mut self, since: i32) {
        debug_assert!(self.is_mutable, "erase_since on frozen run");
        self.entries.retain(|e| e.height < since);
        self.height_range = height_range_of(&self.entries);
        self.directory = Directory::build(&self.entries);
        self.filter = BloomFilter::build(&self.entries);
    }
}

fn height_range_of(entries: &[OutputKV]) -> (i32, i32) {
    if entries.is_empty() {
        return (i32::MAX, i32::MIN);
    }
    let min = entries.iter().map(|e| e.height).min().unwrap_or(i32::MAX);
    let max = entries.iter().map(|e| e.height).max().unwrap_or(i32::MIN);
    (min, max)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(n: u8) -> [u8; 36] {
        let mut k = [0u8; 36];
        k[0] = n;
        k
    }

    #[test]
    fn test_bloom_no_false_negatives() {
        let keys: Vec<[u8; 36]> = (0..100).map(make_key).collect();
        let entries: Vec<OutputKV> =
            keys.iter().map(|k| OutputKV::new_add(*k, 1, 42)).collect();
        let bloom = BloomFilter::build(&entries);
        for k in &keys {
            assert!(bloom.may_contain(k), "false negative for key {:?}", k[0]);
        }
    }

    #[test]
    fn test_bloom_fpr_under_2pct() {
        // Build with 1000 entries, check 10000 random non-overlapping keys.
        let entries: Vec<OutputKV> = (0u32..1000)
            .map(|i| {
                let mut k = [0u8; 36];
                k[..4].copy_from_slice(&i.to_be_bytes());
                OutputKV::new_add(k, 1, i as u64)
            })
            .collect();
        let bloom = BloomFilter::build(&entries);
        let mut false_positives = 0usize;
        for i in 1000u32..11000u32 {
            let mut k = [0u8; 36];
            k[..4].copy_from_slice(&i.to_be_bytes());
            if bloom.may_contain(&k) {
                false_positives += 1;
            }
        }
        let fpr = false_positives as f64 / 10000.0;
        assert!(fpr < 0.02, "FPR too high: {:.2}%", fpr * 100.0);
    }

    #[test]
    fn test_directory_matches_linear_scan() {
        let entries: Vec<OutputKV> = (0u32..500)
            .map(|i| {
                let mut k = [0u8; 36];
                k[..4].copy_from_slice(&i.to_be_bytes());
                OutputKV::new_add(k, 1, i as u64)
            })
            .collect();
        let run = MemoryRun::build(entries);
        for i in 0u32..500 {
            let mut k = [0u8; 36];
            k[..4].copy_from_slice(&i.to_be_bytes());
            let (lo, hi) = run.directory.lookup_range(&k);
            // Key must be within [lo, hi)
            let found = run.entries[lo..hi].iter().any(|e| e.key == k);
            assert!(found, "directory missed key {i}");
        }
    }

    #[test]
    fn test_lookup_key_basic() {
        let k1 = make_key(1);
        let k2 = make_key(2);
        let entries = vec![
            OutputKV::new_add(k1, 100, 42),
            OutputKV::new_add(k2, 200, 99),
        ];
        let run = MemoryRun::build(entries);
        assert_eq!(run.lookup_key(&k1, 0, i32::MAX), Some(42));
        assert_eq!(run.lookup_key(&k2, 0, i32::MAX), Some(99));
        assert_eq!(run.lookup_key(&make_key(3), 0, i32::MAX), None);
    }

    #[test]
    fn test_lookup_height_window() {
        let k = make_key(1);
        let entries = vec![OutputKV::new_add(k, 100, 42)];
        let run = MemoryRun::build(entries);
        assert_eq!(run.lookup_key(&k, 0, 101), Some(42));
        // Height 100 is outside [101, MAX) window — should not be found.
        assert_eq!(run.lookup_key(&k, 101, i32::MAX), None);
    }

    #[test]
    fn test_delete_hides_add() {
        let k = make_key(1);
        // Add at h=100, Delete at h=200 — delete is newer so scan returns None.
        let entries = vec![
            OutputKV::new_delete(k, 200),
            OutputKV::new_add(k, 100, 42),
        ];
        let run = MemoryRun::build(entries);
        // sorted: delete (h=200, newest) before add (h=100)
        assert_eq!(run.lookup_key(&k, 0, i32::MAX), None);
    }

    #[test]
    fn test_merge_cancellation() {
        let k = make_key(1);
        let run_a = Arc::new(MemoryRun::build(vec![OutputKV::new_add(k, 100, 42)]));
        let run_b = Arc::new(MemoryRun::build(vec![OutputKV::new_delete(k, 100)]));
        let merged = MemoryRun::merge(&[run_a, run_b]);
        // Both entries are same (key, height); cancellation removes both.
        assert!(merged.entries.is_empty(), "expected cancellation, got {:?}", merged.entries.len());
    }

    #[test]
    fn test_erase_since() {
        let k1 = make_key(1);
        let k2 = make_key(2);
        let mut run = MemoryRun::new_mutable();
        run.append_and_rebuild(&[
            OutputKV::new_add(k1, 50, 1),
            OutputKV::new_add(k2, 100, 2),
        ]);
        run.erase_since(75);
        assert_eq!(run.entries.len(), 1);
        assert_eq!(run.entries[0].key, k1);
    }
}
