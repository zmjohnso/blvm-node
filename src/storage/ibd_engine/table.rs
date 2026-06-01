//! `UtxoTable`: append-only flat file + in-memory tail for UTXO script data.
//!
//! Rust port of Hornet's `table.h`. Stores `{OutputHeader || raw_script_bytes}` per UTXO.
//! The flat file grows monotonically; the in-memory tail holds recent blocks not yet flushed.
//!
//! No bincode. All reads/writes are raw byte copies. `OutputHeader` is `repr(C)`.
//!
//! ## Fetch algorithm
//! 1. Sort requested IDs by offset (improves sequential read locality).
//! 2. Walk tail blocks (in-memory). On hit: memcpy from `BlockOutputs.data`.
//! 3. Walk committed file (pread). On hit: read `OutputHeader || script` into staging buf.
//! 4. Build `OutputDetail { header, script }` for each resolved ID.
//!
//! ## Flusher
//! A background thread (`flusher_thread`) receives height triggers via `crossbeam::channel`.
//! When `commit_before(h)` is called it writes all tail blocks with `height < h` to the file
//! and removes them from the tail.

use super::types::{IdCodec, OutputDetail, OutputHeader, OutputId, OutputKey, OutputKV};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// In-memory block output buffer. Holds raw `{OutputHeader || script_bytes}` data
/// for one block, indexed by `[begin_offset, begin_offset + data.len())` in the flat file.
#[derive(Debug, Clone)]
pub(super) struct BlockOutputs {
    /// Byte offset in the flat file where this block's data begins.
    pub begin_offset: u64,
    pub height: i32,
    /// Raw bytes: repeated `{OutputHeader (16B) || script_bytes (variable)}`.
    pub data: Vec<u8>,
}

impl BlockOutputs {
    /// Look up an entry by absolute file offset. Returns `(header, script_slice)` if found.
    ///
    /// Uses `read_unaligned` because `data` is a `Vec<u8>` buffer that may not be aligned
    /// to `OutputHeader`'s requirement (8 bytes from the `i64 amount` field).
    pub fn get(&self, offset: u64, length: usize) -> Option<(OutputHeader, &[u8])> {
        let rel = offset.checked_sub(self.begin_offset)? as usize;
        let end = rel + length;
        if end > self.data.len() || length < OutputHeader::SIZE {
            return None;
        }
        // Safety: `self.data` contains valid `{OutputHeader || script}` bytes written by
        // `append_outputs`. `read_unaligned` handles any byte alignment.
        let header = unsafe {
            std::ptr::read_unaligned(self.data[rel..].as_ptr() as *const OutputHeader)
        };
        let script_start = rel + OutputHeader::SIZE;
        let script = &self.data[script_start..rel + length];
        Some((header, script))
    }
}

/// Append-only flat file + in-memory tail for UTXO output data.
pub struct UtxoTable {
    /// Flat file (on disk). Opened for read+write.
    file: Arc<parking_lot::Mutex<File>>,
    /// Next write offset (monotonically increasing).
    next_offset: AtomicU64,
    /// In-memory tail: blocks not yet flushed. Snapshot pattern (same as MemoryAge).
    tail: parking_lot::RwLock<Arc<Vec<Arc<BlockOutputs>>>>,
    /// Heights in the tail within `[next_offset - mutable_window_bytes, next_offset)`.
    mutable_window: AtomicU64,
    /// Channel to trigger background `commit_before(h)`.
    flusher_tx: crossbeam_channel::Sender<FlushRequest>,
}

enum FlushRequest {
    CommitBefore(i32),
    Shutdown,
}

impl UtxoTable {
    /// Open or create the flat table file at `path`.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Arc<Self>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())?;
        let existing_len = file.metadata()?.len();
        let (flusher_tx, flusher_rx) = crossbeam_channel::unbounded::<FlushRequest>();

        let table = Arc::new(Self {
            file: Arc::new(parking_lot::Mutex::new(file)),
            next_offset: AtomicU64::new(existing_len),
            tail: parking_lot::RwLock::new(Arc::new(Vec::new())),
            mutable_window: AtomicU64::new(512),
            flusher_tx,
        });

        // Spawn background flusher.
        let table_weak = Arc::downgrade(&table);
        std::thread::Builder::new()
            .name("utxo-table-flusher".to_string())
            .spawn(move || {
                while let Ok(req) = flusher_rx.recv() {
                    match req {
                        FlushRequest::CommitBefore(h) => {
                            if let Some(t) = table_weak.upgrade() {
                                let _ = t.commit_before_inner(h);
                            }
                        }
                        FlushRequest::Shutdown => break,
                    }
                }
            })
            .expect("spawn table flusher");

        Ok(table)
    }

    /// Append a block's outputs to the tail.
    ///
    /// For each transaction output: encode `{OutputHeader || script_bytes}` and build an
    /// `OutputKV::Add` entry with the table offset. Returns the entries for the index.
    ///
    /// `tx_ids[i]` must be the txid of `block.transactions[i]` (pre-computed, no re-hashing).
    pub fn append_outputs(
        &self,
        block: &blvm_protocol::Block,
        tx_ids: &[[u8; 32]],
        height: i32,
        entries: &mut Vec<OutputKV>,
    ) -> anyhow::Result<usize> {
        debug_assert_eq!(block.transactions.len(), tx_ids.len());

        let mut local_buf: Vec<u8> = Vec::with_capacity(block.transactions.len() * 64);
        let first_entry_idx = entries.len();

        for (tx_idx, (tx, txid)) in block.transactions.iter().zip(tx_ids.iter()).enumerate() {
            for (vout, output) in tx.outputs.iter().enumerate() {
                let script: &[u8] = output.script_pubkey.as_ref();
                let header = OutputHeader {
                    height,
                    flags: if tx_idx == 0 { 1 } else { 0 }, // coinbase flag
                    amount: output.value,
                };
                let entry_len = OutputHeader::SIZE + script.len();

                // Write header bytes (repr(C) — safe to cast).
                let header_bytes = unsafe {
                    std::slice::from_raw_parts(
                        &header as *const OutputHeader as *const u8,
                        OutputHeader::SIZE,
                    )
                };
                local_buf.extend_from_slice(header_bytes);
                local_buf.extend_from_slice(script);

                // Build OutputKey for this output.
                let mut key: OutputKey = [0u8; 36];
                key[..32].copy_from_slice(txid);
                key[32..36].copy_from_slice(&(vout as u32).to_be_bytes());

                // Placeholder id (offset=0); fixed up below once we know block_base.
                entries.push(OutputKV::new_add(key, height, IdCodec::encode(0, entry_len)));
            }
        }

        // Allocate one contiguous region in the flat file for this block's data.
        let total = local_buf.len() as u64;
        let block_base = self.next_offset.fetch_add(total, Ordering::Relaxed);

        // Fix up entry offsets now that we know block_base.
        let mut file_offset = block_base;
        let mut entry_idx = first_entry_idx;
        for tx in block.transactions.iter() {
            for output in tx.outputs.iter() {
                let entry_len = OutputHeader::SIZE + output.script_pubkey.len();
                entries[entry_idx].id = IdCodec::encode(file_offset, entry_len);
                file_offset += entry_len as u64;
                entry_idx += 1;
            }
        }

        // Push to in-memory tail.
        let block_out = Arc::new(BlockOutputs {
            begin_offset: block_base,
            height,
            data: local_buf,
        });
        {
            let mut lock = self.tail.write();
            let old = Arc::clone(&*lock);
            let mut new_tail = (*old).clone();
            new_tail.push(block_out);
            *lock = Arc::new(new_tail);
        }

        // Trigger flusher if tail is large.
        let tail_len = self.tail.read().len();
        if tail_len > self.mutable_window.load(Ordering::Relaxed) as usize {
            let min_h = self.tail.read()
                .first()
                .map(|b| b.height)
                .unwrap_or(i32::MIN);
            let _ = self.flusher_tx.try_send(FlushRequest::CommitBefore(min_h + 1));
        }

        Ok(entries.len())
    }

    /// Fetch `OutputDetail` for a list of `OutputId`s. Sorted by offset for read locality.
    ///
    /// Returns the number of IDs successfully resolved.
    pub fn fetch(
        &self,
        ids: &[OutputId],
        details: &mut Vec<OutputDetail>,
    ) -> anyhow::Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }

        // Sort by offset for sequential read locality.
        let mut sorted: Vec<(usize, u64, usize)> = ids // (original_idx, offset, length)
            .iter()
            .enumerate()
            .map(|(i, &id)| {
                let (offset, length) = IdCodec::decode(id);
                (i, offset, length)
            })
            .collect();
        sorted.sort_unstable_by_key(|&(_, offset, _)| offset);

        let tail_snapshot = Arc::clone(&*self.tail.read());
        let mut resolved = 0usize;
        let mut staging = Vec::new();

        for (orig_idx, offset, length) in &sorted {
            let _ = orig_idx; // details are appended in sort order; caller re-indexes if needed

            // Try tail first.
            let mut found_in_tail = false;
            for block in tail_snapshot.iter() {
                if let Some((header, script)) = block.get(*offset, *length) {
                    details.push(OutputDetail {
                        header,
                        script: script.to_vec(),
                    });
                    resolved += 1;
                    found_in_tail = true;
                    break;
                }
            }

            if !found_in_tail {
                // Read from disk.
                staging.resize(*length, 0u8);
                let file = self.file.lock();
                file.read_at(&mut staging, *offset)?;
                drop(file);

                if staging.len() < OutputHeader::SIZE {
                    continue; // corrupt entry — skip
                }
                let header = unsafe {
                    std::ptr::read_unaligned(staging.as_ptr() as *const OutputHeader)
                };
                let script = staging[OutputHeader::SIZE..*length].to_vec();
                details.push(OutputDetail { header, script });
                resolved += 1;
            }
        }

        Ok(resolved)
    }

    /// Flush tail blocks with `height < limit` to the flat file. Called by flusher thread.
    fn commit_before_inner(&self, limit: i32) -> anyhow::Result<()> {
        let snapshot = Arc::clone(&*self.tail.read());
        let to_flush: Vec<_> = snapshot.iter()
            .filter(|b| b.height < limit)
            .cloned()
            .collect();

        if to_flush.is_empty() {
            return Ok(());
        }

        // Write in offset order.
        let mut ordered = to_flush.clone();
        ordered.sort_by_key(|b| b.begin_offset);

        {
            let mut file = self.file.lock();
            for block in &ordered {
                file.write_all(&block.data)?;
            }
            file.flush()?;
        }

        // Remove flushed blocks from tail.
        let flush_set: std::collections::HashSet<u64> =
            ordered.iter().map(|b| b.begin_offset).collect();
        let mut lock = self.tail.write();
        let old = Arc::clone(&*lock);
        let new_tail: Vec<Arc<BlockOutputs>> = old
            .iter()
            .filter(|b| !flush_set.contains(&b.begin_offset))
            .cloned()
            .collect();
        *lock = Arc::new(new_tail);

        Ok(())
    }

    /// Trigger an explicit `commit_before(limit)` (synchronous). For shutdown/watermark export.
    pub fn commit_before(&self, limit: i32) -> anyhow::Result<()> {
        self.commit_before_inner(limit)
    }
}

impl Drop for UtxoTable {
    fn drop(&mut self) {
        let _ = self.flusher_tx.try_send(FlushRequest::Shutdown);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blvm_protocol::{Block, BlockHeader, Transaction, TransactionInput, TransactionOutput, OutPoint};
    use tempfile::NamedTempFile;

    fn make_output(value: i64, tag: u8) -> TransactionOutput {
        TransactionOutput { value, script_pubkey: vec![0x76, 0xa9, 0x14, tag].into() }
    }

    fn make_coinbase_input() -> TransactionInput {
        TransactionInput {
            prevout: OutPoint { hash: [0u8; 32], index: 0xFFFFFFFF },
            sequence: 0xFFFFFFFF,
            script_sig: vec![].into(),
        }
    }

    fn dummy_block(n_tx: usize, n_out_each: usize, start_value: i64) -> Block {
        let txs: Box<[Transaction]> = (0..n_tx)
            .map(|_| {
                let outputs = (0..n_out_each)
                    .map(|i| make_output(start_value + i as i64, i as u8))
                    .collect::<Vec<_>>();
                Transaction {
                    version: 1,
                    inputs: vec![make_coinbase_input()].into(),
                    outputs: outputs.into(),
                    lock_time: 0,
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Block {
            header: BlockHeader {
                version: 1,
                prev_block_hash: [0u8; 32],
                merkle_root: [0u8; 32],
                timestamp: 0,
                bits: 0,
                nonce: 0,
            },
            transactions: txs,
        }
    }

    #[test]
    fn test_append_and_fetch() {
        let tmp = NamedTempFile::new().unwrap();
        let table = UtxoTable::open(tmp.path()).unwrap();

        let block = dummy_block(2, 3, 1000);
        let tx_ids: Vec<[u8; 32]> = block.transactions.iter().enumerate()
            .map(|(i, _)| { let mut id = [0u8; 32]; id[0] = i as u8; id })
            .collect();
        let mut entries = Vec::new();
        let height = 100i32;

        table.append_outputs(&block, &tx_ids, height, &mut entries).unwrap();

        assert_eq!(entries.len(), 6, "expected 6 output entries");

        // Fetch all entries.
        let ids: Vec<OutputId> = entries.iter().map(|e| e.id).collect();
        let mut details = Vec::new();
        let resolved = table.fetch(&ids, &mut details).unwrap();
        assert_eq!(resolved, 6, "expected 6 outputs resolved");

        // Verify all headers have correct height.
        for detail in &details {
            assert_eq!(detail.header.height, height);
        }
    }
}
