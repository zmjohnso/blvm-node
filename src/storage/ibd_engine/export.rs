//! Phase 3: watermark export — scan all live UTXOs from the age-tiered engine and write
//! them into the production `ibd_utxos` tree (the same tree `IbdUtxoStore` flushes to).
//!
//! Called once at IBD completion when `BLVM_IBD_ENGINE=1`. The engine's `scan_live()` returns
//! all Add-without-paired-Delete entries. For each, we:
//!   1. Fetch the `OutputDetail` from the flat table.
//!   2. Encode it as `bincode::serialize(&UTXO)` (matching `flush_batch_to_disk` format).
//!   3. Batch-write to the tree via `Tree::batch()`.
//!   4. Accumulate MuHash3072 in the same pass (no per-op disk reads).
//!
//! After writing, the normal `IbdUtxoStore` retire path takes over from the watermark height.
//!
//! ## BLVM improvement over Hornet
//! Hornet has no production export. BLVM reuses its existing `Tree` abstraction so the export
//! is backend-agnostic (RocksDB, TidesDB, or Redb). RocksDB SST ingestion (`SstFileWriter` +
//! `ingest_external_file`) is a future optimization for the 10–50× speedup at 100M+ entries.

use super::database::UtxoDatabase;
use super::types::{output_key_to_outpoint, IdCodec, OutputId};
use crate::storage::database::Tree;
use crate::storage::disk_utxo::outpoint_to_key;
use anyhow::Result;
use blvm_muhash::{serialize_coin_for_muhash, MuHash3072};
use blvm_protocol::types::{SharedByteString, UTXO};
use std::sync::Arc;
use tracing::{info, warn};

/// Batch size for `Tree::batch()` writes (stay well under TidesDB's 100k TXN_OPS limit).
const EXPORT_BATCH_SIZE: usize = 10_000;

/// Export all live UTXOs from the engine to `tree` and return the final `MuHash3072`.
///
/// `tip_height` must equal the engine's `contiguous_length()` — i.e. all blocks have been
/// appended. Blocks the caller until the engine reaches `tip_height`.
///
/// The tree must be the `ibd_utxos` tree (same as used by `IbdUtxoStore`).
/// The caller is responsible for persisting the returned MuHash to chain_info.
pub fn watermark_export(
    db: &UtxoDatabase,
    tree: &dyn Tree,
    tip_height: i32,
) -> Result<MuHash3072> {
    db.wait_for_height(tip_height);

    let live_kvs = db.scan_live();
    let total = live_kvs.len();
    info!("IBD engine watermark export: {} live UTXOs to write at height {}", total, tip_height);

    // Collect OutputIds for all live Add entries, sorted by file offset for read locality.
    let mut id_key_pairs: Vec<(OutputId, [u8; 36])> = live_kvs
        .into_iter()
        .filter_map(|kv| {
            if kv.id == 0 {
                None // Delete sentinel — scan_live should not return these
            } else {
                Some((kv.id, kv.key))
            }
        })
        .collect();

    // Sort by file offset (high bits of OutputId) for sequential read performance.
    id_key_pairs.sort_unstable_by_key(|(id, _)| IdCodec::decode(*id).0);

    let ids: Vec<OutputId> = id_key_pairs.iter().map(|(id, _)| *id).collect();
    let mut details = Vec::with_capacity(ids.len());
    let fetched = db.fetch(&ids, &mut details)?;
    if fetched != id_key_pairs.len() {
        warn!(
            "IBD engine export: fetched {} details but expected {} — engine/table mismatch",
            fetched,
            id_key_pairs.len()
        );
    }

    let mut muhash = MuHash3072::new();
    let mut written = 0usize;
    let mut batch_ops: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(EXPORT_BATCH_SIZE);
    let mut ser_buf: Vec<u8> = Vec::with_capacity(200);

    for (fetch_rank, (_, key)) in id_key_pairs.iter().enumerate() {
        let Some(detail) = details.get(fetch_rank) else {
            continue;
        };
        let op = output_key_to_outpoint(key);
        let rocks_key = outpoint_to_key(&op);

        let utxo = UTXO {
            value: detail.header.amount,
            script_pubkey: SharedByteString::from(detail.script.as_slice()),
            height: detail.header.height as u64,
            is_coinbase: detail.header.is_coinbase(),
        };

        // Accumulate MuHash in same pass (no extra disk reads).
        let preimage = serialize_coin_for_muhash(
            &op.hash,
            op.index,
            detail.header.height as u32,
            detail.header.is_coinbase(),
            detail.header.amount,
            detail.script.as_slice(),
        );
        muhash.insert_mut(&preimage);

        ser_buf.clear();
        bincode::serialize_into(&mut ser_buf, &utxo)
            .map_err(|e| anyhow::anyhow!("UTXO serialize: {e}"))?;
        batch_ops.push((rocks_key.to_vec(), ser_buf.clone()));

        if batch_ops.len() >= EXPORT_BATCH_SIZE {
            flush_batch(tree, &batch_ops)?;
            written += batch_ops.len();
            batch_ops.clear();
        }
    }

    if !batch_ops.is_empty() {
        flush_batch(tree, &batch_ops)?;
        written += batch_ops.len();
    }

    info!(
        "IBD engine watermark export complete: wrote {} UTXOs to ibd_utxos tree",
        written
    );
    Ok(muhash)
}

fn flush_batch(tree: &dyn Tree, ops: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
    let mut b = tree.batch()?;
    for (k, v) in ops {
        b.put(k.as_slice(), v.as_slice());
    }
    b.commit()?;
    Ok(())
}

/// Convenience wrapper: export, persist MuHash, and log the result.
/// Returns the MuHash for the caller to set as the watermark in chain_info.
pub fn run_watermark_export(
    db: &UtxoDatabase,
    tree: &Arc<dyn Tree>,
    tip_height: i32,
) -> Result<MuHash3072> {
    let t = std::time::Instant::now();
    let muhash = watermark_export(db, tree.as_ref(), tip_height)?;
    info!(
        "IBD engine watermark export finished in {:.1}s (height={})",
        t.elapsed().as_secs_f64(),
        tip_height
    );
    Ok(muhash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ibd_engine::UtxoDatabase;
    use blvm_protocol::{Block, BlockHeader, OutPoint, Transaction, TransactionInput, TransactionOutput};
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    struct MockTree {
        data: std::sync::Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    }

    impl MockTree {
        fn new() -> Self {
            Self { data: std::sync::Mutex::new(HashMap::new()) }
        }
        fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
            self.data.lock().unwrap().get(key).cloned()
        }
    }

    impl Tree for MockTree {
        fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
            self.data.lock().unwrap().insert(key.to_vec(), value.to_vec());
            Ok(())
        }
        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(self.data.lock().unwrap().get(key).cloned())
        }
        fn remove(&self, key: &[u8]) -> Result<()> {
            self.data.lock().unwrap().remove(key);
            Ok(())
        }
        fn contains_key(&self, key: &[u8]) -> Result<bool> {
            Ok(self.data.lock().unwrap().contains_key(key))
        }
        fn clear(&self) -> Result<()> {
            self.data.lock().unwrap().clear();
            Ok(())
        }
        fn len(&self) -> Result<usize> {
            Ok(self.data.lock().unwrap().len())
        }
        fn iter(&self) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + '_> {
            let snapshot: Vec<_> = self.data.lock().unwrap()
                .iter()
                .map(|(k, v)| Ok((k.clone(), v.clone())))
                .collect();
            Box::new(snapshot.into_iter())
        }
        fn batch(&self) -> Result<Box<dyn crate::storage::database::BatchWriter + '_>> {
            Ok(Box::new(MockBatch { tree: self, ops: Vec::new() }))
        }
    }

    struct MockBatch<'a> {
        tree: &'a MockTree,
        ops: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    }

    impl crate::storage::database::BatchWriter for MockBatch<'_> {
        fn put(&mut self, key: &[u8], value: &[u8]) {
            self.ops.push((key.to_vec(), Some(value.to_vec())));
        }
        fn delete(&mut self, key: &[u8]) {
            self.ops.push((key.to_vec(), None));
        }
        fn commit(self: Box<Self>) -> Result<()> {
            let mut data = self.tree.data.lock().unwrap();
            for (k, v_opt) in self.ops {
                match v_opt {
                    Some(v) => { data.insert(k, v); }
                    None => { data.remove(&k); }
                }
            }
            Ok(())
        }
        fn len(&self) -> usize {
            self.ops.len()
        }
    }

    fn make_coinbase(value: i64) -> Transaction {
        Transaction {
            version: 1,
            inputs: vec![TransactionInput {
                prevout: OutPoint { hash: [0u8; 32], index: 0xFFFFFFFF },
                sequence: 0xFFFFFFFF,
                script_sig: vec![].into(),
            }].into(),
            outputs: vec![TransactionOutput {
                value,
                script_pubkey: vec![0x76, 0xa9, 0x14, 0xde].into(),
            }].into(),
            lock_time: 0,
        }
    }

    fn make_block(txs: Vec<Transaction>) -> Block {
        Block {
            header: BlockHeader {
                version: 1,
                prev_block_hash: [0u8; 32],
                merkle_root: [0u8; 32],
                timestamp: 0,
                bits: 0,
                nonce: 0,
            },
            transactions: txs.into_boxed_slice(),
        }
    }

    #[test]
    fn test_watermark_export_writes_utxos() {
        let tmp = NamedTempFile::new().unwrap();
        let db = UtxoDatabase::open(tmp.path()).unwrap();
        let tree = Arc::new(MockTree::new());

        // Append a block with one coinbase output.
        let txid = [1u8; 32];
        let block = make_block(vec![make_coinbase(5_000_000_000)]);
        let _pin = db.append(&block, &[txid], 100).unwrap();

        let muhash = watermark_export(&db, tree.as_ref(), 100).unwrap();

        // The coinbase output's key in disk format is [txid || vout_le4 || pad4] = 40 bytes.
        let op = OutPoint { hash: txid, index: 0 };
        let key = outpoint_to_key(&op);
        let val = tree.get_value(&key);
        assert!(val.is_some(), "coinbase UTXO should have been written to tree");

        // MuHash should be non-default (at least one entry was inserted).
        let empty = MuHash3072::new();
        // Comparing via serialized state (MuHash3072 doesn't impl PartialEq directly).
        let exported = muhash.serialize_running_state();
        let empty_state = empty.serialize_running_state();
        assert_ne!(exported, empty_state, "MuHash should have at least one entry");
    }
}
