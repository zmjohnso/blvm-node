//! Incremental MuHash3072 over the `ibd_utxos` tree during parallel IBD.

use crate::storage::chainstate::ChainState;
use crate::storage::disk_utxo::key_to_outpoint;
use crate::storage::Storage;
use anyhow::{Context, Result};
use blvm_muhash::{serialize_coin_for_muhash, MuHash3072};
use blvm_protocol::types::UTXO;

pub(crate) fn load_ibd_muhash_from_chain(chain: &ChainState) -> Result<MuHash3072> {
    Ok(match chain.get_ibd_utxo_muhash_running()? {
        Some(bytes) => MuHash3072::deserialize_running_state(&bytes),
        None => MuHash3072::new(),
    })
}

#[inline]
fn utxo_muhash_preimage(op: &blvm_protocol::types::OutPoint, utxo: &UTXO) -> Vec<u8> {
    serialize_coin_for_muhash(
        &op.hash,
        op.index,
        utxo.height as u32,
        utxo.is_coinbase,
        utxo.value,
        utxo.script_pubkey.as_ref(),
    )
}

/// Full-tree scan vs persisted rolling MuHash (optional integrity check).
pub(crate) fn verify_ibd_utxo_muhash_startup(storage: &Storage) -> Result<()> {
    let chain = storage.chain();
    let Some(bytes) = chain.get_ibd_utxo_muhash_running()? else {
        tracing::warn!(
            "BLVM_VERIFY_IBD_UTXO_MUHASH: no ibd_utxo_muhash_running in chain_info — skipping verify \
             (legacy DB or before first MuHash checkpoint)"
        );
        return Ok(());
    };

    let tree = storage.open_tree("ibd_utxos")?;
    let mut scan = MuHash3072::new();
    for row in tree.iter() {
        let (k, v) = row?;
        if k.len() != 40 {
            continue;
        }
        let mut key = [0u8; 40];
        key.copy_from_slice(&k[..40]);
        let op = key_to_outpoint(&key);
        let utxo: UTXO = bincode::deserialize(&v)
            .with_context(|| format!("decode ibd_utxos row {:?}", &k[..8]))?;
        let pre = utxo_muhash_preimage(&op, &utxo);
        scan.insert_mut(&pre);
    }

    let expected = scan.finalize();
    let got = MuHash3072::deserialize_running_state(&bytes).finalize();
    if expected != got {
        anyhow::bail!(
            "IBD UTXO MuHash verify failed: full-tree scan finalized {:02x?} != persisted running state finalized {:02x?}",
            &expected[..],
            &got[..]
        );
    }
    tracing::info!("BLVM_VERIFY_IBD_UTXO_MUHASH: ibd_utxos MuHash OK");
    Ok(())
}
