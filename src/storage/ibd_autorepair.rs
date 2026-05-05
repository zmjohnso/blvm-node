//! Marker-driven IBD UTXO autorepair: after a validation/UTXO consistency failure we write
//! `ibd_utxo_repair_required`. On the **next** startup we clear `ibd_utxos`, reset
//! `ibd_utxo_watermark` to 0, **remove the marker**, and flush — the inconsistent state is fixed
//! in one shot. Later restarts resume using normal `min(chain_tip, watermark)` logic instead of
//! wiping `ibd_utxos` again after every interrupt mid-replay.
//!
//! Set `BLVM_IBD_SKIP_AUTOREPAIR=1` to skip the wipe (marker remains until you delete it manually).

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

const MARKER_FILE: &str = "ibd_utxo_repair_required";

pub fn repair_marker_path(data_dir: &Path) -> PathBuf {
    data_dir.join(MARKER_FILE)
}

pub fn set_ibd_utxo_repair_flag(data_dir: &Path) -> Result<()> {
    let path = repair_marker_path(data_dir);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::write(&path, b"1").context("write ibd_utxo_repair_required")?;
    warn!(
        "Wrote {} — next startup will clear IBD UTXO disk state unless BLVM_IBD_SKIP_AUTOREPAIR is set",
        path.display()
    );
    Ok(())
}

pub fn clear_ibd_utxo_repair_flag(data_dir: &Path) -> Result<()> {
    let path = repair_marker_path(data_dir);
    if path.exists() {
        std::fs::remove_file(&path).context("remove ibd_utxo_repair_required")?;
        info!("Removed IBD UTXO repair marker ({})", path.display());
    }
    Ok(())
}

pub fn ibd_utxo_repair_flag_present(data_dir: &Path) -> bool {
    repair_marker_path(data_dir).exists()
}

/// Best-effort classification: errors where clearing `ibd_utxos` and replaying from on-disk blocks may help.
///
/// Uses stable substrings from `blvm-consensus` connect paths and parallel IBD — not generic
/// "invalid block" text (consensus bugs and bad peers would otherwise trigger destructive repair).
pub fn validation_error_suggests_utxo_repair(err: &anyhow::Error) -> bool {
    let s = err.to_string();
    s.contains("UTXO not found for input")
        || s.contains("IBD UTXO mutex poisoned")
        || s.contains("UTXO flush panicked")
        || s.contains("Failed to open IBD UTXO tree")
}

/// If `ibd_utxo_watermark` is non-zero but the `ibd_utxos` tree has no rows, persisted watermark
/// cannot reflect flushed state (common after watermark was bumped to match chain tip without a
/// matching UTXO flush). Reset watermark to **0** so startup uses safe replay semantics.
#[cfg(feature = "production")]
pub(crate) fn reconcile_ibd_utxo_watermark_with_disk(
    storage: &crate::storage::Storage,
    watermark_val: u64,
) -> Result<u64> {
    if watermark_val == 0 {
        return Ok(0);
    }
    let tree = storage.open_tree("ibd_utxos")?;
    if tree.is_empty()? {
        warn!(
            "[ibd_autorepair] ibd_utxo_watermark={} but ibd_utxos tree is empty — resetting watermark to 0 \
             (watermark likely jumped ahead of durable UTXO flushes)",
            watermark_val
        );
        storage.chain().force_set_ibd_utxo_watermark(0)?;
        Ok(0)
    } else {
        Ok(watermark_val)
    }
}

#[cfg(feature = "production")]
pub fn apply_ibd_utxo_autorepair_if_needed(
    storage: &crate::storage::Storage,
    data_dir: &Path,
) -> Result<()> {
    if std::env::var("BLVM_IBD_SKIP_AUTOREPAIR").is_ok() {
        if ibd_utxo_repair_flag_present(data_dir) {
            warn!(
                "IBD UTXO repair marker present but BLVM_IBD_SKIP_AUTOREPAIR is set — not clearing ibd_utxos"
            );
        }
        return Ok(());
    }
    if !ibd_utxo_repair_flag_present(data_dir) {
        return Ok(());
    }
    info!(
        "IBD UTXO autorepair: clearing ibd_utxos and forcing ibd_utxo_watermark to 0 (marker was present)"
    );
    let tree = storage.open_tree("ibd_utxos")?;
    tree.clear()?;
    storage.chain().force_set_ibd_utxo_watermark(0)?;
    storage.flush()?;
    clear_ibd_utxo_repair_flag(data_dir)?;
    warn!(
        "IBD UTXO autorepair applied; on-disk blocks kept; repair marker cleared — resume uses watermark vs chain tip"
    );
    Ok(())
}

#[cfg(all(test, feature = "production"))]
mod ibd_autorepair_tests {
    use super::*;
    use crate::storage::Storage;
    use tempfile::TempDir;

    #[test]
    fn apply_autorepair_removes_marker_and_zeros_watermark() {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path();
        let storage = Storage::new(data_dir).unwrap();

        storage.chain().set_utxo_watermark(999).unwrap();
        let tree = storage.open_tree("ibd_utxos").unwrap();
        tree.insert(b"tkey", b"tval").unwrap();
        storage.flush().unwrap();

        set_ibd_utxo_repair_flag(data_dir).unwrap();
        assert!(ibd_utxo_repair_flag_present(data_dir));

        apply_ibd_utxo_autorepair_if_needed(&storage, data_dir).unwrap();

        assert!(
            !ibd_utxo_repair_flag_present(data_dir),
            "marker must be removed so later restarts do not wipe ibd_utxos again"
        );
        assert_eq!(storage.chain().get_utxo_watermark().unwrap(), Some(0));
        assert!(tree.is_empty().unwrap());
    }

    #[test]
    fn apply_autorepair_no_op_when_marker_missing_preserves_watermark() {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path();
        let storage = Storage::new(data_dir).unwrap();
        storage.chain().set_utxo_watermark(42).unwrap();

        apply_ibd_utxo_autorepair_if_needed(&storage, data_dir).unwrap();

        assert!(!ibd_utxo_repair_flag_present(data_dir));
        assert_eq!(storage.chain().get_utxo_watermark().unwrap(), Some(42));
    }

    #[test]
    fn validation_error_suggests_utxo_repair_matching_substrings() {
        assert!(validation_error_suggests_utxo_repair(&anyhow::anyhow!(
            "connect: UTXO not found for input x"
        )));
        assert!(validation_error_suggests_utxo_repair(&anyhow::anyhow!(
            "IBD UTXO mutex poisoned"
        )));
        assert!(!validation_error_suggests_utxo_repair(&anyhow::anyhow!(
            "bad peer disconnect"
        )));
    }

    #[test]
    fn reconcile_resets_watermark_when_ibd_utxos_empty() {
        let dir = TempDir::new().unwrap();
        let storage = Storage::new(dir.path()).unwrap();
        storage.chain().force_set_ibd_utxo_watermark(418_000).unwrap();
        assert_eq!(
            reconcile_ibd_utxo_watermark_with_disk(&storage, 418_000).unwrap(),
            0
        );
        assert_eq!(storage.chain().get_utxo_watermark().unwrap(), Some(0));
    }

    #[test]
    fn reconcile_keeps_watermark_when_ibd_utxos_nonempty() {
        let dir = TempDir::new().unwrap();
        let storage = Storage::new(dir.path()).unwrap();
        storage.chain().force_set_ibd_utxo_watermark(100).unwrap();
        storage.open_tree("ibd_utxos").unwrap().insert(b"k", b"v").unwrap();
        assert_eq!(
            reconcile_ibd_utxo_watermark_with_disk(&storage, 100).unwrap(),
            100
        );
    }
}
