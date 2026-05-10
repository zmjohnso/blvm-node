//! Shared host RAM **MiB → GiB tier** helpers for [`MemoryGuard`] and RocksDB sizing.
//!
//! `BLVM_TOTAL_RAM_MB` / `/proc/meminfo` / `BLVM_RAM_GB` handling here matches
//! [`crate::node::parallel_ibd::memory::MemoryGuard::new`] for **`total_mb`** only
//! (no `MemAvailable`, no `BLVM_SYS_AVAIL_MB`).

/// GiB label for tier tables: `(total_ram_mib + 512) / 1024`.
#[inline]
pub(crate) fn total_gb_rounded(total_ram_mib: u64) -> u64 {
    (total_ram_mib + 512) / 1024
}

/// Best-effort `MemTotal` in MiB (same parsing as MemoryGuard `/proc`).
#[cfg(target_os = "linux")]
fn memtotal_mib_from_proc_linux() -> u64 {
    let Ok(content) = std::fs::read_to_string("/proc/meminfo") else {
        return 0;
    };
    for line in content.lines() {
        if line.starts_with("MemTotal:") {
            let kib = line
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            return kib / 1024;
        }
    }
    0
}

/// Total RAM MiB for tier ladders (`MemoryGuard` total_mb through `BLVM_TOTAL_RAM_MB`; then `8192` if unknown).
pub(crate) fn probe_total_ram_mib() -> u64 {
    #[cfg(target_os = "linux")]
    let mut total_mb = memtotal_mib_from_proc_linux();
    #[cfg(not(target_os = "linux"))]
    let mut total_mb = 0u64;

    #[cfg(feature = "sysinfo")]
    if total_mb == 0 {
        let mut sys = sysinfo::System::new_all();
        sys.refresh_memory();
        total_mb = sys.total_memory() / (1024 * 1024);
    }

    if let Some(mb) = std::env::var("BLVM_TOTAL_RAM_MB")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&v| v > 0)
    {
        total_mb = mb;
    }

    if total_mb == 0 {
        total_mb = std::env::var("BLVM_RAM_GB")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(|g| g.saturating_mul(1024))
            .unwrap_or(8192);
    }

    total_mb
}
