//! GPU stat readers — small helpers for reading AMD GPU sysfs nodes.
//!
//! Originally lived in `bench.rs`; moved here so both the bench harness
//! and the pipeline's telemetry sampler can use the same code without
//! duplication.
//!
//! All paths are read via plain `std::fs::read_to_string` per sample,
//! which is fine at <=1 Hz. The sysfs nodes are virtual files updated
//! by the kernel/SMU on each read — there's no caching concern.

use std::path::{Path, PathBuf};

const AMD_VENDOR_ID: &str = "0x1002";

/// Find the first AMD card under `/sys/class/drm`, ignoring connector
/// entries like `card1-DP-1`. Returns the card's sysfs root, e.g.
/// `/sys/class/drm/card1`.
pub fn auto_detect_amd_card() -> Option<PathBuf> {
	let entries = std::fs::read_dir("/sys/class/drm").ok()?;
	for entry in entries.flatten() {
		let file_name = entry.file_name();
		let name = file_name.to_string_lossy();
		// Match `card<N>` exactly — skip connector entries like `card1-DP-1`.
		if !name.starts_with("card") || !name[4..].chars().all(|c| c.is_ascii_digit()) {
			continue;
		}
		let vendor_path = entry.path().join("device/vendor");
		if let Ok(vendor) = std::fs::read_to_string(&vendor_path) {
			if vendor.trim() == AMD_VENDOR_ID {
				return Some(entry.path());
			}
		}
	}
	None
}

/// Read `pp_dpm_sclk` and return the currently-active clock in MHz (the
/// line containing `*`). `None` on any parse error.
pub fn read_active_sclk_mhz(path: &Path) -> Option<u32> {
	let content = std::fs::read_to_string(path).ok()?;
	for line in content.lines() {
		if !line.contains('*') {
			continue;
		}
		// Lines look like: "1: 1330Mhz *"
		let after_colon = line.split_once(':')?.1.trim().trim_end_matches('*').trim();
		// Strip trailing "Mhz" / "MHz".
		let mhz = after_colon.trim_end_matches(|c: char| c.is_alphabetic()).trim();
		return mhz.parse().ok();
	}
	None
}

/// Read `gpu_busy_percent` (0–100). `None` on any parse error.
pub fn read_busy_percent(path: &Path) -> Option<u8> {
	std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Read `mem_info_vram_used` (bytes). `None` on any parse error.
pub fn read_vram_used_bytes(path: &Path) -> Option<u64> {
	std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Convenience wrapper holding all three sysfs paths for a card.
#[derive(Debug, Clone)]
pub struct CardPaths {
	pub sclk: PathBuf,
	pub busy: PathBuf,
	pub vram: PathBuf,
}

impl CardPaths {
	pub fn from_card(card_root: &Path) -> Self {
		Self {
			sclk: card_root.join("device/pp_dpm_sclk"),
			busy: card_root.join("device/gpu_busy_percent"),
			vram: card_root.join("device/mem_info_vram_used"),
		}
	}
}
