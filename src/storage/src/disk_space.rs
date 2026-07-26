// Copyright 2026 MonoTS Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common::{Result, TsdbError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

/// Enter read-only when free/total drops to this ratio or below (default 10%).
pub const DEFAULT_DISK_MIN_FREE_RATIO: f64 = 0.10;

/// How often to re-probe filesystem capacity on the write path.
const REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Global disk watermark for one data directory: blocks user writes + compaction when free space is critical.
pub struct DiskSpaceController {
    root: PathBuf,
    min_free_ratio_bits: AtomicU64,
    read_only: AtomicBool,
    free_bytes: AtomicU64,
    total_bytes: AtomicU64,
    last_check_ms: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub struct DiskUsage {
    pub free_bytes: u64,
    pub total_bytes: u64,
    pub free_ratio: f64,
    pub read_only: bool,
}

impl DiskSpaceController {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_min_free_ratio(root, DEFAULT_DISK_MIN_FREE_RATIO)
    }

    pub fn with_min_free_ratio(root: impl Into<PathBuf>, min_free_ratio: f64) -> Self {
        Self {
            root: root.into(),
            min_free_ratio_bits: AtomicU64::new(min_free_ratio.clamp(0.0, 1.0).to_bits()),
            read_only: AtomicBool::new(false),
            free_bytes: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            last_check_ms: AtomicU64::new(0),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn min_free_ratio(&self) -> f64 {
        f64::from_bits(self.min_free_ratio_bits.load(Ordering::Relaxed))
    }

    /// Override watermark (tests on nearly-full disks, ops tuning).
    pub fn set_min_free_ratio(&self, ratio: f64) {
        self.min_free_ratio_bits
            .store(ratio.clamp(0.0, 1.0).to_bits(), Ordering::Relaxed);
        self.last_check_ms.store(0, Ordering::Relaxed);
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::Acquire)
    }

    pub fn ensure_writable(&self) -> Result<()> {
        let usage = self.refresh_if_due()?;
        if usage.read_only {
            return Err(TsdbError::disk_read_only(
                usage.free_bytes,
                usage.total_bytes,
                self.min_free_ratio(),
            ));
        }
        Ok(())
    }

    pub fn refresh_if_due(&self) -> Result<DiskUsage> {
        let now = now_ms();
        let last = self.last_check_ms.load(Ordering::Relaxed);
        let due = now.saturating_sub(last) >= REFRESH_INTERVAL.as_millis() as u64
            || last == 0
            || self.is_read_only();
        if due {
            self.refresh()
        } else {
            Ok(self.cached_usage())
        }
    }

    pub fn refresh(&self) -> Result<DiskUsage> {
        let (free_bytes, total_bytes) = probe_disk(&self.root)?;
        let free_ratio = if total_bytes == 0 {
            1.0
        } else {
            free_bytes as f64 / total_bytes as f64
        };
        let min_free_ratio = self.min_free_ratio();
        let read_only = free_ratio <= min_free_ratio;
        let was = self.read_only.swap(read_only, Ordering::AcqRel);
        self.free_bytes.store(free_bytes, Ordering::Relaxed);
        self.total_bytes.store(total_bytes, Ordering::Relaxed);
        self.last_check_ms.store(now_ms(), Ordering::Relaxed);

        if read_only && !was {
            warn!(
                path = %self.root.display(),
                free_bytes,
                total_bytes,
                free_ratio,
                min_free_ratio,
                "disk free space critical; storage entered read-only (writes and compaction paused)"
            );
        } else if !read_only && was {
            info!(
                path = %self.root.display(),
                free_bytes,
                total_bytes,
                free_ratio,
                "disk free space recovered; writes and compaction resumed"
            );
        }

        Ok(DiskUsage {
            free_bytes,
            total_bytes,
            free_ratio,
            read_only,
        })
    }

    fn cached_usage(&self) -> DiskUsage {
        let free_bytes = self.free_bytes.load(Ordering::Relaxed);
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);
        let free_ratio = if total_bytes == 0 {
            1.0
        } else {
            free_bytes as f64 / total_bytes as f64
        };
        DiskUsage {
            free_bytes,
            total_bytes,
            free_ratio,
            read_only: self.is_read_only(),
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn probe_disk(path: &Path) -> Result<(u64, u64)> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let c_path = CString::new(path.as_os_str().as_bytes())
            .map_err(|e| TsdbError::Storage(format!("disk probe path invalid: {e}")))?;
        unsafe {
            let mut s: libc::statvfs = std::mem::zeroed();
            if libc::statvfs(c_path.as_ptr(), &mut s) != 0 {
                return Err(TsdbError::Io(std::io::Error::last_os_error()));
            }
            let frsize = s.f_frsize as u64;
            let total = s.f_blocks as u64 * frsize;
            let free = s.f_bavail as u64 * frsize;
            Ok((free, total))
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(TsdbError::Storage(
            "disk space probe is only supported on unix".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn refresh_reports_usage_for_tempdir() {
        let dir = tempfile::tempdir().unwrap();
        let disk = DiskSpaceController::with_min_free_ratio(dir.path(), 0.0);
        let usage = disk.refresh().unwrap();
        assert!(usage.total_bytes > 0);
        assert!(usage.free_bytes <= usage.total_bytes);
        assert!(!usage.read_only || usage.free_bytes == 0);
    }

    #[test]
    fn high_watermark_forces_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let disk = Arc::new(DiskSpaceController::with_min_free_ratio(dir.path(), 0.999));
        let err = disk.ensure_writable().unwrap_err();
        assert!(err.is_disk_read_only(), "got {err}");
        assert!(disk.is_read_only());
    }
}
