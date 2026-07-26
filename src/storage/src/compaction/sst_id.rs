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

//! SST (Parquet) file identity: `{time}-{min_lsn}-{max_lsn}-{inner}-{cross}.parquet`.
//!
//! The inclusive LSN span is the CDC progress key (flush origin or compaction merge).
//! Memtable ids live only on [`crate::compaction::sst::SstMeta`] for WAL GC / recovery.
//!
//! Staging (two-phase bulk load) uses collision-free `staging-{uuid}.parquet` names that are
//! rejected by [`parse_sst_filename`] so incomplete files never enter [`FileIndex`].

use common::{Result, TsdbError, SST_FILE_SUFFIX};
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Parsed SST filename components.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SstIdentity {
    /// Unix timestamp in milliseconds when the file was created (process-monotonic).
    pub creation_time_ms: i64,
    /// Inclusive lower bound of CDC LSNs covered by this SST.
    pub min_lsn: u64,
    /// Inclusive upper bound of CDC LSNs covered by this SST.
    pub max_lsn: u64,
    pub inner_compaction_count: u32,
    pub cross_compaction_count: u32,
    /// Present only for unfinished staging writes (`staging-{uuid}.parquet`).
    staging_uuid: Option<Uuid>,
}

impl SstIdentity {
    /// Sealed (indexable) identity — never a staging UUID name.
    pub fn from_parts(
        creation_time_ms: i64,
        min_lsn: u64,
        max_lsn: u64,
        inner_compaction_count: u32,
        cross_compaction_count: u32,
    ) -> Self {
        let (min_lsn, max_lsn) = if min_lsn <= max_lsn {
            (min_lsn, max_lsn)
        } else {
            (max_lsn, min_lsn)
        };
        Self {
            creation_time_ms,
            min_lsn,
            max_lsn,
            inner_compaction_count,
            cross_compaction_count,
            staging_uuid: None,
        }
    }

    /// Fresh flush / bulk-load SST covering `[min_lsn, max_lsn]`.
    pub fn fresh_flush(min_lsn: u64, max_lsn: u64) -> Self {
        Self::from_parts(now_ms(), min_lsn, max_lsn, 0, 0)
    }

    /// Unique staging identity for two-phase bulk load (not published until sealed with LSN).
    ///
    /// Uses UUIDv4 so names stay unique across process restarts and concurrent writers —
    /// unlike a process-local atomic counter that resets on reboot.
    pub fn staging() -> Self {
        Self {
            creation_time_ms: now_ms(),
            min_lsn: 0,
            max_lsn: 0,
            inner_compaction_count: 0,
            cross_compaction_count: 0,
            staging_uuid: Some(Uuid::new_v4()),
        }
    }

    pub fn is_staging(&self) -> bool {
        self.staging_uuid.is_some()
    }

    pub fn after_inner_merge(a: &Self, b: &Self) -> Self {
        Self::after_inner_merge_run(&[*a, *b])
    }

    /// Identity for the single SST produced by inner-merging a contiguous run of `ids`.
    ///
    /// Spans the full LSN range of inputs and bumps the highest compaction generation
    /// seen in the run once (a whole run collapses into one new generation).
    pub fn after_inner_merge_run(ids: &[Self]) -> Self {
        if ids.is_empty() {
            return Self::fresh_flush(0, 0);
        }
        let min_lsn = ids.iter().map(|i| i.min_lsn).min().unwrap_or(0);
        let max_lsn = ids.iter().map(|i| i.max_lsn).max().unwrap_or(0);
        let inner = ids
            .iter()
            .map(|i| i.inner_compaction_count)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let cross = ids
            .iter()
            .map(|i| i.cross_compaction_count)
            .max()
            .unwrap_or(0);
        Self {
            creation_time_ms: now_ms(),
            min_lsn,
            max_lsn,
            inner_compaction_count: inner,
            cross_compaction_count: cross,
            staging_uuid: None,
        }
    }

    pub fn filename(&self) -> String {
        if let Some(uuid) = self.staging_uuid {
            return format!("staging-{uuid}.{SST_FILE_SUFFIX}");
        }
        format!(
            "{}-{}-{}-{}-{}.{SST_FILE_SUFFIX}",
            self.creation_time_ms,
            self.min_lsn,
            self.max_lsn,
            self.inner_compaction_count,
            self.cross_compaction_count,
        )
    }

    #[inline(always)]
    pub fn covers_lsn(&self, lsn: u64) -> bool {
        lsn >= self.min_lsn && lsn <= self.max_lsn
    }
}

/// Process-monotonic wall-clock millis.
///
/// Uses `SystemTime` when it advances, otherwise ticks a process-local counter so
/// NTP / container clock skew cannot emit a non-monotonic `creation_time_ms` within
/// this process. Across restarts, sealed SST uniqueness still relies on the LSN span
/// (and staging uses UUID).
#[inline]
pub fn now_ms() -> i64 {
    static LAST_MS: AtomicI64 = AtomicI64::new(0);
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "system clock before UNIX_EPOCH; using monotonic fallback");
            0
        });

    loop {
        let prev = LAST_MS.load(Ordering::Relaxed);
        let next = if wall > prev {
            wall
        } else {
            prev.saturating_add(1)
        };
        if LAST_MS
            .compare_exchange_weak(prev, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return next;
        }
    }
}

/// Parse `{time}-{min_lsn}-{max_lsn}-{inner}-{cross}.parquet`.
///
/// Staging names (`staging-*.parquet`) are rejected so they never enter FileIndex.
///
/// Hot path: zero heap allocation beyond the returned [`SstIdentity`] (iterator split only).
pub fn parse_sst_filename(name: &str) -> Result<SstIdentity> {
    let stem = Path::new(name)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(name);

    const SUFFIX: &str = concat!(".", "parquet");
    // Prefer the workspace constant without allocating a format string on every call.
    let stem = if let Some(s) = stem.strip_suffix(SUFFIX) {
        s
    } else if SST_FILE_SUFFIX == "parquet" {
        return Err(TsdbError::Storage(format!("invalid SST suffix: {name}")));
    } else {
        // Fallback if the constant ever diverges from "parquet".
        let dotted = format!(".{SST_FILE_SUFFIX}");
        stem.strip_suffix(dotted.as_str())
            .ok_or_else(|| TsdbError::Storage(format!("invalid SST suffix: {name}")))?
    };

    if stem.starts_with("staging-") {
        return Err(TsdbError::Storage(format!(
            "staging files are not readable indices: {name}"
        )));
    }

    let mut parts = stem.split('-');
    let mut next_part = || -> Result<&str> {
        parts.next().ok_or_else(|| {
            TsdbError::Storage(format!(
                "invalid SST filename format (expected 5 parts): {name}"
            ))
        })
    };

    let p1 = next_part()?;
    let p2 = next_part()?;
    let p3 = next_part()?;
    let p4 = next_part()?;
    let p5 = next_part()?;
    if parts.next().is_some() {
        return Err(TsdbError::Storage(format!(
            "invalid SST filename (too many parts): {name}"
        )));
    }

    Ok(SstIdentity {
        creation_time_ms: parse_field(p1, "time", name)?,
        min_lsn: parse_field(p2, "min_lsn", name)?,
        max_lsn: parse_field(p3, "max_lsn", name)?,
        inner_compaction_count: parse_field(p4, "inner", name)?,
        cross_compaction_count: parse_field(p5, "cross", name)?,
        staging_uuid: None,
    })
}

#[inline(always)]
fn parse_field<T: std::str::FromStr>(raw: &str, field: &str, name: &str) -> Result<T> {
    raw.parse()
        .map_err(|_| TsdbError::Storage(format!("invalid SST {field} in filename: {name}")))
}

pub fn wal_dir_for_memtable(base: &Path, memtable_id: u64) -> std::path::PathBuf {
    base.join(format!("{memtable_id:020}"))
}

/// True when `name` is a two-phase bulk-load staging SST basename.
#[inline]
pub fn is_staging_sst_filename(name: &str) -> bool {
    let stem = Path::new(name)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(name);
    stem.starts_with("staging-") && stem.ends_with(concat!(".", "parquet"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_sst_filename() {
        let id = SstIdentity::from_parts(1_575_028_885_956, 101, 105, 1, 0);
        let name = id.filename();
        assert_eq!(name, "1575028885956-101-105-1-0.parquet");
        assert_eq!(parse_sst_filename(&name).unwrap(), id);
    }

    #[test]
    fn rejects_four_part_filename() {
        assert!(parse_sst_filename("1575028885956-101-0-0.parquet").is_err());
    }

    #[test]
    fn inner_merge_spans_lsn_range() {
        let a = SstIdentity::fresh_flush(5, 5);
        let b = SstIdentity::fresh_flush(6, 8);
        let merged = SstIdentity::after_inner_merge(&a, &b);
        assert_eq!(merged.min_lsn, 5);
        assert_eq!(merged.max_lsn, 8);
        assert_eq!(merged.inner_compaction_count, 1);
        assert!(merged.covers_lsn(5));
        assert!(merged.covers_lsn(8));
        assert!(!merged.covers_lsn(4));
    }

    #[test]
    fn staging_identities_use_uuid_filenames() {
        let a = SstIdentity::staging();
        let b = SstIdentity::staging();
        assert_ne!(a.filename(), b.filename());
        assert!(a.is_staging());
        assert!(a.filename().starts_with("staging-"));
        assert!(a.filename().ends_with(".parquet"));
        assert_eq!(a.min_lsn, 0);
        assert_eq!(a.max_lsn, 0);
        assert!(parse_sst_filename(&a.filename()).is_err());
        assert!(is_staging_sst_filename(&a.filename()));
    }

    #[test]
    fn now_ms_is_process_monotonic() {
        let a = now_ms();
        let b = now_ms();
        assert!(b >= a);
    }

    #[test]
    fn rejects_invalid_sst_filename() {
        assert!(parse_sst_filename("nope.parquet").is_err());
        assert!(parse_sst_filename("1-2-3.parquet").is_err());
        assert!(
            parse_sst_filename("staging-550e8400-e29b-41d4-a716-446655440000.parquet").is_err()
        );
    }
}
