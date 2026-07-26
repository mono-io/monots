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

//! Append-only commit WAL with O(1) tombstone removal.
//!
//! Fsync runs outside the writer mutex (via a cloned FD). Recovery truncates
//! torn/corrupt tails instead of failing open.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use common::{CommitDurability, Result, TsdbError};
use parking_lot::{Mutex, RwLock};

pub trait CommitStore: Send + Sync {
    fn commit(&self, progress_id: &str, lsn: u64, durability: CommitDurability) -> Result<()>;
    fn load(&self) -> Result<HashMap<String, u64>>;
    fn remove(&self, progress_id: &str) -> Result<()>;
    fn flush(&self) -> Result<()>;
}

const COMMIT_RECORD_MAGIC: u32 = 0x434D_5431;
pub const TOMBSTONE_LSN: u64 = u64::MAX;

const WRITE_BUF_CAP: usize = 64 * 1024;

pub struct WalCommitLog {
    path: PathBuf,
    memory_state: RwLock<HashMap<String, u64>>,
    writer: Mutex<BufWriter<File>>,
    /// Cloned FD so `sync_data` can run without holding the writer lock.
    sync_handle: File,
    dirty: AtomicBool,
}

impl WalCommitLog {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let latest = if path.exists() {
            Self::scan_and_recover(&path)?
        } else {
            HashMap::new()
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        let sync_handle = file
            .try_clone()
            .map_err(|e| TsdbError::Storage(format!("failed to clone commit WAL fd: {e}")))?;

        Ok(Self {
            path,
            memory_state: RwLock::new(latest),
            writer: Mutex::new(BufWriter::with_capacity(WRITE_BUF_CAP, file)),
            sync_handle,
            dirty: AtomicBool::new(false),
        })
    }

    fn scan_and_recover(path: &Path) -> Result<HashMap<String, u64>> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);
        let mut latest = HashMap::new();
        let mut good_len: u64 = 0;
        let mut consumed: u64 = 0;

        loop {
            let Some(magic) = read_u32_safe(&mut reader)? else {
                break;
            };
            if magic != COMMIT_RECORD_MAGIC {
                tracing::warn!(
                    file = %path.display(),
                    "commit WAL invalid magic; truncating recovery"
                );
                break;
            }

            let Some(slot_len) = read_u32_safe(&mut reader)? else {
                break;
            };
            let mut progress_id_bytes = vec![0u8; slot_len as usize];
            if !read_exact_or_eof(&mut reader, &mut progress_id_bytes)? {
                break;
            }

            let Some(lsn) = read_u64_safe(&mut reader)? else {
                break;
            };
            let Some(crc) = read_u32_safe(&mut reader)? else {
                break;
            };

            let expected = calculate_crc(&progress_id_bytes, lsn);
            if crc != expected {
                tracing::warn!(
                    file = %path.display(),
                    "commit WAL CRC mismatch; truncating recovery"
                );
                break;
            }

            consumed += 4 + 4 + u64::from(slot_len) + 8 + 4;
            good_len = consumed;

            if let Ok(progress_id) = String::from_utf8(progress_id_bytes) {
                if lsn == TOMBSTONE_LSN {
                    latest.remove(&progress_id);
                } else {
                    let entry = latest.entry(progress_id).or_insert(0);
                    if lsn > *entry {
                        *entry = lsn;
                    }
                }
            }
        }

        if good_len < file_len {
            OpenOptions::new()
                .write(true)
                .open(path)?
                .set_len(good_len)
                .map_err(|e| {
                    TsdbError::Storage(format!(
                        "failed to truncate torn commit WAL {}: {e}",
                        path.display()
                    ))
                })?;
            tracing::warn!(
                file = %path.display(),
                truncated_to = good_len,
                was = file_len,
                "truncated torn/corrupt commit WAL tail"
            );
        }

        tracing::info!(
            file = %path.display(),
            recovered_keys = latest.len(),
            "commit WAL recovered"
        );
        Ok(latest)
    }

    fn append_record_physical(
        &self,
        progress_id: &str,
        lsn: u64,
        durability: CommitDurability,
    ) -> Result<()> {
        let id_bytes = progress_id.as_bytes();
        let checksum = calculate_crc(id_bytes, lsn);

        {
            let mut writer = self.writer.lock();
            writer
                .write_all(&COMMIT_RECORD_MAGIC.to_le_bytes())
                .map_err(|e| self.io_err("write magic", e))?;
            writer
                .write_all(&(id_bytes.len() as u32).to_le_bytes())
                .map_err(|e| self.io_err("write id len", e))?;
            writer
                .write_all(id_bytes)
                .map_err(|e| self.io_err("write id", e))?;
            writer
                .write_all(&lsn.to_le_bytes())
                .map_err(|e| self.io_err("write lsn", e))?;
            writer
                .write_all(&checksum.to_le_bytes())
                .map_err(|e| self.io_err("write crc", e))?;

            if durability == CommitDurability::Sync {
                writer.flush().map_err(|e| self.io_err("flush", e))?;
            } else {
                self.dirty.store(true, Ordering::Release);
            }
        }

        if durability == CommitDurability::Sync {
            self.sync_handle
                .sync_data()
                .map_err(|e| self.io_err("sync_data", e))?;
            self.dirty.store(false, Ordering::Release);
        }
        Ok(())
    }

    fn io_err(&self, op: &str, e: std::io::Error) -> TsdbError {
        TsdbError::Storage(format!("commit WAL {} ({}): {e}", op, self.path.display()))
    }
}

impl CommitStore for WalCommitLog {
    fn commit(&self, progress_id: &str, lsn: u64, durability: CommitDurability) -> Result<()> {
        if lsn == TOMBSTONE_LSN {
            return Err(TsdbError::Storage(
                "commit: TOMBSTONE_LSN is reserved for remove".into(),
            ));
        }

        if let Some(&current) = self.memory_state.read().get(progress_id) {
            if lsn <= current {
                return Ok(());
            }
        }

        self.append_record_physical(progress_id, lsn, durability)?;

        let mut state = self.memory_state.write();
        let entry = state.entry(progress_id.to_string()).or_insert(0);
        if lsn > *entry {
            *entry = lsn;
        }
        Ok(())
    }

    fn load(&self) -> Result<HashMap<String, u64>> {
        Ok(self.memory_state.read().clone())
    }

    fn remove(&self, progress_id: &str) -> Result<()> {
        self.append_record_physical(progress_id, TOMBSTONE_LSN, CommitDurability::Sync)?;
        self.memory_state.write().remove(progress_id);
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        if self.dirty.swap(false, Ordering::AcqRel) {
            self.writer
                .lock()
                .flush()
                .map_err(|e| self.io_err("flush", e))?;
            self.sync_handle
                .sync_data()
                .map_err(|e| self.io_err("sync_data", e))?;
        }
        Ok(())
    }
}

impl Drop for WalCommitLog {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

#[inline]
fn calculate_crc(progress_id: &[u8], lsn: u64) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(progress_id);
    hasher.update(&lsn.to_le_bytes());
    hasher.finalize()
}

fn read_exact_or_eof(r: &mut impl Read, buf: &mut [u8]) -> Result<bool> {
    let mut read = 0;
    while read < buf.len() {
        match r.read(&mut buf[read..]) {
            Ok(0) => {
                if read > 0 {
                    tracing::warn!("torn write detected (partial record at EOF)");
                }
                return Ok(false);
            }
            Ok(n) => read += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(TsdbError::Storage(e.to_string())),
        }
    }
    Ok(true)
}

fn read_u32_safe(r: &mut impl Read) -> Result<Option<u32>> {
    let mut buf = [0u8; 4];
    if read_exact_or_eof(r, &mut buf)? {
        Ok(Some(u32::from_le_bytes(buf)))
    } else {
        Ok(None)
    }
}

fn read_u64_safe(r: &mut impl Read) -> Result<Option<u64>> {
    let mut buf = [0u8; 8];
    if read_exact_or_eof(r, &mut buf)? {
        Ok(Some(u64::from_le_bytes(buf)))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_commit_sync_roundtrips_latest_per_progress() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("commit.wal");
        {
            let log = WalCommitLog::open(&path).unwrap();
            log.commit("s1", 10, CommitDurability::Sync).unwrap();
            log.commit("s1", 25, CommitDurability::Sync).unwrap();
            log.commit("s2", 7, CommitDurability::Sync).unwrap();
            log.commit("s1", 5, CommitDurability::Sync).unwrap();
        }
        let log = WalCommitLog::open(&path).unwrap();
        let map = log.load().unwrap();
        assert_eq!(map.get("s1"), Some(&25));
        assert_eq!(map.get("s2"), Some(&7));
    }

    #[test]
    fn wal_commit_async_survives_after_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("commit.wal");
        {
            let log = WalCommitLog::open(&path).unwrap();
            log.commit("s1", 100, CommitDurability::Async).unwrap();
            log.flush().unwrap();
        }
        let reopened = WalCommitLog::open(&path).unwrap();
        assert_eq!(reopened.load().unwrap().get("s1"), Some(&100));
    }

    #[test]
    fn remove_appends_tombstone_without_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("commit.wal");
        let log = WalCommitLog::open(&path).unwrap();
        log.commit("s1", 10, CommitDurability::Sync).unwrap();
        log.commit("s2", 20, CommitDurability::Sync).unwrap();
        let size_before = std::fs::metadata(&path).unwrap().len();
        log.remove("s1").unwrap();
        let size_after = std::fs::metadata(&path).unwrap().len();
        assert!(
            size_after > size_before,
            "tombstone must append, not rewrite"
        );
        drop(log);
        let reopened = WalCommitLog::open(&path).unwrap();
        let map = reopened.load().unwrap();
        assert!(map.get("s1").is_none());
        assert_eq!(map.get("s2"), Some(&20));
    }

    #[test]
    fn torn_tail_is_truncated_and_new_commits_survive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("commit.wal");
        {
            let log = WalCommitLog::open(&path).unwrap();
            log.commit("s1", 10, CommitDurability::Sync).unwrap();
        }
        let good_len = std::fs::metadata(&path).unwrap().len();
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0x31, 0x54, 0x4D, 0x43, 0x01, 0x00]).unwrap();
            f.sync_all().unwrap();
        }
        assert!(std::fs::metadata(&path).unwrap().len() > good_len);

        {
            let log = WalCommitLog::open(&path).unwrap();
            assert_eq!(log.load().unwrap().get("s1"), Some(&10));
            assert_eq!(std::fs::metadata(&path).unwrap().len(), good_len);
            log.commit("s1", 20, CommitDurability::Sync).unwrap();
        }
        let reopened = WalCommitLog::open(&path).unwrap();
        assert_eq!(reopened.load().unwrap().get("s1"), Some(&20));
    }
}
