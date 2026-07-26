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

//! Append-only metadata WAL with framed proto records.

use common::{Result, TsdbError};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use super::crc32::crc32;

pub const WAL_MAGIC: u32 = 0x5744_424D; // "MDBW" LE
pub const MAX_WAL_BYTES: u64 = 512 * 1024;
pub const MAX_WAL_RECORDS: usize = 64;
const FRAME_HEADER_LEN: u64 = 12;

pub struct MetaWal {
    path: PathBuf,
    file: Option<BufWriter<File>>,
    records: usize,
    bytes: u64,
}

impl MetaWal {
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let path = dir.join("metadata.wal");
        let (records, bytes) = if path.is_file() {
            scan_wal(&path)?
        } else {
            (0, 0)
        };
        Ok(Self {
            path,
            file: None,
            records,
            bytes,
        })
    }

    pub fn needs_compaction(&self) -> bool {
        self.records >= MAX_WAL_RECORDS || self.bytes >= MAX_WAL_BYTES
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    fn writer(&mut self) -> Result<&mut BufWriter<File>> {
        if self.file.is_none() {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?;
            self.file = Some(BufWriter::new(file));
        }
        Ok(self.file.as_mut().expect("writer just opened"))
    }

    /// Append a framed record and physically fsync it before returning, guaranteeing the metadata
    /// log survives power loss / kernel crash. This runs sync `fdatasync`; callers must offload it
    /// to a blocking thread (see `MetaStore::*_async`) to avoid starving tokio worker threads.
    pub fn append(&mut self, payload: &[u8]) -> Result<()> {
        let crc = crc32(payload);
        let len = payload.len() as u32;
        let writer = self.writer()?;
        writer.write_all(&WAL_MAGIC.to_le_bytes())?;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(payload)?;
        writer.write_all(&crc.to_le_bytes())?;

        // 1. Drain the userspace BufWriter into the OS page cache.
        writer.flush()?;
        // 2. Force the page-cache dirty pages onto non-volatile media (fdatasync). On return the
        //    record is durably persisted, so a DDL/manifest ack to the client is crash-safe.
        writer.get_ref().sync_data()?;

        self.records += 1;
        self.bytes += FRAME_HEADER_LEN + payload.len() as u64;
        Ok(())
    }

    /// fsync WAL after snapshot compaction for durability.
    pub fn sync(&self) -> Result<()> {
        if !self.path.is_file() {
            return Ok(());
        }
        let file = OpenOptions::new().read(true).open(&self.path)?;
        file.sync_data()?;
        Ok(())
    }

    pub fn replay(&self) -> Result<Vec<Vec<u8>>> {
        if !self.path.is_file() {
            return Ok(Vec::new());
        }
        let data = fs::read(&self.path)?;
        decode_all_frames(&data)
    }

    pub fn truncate(&mut self) -> Result<()> {
        self.file = None;
        if self.path.is_file() {
            fs::remove_file(&self.path)?;
        }
        self.records = 0;
        self.bytes = 0;
        Ok(())
    }
}

fn decode_all_frames(data: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    let mut offset = 0usize;
    while offset < data.len() {
        if data.len() - offset < 12 {
            tracing::warn!(
                "metadata wal: trailing garbage {} bytes, truncating",
                data.len() - offset
            );
            break;
        }
        let magic = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        if magic != WAL_MAGIC {
            return Err(TsdbError::Storage(format!(
                "invalid wal magic at offset {offset}: {magic:#x}"
            )));
        }
        let len = u32::from_le_bytes(data[offset + 4..offset + 8].try_into().unwrap()) as usize;
        let end = offset + 8 + len;
        let crc_off = end;
        if data.len() < crc_off + 4 {
            tracing::warn!("metadata wal: partial record at {offset}, stopping replay");
            break;
        }
        let payload = &data[offset + 8..end];
        let expected = u32::from_le_bytes(data[crc_off..crc_off + 4].try_into().unwrap());
        let actual = crc32(payload);
        if expected != actual {
            tracing::warn!("metadata wal: crc mismatch at {offset}, stopping replay");
            break;
        }
        out.push(payload.to_vec());
        offset = crc_off + 4;
    }
    Ok(out)
}

fn scan_wal(path: &Path) -> Result<(usize, u64)> {
    let data = fs::read(path)?;
    let frames = decode_all_frames(&data)?;
    Ok((frames.len(), data.len() as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use proto::meta::MetadataRecord;
    use std::fs;

    #[test]
    fn append_and_replay() {
        let root = std::env::temp_dir().join(format!("monots_wal_{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let mut wal = MetaWal::open(&root).unwrap();
        let rec = MetadataRecord {
            seq: 1,
            timestamp_ms: 1,
            op: Some(proto::meta::metadata_record::Op::PutSchema(
                proto::meta::PutSchema {
                    schema: Some(proto::meta::TableSchema {
                        table_name: "t".into(),
                        columns: vec![],
                        data_dir: "/tmp".into(),
                    }),
                },
            )),
        };
        let bytes = rec.encode_to_vec();
        wal.append(&bytes).unwrap();
        let replayed = wal.replay().unwrap();
        assert_eq!(replayed.len(), 1);
        let parsed = MetadataRecord::decode(replayed[0].as_slice()).unwrap();
        assert_eq!(parsed.seq, 1);
        let _ = fs::remove_dir_all(root);
    }
}
