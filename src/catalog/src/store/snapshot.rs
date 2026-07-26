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

//! Snapshot read/write for metadata store.

use common::{Result, TsdbError};
use prost::Message;
use std::fs;
use std::path::{Path, PathBuf};

use super::crc32::crc32;

pub const SNAP_MAGIC: u32 = 0x50414E53; // "SNAP" LE
pub const STORE_VERSION: u32 = 1;

#[derive(Clone)]
pub struct MetaSnapshot {
    path: PathBuf,
}

impl MetaSnapshot {
    pub fn new(root: &Path) -> Self {
        fs::create_dir_all(root.join("snapshots")).ok();
        Self {
            path: root.join("snapshots").join("latest.pb"),
        }
    }

    pub fn exists(&self) -> bool {
        self.path.is_file()
    }

    pub fn load(&self) -> Result<Option<proto::meta::StoreSnapshot>> {
        if !self.path.is_file() {
            return Ok(None);
        }
        let data = fs::read(&self.path)?;
        let snap = decode_snapshot(&data)?;
        validate_store_version(snap.store_version)?;
        Ok(Some(snap))
    }

    pub fn save(&self, snap: &proto::meta::StoreSnapshot) -> Result<()> {
        let payload = snap.encode_to_vec();
        let frame = encode_snapshot_frame(&payload);
        let tmp = self.path.with_extension("pb.tmp");
        fs::write(&tmp, &frame)?;
        std::fs::File::open(&tmp)?.sync_all()?;
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

pub fn encode_snapshot_frame(payload: &[u8]) -> Vec<u8> {
    encode_framed_payload(payload)
}

pub fn encode_framed_payload(payload: &[u8]) -> Vec<u8> {
    let crc = crc32(payload);
    let len = payload.len() as u32;
    let mut out = Vec::with_capacity(12 + payload.len());
    out.extend_from_slice(&SNAP_MAGIC.to_le_bytes());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(payload);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

pub fn decode_framed_payload(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 12 {
        return Err(TsdbError::Storage("framed payload too small".into()));
    }
    let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
    if magic != SNAP_MAGIC {
        return Err(TsdbError::Storage(format!(
            "invalid frame magic: {magic:#x}"
        )));
    }
    let len = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
    let end = 8 + len;
    if data.len() < end + 4 {
        return Err(TsdbError::Storage("truncated framed payload".into()));
    }
    let payload = &data[8..end];
    let expected = u32::from_le_bytes(data[end..end + 4].try_into().unwrap());
    if crc32(payload) != expected {
        return Err(TsdbError::Storage("frame crc mismatch".into()));
    }
    Ok(payload.to_vec())
}

pub fn decode_snapshot(data: &[u8]) -> Result<proto::meta::StoreSnapshot> {
    let payload = decode_framed_payload(data)?;
    proto::meta::StoreSnapshot::decode(payload.as_slice())
        .map_err(|e| TsdbError::Storage(format!("decode snapshot: {e}")))
}

/// Reject snapshots from newer store versions until migration is implemented.
pub fn validate_store_version(version: u32) -> Result<()> {
    if version != STORE_VERSION {
        return Err(TsdbError::Storage(format!(
            "unsupported metadata store_version {version}, expected {STORE_VERSION}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn roundtrip_snapshot() {
        let snap = proto::meta::StoreSnapshot {
            store_version: STORE_VERSION,
            seq: 42,
            schemas: HashMap::new(),
            manifests: HashMap::new(),
        };
        let frame = encode_snapshot_frame(&snap.encode_to_vec());
        let loaded = decode_snapshot(&frame).unwrap();
        assert_eq!(loaded.seq, 42);
    }
}
