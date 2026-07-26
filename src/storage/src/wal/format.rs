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

//! Framed WAL on-disk format (v1).
//!
//! Block = `[Header 36B | Body…]` with `block_len`, hardware CRC32, reused body buffers.
//! Unknown frames / torn tails are skipped; [`WalFrameCursor`] supports live tailing.
//!
//! Size knobs (defaults; also exposed as engine/YAML config):
//! - segment file rotate: [`DEFAULT_WAL_SEGMENT_MAX_BYTES`] (100 MiB)
//! - single block hard cap: [`DEFAULT_WAL_BLOCK_MAX_BYTES`] (5 MiB)

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use bytes::{Buf, BufMut};
use common::{Result, TsdbError};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const SEGMENT_MAGIC: [u8; 8] = *b"FLOWWAL\0";
pub const FRAME_MAGIC: u32 = 0x4657_4C31; // "FWL1"

pub const SEGMENT_FORMAT_VERSION: u16 = 1;
pub const SEGMENT_MIN_READ_VERSION: u16 = 1;
pub const SEGMENT_MAX_READ_VERSION: u16 = SEGMENT_FORMAT_VERSION;

pub const SEGMENT_KNOWN_FLAGS: u32 = 0;
pub const FRAME_KNOWN_FLAGS: u32 = 0;

pub const SEGMENT_HEADER_SIZE: usize = 40;
/// magic(4)+block_len(4)+type(2)+fmt(2)+crc(4)+flags(4)+seq(8)+lsn(8)
pub const FRAME_HEADER_SIZE: usize = 36;
pub const PAYLOAD_FORMAT_ARROW_IPC: u16 = 1;

pub const WAL_SEGMENT_EXT: &str = "wal";
pub const WAL_FILE_NAME: &str = "segment.wal";
/// Default max on-disk size of one WAL segment file before rotating (100 MiB).
pub const DEFAULT_WAL_SEGMENT_MAX_BYTES: u64 = 100 * 1024 * 1024;
/// Hard cap on one WAL block (`block_len` = header + body). Oversized writes are rejected;
/// readers treat larger `block_len` as corrupt and stop the segment.
pub const DEFAULT_WAL_BLOCK_MAX_BYTES: usize = 5 * 1024 * 1024;
pub const SCHEMA_FRAME_SEQUENCE: u64 = 0;

pub fn segment_format_readable(version: u16) -> bool {
    version >= SEGMENT_MIN_READ_VERSION && version <= SEGMENT_MAX_READ_VERSION
}

fn validate_segment_format_version(version: u16) -> Result<()> {
    if segment_format_readable(version) {
        Ok(())
    } else {
        Err(TsdbError::Storage(format!(
            "unsupported WAL segment format version: {version} (supported: {SEGMENT_MIN_READ_VERSION}-{SEGMENT_MAX_READ_VERSION})"
        )))
    }
}

fn validate_segment_flags(flags: u32) -> Result<()> {
    let unknown = flags & !SEGMENT_KNOWN_FLAGS;
    if unknown != 0 {
        return Err(TsdbError::Storage(format!(
            "unsupported WAL segment flags: {unknown:#x}"
        )));
    }
    Ok(())
}

fn frame_has_unknown_flags(flags: u32) -> bool {
    flags & !FRAME_KNOWN_FLAGS != 0
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Schema,
    Batch,
    Footer,
    /// Marks the end of a memtable's LSN span (`frame.lsn` = end LSN). Always durable.
    MemTableEnd,
    /// Skip body via `block_len`; keep recovering later frames.
    Unknown(u16),
}

impl RecordType {
    fn from_u16(v: u16) -> Self {
        match v {
            1 => Self::Schema,
            2 => Self::Batch,
            3 => Self::Footer,
            4 => Self::MemTableEnd,
            other => Self::Unknown(other),
        }
    }

    fn as_u16(self) -> u16 {
        match self {
            Self::Schema => 1,
            Self::Batch => 2,
            Self::Footer => 3,
            Self::MemTableEnd => 4,
            Self::Unknown(v) => v,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub format_version: u16,
    pub segment_flags: u32,
    pub memtable_id: u64,
    pub created_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FrameHeader {
    pub(crate) record_type: RecordType,
    pub(crate) payload_format: u16,
    /// Header + body byte length.
    pub(crate) block_len: u32,
    pub(crate) crc32: u32,
    pub(crate) flags: u32,
    pub(crate) sequence: u64,
    /// Global LSN (`0` = replication off / metadata).
    pub(crate) lsn: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentFooter {
    pub batch_count: u64,
    pub frame_count: u64,
    pub closed_unix_ms: u64,
}

// ---------------------------------------------------------------------------
// Codecs (`bytes` Buf / BufMut on stack buffers)
// ---------------------------------------------------------------------------

impl SegmentHeader {
    pub fn new_v1(memtable_id: u64) -> Self {
        Self {
            format_version: SEGMENT_FORMAT_VERSION,
            segment_flags: 0,
            memtable_id,
            created_unix_ms: current_unix_ms(),
        }
    }

    fn encode(&self, out: &mut impl Write) -> Result<()> {
        let mut buf = [0u8; SEGMENT_HEADER_SIZE];
        {
            let mut w = &mut buf[..];
            w.put_slice(&SEGMENT_MAGIC);
            w.put_u16_le(self.format_version);
            w.put_u16_le(SEGMENT_HEADER_SIZE as u16);
            w.put_u32_le(self.segment_flags);
            w.put_u64_le(self.memtable_id);
            w.put_u64_le(self.created_unix_ms);
            w.put_u64_le(0); // reserved
        }
        out.write_all(&buf).map_err(map_io)
    }

    pub(crate) fn decode(mut r: impl Read) -> Result<Self> {
        let mut buf = [0u8; SEGMENT_HEADER_SIZE];
        r.read_exact(&mut buf).map_err(map_io)?;
        let mut c = &buf[..];

        let mut magic = [0u8; 8];
        c.copy_to_slice(&mut magic);
        if magic != SEGMENT_MAGIC {
            return Err(TsdbError::Storage("invalid WAL segment magic".into()));
        }

        let format_version = c.get_u16_le();
        let header_size = c.get_u16_le();
        if (header_size as usize) < SEGMENT_HEADER_SIZE {
            return Err(TsdbError::Storage(format!(
                "WAL segment header size too small: {header_size} (minimum {SEGMENT_HEADER_SIZE})"
            )));
        }
        validate_segment_format_version(format_version)?;

        let segment_flags = c.get_u32_le();
        validate_segment_flags(segment_flags)?;
        let memtable_id = c.get_u64_le();
        let created_unix_ms = c.get_u64_le();
        let _reserved = c.get_u64_le();

        // Skip unknown trailing header bytes from newer writers.
        let extra = header_size as usize - SEGMENT_HEADER_SIZE;
        if extra > 0 {
            let mut skip = vec![0u8; extra];
            r.read_exact(&mut skip).map_err(map_io)?;
        }

        Ok(Self {
            format_version,
            segment_flags,
            memtable_id,
            created_unix_ms,
        })
    }
}

impl FrameHeader {
    pub(crate) fn body_len(&self) -> usize {
        self.block_len.saturating_sub(FRAME_HEADER_SIZE as u32) as usize
    }

    fn encode(&self, out: &mut impl Write) -> Result<()> {
        let mut buf = [0u8; FRAME_HEADER_SIZE];
        {
            let mut w = &mut buf[..];
            w.put_u32_le(FRAME_MAGIC);
            w.put_u32_le(self.block_len);
            w.put_u16_le(self.record_type.as_u16());
            w.put_u16_le(self.payload_format);
            w.put_u32_le(self.crc32);
            w.put_u32_le(self.flags);
            w.put_u64_le(self.sequence);
            w.put_u64_le(self.lsn);
        }
        out.write_all(&buf).map_err(map_io)
    }

    pub(crate) fn decode(mut r: impl Read) -> Result<Option<Self>> {
        let mut buf = [0u8; FRAME_HEADER_SIZE];
        match r.read_exact(&mut buf[0..4]) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(map_io(e)),
        }

        let magic = {
            let mut c = &buf[0..4];
            c.get_u32_le()
        };
        if magic != FRAME_MAGIC {
            return Err(TsdbError::Storage(format!(
                "invalid WAL frame magic: {magic:#x}"
            )));
        }

        r.read_exact(&mut buf[4..FRAME_HEADER_SIZE])
            .map_err(map_io)?;
        let mut c = &buf[4..];

        let block_len = c.get_u32_le();
        if (block_len as usize) < FRAME_HEADER_SIZE {
            return Err(TsdbError::Storage(format!(
                "invalid WAL block_len {block_len} (min {FRAME_HEADER_SIZE})"
            )));
        }
        if (block_len as usize) > DEFAULT_WAL_BLOCK_MAX_BYTES {
            return Err(TsdbError::Storage(format!(
                "WAL block_len {block_len} exceeds max {DEFAULT_WAL_BLOCK_MAX_BYTES}"
            )));
        }

        let record_raw = c.get_u16_le();
        let payload_format = c.get_u16_le();
        let crc32 = c.get_u32_le();
        let flags = c.get_u32_le();
        let sequence = c.get_u64_le();
        let lsn = c.get_u64_le();

        let mut record_type = RecordType::from_u16(record_raw);
        if frame_has_unknown_flags(flags) {
            tracing::warn!(
                record_type = record_raw,
                flags,
                block_len,
                "skip WAL block with unknown flags"
            );
            record_type = RecordType::Unknown(record_raw);
        } else if matches!(record_type, RecordType::Unknown(_)) {
            tracing::warn!(
                record_type = record_raw,
                block_len,
                "skip unknown WAL record type"
            );
        }

        Ok(Some(Self {
            record_type,
            payload_format,
            block_len,
            crc32,
            flags,
            sequence,
            lsn,
        }))
    }
}

impl SegmentFooter {
    fn encode(&self) -> [u8; 24] {
        let mut buf = [0u8; 24];
        {
            let mut w = &mut buf[..];
            w.put_u64_le(self.batch_count);
            w.put_u64_le(self.frame_count);
            w.put_u64_le(self.closed_unix_ms);
        }
        buf
    }

    fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != 24 {
            return Err(TsdbError::Storage("invalid WAL footer length".into()));
        }
        let mut c = payload;
        Ok(Self {
            batch_count: c.get_u64_le(),
            frame_count: c.get_u64_le(),
            closed_unix_ms: c.get_u64_le(),
        })
    }
}

/// Tail state recovered when reopening an in-progress WAL file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentTailState {
    batches_written: usize,
    next_batch_sequence: u64,
    bytes_written: usize,
    frames_written: u64,
    has_footer: bool,
}

pub struct FramedSegmentWriter {
    path: PathBuf,
    memtable_id: u64,
    writer: BufWriter<File>,
    batches_written: usize,
    frames_written: u64,
    /// Approximate payload accounting (legacy); prefer [`Self::on_disk_bytes`].
    bytes_written: usize,
    next_batch_sequence: u64,
    /// Max `block_len` (header + body) accepted for appends.
    block_max_bytes: usize,
}

impl FramedSegmentWriter {
    pub fn create(path: PathBuf, memtable_id: u64, schema: SchemaRef) -> Result<Self> {
        Self::create_with_block_max(path, memtable_id, schema, DEFAULT_WAL_BLOCK_MAX_BYTES)
    }

    pub fn create_with_block_max(
        path: PathBuf,
        memtable_id: u64,
        schema: SchemaRef,
        block_max_bytes: usize,
    ) -> Result<Self> {
        if path.exists() {
            return Err(TsdbError::Storage(format!(
                "WAL file already exists: {}",
                path.display()
            )));
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let block_max_bytes = block_max_bytes.clamp(FRAME_HEADER_SIZE, DEFAULT_WAL_BLOCK_MAX_BYTES);
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);
        SegmentHeader::new_v1(memtable_id).encode(&mut writer)?;

        let schema_payload = encode_schema_ipc(&schema)?;
        write_frame(
            &mut writer,
            RecordType::Schema,
            PAYLOAD_FORMAT_ARROW_IPC,
            SCHEMA_FRAME_SEQUENCE,
            0, // schema frame carries no global LSN
            &schema_payload,
            block_max_bytes,
        )?;

        Ok(Self {
            path,
            memtable_id,
            writer,
            batches_written: 0,
            frames_written: 1,
            bytes_written: 0,
            next_batch_sequence: 1,
            block_max_bytes,
        })
    }

    /// Reopen an in-progress WAL (no footer) and continue appending frames.
    pub fn resume(path: PathBuf, memtable_id: u64) -> Result<Self> {
        Self::resume_with_block_max(path, memtable_id, DEFAULT_WAL_BLOCK_MAX_BYTES)
    }

    pub fn resume_with_block_max(
        path: PathBuf,
        memtable_id: u64,
        block_max_bytes: usize,
    ) -> Result<Self> {
        let tail = scan_segment_tail(&path, memtable_id)?;
        if tail.has_footer {
            return Err(TsdbError::Storage(format!(
                "WAL already closed: {}",
                path.display()
            )));
        }
        let file = OpenOptions::new().append(true).open(&path)?;
        Ok(Self {
            path,
            memtable_id,
            writer: BufWriter::new(file),
            batches_written: tail.batches_written,
            frames_written: tail.frames_written,
            bytes_written: tail.bytes_written,
            next_batch_sequence: tail.next_batch_sequence,
            block_max_bytes: block_max_bytes.clamp(FRAME_HEADER_SIZE, DEFAULT_WAL_BLOCK_MAX_BYTES),
        })
    }

    /// Resume an in-progress segment; call [`Self::set_memtable_id`] for the logical mid.
    pub fn resume_any(path: PathBuf) -> Result<Self> {
        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);
        let header = SegmentHeader::decode(&mut reader)?;
        drop(reader);
        Self::resume(path, header.memtable_id)
    }

    pub fn set_block_max_bytes(&mut self, block_max_bytes: usize) {
        self.block_max_bytes =
            block_max_bytes.clamp(FRAME_HEADER_SIZE, DEFAULT_WAL_BLOCK_MAX_BYTES);
    }

    pub fn append_batch(&mut self, batch: &RecordBatch, lsn: u64, sync: bool) -> Result<u64> {
        let sequence = self.next_batch_sequence;
        let payload = encode_batch_ipc(batch)?;
        write_frame(
            &mut self.writer,
            RecordType::Batch,
            PAYLOAD_FORMAT_ARROW_IPC,
            sequence,
            lsn,
            &payload,
            self.block_max_bytes,
        )?;
        self.next_batch_sequence += 1;
        self.batches_written += 1;
        self.frames_written += 1;
        self.bytes_written += batch.get_array_memory_size().max(1);
        if sync {
            self.sync()?;
        }
        Ok(sequence)
    }

    /// Append a durable memtable-end marker (`end_lsn` on the frame) and **always** fsync.
    ///
    /// Payload: `closed_memtable_id` (u64 LE) + `new_memtable_id` (u64 LE). Recovery splits
    /// Parquet rebuild partitions on this frame and may synthesize the frame for open tails.
    pub fn append_memtable_end(
        &mut self,
        end_lsn: u64,
        closed_memtable_id: u64,
        new_memtable_id: u64,
    ) -> Result<()> {
        let sequence = self.next_batch_sequence;
        let payload = encode_memtable_end_payload(closed_memtable_id, new_memtable_id);
        write_frame(
            &mut self.writer,
            RecordType::MemTableEnd,
            0,
            sequence,
            end_lsn,
            &payload,
            self.block_max_bytes,
        )?;
        self.next_batch_sequence += 1;
        self.frames_written += 1;
        // User requirement: MemTableEnd must hit disk before freeze continues.
        self.sync()?;
        self.memtable_id = new_memtable_id;
        Ok(())
    }

    /// Update the in-memory logical memtable id (no WAL frame — recover trim is by LSN).
    pub fn set_memtable_id(&mut self, memtable_id: u64) {
        self.memtable_id = memtable_id;
    }

    pub fn finish(mut self) -> Result<()> {
        let footer = SegmentFooter {
            batch_count: self.batches_written as u64,
            frame_count: self.frames_written + 1,
            closed_unix_ms: current_unix_ms(),
        };
        write_frame(
            &mut self.writer,
            RecordType::Footer,
            0,
            self.next_batch_sequence,
            0, // footer frame carries no global LSN
            &footer.encode(),
            self.block_max_bytes,
        )?;
        self.sync()?;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    pub fn batches_written(&self) -> usize {
        self.batches_written
    }

    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    /// On-disk file length (authoritative for size-based rotation).
    pub fn on_disk_bytes(&mut self) -> Result<u64> {
        self.writer.flush()?;
        Ok(self.path.metadata().map(|m| m.len()).unwrap_or(0))
    }

    pub fn memtable_id(&self) -> u64 {
        self.memtable_id
    }

    pub fn sync_data(&mut self) -> Result<()> {
        self.sync()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub fn wal_file_path(memtable_dir: &Path) -> PathBuf {
    memtable_dir.join(WAL_FILE_NAME)
}

/// Flat numbered WAL path: `wal_segments/{file_id:020}.wal`.
pub fn numbered_wal_path(wal_root: &Path, file_id: u64) -> PathBuf {
    wal_root.join(format!("{file_id:020}.{WAL_SEGMENT_EXT}"))
}

/// Sorted numbered WAL file ids under `wal_root` (skips dirs such as `bulk_load/`).
pub fn list_wal_file_ids(wal_root: &Path) -> Result<Vec<u64>> {
    if !wal_root.is_dir() {
        return Ok(vec![]);
    }
    let mut ids = Vec::new();
    for entry in fs::read_dir(wal_root)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(&format!(".{WAL_SEGMENT_EXT}")) else {
            continue;
        };
        if stem.len() == 20 && stem.chars().all(|c| c.is_ascii_digit()) {
            if let Ok(id) = stem.parse::<u64>() {
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// Paths of numbered WAL files in append order.
pub fn list_numbered_wal_paths(wal_root: &Path) -> Result<Vec<PathBuf>> {
    Ok(list_wal_file_ids(wal_root)?
        .into_iter()
        .map(|id| numbered_wal_path(wal_root, id))
        .collect())
}

pub fn read_segment_batches(
    path: &Path,
    expected_memtable_id: u64,
    allow_partial: bool,
) -> Result<Vec<RecordBatch>> {
    if !path.exists() {
        return Ok(vec![]);
    }

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let header = match SegmentHeader::decode(&mut reader) {
        Ok(h) => h,
        Err(e) if allow_partial => {
            tracing::warn!("skip unreadable wal {}: {e}", path.display());
            return Ok(vec![]);
        }
        Err(e) => return Err(e),
    };
    // Creator id in the header is informational; recover trim is by frame LSN.
    let _ = header;
    let _ = expected_memtable_id;

    let mut schema: Option<SchemaRef> = None;
    let mut batches = Vec::new();
    let mut saw_footer = false;
    let mut next_batch_sequence = 1u64;
    // Reuse one body buffer across frames — avoids per-frame heap alloc on hot recovery path.
    let mut payload_buf = Vec::new();

    loop {
        let frame = match FrameHeader::decode(&mut reader) {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) if allow_partial => {
                tracing::warn!(
                    "partial wal {} after {} batches: {e}",
                    path.display(),
                    batches.len()
                );
                break;
            }
            Err(e) => return Err(e),
        };

        payload_buf.resize(frame.body_len(), 0);
        if let Err(e) = reader.read_exact(&mut payload_buf) {
            if allow_partial {
                tracing::warn!(
                    "truncated wal {} after {} batches: {e}",
                    path.display(),
                    batches.len()
                );
                break;
            }
            return Err(TsdbError::Storage(e.to_string()));
        }

        if crc32(&payload_buf) != frame.crc32 {
            if allow_partial {
                tracing::warn!(
                    "crc mismatch in wal {} after {} batches",
                    path.display(),
                    batches.len()
                );
                break;
            }
            return Err(TsdbError::Storage(format!(
                "WAL frame crc mismatch in {}",
                path.display()
            )));
        }

        match frame.record_type {
            RecordType::Schema => {
                if frame.sequence != SCHEMA_FRAME_SEQUENCE {
                    if allow_partial {
                        tracing::warn!(
                            "invalid schema sequence {} in wal {}, stopping replay",
                            frame.sequence,
                            path.display()
                        );
                        break;
                    }
                    return Err(TsdbError::Storage(format!(
                        "invalid WAL schema sequence {}",
                        frame.sequence
                    )));
                }
                if frame.payload_format != PAYLOAD_FORMAT_ARROW_IPC {
                    if allow_partial {
                        tracing::warn!(
                            "unsupported schema payload format {} in wal {}, stopping replay",
                            frame.payload_format,
                            path.display()
                        );
                        break;
                    }
                    return Err(TsdbError::Storage(format!(
                        "unsupported schema payload format {}",
                        frame.payload_format
                    )));
                }
                match decode_schema_ipc(&payload_buf) {
                    Ok(s) => schema = Some(s),
                    Err(e) if allow_partial => {
                        tracing::warn!("failed to decode schema in wal {}: {e}", path.display());
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
            RecordType::Batch => {
                if frame.sequence != next_batch_sequence {
                    if allow_partial {
                        tracing::warn!(
                            "sequence gap in wal {}: expected {next_batch_sequence}, found {}, keeping {} batches",
                            path.display(),
                            frame.sequence,
                            batches.len()
                        );
                        break;
                    }
                    return Err(TsdbError::Storage(format!(
                        "WAL batch sequence gap in {}: expected {next_batch_sequence}, found {}",
                        path.display(),
                        frame.sequence
                    )));
                }
                if frame.payload_format != PAYLOAD_FORMAT_ARROW_IPC {
                    if allow_partial {
                        tracing::warn!(
                            "unsupported batch payload format {} in wal {}, stopping replay",
                            frame.payload_format,
                            path.display()
                        );
                        break;
                    }
                    return Err(TsdbError::Storage(format!(
                        "unsupported batch payload format {}",
                        frame.payload_format
                    )));
                }
                match decode_batch_ipc(schema.as_ref(), &payload_buf) {
                    Ok(batch) => {
                        batches.push(batch);
                        next_batch_sequence += 1;
                    }
                    Err(e) if allow_partial => {
                        tracing::warn!(
                            "failed to decode batch in wal {} after {} batches: {e}",
                            path.display(),
                            batches.len()
                        );
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
            RecordType::Footer => {
                let _footer = SegmentFooter::decode(&payload_buf)?;
                saw_footer = true;
                break;
            }
            RecordType::MemTableEnd => {
                next_batch_sequence = frame.sequence.saturating_add(1).max(next_batch_sequence);
            }
            RecordType::Unknown(raw) => {
                tracing::debug!(
                    path = %path.display(),
                    record_type = raw,
                    sequence = frame.sequence,
                    "skip unknown WAL frame during replay"
                );
                next_batch_sequence = frame.sequence.saturating_add(1).max(next_batch_sequence);
            }
        }
    }

    if !allow_partial && !saw_footer {
        return Err(TsdbError::Storage(format!(
            "WAL segment missing footer: {}",
            path.display()
        )));
    }

    Ok(batches)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalSegmentTail {
    pub batches_written: usize,
    pub next_batch_sequence: u64,
    pub has_footer: bool,
}

pub fn wal_segment_tail(path: &Path, expected_memtable_id: u64) -> Result<WalSegmentTail> {
    let tail = scan_segment_tail(path, expected_memtable_id)?;
    Ok(WalSegmentTail {
        batches_written: tail.batches_written,
        next_batch_sequence: tail.next_batch_sequence,
        has_footer: tail.has_footer,
    })
}

/// Sorted memtable ids from segment headers (best-effort).
/// Prefer [`list_wal_file_ids`] for path navigation.
pub fn list_wal_memtable_ids(wal_root: &Path) -> Result<Vec<u64>> {
    use std::collections::BTreeSet;
    if !wal_root.is_dir() {
        return Ok(vec![]);
    }
    let mut ids = BTreeSet::new();
    for path in list_numbered_wal_paths(wal_root)? {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        let mut reader = BufReader::new(file);
        let Ok(header) = SegmentHeader::decode(&mut reader) else {
            continue;
        };
        ids.insert(header.memtable_id);
    }
    Ok(ids.into_iter().collect())
}

#[derive(Debug, Clone)]
pub struct WalFramedBatch {
    pub sequence: u64,
    /// Global LSN of this frame (0 when written without replication).
    pub lsn: u64,
    pub batch: RecordBatch,
}

/// Read WAL batch frames with `sequence >= from_sequence` (precise realtime capture).
pub fn read_wal_batches_from_sequence(
    path: &Path,
    expected_memtable_id: u64,
    from_sequence: u64,
    allow_partial: bool,
) -> Result<Vec<WalFramedBatch>> {
    if !path.exists() {
        return Ok(vec![]);
    }

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let header = match SegmentHeader::decode(&mut reader) {
        Ok(h) => h,
        Err(e) if allow_partial => {
            tracing::warn!("skip unreadable wal {}: {e}", path.display());
            return Ok(vec![]);
        }
        Err(e) => return Err(e),
    };
    if header.memtable_id != expected_memtable_id {
        if allow_partial {
            return Ok(vec![]);
        }
        return Err(TsdbError::Storage(format!(
            "WAL memtable id mismatch in {}: expected {expected_memtable_id}, found {}",
            path.display(),
            header.memtable_id
        )));
    }

    let mut schema: Option<SchemaRef> = None;
    let mut out = Vec::new();
    let mut saw_footer = false;
    let mut payload_buf = Vec::new();

    loop {
        let frame = match FrameHeader::decode(&mut reader) {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) if allow_partial => {
                tracing::warn!("partial wal {}: {e}", path.display());
                break;
            }
            Err(e) => return Err(e),
        };

        payload_buf.resize(frame.body_len(), 0);
        if reader.read_exact(&mut payload_buf).is_err() {
            if allow_partial {
                break;
            }
            return Err(TsdbError::Storage(format!(
                "truncated WAL frame in {}",
                path.display()
            )));
        }

        if crc32(&payload_buf) != frame.crc32 {
            if allow_partial {
                break;
            }
            return Err(TsdbError::Storage(format!(
                "WAL frame crc mismatch in {}",
                path.display()
            )));
        }

        match frame.record_type {
            RecordType::Schema => {
                if let Ok(s) = decode_schema_ipc(&payload_buf) {
                    schema = Some(s);
                } else if !allow_partial {
                    return Err(TsdbError::Storage("invalid WAL schema frame".into()));
                }
            }
            RecordType::Batch => {
                if frame.sequence < from_sequence {
                    continue;
                }
                let Some(ref schema_ref) = schema else {
                    if allow_partial {
                        break;
                    }
                    return Err(TsdbError::Storage("WAL batch before schema".into()));
                };
                match decode_batch_ipc(Some(schema_ref), &payload_buf) {
                    Ok(batch) => out.push(WalFramedBatch {
                        sequence: frame.sequence,
                        lsn: frame.lsn,
                        batch,
                    }),
                    Err(e) if allow_partial => {
                        tracing::warn!("skip wal batch seq {}: {e}", frame.sequence);
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
            RecordType::Footer => {
                saw_footer = true;
                break;
            }
            RecordType::MemTableEnd | RecordType::Unknown(_) => {}
        }
    }

    // In-progress segments without footer are fine for tailing / allow_partial callers.
    let _ = saw_footer;
    Ok(out)
}

/// A single WAL batch frame with its global ordering key, produced by [`WalFrameCursor`].
#[derive(Debug, Clone)]
pub struct WalFrameEvent {
    pub lsn: u64,
    pub sequence: u64,
    pub batch: RecordBatch,
}

enum FrameStep {
    Schema,
    Batch(WalFrameEvent),
    Footer,
    /// Clean frame-boundary EOF or a torn/incomplete tail (rewound); retry later.
    Eof,
}

/// Streaming WAL reader that keeps an open file handle and advances frame-by-frame.
///
/// This replaces the O(N^2) pattern of re-opening the segment and re-scanning from the start on
/// every pull: the cursor persists its position across calls, so tailing a 100k-frame WAL costs
/// O(1) per frame amortized. It is `tail -f` safe — on a torn/incomplete tail it rewinds to the
/// last complete frame and returns EOF so the caller can retry after more bytes are flushed.
pub struct WalFrameCursor {
    reader: BufReader<File>,
    memtable_id: u64,
    schema: Option<SchemaRef>,
    /// Byte offset just past the last fully-decoded frame (rewind target for torn tails).
    safe_pos: u64,
    /// True once the segment footer was read (segment closed; no more frames ever).
    finished: bool,
    /// Reused body buffer — zero per-frame heap alloc on the streaming path.
    payload_buf: Vec<u8>,
}

impl WalFrameCursor {
    /// Open a cursor positioned just after the segment header. `Ok(None)` if the file is absent.
    /// Header memtable_id may differ from `logical_memtable_id` for size-based shared segments.
    pub fn open(path: &Path, logical_memtable_id: u64) -> Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let header = SegmentHeader::decode(&mut reader)?;
        let _ = header;
        let safe_pos = reader.stream_position()?;
        Ok(Some(Self {
            reader,
            memtable_id: logical_memtable_id,
            schema: None,
            safe_pos,
            finished: false,
            payload_buf: Vec::with_capacity(1024 * 1024),
        }))
    }

    pub fn memtable_id(&self) -> u64 {
        self.memtable_id
    }

    /// Whether the segment is closed (footer seen) — safe to advance to the next memtable's WAL.
    pub fn finished(&self) -> bool {
        self.finished
    }

    /// Next batch frame, or `Ok(None)` when no (complete) frame is currently available. Schema
    /// frames are consumed transparently.
    pub fn next_batch(&mut self) -> Result<Option<WalFrameEvent>> {
        if self.finished {
            return Ok(None);
        }
        loop {
            match self.read_one_frame()? {
                FrameStep::Batch(ev) => return Ok(Some(ev)),
                FrameStep::Schema => continue,
                FrameStep::Footer => {
                    self.finished = true;
                    return Ok(None);
                }
                FrameStep::Eof => return Ok(None),
            }
        }
    }

    fn read_one_frame(&mut self) -> Result<FrameStep> {
        let frame = match FrameHeader::decode(&mut self.reader) {
            Ok(Some(f)) => f,
            // Clean or partial header read: rewind so a later flush is picked up intact.
            Ok(None) | Err(_) => {
                self.rewind()?;
                return Ok(FrameStep::Eof);
            }
        };
        self.payload_buf.resize(frame.body_len(), 0);
        if self.reader.read_exact(&mut self.payload_buf).is_err() {
            self.rewind()?;
            return Ok(FrameStep::Eof);
        }
        if crc32(&self.payload_buf) != frame.crc32 {
            // Torn payload (writer mid-flush) or corruption: stop here, retry from safe point.
            self.rewind()?;
            return Ok(FrameStep::Eof);
        }
        // Full, validated frame consumed — commit the position.
        self.safe_pos = self.reader.stream_position()?;
        match frame.record_type {
            RecordType::Schema => {
                if let Ok(s) = decode_schema_ipc(&self.payload_buf) {
                    self.schema = Some(s);
                }
                Ok(FrameStep::Schema)
            }
            RecordType::Batch => {
                let batch = decode_batch_ipc(self.schema.as_ref(), &self.payload_buf)?;
                Ok(FrameStep::Batch(WalFrameEvent {
                    lsn: frame.lsn,
                    sequence: frame.sequence,
                    batch,
                }))
            }
            RecordType::Footer => Ok(FrameStep::Footer),
            RecordType::MemTableEnd | RecordType::Unknown(_) => Ok(FrameStep::Schema), // continue
        }
    }

    fn rewind(&mut self) -> Result<()> {
        self.reader.seek(SeekFrom::Start(self.safe_pos))?;
        Ok(())
    }
}

fn scan_segment_tail(path: &Path, expected_memtable_id: u64) -> Result<SegmentTailState> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let header = SegmentHeader::decode(&mut reader)?;
    // Shared hard-linked segments keep the creator id in the header; bind frames track ownership.
    let _ = expected_memtable_id;
    let _ = header;

    let mut batches_written = 0usize;
    let mut next_batch_sequence = 1u64;
    let mut bytes_written = 0usize;
    let mut frames_written = 0u64;
    let mut has_footer = false;
    let mut payload_buf = Vec::new();

    loop {
        let frame = match FrameHeader::decode(&mut reader) {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => {
                tracing::warn!(
                    "scan wal {} stopped early at frame {}: {e}",
                    path.display(),
                    frames_written
                );
                break;
            }
        };

        payload_buf.resize(frame.body_len(), 0);
        if reader.read_exact(&mut payload_buf).is_err() {
            tracing::warn!(
                "scan wal {} truncated at frame {}",
                path.display(),
                frames_written
            );
            break;
        }

        if crc32(&payload_buf) != frame.crc32 {
            tracing::warn!(
                "scan wal {} crc mismatch at frame {}",
                path.display(),
                frames_written
            );
            break;
        }

        frames_written += 1;
        match frame.record_type {
            RecordType::Schema => {
                if frame.sequence != SCHEMA_FRAME_SEQUENCE {
                    return Err(TsdbError::Storage(format!(
                        "invalid WAL schema sequence {}",
                        frame.sequence
                    )));
                }
            }
            RecordType::Batch => {
                if frame.sequence != next_batch_sequence {
                    return Err(TsdbError::Storage(format!(
                        "WAL batch sequence gap in {}: expected {next_batch_sequence}, found {}",
                        path.display(),
                        frame.sequence
                    )));
                }
                next_batch_sequence += 1;
                batches_written += 1;
                if let Ok(batch) = decode_batch_ipc(None, &payload_buf) {
                    bytes_written += batch.get_array_memory_size().max(1);
                }
            }
            RecordType::Footer => {
                has_footer = true;
                break;
            }
            RecordType::MemTableEnd | RecordType::Unknown(_) => {
                next_batch_sequence = frame.sequence.saturating_add(1).max(next_batch_sequence);
            }
        }
    }

    if has_footer {
        let mut extra = [0u8; 1];
        if reader.read(&mut extra).unwrap_or(0) > 0 {
            return Err(TsdbError::Storage(format!(
                "trailing bytes after WAL footer in {}",
                path.display()
            )));
        }
    }

    Ok(SegmentTailState {
        batches_written,
        next_batch_sequence,
        bytes_written,
        frames_written,
        has_footer,
    })
}

/// Encode MemTableEnd payload: closed id + active id after the seal.
pub fn encode_memtable_end_payload(closed_memtable_id: u64, new_memtable_id: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16);
    payload.extend_from_slice(&closed_memtable_id.to_le_bytes());
    payload.extend_from_slice(&new_memtable_id.to_le_bytes());
    payload
}

/// Decode MemTableEnd payload. Legacy 8-byte frames only store the closed id.
pub fn decode_memtable_end_payload(payload: &[u8]) -> (u64, u64) {
    if payload.len() >= 16 {
        let closed = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
        let new = u64::from_le_bytes(payload[8..16].try_into().unwrap_or([0; 8]));
        (closed, new)
    } else if payload.len() >= 8 {
        let closed = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
        (closed, closed.saturating_add(1))
    } else {
        (0, 0)
    }
}

fn write_frame(
    writer: &mut impl Write,
    record_type: RecordType,
    payload_format: u16,
    sequence: u64,
    lsn: u64,
    payload: &[u8],
    max_block_bytes: usize,
) -> Result<()> {
    let block_len = (FRAME_HEADER_SIZE as u32)
        .checked_add(payload.len() as u32)
        .ok_or_else(|| TsdbError::Storage("WAL block too large".into()))?;
    if (block_len as usize) > max_block_bytes {
        return Err(TsdbError::Storage(format!(
            "WAL block {} bytes exceeds max {max_block_bytes} \
             (payload {} + header {FRAME_HEADER_SIZE})",
            block_len,
            payload.len()
        )));
    }
    let header = FrameHeader {
        record_type,
        payload_format,
        block_len,
        crc32: crc32(payload),
        flags: 0,
        sequence,
        lsn,
    };
    header.encode(writer)?;
    writer.write_all(payload)?;
    Ok(())
}

fn encode_schema_ipc(schema: &SchemaRef) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())
            .map_err(|e| TsdbError::Storage(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| TsdbError::Storage(e.to_string()))?;
    }
    Ok(buf)
}

pub(crate) fn decode_schema_ipc(payload: &[u8]) -> Result<SchemaRef> {
    let reader =
        StreamReader::try_new(payload, None).map_err(|e| TsdbError::Storage(e.to_string()))?;
    Ok(reader.schema())
}

fn encode_batch_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
            .map_err(|e| TsdbError::Storage(e.to_string()))?;
        writer
            .write(batch)
            .map_err(|e| TsdbError::Storage(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| TsdbError::Storage(e.to_string()))?;
    }
    Ok(buf)
}

pub(crate) fn decode_batch_ipc(_schema: Option<&SchemaRef>, payload: &[u8]) -> Result<RecordBatch> {
    let mut reader =
        StreamReader::try_new(payload, None).map_err(|e| TsdbError::Storage(e.to_string()))?;
    reader
        .next()
        .transpose()
        .map_err(|e| TsdbError::Storage(e.to_string()))?
        .ok_or_else(|| TsdbError::Storage("empty WAL batch frame".into()))
}

#[inline(always)]
fn map_io(e: std::io::Error) -> TsdbError {
    TsdbError::Storage(e.to_string())
}

/// Hardware CRC32 (SSE4.2 / ARMv8 via crc32fast).
#[inline(always)]
pub(crate) fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

fn current_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch(v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    #[test]
    fn framed_segment_roundtrip_preserves_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_file_path(dir.path());
        let memtable_id = 42;
        let schema = sample_batch(1).schema();
        let mut writer = FramedSegmentWriter::create(path.clone(), memtable_id, schema).unwrap();
        writer.append_batch(&sample_batch(1), 1, false).unwrap();
        writer.append_batch(&sample_batch(2), 2, false).unwrap();
        writer.finish().unwrap();

        let batches = read_segment_batches(&path, memtable_id, false).unwrap();
        assert_eq!(batches.len(), 2);
        let v0 = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let v1 = batches[1]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(v0, 1);
        assert_eq!(v1, 2);
    }

    #[test]
    fn partial_last_segment_without_footer() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_file_path(dir.path());
        let memtable_id = 7;
        let schema = sample_batch(1).schema();
        let mut writer = FramedSegmentWriter::create(path.clone(), memtable_id, schema).unwrap();
        writer.append_batch(&sample_batch(9), 1, false).unwrap();
        drop(writer);

        let batches = read_segment_batches(&path, memtable_id, true).unwrap();
        assert_eq!(batches.len(), 1);
    }

    #[test]
    fn resume_appends_to_same_file_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_file_path(dir.path());
        let memtable_id = 99;
        let schema = sample_batch(1).schema();
        {
            let mut writer =
                FramedSegmentWriter::create(path.clone(), memtable_id, schema).unwrap();
            writer.append_batch(&sample_batch(10), 1, false).unwrap();
            drop(writer);
        }
        {
            let mut writer = FramedSegmentWriter::resume(path.clone(), memtable_id).unwrap();
            writer.append_batch(&sample_batch(11), 2, false).unwrap();
            writer.finish().unwrap();
        }

        let batches = read_segment_batches(&path, memtable_id, false).unwrap();
        assert_eq!(batches.len(), 2);
    }

    #[test]
    fn partial_replay_keeps_batches_before_sequence_gap() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_file_path(dir.path());
        let memtable_id = 11;
        let schema = sample_batch(1).schema();
        let mut writer = FramedSegmentWriter::create(path.clone(), memtable_id, schema).unwrap();
        writer.append_batch(&sample_batch(1), 1, false).unwrap();
        writer.append_batch(&sample_batch(2), 2, false).unwrap();
        drop(writer);

        // Corrupt the file by truncating mid-frame area (simulate bad tail).
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.truncate(bytes.len().saturating_sub(16));
        std::fs::write(&path, bytes).unwrap();

        let batches = read_segment_batches(&path, memtable_id, true).unwrap();
        assert!(!batches.is_empty());
        assert!(batches.len() <= 2);
    }

    #[test]
    fn frames_carry_lsn_and_cursor_streams_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_file_path(dir.path());
        let memtable_id = 7;
        let schema = sample_batch(1).schema();
        let mut writer = FramedSegmentWriter::create(path.clone(), memtable_id, schema).unwrap();
        writer.append_batch(&sample_batch(1), 100, false).unwrap();
        writer.append_batch(&sample_batch(2), 105, false).unwrap();
        writer.finish().unwrap();

        // Streaming cursor yields each batch with its global LSN, in order, then EOF + finished.
        let mut cursor = WalFrameCursor::open(&path, memtable_id).unwrap().unwrap();
        let e1 = cursor.next_batch().unwrap().unwrap();
        assert_eq!((e1.lsn, e1.sequence), (100, 1));
        let e2 = cursor.next_batch().unwrap().unwrap();
        assert_eq!((e2.lsn, e2.sequence), (105, 2));
        assert!(cursor.next_batch().unwrap().is_none());
        assert!(cursor.finished(), "footer seen -> segment closed");

        // The batch reader also surfaces the LSN.
        let framed = read_wal_batches_from_sequence(&path, memtable_id, 1, false).unwrap();
        assert_eq!(
            framed.iter().map(|f| f.lsn).collect::<Vec<_>>(),
            vec![100, 105]
        );
    }

    #[test]
    fn cursor_resumes_live_tail_without_rescan() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_file_path(dir.path());
        let memtable_id = 3;
        let schema = sample_batch(1).schema();
        // In-progress segment (no footer), first frame fsync'd to disk.
        let mut writer = FramedSegmentWriter::create(path.clone(), memtable_id, schema).unwrap();
        writer.append_batch(&sample_batch(1), 10, true).unwrap();

        let mut cursor = WalFrameCursor::open(&path, memtable_id).unwrap().unwrap();
        let e = cursor.next_batch().unwrap().unwrap();
        assert_eq!(e.lsn, 10);
        // At the live tail: no complete frame yet, and the segment is NOT closed.
        assert!(cursor.next_batch().unwrap().is_none());
        assert!(!cursor.finished());

        // Append more; the persistent cursor picks it up from where it left off (no re-scan).
        writer.append_batch(&sample_batch(2), 11, true).unwrap();
        let e2 = cursor.next_batch().unwrap().unwrap();
        assert_eq!(e2.lsn, 11);
    }

    #[test]
    fn read_segment_batches_ignores_requested_memtable_id() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_file_path(dir.path());
        let schema = sample_batch(1).schema();
        let mut writer = FramedSegmentWriter::create(path.clone(), 5, schema).unwrap();
        writer.append_batch(&sample_batch(1), 1, false).unwrap();
        writer.finish().unwrap();

        // Recover trim is by LSN; segment replay returns all batches regardless of mid.
        assert_eq!(read_segment_batches(&path, 6, false).unwrap().len(), 1);
        assert_eq!(read_segment_batches(&path, 5, false).unwrap().len(), 1);
    }

    #[test]
    fn rejects_unsupported_future_format_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_file_path(dir.path());
        let memtable_id = 1;
        let schema = sample_batch(1).schema();
        let mut writer = FramedSegmentWriter::create(path.clone(), memtable_id, schema).unwrap();
        writer.append_batch(&sample_batch(1), 1, false).unwrap();
        writer.finish().unwrap();

        let mut bytes = std::fs::read(&path).unwrap();
        // SegmentHeader.format_version is at offset 8 (after magic).
        bytes[8] = 99;
        bytes[9] = 0;
        std::fs::write(&path, bytes).unwrap();

        let err = read_segment_batches(&path, memtable_id, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unsupported WAL segment format version: 99"));
        assert!(!segment_format_readable(99));
        assert!(segment_format_readable(SEGMENT_FORMAT_VERSION));
    }

    #[test]
    fn skips_extended_segment_header_on_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_file_path(dir.path());
        let memtable_id = 55;
        let schema = sample_batch(1).schema();
        let mut writer = FramedSegmentWriter::create(path.clone(), memtable_id, schema).unwrap();
        writer.append_batch(&sample_batch(42), 7, false).unwrap();
        writer.finish().unwrap();

        let mut bytes = std::fs::read(&path).unwrap();
        // header_size field at offset 10; bump 40 -> 48 and insert 8 reserved extension bytes.
        bytes[10] = 48;
        bytes[11] = 0;
        let insert_at = SEGMENT_HEADER_SIZE;
        bytes.splice(insert_at..insert_at, [0xAA; 8]);
        std::fs::write(&path, bytes).unwrap();

        let batches = read_segment_batches(&path, memtable_id, false).unwrap();
        assert_eq!(batches.len(), 1);
    }

    #[test]
    fn skips_unknown_record_type_and_keeps_later_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_file_path(dir.path());
        let memtable_id = 2;
        let schema = sample_batch(1).schema();
        let mut writer = FramedSegmentWriter::create(path.clone(), memtable_id, schema).unwrap();
        writer.append_batch(&sample_batch(1), 10, false).unwrap();
        writer.append_batch(&sample_batch(2), 20, false).unwrap();
        writer.finish().unwrap();

        let mut bytes = std::fs::read(&path).unwrap();
        // Flip first Batch frame's record_type (after schema) to unknown type 99.
        let type_offset = frame_header_type_offset(&bytes, 1);
        bytes[type_offset] = 99;
        bytes[type_offset + 1] = 0;
        std::fs::write(&path, bytes).unwrap();

        let batches = read_segment_batches(&path, memtable_id, false).unwrap();
        assert_eq!(batches.len(), 1);
        let v = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(v, 2);
    }

    #[test]
    fn skips_unknown_frame_flags_and_keeps_segment_readable() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_file_path(dir.path());
        let memtable_id = 2;
        let schema = sample_batch(1).schema();
        let mut writer = FramedSegmentWriter::create(path.clone(), memtable_id, schema).unwrap();
        writer.append_batch(&sample_batch(1), 1, false).unwrap();
        writer.append_batch(&sample_batch(2), 2, false).unwrap();
        writer.finish().unwrap();

        let mut bytes = std::fs::read(&path).unwrap();
        let flags_offset = frame_header_flags_offset(&bytes, 1);
        bytes[flags_offset] = 0x01;
        std::fs::write(&path, bytes).unwrap();

        // Flagged batch is skipped; later batch + footer remain recoverable.
        let batches = read_segment_batches(&path, memtable_id, false).unwrap();
        assert_eq!(batches.len(), 1);
    }

    #[test]
    fn block_len_lets_reader_jump_to_next_block() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_file_path(dir.path());
        let memtable_id = 7;
        let schema = sample_batch(1).schema();
        let mut writer = FramedSegmentWriter::create(path.clone(), memtable_id, schema).unwrap();
        writer.append_batch(&sample_batch(1), 1, false).unwrap();
        writer.append_batch(&sample_batch(2), 2, false).unwrap();
        writer.finish().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        let mut pos = SEGMENT_HEADER_SIZE;
        let mut block_count = 0usize;
        while pos + 8 <= bytes.len() {
            let magic = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
            assert_eq!(magic, FRAME_MAGIC, "misaligned block at {pos}");
            let block_len =
                u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap()) as usize;
            assert!(
                block_len >= FRAME_HEADER_SIZE,
                "block_len must cover header"
            );
            assert!(
                pos + block_len <= bytes.len(),
                "block_len must not overrun file"
            );
            pos += block_len;
            block_count += 1;
        }
        assert_eq!(pos, bytes.len(), "blocks must tile the file exactly");
        // schema + 2 batches + footer
        assert_eq!(block_count, 4);
    }

    /// Returns the byte offset of `FrameHeader.record_type` for the Nth block (0 = schema).
    fn frame_header_type_offset(bytes: &[u8], frame_index: usize) -> usize {
        let mut pos = SEGMENT_HEADER_SIZE;
        for idx in 0..=frame_index {
            assert!(pos + FRAME_HEADER_SIZE <= bytes.len());
            let block_len =
                u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap()) as usize;
            if idx == frame_index {
                return pos + 8; // after magic + block_len
            }
            pos += block_len;
        }
        panic!("frame index {frame_index} not found");
    }

    /// Returns the byte offset of `FrameHeader.flags` for the Nth block (0 = schema).
    fn frame_header_flags_offset(bytes: &[u8], frame_index: usize) -> usize {
        let mut pos = SEGMENT_HEADER_SIZE;
        for idx in 0..=frame_index {
            assert!(pos + FRAME_HEADER_SIZE <= bytes.len());
            let block_len =
                u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap()) as usize;
            if idx == frame_index {
                return pos + 16; // magic+block_len+type+fmt+crc
            }
            pos += block_len;
        }
        panic!("frame index {frame_index} not found");
    }

    #[test]
    fn write_frame_rejects_block_over_limit() {
        let mut sink = Vec::new();
        let limit = 64;
        let oversized = vec![0u8; limit]; // header + body would exceed
        let err = write_frame(
            &mut sink,
            RecordType::Batch,
            PAYLOAD_FORMAT_ARROW_IPC,
            1,
            1,
            &oversized,
            limit,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("exceeds max"),
            "expected block size error, got {err}"
        );
        assert!(sink.is_empty());
    }

    #[test]
    fn decode_rejects_block_len_over_max() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&FRAME_MAGIC.to_le_bytes());
        let bad_len = (DEFAULT_WAL_BLOCK_MAX_BYTES as u32).saturating_add(1);
        bytes.extend_from_slice(&bad_len.to_le_bytes());
        bytes.extend_from_slice(&2u16.to_le_bytes()); // Batch
        bytes.extend_from_slice(&PAYLOAD_FORMAT_ARROW_IPC.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes()); // crc
        bytes.extend_from_slice(&0u32.to_le_bytes()); // flags
        bytes.extend_from_slice(&1u64.to_le_bytes()); // seq
        bytes.extend_from_slice(&1u64.to_le_bytes()); // lsn
        let err = FrameHeader::decode(bytes.as_slice()).unwrap_err();
        assert!(
            err.to_string().contains("exceeds max"),
            "expected block_len error, got {err}"
        );
    }
}
