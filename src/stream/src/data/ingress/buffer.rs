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

//! In-memory capture buffer: LSN-ordered state machine for CDC events.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use common::LsnRange;

use crate::model::event::DataEvent;

#[derive(Debug, Default)]
pub struct FlushDegradeResult {
    pub dropped_inserts: usize,
    pub dropped_watermarks: usize,
}

#[derive(Debug, Default)]
pub struct CompactGcResult {
    pub gc_paths: Vec<PathBuf>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum QType {
    Compact,
    Flush,
    Insert,
}

#[derive(Debug, Default)]
pub struct CaptureBuffer {
    insert_q: VecDeque<DataEvent>,
    flush_q: VecDeque<DataEvent>,
    compact_q: VecDeque<DataEvent>,
}

impl CaptureBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len_inserts(&self) -> usize {
        self.insert_q.len()
    }

    #[inline]
    pub fn len_flushes(&self) -> usize {
        self.flush_q.len()
    }

    #[inline]
    pub fn len_compacts(&self) -> usize {
        self.compact_q.len()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.insert_q.len() + self.flush_q.len() + self.compact_q.len()
    }

    #[inline]
    pub fn push_insert(&mut self, event: DataEvent) {
        Self::insert_or_merge(&mut self.insert_q, event);
    }

    #[inline]
    pub fn push_watermark(&mut self, event: DataEvent) {
        Self::insert_sorted(&mut self.insert_q, event);
    }

    pub fn push_flush(&mut self, event: DataEvent) -> FlushDegradeResult {
        let result = self.drop_covered_inserts(event.lsn());
        Self::insert_sorted(&mut self.flush_q, event);
        result
    }

    /// Replace covered Flush/Compact files; discard orphan Compact with no victims.
    pub fn push_compact(&mut self, event: DataEvent) -> CompactGcResult {
        let compact_lsn = event.lsn();
        let new_path = match &event {
            DataEvent::FlushFile { file_path, .. } => PathBuf::from(file_path.as_ref()),
            _ => return CompactGcResult::default(),
        };

        let mut gc_paths = Self::drop_covered_files(&mut self.flush_q, compact_lsn);
        gc_paths.append(&mut Self::drop_covered_files(
            &mut self.compact_q,
            compact_lsn,
        ));

        if gc_paths.is_empty() {
            return CompactGcResult {
                gc_paths: vec![new_path],
            };
        }

        Self::insert_sorted(&mut self.compact_q, event);
        CompactGcResult { gc_paths }
    }

    pub fn requeue_front(&mut self, event: DataEvent, compact_dir: &Path) {
        match event {
            DataEvent::Insert { .. } => {
                self.insert_q.push_front(event);
                Self::coalesce_at(&mut self.insert_q, 0);
            }
            DataEvent::Watermark { .. } => {
                self.insert_q.push_front(event);
            }
            DataEvent::FlushFile { ref file_path, .. } => {
                if Path::new(file_path.as_ref()).starts_with(compact_dir)
                    || file_path.contains("compact")
                {
                    self.compact_q.push_front(event);
                } else {
                    self.flush_q.push_front(event);
                }
            }
        }
    }

    /// Allocation-free head pick. Priority: Compact(0) > Flush(1) > Insert(2) > Watermark(3).
    fn find_best_head(&self) -> Option<QType> {
        let mut best: Option<((u64, u8), QType)> = None;

        let mut check = |q_type: QType, ev: Option<&DataEvent>| {
            if let Some(e) = ev {
                let prio = match q_type {
                    QType::Compact => 0,
                    QType::Flush => 1,
                    QType::Insert => {
                        if matches!(e, DataEvent::Watermark { .. }) {
                            3
                        } else {
                            2
                        }
                    }
                };
                let key = (e.lsn().base_lsn, prio);
                if best.map_or(true, |(b_key, _)| key < b_key) {
                    best = Some((key, q_type));
                }
            }
        };

        check(QType::Compact, self.compact_q.front());
        check(QType::Flush, self.flush_q.front());
        check(QType::Insert, self.insert_q.front());

        best.map(|(_, q)| q)
    }

    pub fn peek_head_lsn(&self) -> Option<u64> {
        self.peek_next().map(|e| e.lsn().base_lsn)
    }

    pub fn peek_heads(&self) -> (Option<u64>, Option<u64>) {
        let i_lsn = self.insert_q.front().map(|e| e.lsn().base_lsn);
        let f_lsn = self.flush_q.front().map(|e| e.lsn().base_lsn);
        let c_lsn = self.compact_q.front().map(|e| e.lsn().base_lsn);
        (i_lsn, [f_lsn, c_lsn].into_iter().flatten().min())
    }

    pub fn peek_next(&self) -> Option<&DataEvent> {
        match self.find_best_head()? {
            QType::Compact => self.compact_q.front(),
            QType::Flush => self.flush_q.front(),
            QType::Insert => self.insert_q.front(),
        }
    }

    pub fn pop_next(&mut self) -> Option<DataEvent> {
        match self.find_best_head()? {
            QType::Compact => self.compact_q.pop_front(),
            QType::Flush => self.flush_q.pop_front(),
            QType::Insert => self.insert_q.pop_front(),
        }
    }

    #[inline]
    pub fn pop_flush(&mut self) -> Option<DataEvent> {
        self.flush_q.pop_front()
    }

    #[inline]
    pub fn pop_compact(&mut self) -> Option<DataEvent> {
        self.compact_q.pop_front()
    }

    pub fn replace_file_queues(
        &mut self,
        flush_events: Vec<DataEvent>,
        compact_events: Vec<DataEvent>,
    ) {
        self.flush_q = flush_events.into();
        self.compact_q = compact_events.into();
    }

    #[inline]
    fn is_fully_covered(inner: LsnRange, cover: LsnRange) -> bool {
        inner.base_lsn >= cover.base_lsn && inner.max_lsn <= cover.max_lsn
    }

    fn drop_covered_inserts(&mut self, cover: LsnRange) -> FlushDegradeResult {
        let mut result = FlushDegradeResult::default();
        self.insert_q.retain(|e| {
            if !Self::is_fully_covered(e.lsn(), cover) {
                return true;
            }
            match e {
                DataEvent::Insert { .. } => result.dropped_inserts += 1,
                DataEvent::Watermark { .. } => result.dropped_watermarks += 1,
                DataEvent::FlushFile { .. } => {}
            }
            false
        });
        result
    }

    fn drop_covered_files(q: &mut VecDeque<DataEvent>, cover: LsnRange) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        q.retain(|e| {
            if Self::is_fully_covered(e.lsn(), cover) {
                if let DataEvent::FlushFile { file_path, .. } = e {
                    paths.push(PathBuf::from(file_path.as_ref()));
                }
                false
            } else {
                true
            }
        });
        paths
    }

    fn insert_sorted(q: &mut VecDeque<DataEvent>, event: DataEvent) {
        let lsn = event.lsn().base_lsn;
        if q.back().map_or(true, |e| lsn >= e.lsn().base_lsn) {
            q.push_back(event);
        } else {
            let pos = q.partition_point(|e| e.lsn().base_lsn <= lsn);
            q.insert(pos, event);
        }
    }

    fn insert_or_merge(q: &mut VecDeque<DataEvent>, event: DataEvent) {
        let DataEvent::Insert { lsn, arrow } = event else {
            return;
        };

        if q.is_empty() {
            q.push_back(DataEvent::Insert { lsn, arrow });
            return;
        }

        if lsn.base_lsn >= q.back().unwrap().lsn().base_lsn {
            let back = q.pop_back().unwrap();
            match Self::try_merge(back, DataEvent::Insert { lsn, arrow }) {
                Ok(merged) => q.push_back(merged),
                Err((left, right)) => {
                    q.push_back(left);
                    q.push_back(right);
                }
            }
        } else {
            let pos = q.partition_point(|e| e.lsn().base_lsn <= lsn.base_lsn);
            q.insert(pos, DataEvent::Insert { lsn, arrow });
            Self::coalesce_at(q, pos.saturating_sub(1));
        }
    }

    fn coalesce_at(q: &mut VecDeque<DataEvent>, idx: usize) {
        while idx + 1 < q.len() {
            let right = q.remove(idx + 1).unwrap();
            let left = q.remove(idx).unwrap();
            match Self::try_merge(left, right) {
                Ok(merged) => {
                    q.insert(idx, merged);
                }
                Err((left, right)) => {
                    q.insert(idx, left);
                    q.insert(idx + 1, right);
                    break;
                }
            }
        }
    }

    #[inline]
    fn is_contiguous(left: LsnRange, right: LsnRange) -> bool {
        left.max_lsn.saturating_add(1) == right.base_lsn
    }

    fn try_merge(left: DataEvent, right: DataEvent) -> Result<DataEvent, (DataEvent, DataEvent)> {
        match (left, right) {
            (
                DataEvent::Insert {
                    lsn: l_lsn,
                    arrow: l_arrow,
                },
                DataEvent::Insert {
                    lsn: r_lsn,
                    arrow: r_arrow,
                },
            ) if Self::is_contiguous(l_lsn, r_lsn)
                && l_arrow.is_resident() == r_arrow.is_resident() =>
            {
                match l_arrow.merge_same_kind(r_arrow) {
                    Some(arrow) => Ok(DataEvent::Insert {
                        lsn: LsnRange::new(l_lsn.base_lsn, r_lsn.max_lsn),
                        arrow,
                    }),
                    None => unreachable!("same-kind InsertArrow merge"),
                }
            }
            (left, right) => Err((left, right)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(Int64Array::from(vec![1_i64])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn flush_degrades_covered_inserts() {
        let mut buf = CaptureBuffer::new();
        buf.push_insert(DataEvent::insert(LsnRange::new(1, 3), vec![batch()]));
        buf.push_insert(DataEvent::insert(LsnRange::new(10, 11), vec![batch()]));
        let result = buf.push_flush(DataEvent::FlushFile {
            lsn: LsnRange::new(1, 5),
            file_path: "/pending/flush/f.parquet".into(),
            rows: 3,
        });
        assert_eq!(result.dropped_inserts, 1);
        assert_eq!(result.dropped_watermarks, 0);
        assert_eq!(buf.len_flushes(), 1);
        assert_eq!(buf.len_inserts(), 1);
        assert_eq!(buf.pop_next().unwrap().lsn().base_lsn, 1);
        assert_eq!(buf.pop_next().unwrap().lsn().base_lsn, 10);
    }

    #[test]
    fn flush_also_drops_covered_watermarks() {
        let mut buf = CaptureBuffer::new();
        buf.push_insert(DataEvent::insert(LsnRange::new(1, 3), vec![batch()]));
        buf.push_watermark(DataEvent::Watermark { end_lsn: 3 });
        buf.push_watermark(DataEvent::Watermark { end_lsn: 5 });
        buf.push_insert(DataEvent::insert(LsnRange::new(10, 11), vec![batch()]));
        buf.push_watermark(DataEvent::Watermark { end_lsn: 11 });

        let result = buf.push_flush(DataEvent::FlushFile {
            lsn: LsnRange::new(1, 5),
            file_path: "/pending/flush/f.parquet".into(),
            rows: 3,
        });

        assert_eq!(result.dropped_inserts, 1);
        assert_eq!(
            result.dropped_watermarks, 2,
            "watermarks at 3 and 5 are covered"
        );
        assert_eq!(buf.len_inserts(), 2);
        assert!(matches!(
            buf.pop_next().unwrap(),
            DataEvent::FlushFile { .. }
        ));
        assert!(matches!(buf.pop_next().unwrap(), DataEvent::Insert { .. }));
        assert!(matches!(
            buf.pop_next().unwrap(),
            DataEvent::Watermark { end_lsn: 11 }
        ));
    }

    #[test]
    fn compact_degrades_flush_keeps_insert() {
        let mut buf = CaptureBuffer::new();
        buf.push_flush(DataEvent::FlushFile {
            lsn: LsnRange::new(1, 2),
            file_path: "/a.parquet".into(),
            rows: 1,
        });
        buf.push_flush(DataEvent::FlushFile {
            lsn: LsnRange::new(3, 4),
            file_path: "/b.parquet".into(),
            rows: 1,
        });
        buf.push_insert(DataEvent::insert(LsnRange::new(2, 2), vec![batch()]));
        let result = buf.push_compact(DataEvent::FlushFile {
            lsn: LsnRange::new(1, 4),
            file_path: "/c.parquet".into(),
            rows: 2,
        });
        assert_eq!(result.gc_paths.len(), 2);
        assert_eq!(buf.len_compacts(), 1);
        assert_eq!(buf.len_flushes(), 0);
        assert_eq!(buf.len_inserts(), 1);
    }

    #[test]
    fn compact_degrades_both_flush_and_old_compacts() {
        let mut buf = CaptureBuffer::new();
        buf.push_flush(DataEvent::FlushFile {
            lsn: LsnRange::new(1, 2),
            file_path: "/pending/flush/f1.parquet".into(),
            rows: 10,
        });
        buf.push_flush(DataEvent::FlushFile {
            lsn: LsnRange::new(3, 5),
            file_path: "/pending/flush/f2.parquet".into(),
            rows: 20,
        });
        let seed = buf.push_compact(DataEvent::FlushFile {
            lsn: LsnRange::new(3, 5),
            file_path: "/pending/compact/c1.parquet".into(),
            rows: 20,
        });
        assert_eq!(seed.gc_paths.len(), 1);
        assert_eq!(buf.len_flushes(), 1);
        assert_eq!(buf.len_compacts(), 1);

        let result = buf.push_compact(DataEvent::FlushFile {
            lsn: LsnRange::new(1, 8),
            file_path: "/pending/compact/c2_huge.parquet".into(),
            rows: 50,
        });
        assert_eq!(result.gc_paths.len(), 2);
        assert_eq!(
            result.gc_paths[0].to_string_lossy(),
            "/pending/flush/f1.parquet"
        );
        assert_eq!(
            result.gc_paths[1].to_string_lossy(),
            "/pending/compact/c1.parquet"
        );
        assert_eq!(buf.len_flushes(), 0);
        assert_eq!(buf.len_compacts(), 1);
        assert_eq!(buf.pop_next().unwrap().lsn().max_lsn, 8);
    }

    #[test]
    fn compact_without_replacement_discards_new_file() {
        let mut buf = CaptureBuffer::new();
        buf.push_flush(DataEvent::FlushFile {
            lsn: LsnRange::new(10, 12),
            file_path: "/pending/flush/alive.parquet".into(),
            rows: 1,
        });
        let result = buf.push_compact(DataEvent::FlushFile {
            lsn: LsnRange::new(1, 8),
            file_path: "/pending/compact/orphan.parquet".into(),
            rows: 50,
        });
        assert_eq!(result.gc_paths.len(), 1);
        assert_eq!(
            result.gc_paths[0].to_string_lossy(),
            "/pending/compact/orphan.parquet"
        );
        assert_eq!(buf.len_flushes(), 1);
        assert_eq!(buf.len_compacts(), 0);
    }

    #[test]
    fn compact_covering_only_insert_discards_new_file() {
        let mut buf = CaptureBuffer::new();
        buf.push_insert(DataEvent::insert(LsnRange::new(1, 4), vec![batch()]));
        let result = buf.push_compact(DataEvent::FlushFile {
            lsn: LsnRange::new(1, 4),
            file_path: "/pending/compact/c.parquet".into(),
            rows: 1,
        });
        assert_eq!(result.gc_paths.len(), 1);
        assert_eq!(
            result.gc_paths[0].to_string_lossy(),
            "/pending/compact/c.parquet"
        );
        assert_eq!(buf.len_inserts(), 1);
        assert_eq!(buf.len_compacts(), 0);
    }

    #[test]
    fn contiguous_inserts_merge() {
        let mut buf = CaptureBuffer::new();
        buf.push_insert(DataEvent::insert(LsnRange::new(1, 1), vec![batch()]));
        buf.push_insert(DataEvent::insert(LsnRange::new(2, 2), vec![batch()]));
        assert_eq!(buf.len_inserts(), 1);
        match buf.pop_next().unwrap() {
            DataEvent::Insert { lsn, arrow } => {
                assert_eq!(lsn.base_lsn, 1);
                assert_eq!(lsn.max_lsn, 2);
                assert_eq!(arrow.batches().len(), 2);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn watermark_blocks_merge() {
        let mut buf = CaptureBuffer::new();
        buf.push_insert(DataEvent::insert(LsnRange::new(1, 2), vec![batch()]));
        buf.push_watermark(DataEvent::Watermark { end_lsn: 2 });
        buf.push_insert(DataEvent::insert(LsnRange::new(3, 4), vec![batch()]));
        assert_eq!(buf.len_inserts(), 3);
    }

    #[test]
    fn mixed_arrow_and_wal_only_must_not_merge() {
        let mut buf = CaptureBuffer::new();
        buf.push_insert(DataEvent::insert(LsnRange::new(1, 1), vec![batch()]));
        buf.push_insert(DataEvent::insert_deferred(LsnRange::new(2, 2)));
        assert_eq!(buf.len_inserts(), 2);
    }

    #[test]
    fn pop_prefers_insert_over_watermark_at_same_lsn() {
        let mut buf = CaptureBuffer::new();
        buf.push_insert(DataEvent::insert(LsnRange::single(5), vec![batch()]));
        buf.push_watermark(DataEvent::Watermark { end_lsn: 5 });
        assert!(matches!(buf.pop_next().unwrap(), DataEvent::Insert { .. }));
        assert!(matches!(
            buf.pop_next().unwrap(),
            DataEvent::Watermark { .. }
        ));
    }

    #[test]
    fn same_lsn_prefers_compact_over_flush() {
        let mut buf = CaptureBuffer::new();
        buf.push_flush(DataEvent::FlushFile {
            lsn: LsnRange::single(5),
            file_path: "/f.parquet".into(),
            rows: 1,
        });
        let _ = buf.push_compact(DataEvent::FlushFile {
            lsn: LsnRange::single(5),
            file_path: "/c.parquet".into(),
            rows: 1,
        });
        assert_eq!(buf.len_flushes(), 0);
        assert_eq!(buf.len_compacts(), 1);
        match buf.pop_next().unwrap() {
            DataEvent::FlushFile { file_path, .. } => {
                assert!(file_path.contains("c.parquet"));
            }
            other => panic!("{other:?}"),
        }
    }
}
