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

//! SQL query layer — pipelined DataFusion executor + LSM TableProvider bridge.

pub mod executor;
pub mod filter;
pub mod lsm_merge;
pub mod lsm_stream;
pub mod mem_exec;
pub mod provider;
pub mod schema_align_exec;

pub use executor::QuerySession;
pub use lsm_merge::{
    build_lsm_scan_plan, LsmDedupeExec, LsmLayer, LsmLayeredScanExec, LsmPriorityMergeExec,
};
pub use mem_exec::MemTableScanExec;
pub use provider::LsmTableProvider;
pub use schema_align_exec::SchemaAlignExec;
