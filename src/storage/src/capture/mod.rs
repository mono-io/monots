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

//! Storage-side capture **notify only**: write-path → Stream [`TableCaptureListener`].
//!
//! File events must be hard-linked inside the sync callback (see [`table_capturer`]).
//! Hard-link is same-filesystem only — no silent copy fallback.
//! Progress / commit / queues live in the Stream crate (`StreamSource`).

pub mod table_capturer;

pub use table_capturer::{
    hard_link_into_pending, RegisteredTableCapturer, TableCaptureHub, TableCapturer,
    DEFAULT_TABLE_CAPTURE_CAPACITY,
};
