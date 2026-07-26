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

//! Stream metadata persistence (Protobuf on disk).

pub mod codec;
pub mod store;

pub use codec::{
    decode_stream_checkpoint, decode_stream_def, decode_versioned_checkpoint,
    decode_versioned_stream_def, encode_stream_checkpoint, encode_stream_def,
    encode_versioned_checkpoint, encode_versioned_stream_def, Versioned,
    STREAM_SCHEMA_MIN_SUPPORTED, STREAM_SCHEMA_VERSION,
};
pub use store::StreamStore;
