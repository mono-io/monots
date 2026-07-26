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

//! MonoTS SQL parser (FunctionStream-style layout).
//!
//! - [`dialect::MonotsDialect`] — custom sqlparser dialect (cloned from FunctionStream)
//! - [`parse::parse_sql`] — lexical/syntactic parse into [`MonotsStatement`]
//! - [`classify`] — statement classification before execution
//! - [`options`] — `WITH` clause property extraction

pub mod ast;
pub mod classify;
pub mod dialect;
pub mod options;
pub mod parse;

pub use ast::{
    CreateStreamStmt, DropStreamStmt, MonotsStatement, ShowStreamStatusStmt, ShowStreamStmt,
};
pub use classify::{classify, ensure_stream_ddl_only, StreamDdlKind};
pub use dialect::MonotsDialect;
pub use options::sql_options_to_map;
pub use parse::{parse_one, parse_sql};
