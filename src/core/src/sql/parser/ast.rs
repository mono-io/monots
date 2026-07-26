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

use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStreamStmt {
    pub name: String,
    pub if_not_exists: bool,
    /// WITH-clause properties (`'key' = 'value'`).
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropStreamStmt {
    pub name: String,
    pub delete_checkpoint: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowStreamStmt {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowStreamStatusStmt {
    pub stream_id: String,
}

/// MonoTS stream DDL statements produced by [`super::parse::parse_sql`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonotsStatement {
    CreateStream(CreateStreamStmt),
    DropStream(DropStreamStmt),
    ShowStreams,
    ShowStream(ShowStreamStmt),
    ShowStreamStatus(ShowStreamStatusStmt),
}
