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

//! Set the OS-visible process title (shown in Activity Monitor, `ps`, etc.).

/// Default display name for MonoTS server and long-running soak workloads.
pub const DEFAULT_PROCESS_NAME: &str = "monots";

/// Set the process title visible to system process managers.
pub fn set_process_name(name: &str) {
    proctitle::set_title(name);
}
