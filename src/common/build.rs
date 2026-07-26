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

//! Build-time metadata for MonoTS (git commit hash).

use std::path::Path;
use std::process::Command;

fn main() {
    let hash = git_short_hash().unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=MONOTS_GIT_HASH={hash}");

    // Rebuild when HEAD moves (best-effort; works for normal checkouts).
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let git_head = manifest_dir.join("../../.git/HEAD");
    if git_head.exists() {
        println!("cargo:rerun-if-changed={}", git_head.display());
        if let Ok(contents) = std::fs::read_to_string(&git_head) {
            if let Some(r) = contents.strip_prefix("ref: ") {
                let ref_path = manifest_dir.join("../../.git").join(r.trim());
                if ref_path.exists() {
                    println!("cargo:rerun-if-changed={}", ref_path.display());
                }
            }
        }
    }
}

fn git_short_hash() -> Option<String> {
    let out = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?;
    let s = s.trim();
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}
