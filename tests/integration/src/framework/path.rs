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

use std::path::PathBuf;

/// Resolves project and binary paths for integration tests.
pub struct PathManager;

impl PathManager {
    fn integration_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    pub fn project_root() -> PathBuf {
        Self::integration_dir()
            .parent()
            .and_then(|p| p.parent())
            .expect("project root")
            .to_path_buf()
    }

    pub fn target_dir() -> PathBuf {
        Self::integration_dir().join("target")
    }

    pub fn server_binary() -> Result<PathBuf, String> {
        let root = Self::project_root();
        let name = if cfg!(windows) {
            "monots-server.exe"
        } else {
            "monots-server"
        };

        let triple = Self::host_triple();
        let candidates = [
            root.join("target").join(&triple).join("release").join(name),
            root.join("target").join("release").join(name),
            root.join("target").join("debug").join(name),
        ];

        let found: Vec<PathBuf> = candidates.iter().filter(|p| p.is_file()).cloned().collect();
        if let Some(path) = found
            .iter()
            .max_by_key(|p| p.metadata().and_then(|m| m.modified()).ok())
        {
            return Ok(path.clone());
        }

        Err(format!(
            "monots-server binary not found. Build first with: make build-host\nChecked:\n  - {}\n  - {}\n  - {}",
            candidates[0].display(),
            candidates[1].display(),
            candidates[2].display(),
        ))
    }

    fn host_triple() -> String {
        let arch = std::env::consts::ARCH;
        let os = std::env::consts::OS;
        match os {
            "linux" => format!("{arch}-unknown-linux-gnu"),
            "macos" => format!("{arch}-apple-darwin"),
            "windows" => format!("{arch}-pc-windows-msvc"),
            _ => format!("{arch}-unknown-linux-gnu"),
        }
    }
}
