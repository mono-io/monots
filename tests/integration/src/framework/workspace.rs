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

use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

/// On-disk workspace for one integration test instance.
pub struct InstanceWorkspace {
    pub root_dir: PathBuf,
    pub data_dir: PathBuf,
    pub log_dir: PathBuf,
    pub conf_dir: PathBuf,
    pub config_file: PathBuf,
    pub stdout_file: PathBuf,
    pub stderr_file: PathBuf,
}

impl InstanceWorkspace {
    pub fn new(target_dir: &PathBuf, test_name: &str, port: u16) -> Self {
        let timestamp = chrono_lite_now();
        let unique = &Uuid::new_v4().simple().to_string()[..6];
        let instance_id = format!("{timestamp}-{port}-{unique}");
        let root_dir = target_dir.join(test_name).join(instance_id);
        let data_dir = root_dir.join("data");
        let log_dir = root_dir.join("logs");
        let conf_dir = root_dir.join("conf");
        let config_file = conf_dir.join("config.yaml");
        let stdout_file = log_dir.join("stdout.log");
        let stderr_file = log_dir.join("stderr.log");

        Self {
            root_dir,
            data_dir,
            log_dir,
            conf_dir,
            config_file,
            stdout_file,
            stderr_file,
        }
    }

    pub fn setup(&self) -> Result<(), String> {
        for dir in [&self.data_dir, &self.log_dir, &self.conf_dir] {
            fs::create_dir_all(dir).map_err(|e| format!("mkdir {}: {e}", dir.display()))?;
        }
        // `info` so CI failure dumps contain useful server context (errors alone are too sparse
        // when the process is SIGKILL'd / OOMd).
        let config = r#"logging:
  level: info
  file: false
  console: true
storage:
  # IT machines are often nearly full; disable the free-space write gate.
  disk_min_free_ratio: 0.0
"#;
        fs::write(&self.config_file, config)
            .map_err(|e| format!("write {}: {e}", self.config_file.display()))?;
        Ok(())
    }

    /// Successful tests: remove the whole workspace (data + logs).
    pub fn cleanup(&self) {
        if self.root_dir.exists() {
            let _ = fs::remove_dir_all(&self.root_dir);
        }
    }

    /// Failed tests: leave workspace on disk and stamp a marker for CI artifact upload.
    pub fn mark_failed(&self, reason: &str) {
        let marker = self.root_dir.join("FAILED");
        let body = format!(
            "reason={reason}\nroot={}\nstdout={}\nstderr={}\n",
            self.root_dir.display(),
            self.stdout_file.display(),
            self.stderr_file.display()
        );
        let _ = fs::write(marker, body);
    }
}

fn chrono_lite_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{secs}")
}
