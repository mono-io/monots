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

use std::fs::{File, OpenOptions};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::Duration;

use super::workspace::InstanceWorkspace;

pub struct MonotsProcess {
    binary: std::path::PathBuf,
    workspace: InstanceWorkspace,
    child: Option<Child>,
    /// Last observed exit status after stop/kill/wait (if any).
    last_status: Option<ExitStatus>,
}

impl MonotsProcess {
    pub fn new(binary: std::path::PathBuf, workspace: InstanceWorkspace) -> Self {
        Self {
            binary,
            workspace,
            child: None,
            last_status: None,
        }
    }

    pub fn is_running(&mut self) -> bool {
        match self.child.as_mut() {
            Some(child) => child.try_wait().ok().flatten().is_none(),
            None => false,
        }
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(|c| c.id())
    }

    pub fn last_status(&self) -> Option<ExitStatus> {
        self.last_status
    }

    pub fn start(
        &mut self,
        listen: &str,
        username: &str,
        password: &str,
        memtable_max_bytes: Option<usize>,
        global_memory_limit_bytes: Option<usize>,
        extra_env: &[(String, String)],
    ) -> Result<(), String> {
        if self.is_running() {
            return Err("process already running".into());
        }
        if !self.binary.is_file() {
            return Err(format!("binary not found: {}", self.binary.display()));
        }

        self.last_status = None;
        let stdout = open_log(&self.workspace.stdout_file)?;
        let stderr = open_log(&self.workspace.stderr_file)?;

        let mut cmd = Command::new(&self.binary);
        cmd.arg("--config")
            .arg(&self.workspace.config_file)
            .arg("--listen")
            .arg(listen)
            .arg("--data-dir")
            .arg(&self.workspace.data_dir)
            .arg("--username")
            .arg(username)
            .arg("--password")
            .arg(password);
        if let Some(v) = memtable_max_bytes {
            cmd.arg("--memtable-max-bytes").arg(v.to_string());
        }
        if let Some(v) = global_memory_limit_bytes {
            cmd.arg("--global-memory-limit-bytes").arg(v.to_string());
        }
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        cmd.current_dir(&self.workspace.root_dir)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));

        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            unsafe {
                cmd.pre_exec(|| {
                    libc::setsid();
                    Ok(())
                });
            }
        }

        let child = cmd
            .spawn()
            .map_err(|e| format!("spawn {}: {e}", self.binary.display()))?;
        self.child = Some(child);
        Ok(())
    }

    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let pid = child.id();
            #[cfg(unix)]
            {
                let pgid = pid as i32;
                unsafe {
                    libc::killpg(pgid, libc::SIGTERM);
                }
            }
            #[cfg(not(unix))]
            {
                let _ = child.kill();
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        self.last_status = Some(status);
                        break;
                    }
                    Ok(None) => {
                        if std::time::Instant::now() >= deadline {
                            #[cfg(unix)]
                            unsafe {
                                libc::killpg(pid as i32, libc::SIGKILL);
                            }
                            #[cfg(not(unix))]
                            let _ = child.kill();
                            if let Ok(status) = child.wait() {
                                self.last_status = Some(status);
                            }
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    Err(_) => break,
                }
            }
        }
    }

    pub fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            #[cfg(unix)]
            {
                let pid = child.id() as i32;
                unsafe {
                    libc::killpg(pid, libc::SIGKILL);
                }
            }
            #[cfg(not(unix))]
            {
                let _ = child.kill();
            }
            if let Ok(status) = child.wait() {
                self.last_status = Some(status);
            }
        }
    }

    /// Non-destructive poll: if the child has exited, record status and clear the handle.
    pub fn refresh_status(&mut self) {
        if let Some(child) = self.child.as_mut() {
            match child.try_wait() {
                Ok(Some(status)) => {
                    self.last_status = Some(status);
                    self.child = None;
                }
                Ok(None) | Err(_) => {}
            }
        }
    }
}

fn open_log(path: &Path) -> Result<File, String> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|e| format!("open {}: {e}", path.display()))
}
