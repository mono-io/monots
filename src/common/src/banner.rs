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

//! Startup banner shared by `monots-server` and `monots` CLI.

/// Soft release channel label appended to `CARGO_PKG_VERSION`.
pub const RELEASE_CHANNEL: &str = "alpha";

const LOGO: &str = r#"
███╗   ███╗ ██████╗ ███╗   ██╗ ██████╗ ████████╗ ███████╗
████╗ ████║██╔═══██╗████╗  ██║██╔═══██╗╚══██╔══╝ ██╔════╝
██╔████╔██║██║   ██║██╔██╗ ██║██║   ██║   ██║    ███████╗
██║╚██╔╝██║██║   ██║██║╚██╗██║██║   ██║   ██║    ╚════██║
██║ ╚═╝ ██║╚██████╔╝██║ ╚████║╚██████╔╝   ██║    ███████║
╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝    ╚═╝    ╚══════╝"#;

/// Package version with channel, e.g. `v0.1.0-alpha`.
pub fn version_label() -> String {
    format!("v{}-{}", env!("CARGO_PKG_VERSION"), RELEASE_CHANNEL)
}

/// Short git commit from build.rs (`unknown` when git is unavailable).
pub fn git_hash() -> &'static str {
    option_env!("MONOTS_GIT_HASH").unwrap_or("unknown")
}

/// Print the MonoTS ASCII banner to stdout.
pub fn print_banner() {
    println!("{LOGO}");
    println!(
        " :: The Arrow-Native Streaming TSDB ::       {}  {}",
        version_label(),
        git_hash()
    );
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_and_git_are_populated() {
        let v = version_label();
        assert!(v.starts_with('v'), "{v}");
        assert!(v.contains(RELEASE_CHANNEL), "{v}");
        let h = git_hash();
        assert!(!h.is_empty());
        assert_ne!(h, "unknown", "expected real git hash in workspace builds");
    }
}
