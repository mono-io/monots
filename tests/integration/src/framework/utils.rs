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

use std::net::{SocketAddr, TcpListener};
use std::time::{Duration, Instant};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::time::sleep;

pub fn find_free_port(host: &str) -> Result<u16, String> {
    let addr: SocketAddr = format!("{host}:0")
        .parse()
        .map_err(|e| format!("invalid host {host}: {e}"))?;
    let listener = TcpListener::bind(addr).map_err(|e| format!("bind failed: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("local_addr: {e}"))?
        .port();
    Ok(port)
}

pub async fn wait_for_port<A: ToSocketAddrs>(
    addr: A,
    timeout: Duration,
    poll_interval: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if TcpStream::connect(&addr).await.is_ok() {
            return true;
        }
        sleep(poll_interval).await;
    }
    false
}

pub fn read_tail(path: &std::path::Path, max_chars: usize) -> String {
    if !path.is_file() {
        return "<file not found>".into();
    }
    match std::fs::read_to_string(path) {
        Ok(text) => {
            if text.len() > max_chars {
                text[text.len() - max_chars..].to_string()
            } else {
                text
            }
        }
        Err(e) => format!("<failed to read log: {e}>"),
    }
}
