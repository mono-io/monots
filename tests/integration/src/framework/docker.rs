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

//! Docker Compose helpers for Kafka / MinIO / Iceberg REST integration tests.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use super::path::PathManager;
use super::utils::wait_for_port;

/// Host-advertised Kafka bootstrap (see `docker-compose.yml`).
pub const KAFKA_BOOTSTRAP: &str = "127.0.0.1:19092";
/// MinIO S3 API on the host.
pub const MINIO_ENDPOINT: &str = "http://127.0.0.1:19000";
pub const MINIO_ACCESS_KEY: &str = "minioadmin";
pub const MINIO_SECRET_KEY: &str = "minioadmin";
pub const MINIO_BUCKET: &str = "monots";
/// Iceberg REST Catalog fixture on the host (`iceberg-rest` service).
pub const ICEBERG_REST_URI: &str = "http://127.0.0.1:18181";
/// Warehouse prefix used by the REST fixture (inside [`MINIO_BUCKET`]).
pub const ICEBERG_REST_WAREHOUSE_PREFIX: &str = "iceberg-rest-warehouse";

static COMPOSE_FILE: OnceLock<PathBuf> = OnceLock::new();

fn compose_file() -> &'static Path {
    COMPOSE_FILE.get_or_init(|| {
        PathManager::project_root()
            .join("tests")
            .join("integration")
            .join("docker-compose.yml")
    })
}

fn docker_available() -> bool {
    Command::new("docker")
        .args(["info"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn compose_cmd() -> Result<Command, String> {
    if !docker_available() {
        return Err(
            "Docker is not available; start Docker Desktop / daemon to run Kafka/MinIO ITs".into(),
        );
    }
    let file = compose_file();
    if !file.is_file() {
        return Err(format!("compose file missing: {}", file.display()));
    }
    let mut cmd = Command::new("docker");
    cmd.arg("compose")
        .arg("-f")
        .arg(file)
        .arg("-p")
        .arg("monots-it");
    Ok(cmd)
}

fn compose_run(args: &[&str]) -> Result<(), String> {
    let mut cmd = compose_cmd()?;
    cmd.args(args);
    let out = cmd
        .output()
        .map_err(|e| format!("failed to run docker compose {:?}: {e}", args))?;
    if !out.status.success() {
        return Err(format!(
            "docker compose {:?} failed ({}):\nstdout:\n{}\nstderr:\n{}",
            args,
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(())
}

/// Bring up Kafka + MinIO and wait until host ports accept TCP.
pub async fn ensure_stack_up() -> Result<(), String> {
    compose_run(&["up", "-d", "kafka", "minio"])?;
    let _ = compose_run(&["up", "-d", "minio-init"]);

    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let kafka_ok = wait_for_port(
            KAFKA_BOOTSTRAP,
            Duration::from_secs(2),
            Duration::from_millis(200),
        )
        .await;
        let minio_ok = wait_for_port(
            "127.0.0.1:19000",
            Duration::from_secs(2),
            Duration::from_millis(200),
        )
        .await;
        if kafka_ok && minio_ok {
            ensure_minio_bucket().await?;
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(format!(
                "Kafka/MinIO not ready within 120s (kafka={kafka_ok} minio={minio_ok})"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Bring up Kafka + MinIO + Iceberg REST fixture.
pub async fn ensure_iceberg_stack_up() -> Result<(), String> {
    ensure_stack_up().await?;
    compose_run(&["up", "-d", "iceberg-rest"])?;

    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let rest_ok = wait_for_port(
            "127.0.0.1:18181",
            Duration::from_secs(2),
            Duration::from_millis(200),
        )
        .await;
        if rest_ok {
            // Confirm the catalog HTTP API is live (not just the port).
            let url = format!("{ICEBERG_REST_URI}/v1/config");
            match reqwest::get(&url).await {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) => {
                    if Instant::now() > deadline {
                        return Err(format!(
                            "Iceberg REST /v1/config returned HTTP {}",
                            resp.status()
                        ));
                    }
                }
                Err(e) => {
                    if Instant::now() > deadline {
                        return Err(format!("GET {url} failed: {e}"));
                    }
                }
            }
        }
        if Instant::now() > deadline {
            return Err(format!(
                "Iceberg REST Catalog not ready within 180s at {ICEBERG_REST_URI} \
                 (rest_port={rest_ok})"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn ensure_minio_bucket() -> Result<(), String> {
    let script = format!(
        "mc alias set local http://minio:9000 {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} && \
         mc mb -p local/{MINIO_BUCKET} || true"
    );
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--network",
            "monots-it_default",
            "minio/mc:latest",
            "/bin/sh",
            "-c",
            &script,
        ])
        .output()
        .map_err(|e| format!("docker run minio/mc failed: {e}"))?;
    if !out.status.success() {
        return ensure_minio_bucket_via_put().await.map_err(|e| {
            format!(
                "mc bucket ensure failed ({}): {}\nfallback put also failed: {e}",
                out.status,
                String::from_utf8_lossy(&out.stderr)
            )
        });
    }
    Ok(())
}

async fn ensure_minio_bucket_via_put() -> Result<(), String> {
    use object_store::aws::AmazonS3Builder;
    use object_store::ObjectStore;

    let store = AmazonS3Builder::new()
        .with_endpoint(MINIO_ENDPOINT)
        .with_access_key_id(MINIO_ACCESS_KEY)
        .with_secret_access_key(MINIO_SECRET_KEY)
        .with_bucket_name(MINIO_BUCKET)
        .with_region("us-east-1")
        .with_allow_http(true)
        .build()
        .map_err(|e| format!("minio object_store build failed: {e}"))?;

    let path = object_store::path::Path::from("_monots_it_ready");
    store
        .put(&path, object_store::PutPayload::from_static(b"ok"))
        .await
        .map(|_| ())
        .map_err(|e| format!("failed to touch MinIO bucket {MINIO_BUCKET}: {e}"))
}

/// Tear down the compose project (best-effort).
pub fn stack_down() {
    let _ = compose_run(&["down", "-v", "--remove-orphans"]);
}

/// Fail hard unless Kafka + MinIO compose stack is up (no soft-skip).
pub async fn require_docker_stack() -> Result<(), String> {
    ensure_stack_up().await
}

/// Fail hard unless Iceberg REST Catalog fixture is up (implies MinIO).
pub async fn require_iceberg_rest() -> Result<(), String> {
    ensure_iceberg_stack_up().await
}
