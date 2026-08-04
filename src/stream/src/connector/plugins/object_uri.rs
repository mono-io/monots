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

//! Shared URI helpers for Delta / Filesystem sinks (local, `file://`, `s3://`, `s3a://`).

use std::path::PathBuf;
use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::buffered::BufWriter;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::connector::SinkError;
use crate::model::DeltaSinkOptions;

const UPLOAD_CHUNK_BYTES: usize = 8 * 1024 * 1024;

pub fn is_s3_uri(uri: &str) -> bool {
    let u = uri.to_ascii_lowercase();
    u.starts_with("s3://") || u.starts_with("s3a://")
}

pub fn is_unsupported_object_uri(uri: &str) -> bool {
    let u = uri.to_ascii_lowercase();
    u.starts_with("gs://")
        || u.starts_with("gcs://")
        || u.starts_with("abfs://")
        || u.starts_with("abfss://")
}

pub fn is_object_uri(uri: &str) -> bool {
    is_s3_uri(uri) || is_unsupported_object_uri(uri)
}

/// Strip `file://` / `file://localhost` to a native path; leave other schemes intact.
pub fn normalize_uri(raw: &str) -> String {
    let s = raw.trim();
    if let Some(rest) = s.strip_prefix("file://") {
        let path = rest.strip_prefix("localhost").unwrap_or(rest);
        path.to_string()
    } else {
        s.to_string()
    }
}

/// Local staging directory: destination itself for local URIs; temp dir for object URIs.
pub fn staging_root_for(uri: &str, kind: &str) -> PathBuf {
    if is_object_uri(uri) {
        let safe: String = uri
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        std::env::temp_dir()
            .join(format!("monots_{kind}_staging"))
            .join(safe)
    } else {
        PathBuf::from(uri)
    }
}

/// `(bucket, key_prefix)` from `s3://bucket/optional/prefix` (also `s3a://`).
pub fn parse_s3_bucket_prefix(uri: &str) -> Result<(String, String), SinkError> {
    let lower = uri.to_ascii_lowercase();
    let rest = if lower.starts_with("s3://") {
        &uri[5..]
    } else if lower.starts_with("s3a://") {
        &uri[6..]
    } else {
        return Err(SinkError::Fatal(format!("not an s3 URI: {uri}")));
    };

    let rest = rest.trim_start_matches('/');
    if rest.is_empty() {
        return Err(SinkError::Fatal(format!("S3 URI missing bucket: {uri}")));
    }
    let (bucket, prefix) = match rest.split_once('/') {
        Some((b, p)) => (b.to_string(), p.trim_matches('/').to_string()),
        None => (rest.to_string(), String::new()),
    };
    if bucket.is_empty() {
        return Err(SinkError::Fatal(format!("S3 URI missing bucket: {uri}")));
    }
    Ok((bucket, prefix))
}

/// Build an S3-compatible [`ObjectStore`] and the object-key prefix from the URI.
pub fn build_s3_store(
    uri: &str,
    endpoint: Option<&str>,
    options: &DeltaSinkOptions,
) -> Result<(Arc<dyn ObjectStore>, ObjectPath), SinkError> {
    let (bucket, prefix) = parse_s3_bucket_prefix(uri)?;

    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .with_region(&options.region);

    if let Some(ak) = &options.access_key {
        builder = builder.with_access_key_id(ak);
    }
    if let Some(sk) = &options.secret_key {
        builder = builder.with_secret_access_key(sk);
    }
    if let Some(ep) = endpoint.filter(|s| !s.is_empty()) {
        builder = builder.with_endpoint(ep).with_allow_http(true);
    }
    builder = builder.with_virtual_hosted_style_request(!options.effective_path_style(endpoint));

    let store = builder.build().map_err(|e| {
        SinkError::Fatal(format!(
            "failed to build S3 client for {uri} (check credentials / endpoint): {e}"
        ))
    })?;

    let root = if prefix.is_empty() {
        ObjectPath::default()
    } else {
        ObjectPath::from(prefix)
    };
    Ok((Arc::new(store), root))
}

/// Object key for a staged local file relative to `staging_root`, under URI `root` prefix.
pub fn object_key_for_staged(
    root: &ObjectPath,
    staging_root: &std::path::Path,
    file_path: &std::path::Path,
) -> Result<ObjectPath, SinkError> {
    let rel = file_path.strip_prefix(staging_root).map_err(|_| {
        SinkError::Fatal(format!(
            "staged file {} is outside staging root {}",
            file_path.display(),
            staging_root.display()
        ))
    })?;
    let rel = rel.to_string_lossy().replace('\\', "/");
    if rel.is_empty() {
        return Err(SinkError::Fatal("empty relative object key".into()));
    }
    let combined = if root.as_ref().is_empty() {
        rel
    } else {
        format!("{}/{rel}", root.as_ref())
    };
    Ok(ObjectPath::from(combined.as_str()))
}

pub async fn upload_file_chunked(
    store: Arc<dyn ObjectStore>,
    key: ObjectPath,
    file_path: &std::path::Path,
    uri: &str,
) -> Result<(), SinkError> {
    let mut file = fs::File::open(file_path).await.map_err(|e| {
        SinkError::Transient(format!("open {} for upload: {e}", file_path.display()))
    })?;
    let mut writer = BufWriter::with_capacity(store, key.clone(), UPLOAD_CHUNK_BYTES);
    let mut buf = vec![0u8; UPLOAD_CHUNK_BYTES];
    loop {
        let n = file
            .read(&mut buf)
            .await
            .map_err(|e| SinkError::Transient(format!("read {}: {e}", file_path.display())))?;
        if n == 0 {
            break;
        }
        writer.write_all(&buf[..n]).await.map_err(|e| {
            SinkError::Transient(format!(
                "stream upload write failed for {} → {uri}/{key}: {e}",
                file_path.display()
            ))
        })?;
    }
    writer.shutdown().await.map_err(|e| {
        let msg = e.to_string();
        let lower = msg.to_ascii_lowercase();
        if lower.contains("403")
            || lower.contains("forbidden")
            || lower.contains("expired")
            || lower.contains("401")
        {
            SinkError::Transient(format!("auth during upload to {uri}: {msg}"))
        } else {
            SinkError::Transient(format!("stream upload finalize failed for {uri}: {msg}"))
        }
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_s3_uris() {
        assert_eq!(
            parse_s3_bucket_prefix("s3://bucket").unwrap(),
            ("bucket".into(), "".into())
        );
        assert_eq!(
            parse_s3_bucket_prefix("s3a://bucket/export/metrics").unwrap(),
            ("bucket".into(), "export/metrics".into())
        );
        assert_eq!(
            parse_s3_bucket_prefix("s3://bucket/a/b/").unwrap(),
            ("bucket".into(), "a/b".into())
        );
    }

    #[test]
    fn normalizes_file_uri() {
        assert_eq!(normalize_uri("file:///tmp/out"), "/tmp/out");
        assert_eq!(normalize_uri("file://localhost/tmp/out"), "/tmp/out");
        assert_eq!(normalize_uri("s3://b/t"), "s3://b/t");
        assert_eq!(normalize_uri("/data/x"), "/data/x");
    }
}
