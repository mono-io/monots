//! Build Iceberg Catalog backends from `IcebergSinkOptions`.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::{
    FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA,
    S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_glue::{GlueCatalog, GlueCatalogConfig};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

use crate::connector::api::SinkError;
use crate::connector::plugins::object_uri::{is_object_uri, normalize_uri};
use crate::model::{IcebergCatalogType, IcebergSinkOptions};

use super::hadoop::HadoopCatalog;

pub(super) enum IcebergCatalogHandle {
    Hadoop(Arc<HadoopCatalog>),
    Rest(Arc<RestCatalog>),
    Glue(Arc<GlueCatalog>),
}

impl IcebergCatalogHandle {
    pub fn as_catalog(&self) -> &dyn Catalog {
        match self {
            Self::Hadoop(c) => c.as_ref(),
            Self::Rest(c) => c.as_ref(),
            Self::Glue(c) => c.as_ref(),
        }
    }

    pub fn supports_commits(&self) -> bool {
        matches!(self, Self::Hadoop(_) | Self::Rest(_))
    }
}

pub(super) fn table_ident(opts: &IcebergSinkOptions) -> Result<TableIdent, SinkError> {
    let ns = parse_namespace(&opts.namespace)?;
    Ok(TableIdent::new(ns, opts.table.clone()))
}

pub(super) fn parse_namespace(raw: &str) -> Result<NamespaceIdent, SinkError> {
    let parts: Vec<String> = raw
        .split(|c| c == '.' || c == '/')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    NamespaceIdent::from_vec(parts).map_err(|e| SinkError::Fatal(format!("invalid namespace: {e}")))
}

pub(super) async fn build_catalog(
    opts: &IcebergSinkOptions,
) -> Result<IcebergCatalogHandle, SinkError> {
    match opts.catalog_type {
        IcebergCatalogType::Hadoop => {
            let warehouse = opts.warehouse.as_deref().ok_or_else(|| {
                SinkError::Fatal("sink.iceberg.warehouse is required for hadoop".into())
            })?;
            let warehouse = normalize_warehouse(warehouse);
            let file_io = build_file_io(&warehouse, opts)?;
            Ok(IcebergCatalogHandle::Hadoop(Arc::new(HadoopCatalog::new(
                opts.catalog_name.clone(),
                warehouse,
                file_io,
            ))))
        }
        IcebergCatalogType::Rest => {
            let uri = opts
                .uri
                .as_deref()
                .ok_or_else(|| SinkError::Fatal("sink.iceberg.uri is required for rest".into()))?;
            let mut props = file_io_props(opts);
            if let Some(ep) = &opts.endpoint {
                props.insert("s3.endpoint".into(), ep.clone());
            }
            let config = match &opts.warehouse {
                Some(wh) => RestCatalogConfig::builder()
                    .uri(uri.to_string())
                    .warehouse(normalize_warehouse(wh))
                    .props(props)
                    .build(),
                None => RestCatalogConfig::builder()
                    .uri(uri.to_string())
                    .props(props)
                    .build(),
            };
            Ok(IcebergCatalogHandle::Rest(Arc::new(RestCatalog::new(
                config,
            ))))
        }
        IcebergCatalogType::Hive => Err(SinkError::Fatal(
            "iceberg catalog-type=hive is not available in this MonoTS build \
             (hive_metastore thrift client is incompatible with the current rustc); \
             use catalog-type=hadoop or rest"
                .into(),
        )),
        IcebergCatalogType::Glue => {
            let warehouse = opts
                .warehouse
                .as_deref()
                .map(normalize_warehouse)
                .ok_or_else(|| {
                    SinkError::Fatal(
                        "sink.iceberg.warehouse is required for glue catalog writes".into(),
                    )
                })?;
            let props = file_io_props(opts);
            let config = match &opts.uri {
                Some(uri) => GlueCatalogConfig::builder()
                    .warehouse(warehouse)
                    .uri(uri.clone())
                    .props(props)
                    .build(),
                None => GlueCatalogConfig::builder()
                    .warehouse(warehouse)
                    .props(props)
                    .build(),
            };
            let catalog = GlueCatalog::new(config)
                .await
                .map_err(|e| SinkError::Fatal(format!("failed to create Glue catalog: {e}")))?;
            Ok(IcebergCatalogHandle::Glue(Arc::new(catalog)))
        }
    }
}

fn normalize_warehouse(raw: &str) -> String {
    let mut s = normalize_uri(raw);
    if s.starts_with("s3a://") {
        s = format!("s3://{}", s.trim_start_matches("s3a://"));
    }
    s.trim_end_matches('/').to_string()
}

fn file_io_props(opts: &IcebergSinkOptions) -> HashMap<String, String> {
    let mut props = HashMap::new();
    let store = &opts.object_store;
    props.insert(S3_REGION.to_string(), store.region.clone());
    if let Some(ak) = &store.access_key {
        props.insert(S3_ACCESS_KEY_ID.to_string(), ak.clone());
    }
    if let Some(sk) = &store.secret_key {
        props.insert(S3_SECRET_ACCESS_KEY.to_string(), sk.clone());
    }
    if let Some(ep) = &opts.endpoint {
        props.insert(S3_ENDPOINT.to_string(), ep.clone());
    }
    let want_path_style = store.effective_path_style(opts.endpoint.as_deref());
    // iceberg-rust 0.4 bug: truthy `s3.path-style-access` sets `enable_virtual_host_style=true`
    // (inverted). Path-style (MinIO) ⇒ leave unset; virtual-host ⇒ set "true".
    if !want_path_style {
        props.insert(S3_PATH_STYLE_ACCESS.to_string(), "true".into());
    }
    // Skip IMDS / shared AWS config when talking to MinIO or explicit keys.
    props.insert(S3_DISABLE_EC2_METADATA.to_string(), "true".into());
    props.insert(S3_DISABLE_CONFIG_LOAD.to_string(), "true".into());
    props
}

fn build_file_io(warehouse: &str, opts: &IcebergSinkOptions) -> Result<FileIO, SinkError> {
    let builder = if is_object_uri(warehouse) || warehouse.starts_with("s3://") {
        FileIO::from_path(warehouse)
            .map_err(|e| SinkError::Fatal(format!("FileIO from warehouse path: {e}")))?
            .with_props(file_io_props(opts))
    } else {
        FileIOBuilder::new_fs_io().with_props(file_io_props(opts))
    };
    builder
        .build()
        .map_err(|e| SinkError::Fatal(format!("build FileIO: {e}")))
}
