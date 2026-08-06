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

//! Iceberg sink options (`sink.iceberg.*`).
//!
//! Catalog configuration mirrors the Flink Iceberg connector:
//! - `catalog-type`: hive | hadoop | rest | glue
//! - `catalog-name`: Flink-style catalog registration name
//! - `uri`: required for hive / rest
//! - `warehouse`: required for hadoop; optional for others

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use common::{Result, TsdbError};

use super::delta_options::{parse_bool as parse_bool_raw, DeltaSinkOptions};

/// DDL key prefix for Iceberg options.
pub const ICEBERG_OPTION_PREFIX: &str = "sink.iceberg";

/// DDL / property keys owned by the Iceberg sink (catalog + table).
pub const ICEBERG_OPTION_KEYS: &[&str] = &[
    "sink.iceberg.catalog-type",
    "sink.iceberg.catalog-name",
    "sink.iceberg.uri",
    "sink.iceberg.warehouse",
    "sink.iceberg.namespace",
    "sink.iceberg.table",
    "sink.iceberg.create-table-if-not-exists",
    "sink.iceberg.endpoint",
    "sink.iceberg.access.key",
    "sink.iceberg.secret.key",
    "sink.iceberg.region",
    "sink.iceberg.path.style.access",
    "sink.iceberg.connection.maximum",
    "sink.iceberg.connection.timeout",
    "sink.iceberg.attempts.maximum",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergCatalogType {
    Hive,
    Hadoop,
    Rest,
    Glue,
}

impl IcebergCatalogType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hive => "hive",
            Self::Hadoop => "hadoop",
            Self::Rest => "rest",
            Self::Glue => "glue",
        }
    }
}

impl fmt::Display for IcebergCatalogType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for IcebergCatalogType {
    type Err = TsdbError;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "hive" => Ok(Self::Hive),
            "hadoop" => Ok(Self::Hadoop),
            "rest" => Ok(Self::Rest),
            "glue" => Ok(Self::Glue),
            other => Err(TsdbError::Query(format!(
                "invalid sink.iceberg.catalog-type: {other} (supported: hive | hadoop | rest | glue)"
            ))),
        }
    }
}

/// Strongly-typed Iceberg sink configuration (catalog + table + S3 client knobs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergSinkOptions {
    pub catalog_type: IcebergCatalogType,
    pub catalog_name: String,
    pub uri: Option<String>,
    pub warehouse: Option<String>,
    pub namespace: String,
    pub table: String,
    pub create_table_if_not_exists: bool,
    /// Custom S3-compatible endpoint (MinIO / OSS) for `warehouse`.
    pub endpoint: Option<String>,
    /// S3 client knobs (same industrial defaults as Delta).
    pub object_store: DeltaSinkOptions,
}

impl IcebergSinkOptions {
    pub fn from_ddl(options: &HashMap<String, String>) -> Result<Self> {
        let catalog_type = IcebergCatalogType::from_str(&required(options, "catalog-type")?)?;
        let catalog_name = required(options, "catalog-name")?;
        let uri = optional(options, "uri");
        let warehouse = optional(options, "warehouse");

        match catalog_type {
            IcebergCatalogType::Hive | IcebergCatalogType::Rest => {
                if uri.is_none() {
                    return Err(TsdbError::Query(format!(
                        "sink.iceberg.uri is required when catalog-type={}",
                        catalog_type.as_str()
                    )));
                }
            }
            IcebergCatalogType::Hadoop => {
                if warehouse.is_none() {
                    return Err(TsdbError::Query(
                        "sink.iceberg.warehouse is required when catalog-type=hadoop".into(),
                    ));
                }
            }
            IcebergCatalogType::Glue => {
                // Glue uses the AWS SDK; warehouse is strongly recommended for table locations.
            }
        }

        let namespace = required(options, "namespace")?;
        let table = required(options, "table")?;
        let create_table_if_not_exists = optional(options, "create-table-if-not-exists")
            .map(|s| parse_bool_raw(&s))
            .transpose()?
            .unwrap_or(true);
        let endpoint = optional(options, "endpoint");
        let object_store = DeltaSinkOptions::from_ddl_prefixed(ICEBERG_OPTION_PREFIX, options)?;

        Ok(Self {
            catalog_type,
            catalog_name,
            uri,
            warehouse,
            namespace,
            table,
            create_table_if_not_exists,
            endpoint,
            object_store,
        })
    }

    pub fn from_properties(props: &HashMap<String, String>) -> Result<Self> {
        Self::from_ddl(props)
    }

    pub fn to_properties(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert(
            format!("{ICEBERG_OPTION_PREFIX}.catalog-type"),
            self.catalog_type.as_str().to_string(),
        );
        m.insert(
            format!("{ICEBERG_OPTION_PREFIX}.catalog-name"),
            self.catalog_name.clone(),
        );
        if let Some(uri) = &self.uri {
            m.insert(format!("{ICEBERG_OPTION_PREFIX}.uri"), uri.clone());
        }
        if let Some(wh) = &self.warehouse {
            m.insert(format!("{ICEBERG_OPTION_PREFIX}.warehouse"), wh.clone());
        }
        m.insert(
            format!("{ICEBERG_OPTION_PREFIX}.namespace"),
            self.namespace.clone(),
        );
        m.insert(format!("{ICEBERG_OPTION_PREFIX}.table"), self.table.clone());
        m.insert(
            format!("{ICEBERG_OPTION_PREFIX}.create-table-if-not-exists"),
            bool_str(self.create_table_if_not_exists).into(),
        );
        if let Some(ep) = &self.endpoint {
            m.insert(format!("{ICEBERG_OPTION_PREFIX}.endpoint"), ep.clone());
        }
        m.extend(
            self.object_store
                .to_properties_prefixed(ICEBERG_OPTION_PREFIX),
        );
        m
    }

    pub fn ddl_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        pairs.push((
            format!("{ICEBERG_OPTION_PREFIX}.catalog-type"),
            self.catalog_type.as_str().to_string(),
        ));
        pairs.push((
            format!("{ICEBERG_OPTION_PREFIX}.catalog-name"),
            self.catalog_name.clone(),
        ));
        if let Some(uri) = &self.uri {
            pairs.push((format!("{ICEBERG_OPTION_PREFIX}.uri"), uri.clone()));
        }
        if let Some(wh) = &self.warehouse {
            pairs.push((format!("{ICEBERG_OPTION_PREFIX}.warehouse"), wh.clone()));
        }
        pairs.push((
            format!("{ICEBERG_OPTION_PREFIX}.namespace"),
            self.namespace.clone(),
        ));
        pairs.push((format!("{ICEBERG_OPTION_PREFIX}.table"), self.table.clone()));
        pairs.push((
            format!("{ICEBERG_OPTION_PREFIX}.create-table-if-not-exists"),
            bool_str(self.create_table_if_not_exists).into(),
        ));
        if let Some(ep) = &self.endpoint {
            pairs.push((format!("{ICEBERG_OPTION_PREFIX}.endpoint"), ep.clone()));
        }
        pairs.extend(
            self.object_store
                .ddl_pairs_prefixed(ICEBERG_OPTION_PREFIX, self.endpoint.as_deref()),
        );
        pairs
    }

    pub fn table_ident_display(&self) -> String {
        format!("{}.{}", self.namespace, self.table)
    }
}

fn required(options: &HashMap<String, String>, key: &str) -> Result<String> {
    let full = format!("{ICEBERG_OPTION_PREFIX}.{key}");
    options
        .get(&full)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| TsdbError::Query(format!("{full} is required")))
}

fn optional(options: &HashMap<String, String>, key: &str) -> Option<String> {
    let full = format!("{ICEBERG_OPTION_PREFIX}.{key}");
    options
        .get(&full)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn bool_str(v: bool) -> &'static str {
    if v {
        "true"
    } else {
        "false"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_hadoop() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("sink.iceberg.catalog-type".into(), "hadoop".into());
        m.insert("sink.iceberg.catalog-name".into(), "my_cat".into());
        m.insert("sink.iceberg.warehouse".into(), "/tmp/wh".into());
        m.insert("sink.iceberg.namespace".into(), "ns".into());
        m.insert("sink.iceberg.table".into(), "t".into());
        m
    }

    #[test]
    fn hadoop_requires_warehouse() {
        let mut m = base_hadoop();
        m.remove("sink.iceberg.warehouse");
        assert!(IcebergSinkOptions::from_ddl(&m).is_err());
    }

    #[test]
    fn hive_requires_uri() {
        let mut m = base_hadoop();
        m.insert("sink.iceberg.catalog-type".into(), "hive".into());
        assert!(IcebergSinkOptions::from_ddl(&m).is_err());
        m.insert("sink.iceberg.uri".into(), "thrift://localhost:9083".into());
        assert!(IcebergSinkOptions::from_ddl(&m).is_ok());
    }

    #[test]
    fn hadoop_ok_fills_defaults() {
        let o = IcebergSinkOptions::from_ddl(&base_hadoop()).unwrap();
        assert_eq!(o.catalog_type, IcebergCatalogType::Hadoop);
        assert_eq!(o.catalog_name, "my_cat");
        assert!(o.create_table_if_not_exists);
        assert_eq!(o.object_store.region, "us-east-1");
        assert_eq!(o.object_store.connection_maximum, 500);
    }
}
