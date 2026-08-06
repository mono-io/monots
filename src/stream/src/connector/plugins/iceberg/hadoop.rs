//! Hadoop-style Iceberg Catalog backed by warehouse + `version-hint.text`.
//!
//! Layout (Java HadoopCatalog compatible enough for MonoTS writes):
//! `{warehouse}/{ns}/{table}/metadata/v{N}.metadata.json`
//! `{warehouse}/{ns}/{table}/metadata/version-hint.text` → `N`

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};

pub struct HadoopCatalog {
    warehouse: String,
    file_io: FileIO,
    catalog_name: String,
}

impl Debug for HadoopCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HadoopCatalog")
            .field("warehouse", &self.warehouse)
            .field("catalog_name", &self.catalog_name)
            .finish_non_exhaustive()
    }
}

impl HadoopCatalog {
    pub fn new(catalog_name: String, warehouse: String, file_io: FileIO) -> Self {
        let warehouse = warehouse.trim_end_matches('/').to_string();
        Self {
            warehouse,
            file_io,
            catalog_name,
        }
    }

    fn ns_path(&self, ns: &NamespaceIdent) -> String {
        format!("{}/{}", self.warehouse, ns.as_ref().join("/"))
    }

    fn table_path(&self, table: &TableIdent) -> String {
        format!("{}/{}", self.ns_path(table.namespace()), table.name())
    }

    fn version_hint_path(&self, table: &TableIdent) -> String {
        format!("{}/metadata/version-hint.text", self.table_path(table))
    }

    fn metadata_path(&self, table: &TableIdent, version: i32) -> String {
        format!(
            "{}/metadata/v{version}.metadata.json",
            self.table_path(table)
        )
    }

    fn ns_marker_path(&self, ns: &NamespaceIdent) -> String {
        format!("{}/namespace.properties", self.ns_path(ns))
    }

    async fn read_version(&self, table: &TableIdent) -> Result<Option<i32>> {
        let hint = self.version_hint_path(table);
        if !self.file_io.exists(&hint).await? {
            return Ok(None);
        }
        let bytes = self.file_io.new_input(&hint)?.read().await?;
        let text = String::from_utf8_lossy(&bytes).trim().to_string();
        let version: i32 = text.parse().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("invalid version-hint.text at {hint}: {e}"),
            )
        })?;
        Ok(Some(version))
    }

    async fn write_bytes(&self, path: &str, bytes: Vec<u8>) -> Result<()> {
        self.file_io
            .new_output(path)?
            .write(bytes.into())
            .await
            .map_err(Into::into)
    }

    async fn load_metadata(
        &self,
        table: &TableIdent,
        version: i32,
    ) -> Result<(TableMetadata, String)> {
        let location = self.metadata_path(table, version);
        let bytes = self.file_io.new_input(&location)?.read().await?;
        let metadata = serde_json::from_slice::<TableMetadata>(&bytes).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("failed to parse metadata at {location}: {e}"),
            )
        })?;
        Ok((metadata, location))
    }
}

#[async_trait]
impl Catalog for HadoopCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        // Sink path does not require listing; keep minimal.
        Ok(vec![])
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let marker = self.ns_marker_path(namespace);
        if self.file_io.exists(&marker).await? {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("namespace already exists: {namespace:?}"),
            ));
        }
        let body = serde_json::to_vec(&properties).unwrap_or_else(|_| b"{}".to_vec());
        self.write_bytes(&marker, body).await?;
        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let marker = self.ns_marker_path(namespace);
        if !self.file_io.exists(&marker).await? {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("namespace not found: {namespace:?}"),
            ));
        }
        let bytes = self.file_io.new_input(&marker)?.read().await?;
        let props: HashMap<String, String> = serde_json::from_slice(&bytes).unwrap_or_default();
        Ok(Namespace::with_properties(namespace.clone(), props))
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        Ok(self.file_io.exists(&self.ns_marker_path(namespace)).await?)
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        if !self.namespace_exists(namespace).await? {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("namespace not found: {namespace:?}"),
            ));
        }
        let body = serde_json::to_vec(&properties).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("serialize namespace props: {e}"),
            )
        })?;
        self.write_bytes(&self.ns_marker_path(namespace), body)
            .await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let marker = self.ns_marker_path(namespace);
        if self.file_io.exists(&marker).await? {
            self.file_io.delete(&marker).await?;
        }
        Ok(())
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        Ok(vec![])
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        if !self.namespace_exists(namespace).await? {
            let _ = self.create_namespace(namespace, HashMap::new()).await?;
        }

        let table_name = creation.name.clone();
        let table_ident = TableIdent::new(namespace.clone(), table_name.clone());
        if self.table_exists(&table_ident).await? {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("table already exists: {table_ident:?}"),
            ));
        }

        let location = creation
            .location
            .clone()
            .unwrap_or_else(|| self.table_path(&table_ident));
        let creation = TableCreation {
            location: Some(location.clone()),
            ..creation
        };

        let metadata = TableMetadataBuilder::from_table_creation(creation)?
            .build()?
            .metadata;
        let version = 0;
        let metadata_location = self.metadata_path(&table_ident, version);
        self.write_bytes(
            &metadata_location,
            serde_json::to_vec(&metadata).map_err(|e| {
                Error::new(ErrorKind::DataInvalid, format!("serialize metadata: {e}"))
            })?,
        )
        .await?;
        self.write_bytes(&self.version_hint_path(&table_ident), b"0".to_vec())
            .await?;

        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(table_ident)
            .build()
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        let version = self.read_version(table).await?.ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, format!("table not found: {table:?}"))
        })?;
        let (metadata, metadata_location) = self.load_metadata(table, version).await?;
        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(table.clone())
            .build()
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let hint = self.version_hint_path(table);
        if self.file_io.exists(&hint).await? {
            self.file_io.delete(&hint).await?;
        }
        Ok(())
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        Ok(self.file_io.exists(&self.version_hint_path(table)).await?)
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "HadoopCatalog rename_table is not supported",
        ))
    }

    async fn update_table(&self, mut commit: TableCommit) -> Result<Table> {
        let ident = commit.identifier().clone();
        let current_version = self.read_version(&ident).await?.ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!("table not found for commit: {ident:?}"),
            )
        })?;
        let (metadata, metadata_location) = self.load_metadata(&ident, current_version).await?;

        for requirement in commit.take_requirements() {
            requirement.check(Some(&metadata))?;
        }

        let mut builder = metadata.into_builder(Some(metadata_location));
        for update in commit.take_updates() {
            builder = update.apply(builder)?;
        }
        let new_metadata = builder.build()?.metadata;

        let next_version = current_version + 1;
        let new_location = self.metadata_path(&ident, next_version);
        self.write_bytes(
            &new_location,
            serde_json::to_vec(&new_metadata).map_err(|e| {
                Error::new(ErrorKind::DataInvalid, format!("serialize metadata: {e}"))
            })?,
        )
        .await?;
        self.write_bytes(
            &self.version_hint_path(&ident),
            next_version.to_string().into_bytes(),
        )
        .await?;

        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(new_location)
            .metadata(new_metadata)
            .identifier(ident)
            .build()
    }
}
