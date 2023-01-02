use std::collections::HashMap;
use std::sync::Arc;

use object_store::aws::AmazonS3;
use object_store::azure::MicrosoftAzure;
use object_store::gcp::GoogleCloudStorage;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::prefix::PrefixObjectStore;
use object_store::{
    ClientOptions, DynObjectStore, Error as ObjectStoreError, Result as ObjectStoreResult,
    RetryConfig,
};
use url::Url;

use crate::config::{aws::S3Config, azure::AzureConfig, google::GoogleConfig};

enum ObjectStoreKind {
    Local,
    InMemory,
    S3,
    Google,
    Azure,
}

impl ObjectStoreKind {
    pub fn parse_url(url: &Url) -> ObjectStoreResult<Self> {
        match url.scheme() {
            "file" => Ok(ObjectStoreKind::Local),
            "memory" => Ok(ObjectStoreKind::InMemory),
            "az" | "abfs" | "abfss" | "azure" | "wasb" | "adl" => Ok(ObjectStoreKind::Azure),
            "s3" | "s3a" => Ok(ObjectStoreKind::S3),
            "gs" => Ok(ObjectStoreKind::Google),
            "https" => {
                let host = url.host_str().unwrap_or_default();
                if host.contains("amazonaws.com") {
                    Ok(ObjectStoreKind::S3)
                } else if host.contains("dfs.core.windows.net")
                    || host.contains("blob.core.windows.net")
                {
                    Ok(ObjectStoreKind::Azure)
                } else {
                    Err(ObjectStoreError::NotImplemented)
                }
            }
            _ => Err(ObjectStoreError::NotImplemented),
        }
    }
}

enum ObjectStoreImpl {
    Local(LocalFileSystem),
    InMemory(InMemory),
    Azrue(MicrosoftAzure),
    S3(AmazonS3),
    Gcp(GoogleCloudStorage),
}

impl ObjectStoreImpl {
    pub fn into_prefix(self, prefix: Path) -> Arc<DynObjectStore> {
        match self {
            ObjectStoreImpl::Local(store) => Arc::new(PrefixObjectStore::new(store, prefix)),
            ObjectStoreImpl::InMemory(store) => Arc::new(PrefixObjectStore::new(store, prefix)),
            ObjectStoreImpl::Azrue(store) => Arc::new(PrefixObjectStore::new(store, prefix)),
            ObjectStoreImpl::S3(store) => Arc::new(PrefixObjectStore::new(store, prefix)),
            ObjectStoreImpl::Gcp(store) => Arc::new(PrefixObjectStore::new(store, prefix)),
        }
    }

    pub fn into_store(self) -> Arc<DynObjectStore> {
        match self {
            ObjectStoreImpl::Local(store) => Arc::new(store),
            ObjectStoreImpl::InMemory(store) => Arc::new(store),
            ObjectStoreImpl::Azrue(store) => Arc::new(store),
            ObjectStoreImpl::S3(store) => Arc::new(store),
            ObjectStoreImpl::Gcp(store) => Arc::new(store),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ObjectStoreBuilder {
    url: String,
    prefix: Option<Path>,
    path_as_prefix: bool,
    options: HashMap<String, String>,
    client_options: Option<ClientOptions>,
    retry_config: Option<RetryConfig>,
}

impl ObjectStoreBuilder {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            prefix: None,
            path_as_prefix: false,
            options: Default::default(),
            client_options: None,
            retry_config: None,
        }
    }

    pub fn with_options(mut self, options: HashMap<String, String>) -> Self {
        self.options = options;
        self
    }

    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    pub fn with_prefix(mut self, prefix: impl Into<Path>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    pub fn with_path_as_prefix(mut self, path_as_prefix: bool) -> Self {
        self.path_as_prefix = path_as_prefix;
        self
    }

    pub fn with_client_options(mut self, options: ClientOptions) -> Self {
        self.client_options = Some(options);
        self
    }

    pub fn with_retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = Some(retry_config);
        self
    }

    pub fn build(mut self) -> ObjectStoreResult<Arc<DynObjectStore>> {
        let url = Url::parse(&self.url).map_err(|err| ObjectStoreError::Generic {
            store: "Generic",
            source: Box::new(err),
        })?;
        let root_store = match ObjectStoreKind::parse_url(&url).unwrap() {
            ObjectStoreKind::Local => ObjectStoreImpl::Local(LocalFileSystem::new()),
            ObjectStoreKind::InMemory => ObjectStoreImpl::InMemory(InMemory::new()),
            ObjectStoreKind::Azure => {
                let mut builder = AzureConfig::get_builder(&self.url, &self.options)?;
                builder = builder
                    .with_client_options(self.client_options.unwrap_or_default())
                    .with_retry(self.retry_config.unwrap_or_default());
                ObjectStoreImpl::Azrue(builder.build()?)
            }
            ObjectStoreKind::S3 => {
                let mut builder = S3Config::get_builder(&self.url, &self.options)?;
                builder = builder
                    .with_client_options(self.client_options.unwrap_or_default())
                    .with_retry(self.retry_config.unwrap_or_default());
                ObjectStoreImpl::S3(builder.build()?)
            }
            ObjectStoreKind::Google => {
                let mut builder = GoogleConfig::get_builder(&self.url, &self.options)?;
                builder = builder
                    .with_client_options(self.client_options.unwrap_or_default())
                    .with_retry(self.retry_config.unwrap_or_default());
                ObjectStoreImpl::Gcp(builder.build()?)
            }
        };

        if self.path_as_prefix && !url.path().is_empty() && self.prefix.is_none() {
            self.prefix = Some(Path::from(url.path()))
        }

        if let Some(prefix) = self.prefix {
            Ok(root_store.into_prefix(prefix))
        } else {
            Ok(root_store.into_store())
        }
    }
}
