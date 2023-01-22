use std::collections::HashMap;
use std::sync::Arc;

use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::azure::{MicrosoftAzure, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::prefix::PrefixObjectStore;
use object_store::{
    ClientOptions, DynObjectStore, Error as ObjectStoreError, Result as ObjectStoreResult,
    RetryConfig,
};
use url::Url;

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

    pub fn with_options<I: IntoIterator<Item = (impl Into<String>, impl Into<String>)>>(
        mut self,
        options: I,
    ) -> Self {
        self.options
            .extend(options.into_iter().map(|(k, v)| (k.into(), v.into())));
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
        let maybe_url = Url::parse(&self.url);
        let url =
            match maybe_url {
                Ok(url) => Ok(url),
                Err(url::ParseError::RelativeUrlWithoutBase) => {
                    let abs_path = std::fs::canonicalize(std::path::PathBuf::from(&self.url))
                        .map_err(|err| ObjectStoreError::Generic {
                            store: "Generic",
                            source: Box::new(err),
                        })?;
                    Url::parse(&format!("file://{}", abs_path.to_str().unwrap())).map_err(|err| {
                        ObjectStoreError::Generic {
                            store: "Generic",
                            source: Box::new(err),
                        }
                    })
                }
                Err(err) => Err(ObjectStoreError::Generic {
                    store: "Generic",
                    source: Box::new(err),
                }),
            }?;
        let root_store = match ObjectStoreKind::parse_url(&url)? {
            ObjectStoreKind::Local => ObjectStoreImpl::Local(LocalFileSystem::new()),
            ObjectStoreKind::InMemory => ObjectStoreImpl::InMemory(InMemory::new()),
            ObjectStoreKind::Azure => {
                let store = MicrosoftAzureBuilder::new()
                    .with_url(url.clone())
                    .try_with_options(&self.options)?
                    .with_client_options(self.client_options.clone().unwrap_or_default())
                    .with_retry(self.retry_config.clone().unwrap_or_default())
                    .build()
                    .or_else(|_| {
                        MicrosoftAzureBuilder::from_env()
                            .with_url(url.clone())
                            .try_with_options(&self.options)?
                            .with_client_options(self.client_options.clone().unwrap_or_default())
                            .with_retry(self.retry_config.clone().unwrap_or_default())
                            .build()
                    })?;
                ObjectStoreImpl::Azrue(store)
            }
            ObjectStoreKind::S3 => {
                let store = AmazonS3Builder::new()
                    .with_url(url.clone())
                    .try_with_options(&self.options)?
                    .with_client_options(self.client_options.clone().unwrap_or_default())
                    .with_retry(self.retry_config.clone().unwrap_or_default())
                    .build()
                    .or_else(|_| {
                        AmazonS3Builder::from_env()
                            .with_url(url.clone())
                            .try_with_options(&self.options)?
                            .with_client_options(self.client_options.unwrap_or_default())
                            .with_retry(self.retry_config.unwrap_or_default())
                            .build()
                    })?;
                ObjectStoreImpl::S3(store)
            }
            ObjectStoreKind::Google => {
                let store = GoogleCloudStorageBuilder::new()
                    .with_url(url.clone())
                    .try_with_options(&self.options)?
                    .with_client_options(self.client_options.clone().unwrap_or_default())
                    .with_retry(self.retry_config.clone().unwrap_or_default())
                    .build()
                    .or_else(|_| {
                        GoogleCloudStorageBuilder::from_env()
                            .with_url(url.clone())
                            .try_with_options(&self.options)?
                            .with_client_options(self.client_options.unwrap_or_default())
                            .with_retry(self.retry_config.unwrap_or_default())
                            .build()
                    })?;
                ObjectStoreImpl::Gcp(store)
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
