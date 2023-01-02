use std::collections::HashMap;
use std::sync::Arc;

use object_store::aws::AmazonS3;
use object_store::azure::MicrosoftAzure;
use object_store::gcp::GoogleCloudStorage;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::prefix::PrefixObjectStore;
use object_store::{ClientOptions, DynObjectStore, Result as ObjectStoreResult};
use url::Url;

use crate::settings::AzureConfig;

pub enum ObjectStoreImpl {
    Local(LocalFileSystem),
    Azrue(MicrosoftAzure),
    S3(AmazonS3),
    Gcp(GoogleCloudStorage),
    InMemory(InMemory),
}

pub struct StorageBuilder {
    url: String,
    prefix: Option<Path>,
    path_as_prefix: bool,
    options: HashMap<String, String>,
    client_options: ClientOptions,
}

impl StorageBuilder {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            prefix: None,
            path_as_prefix: false,
            options: Default::default(),
            client_options: Default::default(),
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

    pub fn build(mut self) -> ObjectStoreResult<Arc<DynObjectStore>> {
        let url = Url::parse(&self.url).unwrap();
        let root_store = match url.scheme() {
            "file" => ObjectStoreImpl::Local(LocalFileSystem::new()),
            "memory" => ObjectStoreImpl::InMemory(InMemory::new()),
            "az" | "abfs" | "abfss" | "azure" | "wasb" | "adl" => {
                let mut builder = AzureConfig::get_builder(&url, &self.options)?;
                builder = builder.with_client_options(self.client_options);
                ObjectStoreImpl::Azrue(builder.build()?)
            }
            "s3" | "s3a" => todo!(),
            "gs" => todo!(),
            "https" => todo!(),
            _ => todo!(),
        };

        if self.path_as_prefix && !url.path().is_empty() && self.prefix.is_none() {
            self.prefix = Some(Path::from(url.path()))
        }

        let store: Arc<DynObjectStore> = if let Some(prefix) = self.prefix {
            match root_store {
                ObjectStoreImpl::Local(store) => Arc::new(PrefixObjectStore::new(store, prefix)),
                ObjectStoreImpl::InMemory(store) => Arc::new(PrefixObjectStore::new(store, prefix)),
                _ => todo!(),
            }
        } else {
            match root_store {
                ObjectStoreImpl::Local(store) => Arc::new(store),
                ObjectStoreImpl::InMemory(store) => Arc::new(store),
                _ => todo!(),
            }
        };
        Ok(store)
    }
}
