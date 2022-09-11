use std::collections::HashMap;
use std::sync::Arc;

// use object_store::aws::AmazonS3Builder;
// use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{DynObjectStore, Error as ObjectStoreError, Result as ObjectStoreResult};
use url::Url;

use crate::settings::AzureConfig;

#[derive(Debug, PartialEq, Eq)]
/// Well known storage services
pub enum StorageService {
    /// Local filesystem storage
    Local,
    /// S3 compliant service
    S3,
    /// Azure blob service
    Azure,
    /// Google cloud storage
    Gcs,
    /// In-memory store
    InMemory,
    /// Unrecognized service
    Unknown,
}

/// A parsed URL identifying a storage location
/// for more information on the supported expressions
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageUrl {
    /// A URL that identifies a file or directory to list files from
    pub(crate) url: Url,
    /// The path prefix
    pub(crate) prefix: Path,
}

impl StorageUrl {
    /// Parse a provided string as a `StorageUrl`
    ///
    /// # Paths without a Scheme
    ///
    /// If no scheme is provided, or the string is an absolute filesystem path
    /// as determined [`std::path::Path::is_absolute`], the string will be
    /// interpreted as a path on the local filesystem using the operating
    /// system's standard path delimiter, i.e. `\` on Windows, `/` on Unix.
    ///
    /// Otherwise, the path will be resolved to an absolute path, returning
    /// an error if it does not exist, and converted to a [file URI]
    ///
    /// If you wish to specify a path that does not exist on the local
    /// machine you must provide it as a fully-qualified [file URI]
    /// e.g. `file:///myfile.txt`
    ///
    /// [file URI]: https://en.wikipedia.org/wiki/File_URI_scheme
    ///
    /// # Well-known formats
    ///
    /// The lists below enumerates some well known uris, that are understood by the
    /// parse function. We parse uris to refer to a specific storage location, which
    /// is accessed using the internal storage backends.
    ///
    /// ## Azure
    ///
    /// URIs according to <https://github.com/fsspec/adlfs#filesystem-interface-to-azure-datalake-gen1-and-gen2-storage>:
    ///
    ///   * az://<container>/<path>
    ///   * adl://<container>/<path>
    ///   * abfs(s)://<container>/<path>
    ///
    /// URIs according to <https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri>:
    ///
    ///   * abfs(s)://<file_system>@<account_name>.dfs.core.windows.net/<path>
    ///
    /// and a custom one
    ///
    ///   * azure://<container>/<path>
    ///
    /// ## S3
    ///   * s3://<bucket>/<path>
    ///   * s3a://<bucket>/<path>
    ///
    /// ## GCS
    ///   * gs://<bucket>/<path>
    pub fn parse(s: impl AsRef<str>) -> ObjectStoreResult<Self> {
        let s = s.as_ref();

        // This is necessary to handle the case of a path starting with a drive letter
        if std::path::Path::new(s).is_absolute() {
            return Self::parse_path(s);
        }

        match Url::parse(s) {
            Ok(url) => Ok(Self::new(url)),
            Err(url::ParseError::RelativeUrlWithoutBase) => Self::parse_path(s),
            Err(e) => Err(ObjectStoreError::Generic {
                store: "DeltaObjectStore",
                source: Box::new(e),
            }),
        }
    }

    /// Creates a new [`StorageUrl`] interpreting `s` as a filesystem path
    fn parse_path(s: &str) -> ObjectStoreResult<Self> {
        let path =
            std::path::Path::new(s)
                .canonicalize()
                .map_err(|e| ObjectStoreError::Generic {
                    store: "DeltaObjectStore",
                    source: Box::new(e),
                })?;
        let url = match path.is_file() {
            true => Url::from_file_path(path).unwrap(),
            false => Url::from_directory_path(path).unwrap(),
        };

        Ok(Self::new(url))
    }

    /// Creates a new [`StorageUrl`] from a url
    fn new(url: Url) -> Self {
        let prefix = Path::parse(url.path()).expect("should be URL safe");
        Self { url, prefix }
    }

    /// Returns the URL scheme
    pub fn scheme(&self) -> &str {
        self.url.scheme()
    }

    /// Returns the URL host
    pub fn host(&self) -> Option<&str> {
        self.url.host_str()
    }

    /// Returns the path prefix relative to location root
    pub fn prefix(&self) -> Path {
        self.prefix.clone()
    }

    /// Returns this [`StorageUrl`] as a string
    pub fn as_str(&self) -> &str {
        self.as_ref()
    }

    /// Returns the type of storage the URl refers to
    pub fn service_type(&self) -> StorageService {
        match self.url.scheme() {
            "file" => StorageService::Local,
            "az" | "abfs" | "abfss" | "azure" | "wasb" | "adl" => StorageService::Azure,
            "s3" | "s3a" => StorageService::S3,
            "gs" => StorageService::Gcs,
            "memory" => StorageService::InMemory,
            _ => StorageService::Unknown,
        }
    }
}

impl AsRef<str> for StorageUrl {
    fn as_ref(&self) -> &str {
        self.url.as_ref()
    }
}

impl AsRef<Url> for StorageUrl {
    fn as_ref(&self) -> &Url {
        &self.url
    }
}

impl std::fmt::Display for StorageUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

pub(crate) fn get_storage_backend(
    table_uri: impl AsRef<str>,
    options: Option<HashMap<String, String>>,
) -> ObjectStoreResult<(Arc<DynObjectStore>, StorageUrl)> {
    let storage_url = StorageUrl::parse(table_uri)?;
    match storage_url.service_type() {
        StorageService::Local => Ok((Arc::new(LocalFileSystem::new()), storage_url)),
        StorageService::InMemory => Ok((Arc::new(InMemory::new()), storage_url)),
        StorageService::Azure => {
            let (container_name, url_account) = match storage_url.scheme() {
                "az" | "adl" | "azure" => {
                    let container = storage_url.host().ok_or(ObjectStoreError::NotImplemented)?;
                    (container.to_owned(), None)
                }
                "abfs" | "abfss" => {
                    // abfs(s) might refer to the fsspec convention abfs://<container>/<path>
                    // or the convention for the hadoop driver abfs[s]://<file_system>@<account_name>.dfs.core.windows.net/<path>
                    let url: &Url = storage_url.as_ref();
                    if url.username().is_empty() {
                        (
                            url.host_str()
                                .ok_or(ObjectStoreError::NotImplemented)?
                                .to_string(),
                            None,
                        )
                    } else {
                        let parts: Vec<&str> = url
                            .host_str()
                            .ok_or(ObjectStoreError::NotImplemented)?
                            .splitn(2, '.')
                            .collect();
                        if parts.len() != 2 {
                            Err(ObjectStoreError::NotImplemented)
                        } else {
                            Ok((url.username().to_owned(), Some(parts[0])))
                        }?
                    }
                }
                _ => todo!(),
            };
            let mut options = options.unwrap_or_default();
            if let Some(account) = url_account {
                options.insert("account_name".into(), account.into());
            }
            let builder = AzureConfig::get_builder(&options)?.with_container_name(container_name);
            Ok((Arc::new(builder.build()?), storage_url))
        }
        _ => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use super::{StorageService, StorageUrl};
    use object_store::path::Path;

    #[test]
    fn parse_storage_location() {
        let known_urls = vec![
            (
                "az://bucket/path/file.foo",
                StorageService::Azure,
                Path::from("path/file.foo"),
                Some("bucket"),
            ),
            (
                "az://bucket",
                StorageService::Azure,
                Path::default(),
                Some("bucket"),
            ),
            (
                "az://bucket/",
                StorageService::Azure,
                Path::default(),
                Some("bucket"),
            ),
            (
                "adl://bucket/path/file.foo",
                StorageService::Azure,
                Path::from("path/file.foo"),
                Some("bucket"),
            ),
            (
                "abfs://bucket/path/file.foo",
                StorageService::Azure,
                Path::from("path/file.foo"),
                Some("bucket"),
            ),
            (
                "azure://bucket/path/file.foo",
                StorageService::Azure,
                Path::from("path/file.foo"),
                Some("bucket"),
            ),
            (
                "s3://bucket/path/file.foo",
                StorageService::S3,
                Path::from("path/file.foo"),
                Some("bucket"),
            ),
            (
                "s3a://bucket/path/file.foo",
                StorageService::S3,
                Path::from("path/file.foo"),
                Some("bucket"),
            ),
            (
                "gs://bucket/path/file.foo",
                StorageService::Gcs,
                Path::from("path/file.foo"),
                Some("bucket"),
            ),
            (
                "file:///path/file.foo",
                StorageService::Local,
                Path::from("path/file.foo"),
                None,
            ),
            ("file:///", StorageService::Local, Path::default(), None),
            (
                "memory://bucket/path/file.foo",
                StorageService::InMemory,
                Path::from("path/file.foo"),
                Some("bucket"),
            ),
            ("memory://", StorageService::InMemory, Path::default(), None),
        ];

        for (raw, service, prefix, host) in known_urls {
            let parsed = StorageUrl::parse(raw).unwrap();
            assert_eq!(parsed.service_type(), service);
            assert_eq!(parsed.prefix(), prefix);
            assert_eq!(parsed.as_str(), raw);
            assert_eq!(parsed.host(), host);
        }
    }
}
