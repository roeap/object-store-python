use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::path::{Path, DELIMITER};
use object_store::{
    DynObjectStore, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    Result as ObjectStoreResult,
};
use tokio::io::AsyncWrite;

#[derive(Debug, Clone)]
pub struct PrefixObjectStore {
    prefix: Path,
    inner: Arc<DynObjectStore>,
}

impl std::fmt::Display for PrefixObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PrefixObjectStore({})", self.prefix.as_ref())
    }
}

impl PrefixObjectStore {
    /// Create a new instance of [`PrefixObjectStore`]
    pub fn new(prefix: Path, store: Arc<DynObjectStore>) -> Self {
        Self {
            prefix,
            inner: store,
        }
    }

    fn full_path(&self, location: &Path) -> ObjectStoreResult<Path> {
        let path: &str = location.as_ref();
        let stripped = match self.prefix.as_ref() {
            "" => path.to_string(),
            p => format!("{}/{}", p, path),
        };
        Ok(Path::parse(stripped.trim_end_matches(DELIMITER))?)
    }

    fn strip_prefix(&self, path: &Path) -> Option<Path> {
        let path: &str = path.as_ref();
        let stripped = match self.prefix.as_ref() {
            "" => path,
            p => path.strip_prefix(p)?.strip_prefix(DELIMITER)?,
        };
        Path::parse(stripped).ok()
    }
}

#[async_trait::async_trait]
impl ObjectStore for PrefixObjectStore {
    /// Save the provided bytes to the specified location.
    async fn put(&self, location: &Path, bytes: Bytes) -> ObjectStoreResult<()> {
        let full_path = self.full_path(location)?;
        self.inner.put(&full_path, bytes).await
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        let full_path = self.full_path(location)?;
        self.inner.get(&full_path).await
    }

    /// Return the bytes that are stored at the specified location
    /// in the given byte range
    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        let full_path = self.full_path(location)?;
        self.inner.get_range(&full_path, range).await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let full_path = self.full_path(location)?;
        self.inner.head(&full_path).await.map(|meta| ObjectMeta {
            last_modified: meta.last_modified,
            size: meta.size,
            location: self.strip_prefix(&meta.location).unwrap_or(meta.location),
        })
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        let full_path = self.full_path(location)?;
        self.inner.delete(&full_path).await
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        let prefix = prefix.and_then(|p| self.full_path(p).ok());
        Ok(self
            .inner
            .list(Some(&prefix.unwrap_or_else(|| self.prefix.clone())))
            .await?
            .map_ok(|meta| ObjectMeta {
                last_modified: meta.last_modified,
                size: meta.size,
                location: self.strip_prefix(&meta.location).unwrap_or(meta.location),
            })
            .boxed())
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let prefix = prefix.and_then(|p| self.full_path(p).ok());
        self.inner
            .list_with_delimiter(Some(&prefix.unwrap_or_else(|| self.prefix.clone())))
            .await
            .map(|lst| ListResult {
                common_prefixes: lst
                    .common_prefixes
                    .iter()
                    .map(|p| self.strip_prefix(p).unwrap_or_else(|| p.clone()))
                    .collect(),
                objects: lst
                    .objects
                    .iter()
                    .map(|meta| ObjectMeta {
                        last_modified: meta.last_modified,
                        size: meta.size,
                        location: self
                            .strip_prefix(&meta.location)
                            .unwrap_or_else(|| meta.location.clone()),
                    })
                    .collect(),
            })
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.full_path(from)?;
        let full_to = self.full_path(to)?;
        self.inner.copy(&full_from, &full_to).await
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.full_path(from)?;
        let full_to = self.full_path(to)?;
        self.inner.copy_if_not_exists(&full_from, &full_to).await
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.full_path(from)?;
        let full_to = self.full_path(to)?;
        self.inner.rename_if_not_exists(&full_from, &full_to).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let full_path = self.full_path(location)?;
        self.inner.put_multipart(&full_path).await
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        let full_path = self.full_path(location)?;
        self.inner.abort_multipart(&full_path, multipart_id).await
    }
}
