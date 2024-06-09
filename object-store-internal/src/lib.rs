mod builder;
mod file;
mod utils;

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

pub use crate::file::{ArrowFileSystemHandler, ObjectInputFile, ObjectOutputStream};
use crate::utils::{flatten_list_stream, get_bytes};

use object_store::path::{Error as PathError, Path};
use object_store::{
    BackoffConfig, ClientOptions, DynObjectStore, Error as InnerObjectStoreError, ListResult,
    ObjectMeta, RetryConfig,
};
use pyo3::exceptions::{
    PyException, PyFileExistsError, PyFileNotFoundError, PyNotImplementedError,
};
use pyo3::prelude::*;
use pyo3::PyErr;
use tokio::runtime::Runtime;

pub use builder::ObjectStoreBuilder;

#[derive(Debug)]
pub enum ObjectStoreError {
    ObjectStore(InnerObjectStoreError),
    Common(String),
    Python(PyErr),
    IO(std::io::Error),
    Task(tokio::task::JoinError),
    Path(PathError),
    InputValue(String),
}

impl fmt::Display for ObjectStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ObjectStoreError::ObjectStore(e) => write!(f, "ObjectStore error: {:?}", e),
            ObjectStoreError::Python(e) => write!(f, "Python error {:?}", e),
            ObjectStoreError::Path(e) => write!(f, "Path error {:?}", e),
            ObjectStoreError::IO(e) => write!(f, "IOError error {:?}", e),
            ObjectStoreError::Task(e) => write!(f, "Task error {:?}", e),
            ObjectStoreError::Common(e) => write!(f, "{}", e),
            ObjectStoreError::InputValue(e) => write!(f, "Invalid input value: {}", e),
        }
    }
}

impl From<InnerObjectStoreError> for ObjectStoreError {
    fn from(err: InnerObjectStoreError) -> ObjectStoreError {
        ObjectStoreError::ObjectStore(err)
    }
}

impl From<PathError> for ObjectStoreError {
    fn from(err: PathError) -> ObjectStoreError {
        ObjectStoreError::Path(err)
    }
}

impl From<tokio::task::JoinError> for ObjectStoreError {
    fn from(err: tokio::task::JoinError) -> ObjectStoreError {
        ObjectStoreError::Task(err)
    }
}

impl From<std::io::Error> for ObjectStoreError {
    fn from(err: std::io::Error) -> ObjectStoreError {
        ObjectStoreError::IO(err)
    }
}

impl From<PyErr> for ObjectStoreError {
    fn from(err: PyErr) -> ObjectStoreError {
        ObjectStoreError::Python(err)
    }
}

impl From<ObjectStoreError> for PyErr {
    fn from(err: ObjectStoreError) -> PyErr {
        match err {
            ObjectStoreError::Python(py_err) => py_err,
            ObjectStoreError::ObjectStore(store_err) => match store_err {
                InnerObjectStoreError::NotFound { .. } => {
                    PyFileNotFoundError::new_err(store_err.to_string())
                }
                InnerObjectStoreError::AlreadyExists { .. } => {
                    PyFileExistsError::new_err(store_err.to_string())
                }
                _ => PyException::new_err(store_err.to_string()),
            },
            _ => PyException::new_err(err.to_string()),
        }
    }
}

#[pyclass(name = "Path", subclass)]
#[derive(Clone)]
pub struct PyPath(Path);

impl From<PyPath> for Path {
    fn from(path: PyPath) -> Self {
        path.0
    }
}

impl From<Path> for PyPath {
    fn from(path: Path) -> Self {
        Self(path)
    }
}

#[pymethods]
impl PyPath {
    #[new]
    fn new(path: String) -> PyResult<Self> {
        Ok(Self(Path::parse(path).map_err(ObjectStoreError::from)?))
    }

    /// Creates a new child of this [`Path`]
    fn child(&self, part: String) -> Self {
        Self(self.0.child(part))
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __richcmp__(&self, other: PyPath, cmp: pyo3::basic::CompareOp) -> PyResult<bool> {
        match cmp {
            pyo3::basic::CompareOp::Eq => Ok(self.0 == other.0),
            pyo3::basic::CompareOp::Ne => Ok(self.0 != other.0),
            _ => Err(PyNotImplementedError::new_err(
                "Only == and != are supported.",
            )),
        }
    }
}

#[pyclass(name = "ObjectMeta", subclass)]
#[derive(Clone)]
pub struct PyObjectMeta(ObjectMeta);

impl From<ObjectMeta> for PyObjectMeta {
    fn from(meta: ObjectMeta) -> Self {
        Self(meta)
    }
}

#[pymethods]
impl PyObjectMeta {
    #[getter]
    fn location(&self) -> PyPath {
        self.0.location.clone().into()
    }

    #[getter]
    fn size(&self) -> usize {
        self.0.size
    }

    #[getter]
    fn last_modified(&self) -> i64 {
        self.0.last_modified.timestamp()
    }

    fn __str__(&self) -> String {
        format!("{:?}", self.0)
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }

    fn __richcmp__(&self, other: PyObjectMeta, cmp: pyo3::basic::CompareOp) -> PyResult<bool> {
        match cmp {
            pyo3::basic::CompareOp::Eq => Ok(self.0 == other.0),
            pyo3::basic::CompareOp::Ne => Ok(self.0 != other.0),
            _ => Err(PyNotImplementedError::new_err(
                "Only == and != are supported.",
            )),
        }
    }
}

#[pyclass(name = "ListResult", subclass)]
pub struct PyListResult(ListResult);

#[pymethods]
impl PyListResult {
    #[getter]
    fn common_prefixes(&self) -> Vec<PyPath> {
        self.0
            .common_prefixes
            .iter()
            .cloned()
            .map(PyPath::from)
            .collect()
    }

    #[getter]
    fn objects(&self) -> Vec<PyObjectMeta> {
        self.0
            .objects
            .iter()
            .cloned()
            .map(PyObjectMeta::from)
            .collect()
    }
}

impl From<ListResult> for PyListResult {
    fn from(result: ListResult) -> Self {
        Self(result)
    }
}

#[pyclass(name = "ClientOptions")]
#[derive(Debug, Clone, Default)]
pub struct PyClientOptions {
    #[pyo3(get, set)]
    user_agent: Option<String>,
    #[pyo3(get, set)]
    content_type_map: HashMap<String, String>,
    #[pyo3(get, set)]
    default_content_type: Option<String>,
    // default_headers: Option<HeaderMap>,
    #[pyo3(get, set)]
    proxy_url: Option<String>,
    #[pyo3(get, set)]
    allow_http: bool,
    #[pyo3(get, set)]
    allow_insecure: bool,
    #[pyo3(get, set)]
    timeout: Option<u64>,
    #[pyo3(get, set)]
    connect_timeout: Option<u64>,
    #[pyo3(get, set)]
    pool_idle_timeout: Option<u64>,
    #[pyo3(get, set)]
    pool_max_idle_per_host: Option<usize>,
    #[pyo3(get, set)]
    http2_keep_alive_interval: Option<u64>,
    #[pyo3(get, set)]
    http2_keep_alive_timeout: Option<u64>,
    #[pyo3(get, set)]
    http2_keep_alive_while_idle: bool,
    #[pyo3(get, set)]
    http1_only: bool,
    #[pyo3(get, set)]
    http2_only: bool,
    #[pyo3(get, set)]
    retry_init_backoff: Option<u64>,
    #[pyo3(get, set)]
    retry_max_backoff: Option<u64>,
    #[pyo3(get, set)]
    retry_backoff_base: Option<f64>,
    #[pyo3(get, set)]
    retry_max_retries: Option<usize>,
    #[pyo3(get, set)]
    retry_timeout: Option<u64>,
}

impl PyClientOptions {
    fn client_options(&self) -> Result<ClientOptions, ObjectStoreError> {
        let mut options = ClientOptions::new()
            .with_allow_http(self.allow_http)
            .with_allow_invalid_certificates(self.allow_insecure);
        if let Some(user_agent) = &self.user_agent {
            options = options.with_user_agent(
                user_agent
                    .clone()
                    .try_into()
                    .map_err(|_| ObjectStoreError::InputValue(user_agent.into()))?,
            );
        }
        if let Some(default_content_type) = &self.default_content_type {
            options = options.with_default_content_type(default_content_type);
        }
        if let Some(proxy_url) = &self.proxy_url {
            options = options.with_proxy_url(proxy_url);
        }
        if let Some(timeout) = self.timeout {
            options = options.with_timeout(Duration::from_secs(timeout));
        }
        if let Some(connect_timeout) = self.connect_timeout {
            options = options.with_connect_timeout(Duration::from_secs(connect_timeout));
        }
        if let Some(pool_idle_timeout) = self.pool_idle_timeout {
            options = options.with_pool_idle_timeout(Duration::from_secs(pool_idle_timeout));
        }
        if let Some(pool_max_idle_per_host) = self.pool_max_idle_per_host {
            options = options.with_pool_max_idle_per_host(pool_max_idle_per_host);
        }
        if let Some(http2_keep_alive_interval) = self.http2_keep_alive_interval {
            options = options
                .with_http2_keep_alive_interval(Duration::from_secs(http2_keep_alive_interval));
        }
        if let Some(http2_keep_alive_timeout) = self.http2_keep_alive_timeout {
            options = options
                .with_http2_keep_alive_timeout(Duration::from_secs(http2_keep_alive_timeout));
        }
        if self.http2_keep_alive_while_idle {
            options = options.with_http2_keep_alive_while_idle();
        }
        if self.http1_only {
            options = options.with_http1_only();
        }
        if self.http2_only {
            options = options.with_http2_only();
        }
        Ok(options)
    }

    fn retry_config(&self) -> Result<RetryConfig, ObjectStoreError> {
        let mut backoff = BackoffConfig::default();
        if let Some(init_backoff) = self.retry_init_backoff {
            backoff.init_backoff = Duration::from_secs(init_backoff);
        }
        if let Some(max_backoff) = self.retry_max_backoff {
            backoff.max_backoff = Duration::from_secs(max_backoff);
        }
        if let Some(backoff_base) = self.retry_backoff_base {
            backoff.base = backoff_base;
        }
        let mut config = RetryConfig {
            backoff,
            ..Default::default()
        };
        if let Some(max_retries) = self.retry_max_retries {
            config.max_retries = max_retries;
        }
        if let Some(timeout) = self.retry_timeout {
            config.retry_timeout = Duration::from_secs(timeout);
        }
        Ok(config)
    }
}

impl TryFrom<PyClientOptions> for ClientOptions {
    type Error = ObjectStoreError;

    fn try_from(value: PyClientOptions) -> Result<ClientOptions, Self::Error> {
        let mut options = ClientOptions::new()
            .with_allow_http(value.allow_http)
            .with_allow_invalid_certificates(value.allow_insecure);
        if let Some(user_agent) = value.user_agent {
            options = options.with_user_agent(
                user_agent
                    .clone()
                    .try_into()
                    .map_err(|_| ObjectStoreError::InputValue(user_agent))?,
            );
        }
        if let Some(default_content_type) = value.default_content_type {
            options = options.with_default_content_type(default_content_type);
        }
        if let Some(proxy_url) = value.proxy_url {
            options = options.with_proxy_url(proxy_url);
        }
        if let Some(timeout) = value.timeout {
            options = options.with_timeout(Duration::from_secs(timeout));
        }
        if let Some(connect_timeout) = value.connect_timeout {
            options = options.with_connect_timeout(Duration::from_secs(connect_timeout));
        }
        if let Some(pool_idle_timeout) = value.pool_idle_timeout {
            options = options.with_pool_idle_timeout(Duration::from_secs(pool_idle_timeout));
        }
        if let Some(pool_max_idle_per_host) = value.pool_max_idle_per_host {
            options = options.with_pool_max_idle_per_host(pool_max_idle_per_host);
        }
        if let Some(http2_keep_alive_interval) = value.http2_keep_alive_interval {
            options = options
                .with_http2_keep_alive_interval(Duration::from_secs(http2_keep_alive_interval));
        }
        if let Some(http2_keep_alive_timeout) = value.http2_keep_alive_timeout {
            options = options
                .with_http2_keep_alive_timeout(Duration::from_secs(http2_keep_alive_timeout));
        }
        if value.http2_keep_alive_while_idle {
            options = options.with_http2_keep_alive_while_idle();
        }
        if value.http1_only {
            options = options.with_http1_only();
        }
        if value.http2_only {
            options = options.with_http2_only();
        }
        Ok(options)
    }
}

#[pymethods]
impl PyClientOptions {
    #[new]
    #[pyo3(signature = (
        user_agent = None,
        content_type_map = None,
        default_content_type = None,
        proxy_url = None,
        allow_http = false,
        allow_insecure = false,
        timeout = None,
        connect_timeout = None,
        pool_idle_timeout = None,
        pool_max_idle_per_host = None,
        http2_keep_alive_interval = None,
        http2_keep_alive_timeout = None,
        http2_keep_alive_while_idle = false,
        http1_only = false,
        http2_only = false,
        retry_init_backoff = None,
        retry_max_backoff = None,
        retry_backoff_base = None,
        retry_max_retries = None,
        retry_timeout = None,
    ))]
    /// Create a new ObjectStore instance
    #[allow(clippy::too_many_arguments)]
    fn new(
        user_agent: Option<String>,
        content_type_map: Option<HashMap<String, String>>,
        default_content_type: Option<String>,
        proxy_url: Option<String>,
        allow_http: bool,
        allow_insecure: bool,
        timeout: Option<u64>,
        connect_timeout: Option<u64>,
        pool_idle_timeout: Option<u64>,
        pool_max_idle_per_host: Option<usize>,
        http2_keep_alive_interval: Option<u64>,
        http2_keep_alive_timeout: Option<u64>,
        http2_keep_alive_while_idle: bool,
        http1_only: bool,
        http2_only: bool,
        retry_init_backoff: Option<u64>,
        retry_max_backoff: Option<u64>,
        retry_backoff_base: Option<f64>,
        retry_max_retries: Option<usize>,
        retry_timeout: Option<u64>,
    ) -> Self {
        Self {
            user_agent,
            content_type_map: content_type_map.unwrap_or_default(),
            default_content_type,
            proxy_url,
            allow_http,
            allow_insecure,
            timeout,
            connect_timeout,
            pool_idle_timeout,
            pool_max_idle_per_host,
            http2_keep_alive_interval,
            http2_keep_alive_timeout,
            http2_keep_alive_while_idle,
            http1_only,
            http2_only,
            retry_init_backoff,
            retry_max_backoff,
            retry_backoff_base,
            retry_max_retries,
            retry_timeout,
        }
    }
}

#[pyclass(name = "ObjectStore", subclass)]
#[derive(Debug, Clone)]
/// A generic object store interface for uniformly interacting with AWS S3, Google Cloud Storage,
/// Azure Blob Storage and local files.
pub struct PyObjectStore {
    inner: Arc<DynObjectStore>,
    rt: Arc<Runtime>,
    root_url: String,
    options: Option<HashMap<String, String>>,
}

#[pymethods]
impl PyObjectStore {
    #[new]
    #[pyo3(signature = (root, options = None, client_options = None))]
    /// Create a new ObjectStore instance
    fn new(
        root: String,
        options: Option<HashMap<String, String>>,
        client_options: Option<PyClientOptions>,
    ) -> PyResult<Self> {
        let client_options = client_options.unwrap_or_default();
        let inner = ObjectStoreBuilder::new(root.clone())
            .with_path_as_prefix(true)
            .with_options(options.clone().unwrap_or_default())
            .with_client_options(client_options.client_options()?)
            .with_retry_config(client_options.retry_config()?)
            .build()
            .map_err(ObjectStoreError::from)?;
        Ok(Self {
            root_url: root,
            inner,
            rt: Arc::new(Runtime::new()?),
            options,
        })
    }

    /// Save the provided bytes to the specified location.
    #[pyo3(text_signature = "($self, location, bytes)")]
    fn put(&self, location: PyPath, bytes: Vec<u8>) -> PyResult<()> {
        self.rt
            .block_on(self.inner.put(&location.into(), bytes.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    /// Save the provided bytes to the specified location.
    #[pyo3(text_signature = "($self, location, bytes)")]
    fn put_async<'a>(
        &'a self,
        py: Python<'a>,
        location: PyPath,
        bytes: Vec<u8>,
    ) -> PyResult<&PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .put(&location.into(), bytes.into())
                .await
                .map_err(ObjectStoreError::from)?;
            Ok(())
        })
    }

    /// Return the bytes that are stored at the specified location.
    #[pyo3(text_signature = "($self, location)")]
    fn get(&self, location: PyPath) -> PyResult<Cow<[u8]>> {
        let obj = self
            .rt
            .block_on(get_bytes(self.inner.as_ref(), &location.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(Cow::Owned(obj.to_vec()))
    }

    /// Return the bytes that are stored at the specified location.
    #[pyo3(text_signature = "($self, location)")]
    fn get_async<'a>(&'a self, py: Python<'a>, location: PyPath) -> PyResult<&PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let obj = get_bytes(inner.as_ref(), &location.into())
                .await
                .map_err(ObjectStoreError::from)?;
            Ok(Cow::<[u8]>::Owned(obj.to_vec()))
        })
    }

    /// Return the bytes that are stored at the specified location in the given byte range
    #[pyo3(text_signature = "($self, location, start, length)")]
    fn get_range(&self, location: PyPath, start: usize, length: usize) -> PyResult<Cow<[u8]>> {
        let range = std::ops::Range {
            start,
            end: start + length,
        };
        let obj = self
            .rt
            .block_on(self.inner.get_range(&location.into(), range))
            .map_err(ObjectStoreError::from)?;
        Ok(Cow::Owned(obj.to_vec()))
    }

    /// Return the bytes that are stored at the specified location in the given byte range
    #[pyo3(text_signature = "($self, location, start, length)")]
    fn get_range_async<'a>(
        &'a self,
        py: Python<'a>,
        location: PyPath,
        start: usize,
        length: usize,
    ) -> PyResult<&PyAny> {
        let inner = self.inner.clone();
        let range = std::ops::Range {
            start,
            end: start + length,
        };

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let obj = inner
                .get_range(&location.into(), range)
                .await
                .map_err(ObjectStoreError::from)?;
            Ok(Cow::<[u8]>::Owned(obj.to_vec()))
        })
    }

    /// Return the metadata for the specified location
    #[pyo3(text_signature = "($self, location)")]
    fn head(&self, location: PyPath) -> PyResult<PyObjectMeta> {
        let meta = self
            .rt
            .block_on(self.inner.head(&location.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(meta.into())
    }

    /// Return the metadata for the specified location
    #[pyo3(text_signature = "($self, location)")]
    fn head_async<'a>(&'a self, py: Python<'a>, location: PyPath) -> PyResult<&PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let meta = inner
                .head(&location.into())
                .await
                .map_err(ObjectStoreError::from)?;
            Ok(PyObjectMeta::from(meta))
        })
    }

    /// Delete the object at the specified location.
    #[pyo3(text_signature = "($self, location)")]
    fn delete(&self, location: PyPath) -> PyResult<()> {
        self.rt
            .block_on(self.inner.delete(&location.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    /// Delete the object at the specified location.
    #[pyo3(text_signature = "($self, location)")]
    fn delete_async<'a>(&'a self, py: Python<'a>, location: PyPath) -> PyResult<&PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .delete(&location.into())
                .await
                .map_err(ObjectStoreError::from)?;
            Ok(())
        })
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix
    /// of `foo/bar/x` but not of `foo/bar_baz/x`.
    #[pyo3(text_signature = "($self, prefix)")]
    fn list(&self, prefix: Option<PyPath>) -> PyResult<Vec<PyObjectMeta>> {
        Ok(self
            .rt
            .block_on(flatten_list_stream(
                self.inner.as_ref(),
                prefix.map(Path::from).as_ref(),
            ))
            .map_err(ObjectStoreError::from)?
            .into_iter()
            .map(PyObjectMeta::from)
            .collect())
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix
    /// of `foo/bar/x` but not of `foo/bar_baz/x`.
    #[pyo3(text_signature = "($self, prefix)")]
    fn list_async<'a>(&'a self, py: Python<'a>, prefix: Option<PyPath>) -> PyResult<&PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let object_metas = flatten_list_stream(inner.as_ref(), prefix.map(Path::from).as_ref())
                .await
                .map_err(ObjectStoreError::from)?;
            let py_object_metas = object_metas
                .into_iter()
                .map(PyObjectMeta::from)
                .collect::<Vec<_>>();
            Ok(py_object_metas)
        })
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix
    /// of `foo/bar/x` but not of `foo/bar_baz/x`.
    #[pyo3(text_signature = "($self, prefix)")]
    fn list_with_delimiter(&self, prefix: Option<PyPath>) -> PyResult<PyListResult> {
        let list = self
            .rt
            .block_on(
                self.inner
                    .list_with_delimiter(prefix.map(Path::from).as_ref()),
            )
            .map_err(ObjectStoreError::from)?;
        Ok(list.into())
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix
    /// of `foo/bar/x` but not of `foo/bar_baz/x`.
    #[pyo3(text_signature = "($self, prefix)")]
    fn list_with_delimiter_async<'a>(
        &'a self,
        py: Python<'a>,
        prefix: Option<PyPath>,
    ) -> PyResult<&PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let list_result = inner
                .list_with_delimiter(prefix.map(Path::from).as_ref())
                .await
                .map_err(ObjectStoreError::from)?;
            Ok(PyListResult::from(list_result))
        })
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    #[pyo3(text_signature = "($self, from, to)")]
    fn copy(&self, from: PyPath, to: PyPath) -> PyResult<()> {
        self.rt
            .block_on(self.inner.copy(&from.into(), &to.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    #[pyo3(text_signature = "($self, from, to)")]
    fn copy_async<'a>(&'a self, py: Python<'a>, from: PyPath, to: PyPath) -> PyResult<&PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .copy(&from.into(), &to.into())
                .await
                .map_err(ObjectStoreError::from)?;
            Ok(())
        })
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    #[pyo3(text_signature = "($self, from, to)")]
    fn copy_if_not_exists(&self, from: PyPath, to: PyPath) -> PyResult<()> {
        self.rt
            .block_on(self.inner.copy_if_not_exists(&from.into(), &to.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    #[pyo3(text_signature = "($self, from, to)")]
    fn copy_if_not_exists_async<'a>(
        &'a self,
        py: Python<'a>,
        from: PyPath,
        to: PyPath,
    ) -> PyResult<&PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .copy_if_not_exists(&from.into(), &to.into())
                .await
                .map_err(ObjectStoreError::from)?;
            Ok(())
        })
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// By default, this is implemented as a copy and then delete source. It may not
    /// check when deleting source that it was the same object that was originally copied.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    #[pyo3(text_signature = "($self, from, to)")]
    fn rename(&self, from: PyPath, to: PyPath) -> PyResult<()> {
        self.rt
            .block_on(self.inner.rename(&from.into(), &to.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// By default, this is implemented as a copy and then delete source. It may not
    /// check when deleting source that it was the same object that was originally copied.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    #[pyo3(text_signature = "($self, from, to)")]
    fn rename_async<'a>(&'a self, py: Python<'a>, from: PyPath, to: PyPath) -> PyResult<&PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .rename(&from.into(), &to.into())
                .await
                .map_err(ObjectStoreError::from)?;
            Ok(())
        })
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    #[pyo3(text_signature = "($self, from, to)")]
    fn rename_if_not_exists(&self, from: PyPath, to: PyPath) -> PyResult<()> {
        self.rt
            .block_on(self.inner.rename_if_not_exists(&from.into(), &to.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    #[pyo3(text_signature = "($self, from, to)")]
    fn rename_if_not_exists_async<'a>(
        &'a self,
        py: Python<'a>,
        from: PyPath,
        to: PyPath,
    ) -> PyResult<&PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .rename_if_not_exists(&from.into(), &to.into())
                .await
                .map_err(ObjectStoreError::from)?;
            Ok(())
        })
    }

    pub fn __getnewargs__(&self) -> PyResult<(String, Option<HashMap<String, String>>)> {
        Ok((self.root_url.clone(), self.options.clone()))
    }
}
