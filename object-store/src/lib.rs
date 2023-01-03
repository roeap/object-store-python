mod builder;
mod file;
mod utils;

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::file::{ArrowFileSystemHandler, ObjectInputFile, ObjectOutputStream};
use crate::utils::{flatten_list_stream, get_bytes};

use object_store::path::{Error as PathError, Path};
use object_store::{DynObjectStore, Error as InnerObjectStoreError, ListResult, ObjectMeta};
use pyo3::exceptions::{
    PyException, PyFileExistsError, PyFileNotFoundError, PyNotImplementedError,
};
use pyo3::prelude::*;
use pyo3::{types::PyBytes, PyErr};
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
struct PyPath(Path);

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
struct PyObjectMeta(ObjectMeta);

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
struct PyListResult(ListResult);

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

#[pyclass(name = "ObjectStore", subclass)]
#[derive(Debug, Clone)]
/// A generic object store interface for uniformly interacting with AWS S3, Google Cloud Storage,
/// Azure Blob Storage and local files.
struct PyObjectStore {
    inner: Arc<DynObjectStore>,
    rt: Arc<Runtime>,
    root_url: String,
    options: Option<HashMap<String, String>>,
}

#[pymethods]
impl PyObjectStore {
    #[new]
    #[args(options = "None")]
    /// Create a new ObjectStore instance
    fn new(root: String, options: Option<HashMap<String, String>>) -> PyResult<Self> {
        let inner = ObjectStoreBuilder::new(root.clone())
            .with_path_as_prefix(true)
            .with_options(options.clone().unwrap_or_default())
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

    /// Return the bytes that are stored at the specified location.
    #[pyo3(text_signature = "($self, location)")]
    fn get(&self, location: PyPath) -> PyResult<Py<PyBytes>> {
        let obj = self
            .rt
            .block_on(get_bytes(self.inner.as_ref(), &location.into()))
            .map_err(ObjectStoreError::from)?;
        Python::with_gil(|py| Ok(PyBytes::new(py, &obj).into_py(py)))
    }

    /// Return the bytes that are stored at the specified location in the given byte range
    #[pyo3(text_signature = "($self, location, start, length)")]
    fn get_range(&self, location: PyPath, start: usize, length: usize) -> PyResult<Py<PyBytes>> {
        let range = std::ops::Range {
            start,
            end: start + length,
        };
        let obj = self
            .rt
            .block_on(self.inner.get_range(&location.into(), range))
            .map_err(ObjectStoreError::from)?
            .to_vec();
        Python::with_gil(|py| Ok(PyBytes::new(py, &obj).into_py(py)))
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

    /// Delete the object at the specified location.
    #[pyo3(text_signature = "($self, location)")]
    fn delete(&self, location: PyPath) -> PyResult<()> {
        self.rt
            .block_on(self.inner.delete(&location.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(())
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
    /// Will return an error if the destination already has an object.
    #[pyo3(text_signature = "($self, from, to)")]
    fn rename_if_not_exists(&self, from: PyPath, to: PyPath) -> PyResult<()> {
        self.rt
            .block_on(self.inner.rename_if_not_exists(&from.into(), &to.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    pub fn __getnewargs__(&self) -> PyResult<(String, Option<HashMap<String, String>>)> {
        Ok((self.root_url.clone(), self.options.clone()))
    }
}

#[pymodule]
fn _internal(_py: Python, m: &PyModule) -> PyResult<()> {
    // Register the python classes
    m.add_class::<PyObjectStore>()?;
    m.add_class::<PyPath>()?;
    m.add_class::<PyObjectMeta>()?;
    m.add_class::<PyListResult>()?;
    m.add_class::<ArrowFileSystemHandler>()?;
    m.add_class::<ObjectInputFile>()?;
    m.add_class::<ObjectOutputStream>()?;

    Ok(())
}
