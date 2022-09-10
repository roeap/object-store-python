mod builder;
mod file;
// mod filesystem;
mod prefix;
mod settings;
mod utils;

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::prefix::PrefixObjectStore;
use crate::utils::{flatten_list_stream, wait_for_future};

use builder::get_storage_backend;
use futures::StreamExt;
use object_store::path::{Error as PathError, Path};
use object_store::{DynObjectStore, Error as InnerObjectStoreError, ListResult, ObjectMeta};
use pyo3::exceptions::{
    PyException, PyFileExistsError, PyFileNotFoundError, PyNotImplementedError,
};
use pyo3::prelude::*;
use pyo3::{
    types::{IntoPyDict, PyBytes},
    PyErr,
};

const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;

#[derive(Debug)]
pub enum ObjectStoreError {
    ObjectStore(InnerObjectStoreError),
    Common(String),
    Python(PyErr),
    Path(PathError),
}

impl fmt::Display for ObjectStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ObjectStoreError::ObjectStore(e) => write!(f, "ObjectStore error: {:?}", e),
            ObjectStoreError::Python(e) => write!(f, "Python error {:?}", e),
            ObjectStoreError::Path(e) => write!(f, "Path error {:?}", e),
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

#[pyclass(name = "Path", module = "object_store", subclass)]
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

#[pyclass(name = "ObjectMeta", module = "object_store", subclass)]
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

#[pyclass(name = "ListResult", module = "object_store", subclass)]
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

/// File and directory selector.
///
/// It contains a set of options that describes how to search for files and directories.
#[pyclass(name = "FileSelector", module = "object_store")]
#[derive(Debug, Clone)]
pub struct FileSelector {
    /// The directory in which to select files.
    #[pyo3(get)]
    base_dir: String,

    /// The behavior if base_dir doesn’t exist in the filesystem. If false, an error is returned.
    /// If true, an empty selection is returned.
    #[pyo3(get)]
    allow_not_found: bool,

    /// Whether to recurse into subdirectories.
    #[pyo3(get)]
    recursive: bool,

    /// Denotes if a directory is to be traversed
    pub(self) directory: bool,
}

#[pymethods]
impl FileSelector {
    #[new]
    #[args(allow_not_found = "false", recursive = "false", directory = "false")]
    fn new(base_dir: String, allow_not_found: bool, recursive: bool, directory: bool) -> Self {
        Self {
            base_dir,
            allow_not_found,
            recursive,
            directory,
        }
    }
}

#[pyclass(name = "ObjectStore", module = "object_store", subclass)]
#[derive(Debug, Clone)]
struct PyObjectStore {
    inner: Arc<DynObjectStore>,
}

impl PyObjectStore {
    async fn get_inner(&self, location: &Path) -> PyResult<Vec<u8>> {
        Ok(self
            .inner
            .get(location)
            .await
            .map_err(ObjectStoreError::from)?
            .bytes()
            .await
            .map_err(ObjectStoreError::from)?
            .into())
    }
}

#[pymethods]
impl PyObjectStore {
    #[new]
    #[args(options = "None")]
    fn new(root: String, options: Option<HashMap<String, String>>) -> PyResult<Self> {
        let (root_store, storage_url) =
            get_storage_backend(root, options).map_err(ObjectStoreError::from)?;
        let store = PrefixObjectStore::new(storage_url.prefix(), root_store);
        Ok(Self {
            inner: Arc::new(store),
        })
    }

    /// Save the provided bytes to the specified location.
    fn put(&self, location: PyPath, bytes: Vec<u8>, py: Python) -> PyResult<()> {
        wait_for_future(py, self.inner.put(&location.into(), bytes.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    /// Return the bytes that are stored at the specified location.
    fn get<'py>(&self, location: PyPath, py: Python<'py>) -> PyResult<&'py PyBytes> {
        let obj = wait_for_future(py, self.get_inner(&location.into()))?;
        Ok(PyBytes::new(py, &obj))
    }

    /// Return the bytes that are stored at the specified location in the given byte range
    fn get_range<'py>(
        &self,
        location: PyPath,
        start: usize,
        length: usize,
        py: Python<'py>,
    ) -> PyResult<&'py PyBytes> {
        let range = std::ops::Range {
            start,
            end: start + length,
        };
        let obj = wait_for_future(py, self.inner.get_range(&location.into(), range))
            .map_err(ObjectStoreError::from)?
            .to_vec();
        Ok(PyBytes::new(py, &obj))
    }

    /// Return the metadata for the specified location
    fn head(&self, location: PyPath, py: Python) -> PyResult<PyObjectMeta> {
        let meta = wait_for_future(py, self.inner.head(&location.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(meta.into())
    }

    /// Delete the object at the specified location.
    fn delete(&self, location: PyPath, py: Python) -> PyResult<()> {
        wait_for_future(py, self.inner.delete(&location.into())).map_err(ObjectStoreError::from)?;
        Ok(())
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix
    /// of `foo/bar/x` but not of `foo/bar_baz/x`.
    fn list(&self, prefix: Option<PyPath>, py: Python) -> PyResult<Vec<PyObjectMeta>> {
        Ok(wait_for_future(
            py,
            flatten_list_stream(self.inner.as_ref(), prefix.map(Path::from).as_ref()),
        )
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
    fn list_with_delimiter(&self, prefix: Option<PyPath>, py: Python) -> PyResult<PyListResult> {
        let list = wait_for_future(
            py,
            self.inner
                .list_with_delimiter(prefix.map(Path::from).as_ref()),
        )
        .map_err(ObjectStoreError::from)?;
        Ok(list.into())
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    fn copy(&self, from: PyPath, to: PyPath, py: Python) -> PyResult<()> {
        wait_for_future(py, self.inner.copy(&from.into(), &to.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    fn copy_if_not_exists(&self, from: PyPath, to: PyPath, py: Python) -> PyResult<()> {
        wait_for_future(py, self.inner.copy_if_not_exists(&from.into(), &to.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// By default, this is implemented as a copy and then delete source. It may not
    /// check when deleting source that it was the same object that was originally copied.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    fn rename(&self, from: PyPath, to: PyPath, py: Python) -> PyResult<()> {
        wait_for_future(py, self.inner.rename(&from.into(), &to.into()))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    fn rename_if_not_exists(&self, from: PyPath, to: PyPath, py: Python) -> PyResult<()> {
        wait_for_future(
            py,
            self.inner.rename_if_not_exists(&from.into(), &to.into()),
        )
        .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    fn copy_file(&self, src: String, dest: String, py: Python) -> PyResult<()> {
        let from_path = Path::from(src);
        let to_path = Path::from(dest);
        wait_for_future(py, self.inner.copy(&from_path, &to_path))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    fn create_dir(&self, _path: String, _recursive: bool) -> PyResult<()> {
        // TODO creating a dir should be a no-op with object_store, right?
        Ok(())
    }

    fn delete_dir(&self, path: String, py: Python) -> PyResult<()> {
        let path = Path::from(path);
        wait_for_future(py, delete_dir(self.inner.as_ref(), &path))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    fn delete_file(&self, path: String, py: Python) -> PyResult<()> {
        let path = Path::from(path);
        wait_for_future(py, self.inner.delete(&path)).map_err(ObjectStoreError::from)?;
        Ok(())
    }

    fn equals(&self, other: &PyObjectStore) -> PyResult<bool> {
        Ok(format!("{:?}", self) == format!("{:?}", other))
    }

    fn get_file_info<'py>(
        &self,
        selectors: Vec<FileSelector>,
        py: Python<'py>,
    ) -> PyResult<Vec<&'py PyAny>> {
        let fs = PyModule::import(py, "pyarrow.fs")?;
        let file_types = fs.getattr("FileType")?;

        let to_file_info = |loc: String, size: usize, type_: &PyAny| {
            let kwargs = HashMap::from([("size", size)]);
            fs.call_method("FileInfo", (loc, type_), Some(kwargs.into_py_dict(py)))
        };

        let mut infos = Vec::new();
        for selector in selectors {
            if selector.directory {
                return Err(PyNotImplementedError::new_err(
                    "directory selectors not yet implemented",
                ));
            }
            let path = Path::from(selector.base_dir);
            let maybe_meta = wait_for_future(py, self.inner.head(&path));
            match maybe_meta {
                Ok(meta) => {
                    infos.push(to_file_info(
                        meta.location.to_string(),
                        meta.size,
                        file_types.getattr("File")?,
                    )?);
                }
                Err(object_store::Error::NotFound { .. }) => {
                    infos.push(to_file_info(
                        path.to_string(),
                        0,
                        file_types.getattr("NotFound")?,
                    )?);
                }
                Err(err) => {
                    return Err(ObjectStoreError::from(err).into());
                }
            }
        }

        Ok(infos)
    }

    fn move_file(&self, src: String, dest: String, py: Python) -> PyResult<()> {
        let from_path = Path::from(src);
        let to_path = Path::from(dest);
        // TODO check the if not exists semantics
        wait_for_future(py, self.inner.rename(&from_path, &to_path))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }
}

async fn delete_dir(storage: &DynObjectStore, prefix: &Path) -> Result<(), ObjectStoreError> {
    // TODO batch delete would be really useful now...
    let mut stream = storage.list(Some(prefix)).await?;
    while let Some(maybe_meta) = stream.next().await {
        let meta = maybe_meta?;
        storage.delete(&meta.location).await?;
    }
    Ok(())
}

#[pymodule]
fn _internal(_py: Python, m: &PyModule) -> PyResult<()> {
    // Register the python classes
    m.add_class::<PyObjectStore>()?;
    m.add_class::<PyPath>()?;
    m.add_class::<PyObjectMeta>()?;
    m.add_class::<PyListResult>()?;
    m.add_class::<FileSelector>()?;

    Ok(())
}
