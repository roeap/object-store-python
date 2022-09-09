//! A filesystem implementation compliant with pyarrow's FileSystem
//! <https://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html#pyarrow.fs.FileSystem>

use std::collections::HashMap;
use std::sync::Arc;

use crate::builder::get_storage_backend;
use crate::prefix::PrefixObjectStore;
use crate::utils::wait_for_future;
use crate::ObjectStoreError;

use futures::StreamExt;
use object_store::{path::Path, DynObjectStore};
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyType};

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

#[pyclass(name = "ArrowFileSystem", module = "object_store", subclass)]
#[derive(Debug, Clone)]
pub struct ArrowFileSystem {
    inner: Arc<DynObjectStore>,
}

#[pymethods]
impl ArrowFileSystem {
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

    #[classmethod]
    fn from_uri(_cls: &PyType) -> Self {
        todo!()
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

    fn equals(&self, other: &ArrowFileSystem) -> PyResult<bool> {
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
