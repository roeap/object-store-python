use std::collections::HashMap;
use std::sync::Arc;

use crate::prefix::PrefixObjectStore;
use crate::utils::{delete_dir, wait_for_future, walk_tree};
use crate::{get_storage_backend, ObjectStoreError};

use object_store::MultipartId;
use object_store::{path::Path, DynObjectStore, Error as InnerObjectStoreError, ListResult};
use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyBytes};
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[pyclass(subclass)]
#[derive(Debug, Clone)]
pub struct ArrowFileSystemHandler {
    inner: Arc<DynObjectStore>,
}

#[pymethods]
impl ArrowFileSystemHandler {
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

    fn get_type_name(&self) -> String {
        "object-store".into()
    }

    fn normalize_path(&self, path: String) -> PyResult<String> {
        let path = Path::parse(path).map_err(ObjectStoreError::from)?;
        Ok(path.to_string())
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

    fn equals(&self, other: &ArrowFileSystemHandler) -> PyResult<bool> {
        Ok(format!("{:?}", self) == format!("{:?}", other))
    }

    fn get_file_info<'py>(&self, paths: Vec<String>, py: Python<'py>) -> PyResult<Vec<&'py PyAny>> {
        let fs = PyModule::import(py, "pyarrow.fs")?;
        let file_types = fs.getattr("FileType")?;

        let to_file_info = |loc: String, type_: &PyAny, kwargs: HashMap<&str, i64>| {
            fs.call_method("FileInfo", (loc, type_), Some(kwargs.into_py_dict(py)))
        };

        let mut infos = Vec::new();
        for file_path in paths {
            let path = Path::from(file_path);
            let listed = wait_for_future(py, self.inner.list_with_delimiter(Some(&path)))
                .map_err(ObjectStoreError::from)?;

            // TODO is there a better way to figure out if we are in a directory?
            if listed.objects.is_empty() && listed.common_prefixes.is_empty() {
                let maybe_meta = wait_for_future(py, self.inner.head(&path));
                match maybe_meta {
                    Ok(meta) => {
                        let kwargs = HashMap::from([
                            ("size", meta.size as i64),
                            ("mtime_ns", meta.last_modified.timestamp_nanos()),
                        ]);
                        infos.push(to_file_info(
                            meta.location.to_string(),
                            file_types.getattr("File")?,
                            kwargs,
                        )?);
                    }
                    Err(object_store::Error::NotFound { .. }) => {
                        infos.push(to_file_info(
                            path.to_string(),
                            file_types.getattr("NotFound")?,
                            HashMap::new(),
                        )?);
                    }
                    Err(err) => {
                        return Err(ObjectStoreError::from(err).into());
                    }
                }
            } else {
                infos.push(to_file_info(
                    path.to_string(),
                    file_types.getattr("Directory")?,
                    HashMap::new(),
                )?);
            }
        }

        Ok(infos)
    }

    #[args(allow_not_found = "false", recursive = "false")]
    fn get_file_info_selector<'py>(
        &self,
        base_dir: String,
        allow_not_found: bool,
        recursive: bool,
        py: Python<'py>,
    ) -> PyResult<Vec<&'py PyAny>> {
        let fs = PyModule::import(py, "pyarrow.fs")?;
        let file_types = fs.getattr("FileType")?;

        let to_file_info = |loc: String, type_: &PyAny, kwargs: HashMap<&str, i64>| {
            fs.call_method("FileInfo", (loc, type_), Some(kwargs.into_py_dict(py)))
        };

        let path = Path::from(base_dir);
        let list_result =
            match wait_for_future(py, walk_tree(self.inner.clone(), &path, recursive)) {
                Ok(res) => Ok(res),
                Err(InnerObjectStoreError::NotFound { path, source }) => {
                    if allow_not_found {
                        Ok(ListResult {
                            common_prefixes: vec![],
                            objects: vec![],
                        })
                    } else {
                        Err(InnerObjectStoreError::NotFound { path, source })
                    }
                }
                Err(err) => Err(err),
            }
            .map_err(ObjectStoreError::from)?;

        let mut infos = vec![];
        infos.extend(
            list_result
                .common_prefixes
                .into_iter()
                .map(|p| {
                    to_file_info(
                        p.to_string(),
                        file_types.getattr("Directory")?,
                        HashMap::new(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        infos.extend(
            list_result
                .objects
                .into_iter()
                .map(|meta| {
                    let kwargs = HashMap::from([
                        ("size", meta.size as i64),
                        ("mtime_ns", meta.last_modified.timestamp_nanos()),
                    ]);
                    to_file_info(
                        meta.location.to_string(),
                        file_types.getattr("File")?,
                        kwargs,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        );

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

    fn open_input_file(&self, path: String, py: Python) -> PyResult<ObjectInputFile> {
        let path = Path::from(path);
        let file = wait_for_future(py, ObjectInputFile::try_new(self.inner.clone(), path))
            .map_err(ObjectStoreError::from)?;
        Ok(file)
    }

    #[args(metadata = "None")]
    fn open_output_stream(
        &self,
        path: String,
        #[allow(unused)] metadata: Option<HashMap<String, String>>,
        py: Python,
    ) -> PyResult<ObjectOutputStream> {
        let path = Path::from(path);
        let file = wait_for_future(py, ObjectOutputStream::try_new(self.inner.clone(), path))
            .map_err(ObjectStoreError::from)?;
        Ok(file)
    }
}

// TODO the C++ implementation track an internal lock on all random access files, DO we need this here?
// TODO add buffer to store data ...
#[pyclass(weakref)]
#[derive(Debug, Clone)]
pub struct ObjectInputFile {
    store: Arc<DynObjectStore>,
    path: Path,
    content_length: i64,
    #[pyo3(get)]
    closed: bool,
    pos: i64,
    #[pyo3(get)]
    mode: String,
}

impl ObjectInputFile {
    pub async fn try_new(store: Arc<DynObjectStore>, path: Path) -> Result<Self, ObjectStoreError> {
        // Issue a HEAD Object to get the content-length and ensure any
        // errors (e.g. file not found) don't wait until the first read() call.
        let meta = store.head(&path).await?;
        let content_length = meta.size as i64;
        // TODO make sure content length is valid
        // https://github.com/apache/arrow/blob/f184255cbb9bf911ea2a04910f711e1a924b12b8/cpp/src/arrow/filesystem/s3fs.cc#L1083
        Ok(Self {
            store,
            path,
            content_length,
            closed: false,
            pos: 0,
            mode: "rb".into(),
        })
    }

    fn check_closed(&self) -> Result<(), ObjectStoreError> {
        if self.closed {
            return Err(ObjectStoreError::Common(
                "Operation on closed stream".into(),
            ));
        }

        Ok(())
    }

    fn check_position(&self, position: i64, action: &str) -> Result<(), ObjectStoreError> {
        if position < 0 {
            return Err(ObjectStoreError::Common(format!(
                "Cannot {} for negative position.",
                action
            )));
        }
        if position > self.content_length {
            return Err(ObjectStoreError::Common(format!(
                "Cannot {} past end of file.",
                action
            )));
        }
        Ok(())
    }
}

#[pymethods]
impl ObjectInputFile {
    fn close(&mut self) -> PyResult<()> {
        self.closed = true;
        Ok(())
    }

    fn isatty(&self) -> PyResult<bool> {
        Ok(false)
    }

    fn readable(&self) -> PyResult<bool> {
        Ok(true)
    }

    fn seekable(&self) -> PyResult<bool> {
        Ok(true)
    }

    fn writable(&self) -> PyResult<bool> {
        Ok(false)
    }

    fn tell(&self) -> PyResult<i64> {
        self.check_closed()?;
        Ok(self.pos)
    }

    fn size(&self) -> PyResult<i64> {
        self.check_closed()?;
        Ok(self.content_length)
    }

    #[args(whence = "0")]
    fn seek(&mut self, offset: i64, whence: i64) -> PyResult<i64> {
        self.check_closed()?;
        self.check_position(offset, "seek")?;
        match whence {
            // reference is start of the stream (the default); offset should be zero or positive
            0 => {
                self.pos = offset;
            }
            // reference is current stream position; offset may be negative
            1 => {
                self.pos += offset;
            }
            // reference is  end of the stream; offset is usually negative
            2 => {
                self.pos = self.content_length as i64 - offset;
            }
            _ => {
                return Err(PyValueError::new_err(
                    "'whence' must be between  0 <= whence <= 2.",
                ));
            }
        }
        Ok(self.pos)
    }

    #[args(nbytes = "None")]
    fn read<'py>(&mut self, nbytes: Option<i64>, py: Python<'py>) -> PyResult<&'py PyBytes> {
        self.check_closed()?;
        let range = match nbytes {
            Some(len) => {
                let end = i64::min(self.pos + len, self.content_length) as usize;
                std::ops::Range {
                    start: self.pos as usize,
                    end,
                }
            }
            _ => std::ops::Range {
                start: self.pos as usize,
                end: self.content_length as usize,
            },
        };
        let nbytes = (range.end - range.start) as i64;
        self.pos += nbytes;
        let obj = if nbytes > 0 {
            wait_for_future(py, self.store.get_range(&self.path, range))
                .map_err(ObjectStoreError::from)?
                .to_vec()
        } else {
            Vec::new()
        };
        Ok(PyBytes::new(py, &obj))
    }

    fn fileno(&self) -> PyResult<()> {
        Err(PyNotImplementedError::new_err("'fileno' not implemented"))
    }

    fn truncate(&self) -> PyResult<()> {
        Err(PyNotImplementedError::new_err("'truncate' not implemented"))
    }

    fn readline(&self, _size: Option<i64>) -> PyResult<()> {
        Err(PyNotImplementedError::new_err("'readline' not implemented"))
    }

    fn readlines(&self, _hint: Option<i64>) -> PyResult<()> {
        Err(PyNotImplementedError::new_err(
            "'readlines' not implemented",
        ))
    }
}

// TODO the C++ implementation track an internal lock on all random access files, DO we need this here?
// TODO add buffer to store data ...
#[pyclass(weakref)]
pub struct ObjectOutputStream {
    store: Arc<DynObjectStore>,
    path: Path,
    writer: Box<dyn AsyncWrite + Send + Unpin>,
    multipart_id: MultipartId,
    pos: i64,
    #[pyo3(get)]
    closed: bool,
    #[pyo3(get)]
    mode: String,
}

impl ObjectOutputStream {
    pub async fn try_new(store: Arc<DynObjectStore>, path: Path) -> Result<Self, ObjectStoreError> {
        let (multipart_id, writer) = store.put_multipart(&path).await.unwrap();
        Ok(Self {
            store,
            path,
            writer,
            multipart_id,
            pos: 0,
            closed: false,
            mode: "wb".into(),
        })
    }

    fn check_closed(&self) -> Result<(), ObjectStoreError> {
        if self.closed {
            return Err(ObjectStoreError::Common(
                "Operation on closed stream".into(),
            ));
        }

        Ok(())
    }
}

#[pymethods]
impl ObjectOutputStream {
    fn close(&mut self, py: Python) -> PyResult<()> {
        self.closed = true;
        match wait_for_future(py, self.writer.shutdown()) {
            Ok(_) => Ok(()),
            Err(err) => {
                wait_for_future(
                    py,
                    self.store.abort_multipart(&self.path, &self.multipart_id),
                )
                .map_err(ObjectStoreError::from)?;
                Err(ObjectStoreError::from(err).into())
            }
        }
    }

    fn isatty(&self) -> PyResult<bool> {
        Ok(false)
    }

    fn readable(&self) -> PyResult<bool> {
        Ok(false)
    }

    fn seekable(&self) -> PyResult<bool> {
        Ok(false)
    }

    fn writable(&self) -> PyResult<bool> {
        Ok(true)
    }

    fn tell(&self) -> PyResult<i64> {
        self.check_closed()?;
        Ok(self.pos)
    }

    fn size(&self) -> PyResult<i64> {
        self.check_closed()?;
        Err(PyNotImplementedError::new_err("'size' not implemented"))
    }

    #[args(whence = "0")]
    fn seek(&mut self, _offset: i64, _whence: i64) -> PyResult<i64> {
        self.check_closed()?;
        Err(PyNotImplementedError::new_err("'seek' not implemented"))
    }

    #[args(nbytes = "None")]
    fn read(&mut self, _nbytes: Option<i64>) -> PyResult<()> {
        self.check_closed()?;
        Err(PyNotImplementedError::new_err("'read' not implemented"))
    }

    fn write(&mut self, data: Vec<u8>, py: Python) -> PyResult<i64> {
        self.check_closed()?;
        let len = data.len() as i64;
        match wait_for_future(py, self.writer.write_all(&data)) {
            Ok(_) => Ok(len),
            Err(err) => {
                wait_for_future(
                    py,
                    self.store.abort_multipart(&self.path, &self.multipart_id),
                )
                .map_err(ObjectStoreError::from)?;
                Err(ObjectStoreError::from(err).into())
            }
        }
    }

    fn flush(&mut self, py: Python) -> PyResult<()> {
        match wait_for_future(py, self.writer.flush()) {
            Ok(_) => Ok(()),
            Err(err) => {
                wait_for_future(
                    py,
                    self.store.abort_multipart(&self.path, &self.multipart_id),
                )
                .map_err(ObjectStoreError::from)?;
                Err(ObjectStoreError::from(err).into())
            }
        }
    }

    fn fileno(&self) -> PyResult<()> {
        Err(PyNotImplementedError::new_err("'fileno' not implemented"))
    }

    fn truncate(&self) -> PyResult<()> {
        Err(PyNotImplementedError::new_err("'truncate' not implemented"))
    }

    fn readline(&self, _size: Option<i64>) -> PyResult<()> {
        Err(PyNotImplementedError::new_err("'readline' not implemented"))
    }

    fn readlines(&self, _hint: Option<i64>) -> PyResult<()> {
        Err(PyNotImplementedError::new_err(
            "'readlines' not implemented",
        ))
    }
}