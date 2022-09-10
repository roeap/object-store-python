use std::collections::HashMap;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use crate::prefix::PrefixObjectStore;
use crate::utils::{delete_dir, wait_for_future};
use crate::{get_storage_backend, ObjectStoreError};

use object_store::MultipartId;
use object_store::{path::Path, DynObjectStore};
use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::types::{IntoPyDict, PyBytes, PyString, PyType};
use pyo3::{exceptions::PyTypeError, prelude::*};
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[pyclass(name = "ArrowFileSystemHandler", module = "object_store", subclass)]
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
            if listed.objects.is_empty() {
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

        let mut infos = Vec::new();
        let path = Path::from(base_dir);
        let listed = wait_for_future(py, self.inner.list_with_delimiter(Some(&path)))
            .map_err(ObjectStoreError::from)?;
        let listed = listed
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
            .collect::<Result<Vec<_>, _>>()?;
        infos.extend(listed);
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
#[pyclass(name = "ObjectInputFile", module = "object_store", weakref)]
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
#[pyclass(name = "ObjectOutputStream", module = "object_store", weakref)]
pub struct ObjectOutputStream {
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
        wait_for_future(py, self.writer.shutdown()).map_err(ObjectStoreError::from)?;
        self.closed = true;
        Ok(())
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
        wait_for_future(py, self.writer.write_all(&data)).map_err(ObjectStoreError::from)?;
        Ok(len)
    }

    fn flush(&mut self, py: Python) -> PyResult<()> {
        Ok(wait_for_future(py, self.writer.flush()).map_err(ObjectStoreError::from)?)
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

#[derive(Debug)]
pub struct PyFileLikeObject {
    inner: PyObject,
    is_text_io: bool,
}

/// Wraps a `PyObject`, and implements read, seek, and write for it.
impl PyFileLikeObject {
    /// Creates an instance of a `PyFileLikeObject` from a `PyObject`.
    /// To assert the object has the required methods methods,
    /// instantiate it with `PyFileLikeObject::require`
    pub fn new(object: PyObject) -> PyResult<Self> {
        Python::with_gil(|py| {
            let io = PyModule::import(py, "io")?;
            let text_io = io.getattr("TextIOBase")?;

            let text_io_type = text_io.extract::<&PyType>()?;
            let is_text_io = text_io_type.is_instance(&object)?;

            Ok(PyFileLikeObject {
                inner: object,
                is_text_io,
            })
        })
    }

    /// Same as `PyFileLikeObject::new`, but validates that the underlying
    /// python object has a `read`, `write`, and `seek` methods in respect to parameters.
    /// Will return a `TypeError` if object does not have `read`, `seek`, and `write` methods.
    pub fn with_requirements(
        object: PyObject,
        read: bool,
        write: bool,
        seek: bool,
    ) -> PyResult<Self> {
        Python::with_gil(|py| {
            if read && object.getattr(py, "read").is_err() {
                return Err(PyErr::new::<PyTypeError, _>(
                    "Object does not have a .read() method.",
                ));
            }

            if seek && object.getattr(py, "seek").is_err() {
                return Err(PyErr::new::<PyTypeError, _>(
                    "Object does not have a .seek() method.",
                ));
            }

            if write && object.getattr(py, "write").is_err() {
                return Err(PyErr::new::<PyTypeError, _>(
                    "Object does not have a .write() method.",
                ));
            }

            Self::new(object)
        })
    }
}

/// Extracts a string repr from, and returns an IO error to send back to rust.
fn pyerr_to_io_err(e: PyErr) -> io::Error {
    Python::with_gil(|py| {
        let e_as_object: PyObject = e.into_py(py);

        match e_as_object.call_method(py, "__str__", (), None) {
            Ok(repr) => match repr.extract::<String>(py) {
                Ok(s) => io::Error::new(io::ErrorKind::Other, s),
                Err(_e) => io::Error::new(io::ErrorKind::Other, "An unknown error has occurred"),
            },
            Err(_) => io::Error::new(io::ErrorKind::Other, "Err doesn't have __str__"),
        }
    })
}

impl Read for PyFileLikeObject {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, io::Error> {
        Python::with_gil(|py| {
            if self.is_text_io {
                let res = self
                    .inner
                    .call_method(py, "read", (buf.len(),), None)
                    .map_err(pyerr_to_io_err)?;
                let py_string: &PyString = res
                    .cast_as(py)
                    .expect("Expecting to be able to downcast into str from read result.");
                let bytes = py_string.to_str().unwrap().as_bytes();
                buf.write_all(bytes)?;
                Ok(bytes.len())
            } else {
                let res = self
                    .inner
                    .call_method(py, "read", (buf.len(),), None)
                    .map_err(pyerr_to_io_err)?;
                let py_bytes: &PyBytes = res
                    .cast_as(py)
                    .expect("Expecting to be able to downcast into bytes from read result.");
                let bytes = py_bytes.as_bytes();
                buf.write_all(bytes)?;
                Ok(bytes.len())
            }
        })
    }
}

impl Write for PyFileLikeObject {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        Python::with_gil(|py| {
            let arg = if self.is_text_io {
                let s = std::str::from_utf8(buf)
                    .expect("Tried to write non-utf8 data to a TextIO object.");
                PyString::new(py, s).to_object(py)
            } else {
                PyBytes::new(py, buf).to_object(py)
            };

            let number_bytes_written = self
                .inner
                .call_method(py, "write", (arg,), None)
                .map_err(pyerr_to_io_err)?;

            number_bytes_written.extract(py).map_err(pyerr_to_io_err)
        })
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        Python::with_gil(|py| {
            self.inner
                .call_method(py, "flush", (), None)
                .map_err(pyerr_to_io_err)?;

            Ok(())
        })
    }
}

impl Seek for PyFileLikeObject {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        Python::with_gil(|py| {
            let (whence, offset) = match pos {
                SeekFrom::Start(i) => (0, i as i64),
                SeekFrom::Current(i) => (1, i as i64),
                SeekFrom::End(i) => (2, i as i64),
            };

            let new_position = self
                .inner
                .call_method(py, "seek", (offset, whence), None)
                .map_err(pyerr_to_io_err)?;

            new_position.extract(py).map_err(pyerr_to_io_err)
        })
    }
}
