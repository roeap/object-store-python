use std::collections::HashMap;
use std::sync::Arc;

use crate::builder::ObjectStoreBuilder;
use crate::utils::{delete_dir, walk_tree};
use crate::{ObjectStoreError, PyClientOptions};

use object_store::path::Path;
use object_store::{DynObjectStore, Error as InnerObjectStoreError, ListResult, MultipartUpload};
use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyBytes};
use tokio::runtime::Runtime;

#[pyclass(subclass, weakref)]
#[derive(Debug, Clone)]
pub struct ArrowFileSystemHandler {
    inner: Arc<DynObjectStore>,
    rt: Arc<Runtime>,
    root_url: String,
    options: Option<HashMap<String, String>>,
}

#[pymethods]
impl ArrowFileSystemHandler {
    #[new]
    #[pyo3(signature = (root, options = None, client_options = None))]
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

    fn get_type_name(&self) -> String {
        "object-store".into()
    }

    fn normalize_path(&self, path: String) -> PyResult<String> {
        let path = Path::parse(path).map_err(ObjectStoreError::from)?;
        Ok(path.to_string())
    }

    fn copy_file(&self, src: String, dest: String) -> PyResult<()> {
        let from_path = Path::from(src);
        let to_path = Path::from(dest);
        self.rt
            .block_on(self.inner.copy(&from_path, &to_path))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    fn create_dir(&self, _path: String, _recursive: bool) -> PyResult<()> {
        // TODO creating a dir should be a no-op with object_store, right?
        Ok(())
    }

    fn delete_dir(&self, path: String) -> PyResult<()> {
        let path = Path::from(path);
        self.rt
            .block_on(delete_dir(self.inner.as_ref(), &path))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    fn delete_file(&self, path: String) -> PyResult<()> {
        let path = Path::from(path);
        self.rt
            .block_on(self.inner.delete(&path))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    fn equals(&self, other: &ArrowFileSystemHandler) -> PyResult<bool> {
        Ok(format!("{:?}", self) == format!("{:?}", other))
    }

    fn get_file_info<'py>(
        &self,
        paths: Vec<String>,
        py: Python<'py>,
    ) -> PyResult<Vec<Bound<'py, pyo3::PyAny>>> {
        let fs = PyModule::import_bound(py, "pyarrow.fs")?;
        let file_types = fs.getattr("FileType")?;

        let to_file_info = |loc: String, type_: Bound<'_, PyAny>, kwargs: HashMap<&str, i64>| {
            fs.call_method(
                "FileInfo",
                (loc, type_),
                Some(&kwargs.into_py_dict_bound(py)),
            )
        };

        let mut infos = Vec::new();
        for file_path in paths {
            let path = Path::from(file_path);
            let listed = self
                .rt
                .block_on(self.inner.list_with_delimiter(Some(&path)))
                .map_err(ObjectStoreError::from)?;

            // TODO is there a better way to figure out if we are in a directory?
            if listed.objects.is_empty() && listed.common_prefixes.is_empty() {
                let maybe_meta = self.rt.block_on(self.inner.head(&path));
                match maybe_meta {
                    Ok(meta) => {
                        let kwargs = HashMap::from([
                            ("size", meta.size as i64),
                            (
                                "mtime_ns",
                                meta.last_modified.timestamp_nanos_opt().unwrap(),
                            ),
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

    #[pyo3(signature = (base_dir, allow_not_found = false, recursive = false))]
    fn get_file_info_selector<'py>(
        &self,
        base_dir: String,
        allow_not_found: bool,
        recursive: bool,
        py: Python<'py>,
    ) -> PyResult<Vec<Bound<'py, pyo3::PyAny>>> {
        let fs = PyModule::import_bound(py, "pyarrow.fs")?;
        let file_types = fs.getattr("FileType")?;

        let to_file_info =
            |loc: String, type_: Bound<'_, pyo3::PyAny>, kwargs: HashMap<&str, i64>| {
                fs.call_method(
                    "FileInfo",
                    (loc, type_),
                    Some(&kwargs.into_py_dict_bound(py)),
                )
            };

        let path = Path::from(base_dir);
        let list_result = match self
            .rt
            .block_on(walk_tree(self.inner.clone(), &path, recursive))
        {
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
                        (
                            "mtime_ns",
                            meta.last_modified.timestamp_nanos_opt().unwrap(),
                        ),
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

    fn move_file(&self, src: String, dest: String) -> PyResult<()> {
        let from_path = Path::from(src);
        let to_path = Path::from(dest);
        // TODO check the if not exists semantics
        self.rt
            .block_on(self.inner.rename(&from_path, &to_path))
            .map_err(ObjectStoreError::from)?;
        Ok(())
    }

    fn open_input_file(&self, path: String) -> PyResult<ObjectInputFile> {
        let path = Path::from(path);
        let file = self
            .rt
            .block_on(ObjectInputFile::try_new(
                self.rt.clone(),
                self.inner.clone(),
                path,
            ))
            .map_err(ObjectStoreError::from)?;
        Ok(file)
    }

    #[pyo3(signature = (path, metadata = None))]
    fn open_output_stream(
        &self,
        path: String,
        #[allow(unused)] metadata: Option<HashMap<String, String>>,
    ) -> PyResult<ObjectOutputStream> {
        let path = Path::from(path);
        let file = self
            .rt
            .block_on(ObjectOutputStream::try_new(
                self.rt.clone(),
                self.inner.clone(),
                path,
            ))
            .map_err(ObjectStoreError::from)?;
        Ok(file)
    }

    pub fn __getnewargs__(&self) -> PyResult<(String, Option<HashMap<String, String>>)> {
        Ok((self.root_url.clone(), self.options.clone()))
    }
}

// TODO the C++ implementation track an internal lock on all random access files, DO we need this here?
// TODO add buffer to store data ...
#[pyclass(weakref)]
#[derive(Debug, Clone)]
pub struct ObjectInputFile {
    store: Arc<DynObjectStore>,
    rt: Arc<Runtime>,
    path: Path,
    content_length: i64,
    #[pyo3(get)]
    closed: bool,
    pos: i64,
    #[pyo3(get)]
    mode: String,
}

impl ObjectInputFile {
    pub async fn try_new(
        rt: Arc<Runtime>,
        store: Arc<DynObjectStore>,
        path: Path,
    ) -> Result<Self, ObjectStoreError> {
        // Issue a HEAD Object to get the content-length and ensure any
        // errors (e.g. file not found) don't wait until the first read() call.
        let meta = store.head(&path).await?;
        let content_length = meta.size as i64;
        // TODO make sure content length is valid
        // https://github.com/apache/arrow/blob/f184255cbb9bf911ea2a04910f711e1a924b12b8/cpp/src/arrow/filesystem/s3fs.cc#L1083
        Ok(Self {
            store,
            rt,
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

    #[pyo3(signature = (offset, whence = 0))]
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
                self.pos = self.content_length - offset;
            }
            _ => {
                return Err(PyValueError::new_err(
                    "'whence' must be between  0 <= whence <= 2.",
                ));
            }
        }
        Ok(self.pos)
    }

    #[pyo3(signature = (nbytes = None))]
    fn read(&mut self, nbytes: Option<i64>) -> PyResult<Py<PyAny>> {
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
        let data = if nbytes > 0 {
            self.rt
                .block_on(self.store.get_range(&self.path, range))
                .map_err(ObjectStoreError::from)?
        } else {
            "".into()
        };
        Python::with_gil(|py| Ok(PyBytes::new_bound(py, data.as_ref()).into_py(py)))
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
    pub store: Arc<DynObjectStore>,
    rt: Arc<Runtime>,
    pub path: Path,
    writer: Box<dyn MultipartUpload>,
    pos: i64,
    #[pyo3(get)]
    closed: bool,
    #[pyo3(get)]
    mode: String,
}

impl ObjectOutputStream {
    pub async fn try_new(
        rt: Arc<Runtime>,
        store: Arc<DynObjectStore>,
        path: Path,
    ) -> Result<Self, ObjectStoreError> {
        match store.put_multipart(&path).await {
            Ok(writer) => Ok(Self {
                store,
                rt,
                path,
                writer,
                pos: 0,
                closed: false,
                mode: "wb".into(),
            }),
            Err(err) => Err(ObjectStoreError::ObjectStore(err)),
        }
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
    fn close(&mut self) -> PyResult<()> {
        self.closed = true;
        match self.rt.block_on(self.writer.complete()) {
            Ok(_) => Ok(()),
            Err(err) => {
                self.rt
                    .block_on(self.writer.abort())
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

    fn seek(&mut self, _offset: i64, _whence: i64) -> PyResult<i64> {
        self.check_closed()?;
        Err(PyNotImplementedError::new_err("'seek' not implemented"))
    }

    fn read(&mut self, _nbytes: Option<i64>) -> PyResult<()> {
        self.check_closed()?;
        Err(PyNotImplementedError::new_err("'read' not implemented"))
    }

    fn write(&mut self, data: Bound<'_, PyBytes>) -> PyResult<i64> {
        self.check_closed()?;
        let bytes = data.as_bytes().to_vec();
        let len = bytes.len() as i64;
        match self.rt.block_on(self.writer.put_part(bytes.into())) {
            Ok(_) => Ok(len),
            Err(err) => {
                self.rt
                    .block_on(self.writer.abort())
                    .map_err(ObjectStoreError::from)?;
                Err(ObjectStoreError::from(err).into())
            }
        }
    }

    fn flush(&mut self) -> PyResult<()> {
        match self.rt.block_on(self.writer.complete()) {
            Ok(_) => Ok(()),
            Err(err) => {
                self.rt
                    .block_on(self.writer.abort())
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
