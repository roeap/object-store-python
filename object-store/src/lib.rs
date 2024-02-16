use object_store_internal::{
    ArrowFileSystemHandler, ObjectInputFile, ObjectOutputStream, PyClientOptions, PyListResult,
    PyObjectMeta, PyObjectStore, PyPath,
};
use pyo3::prelude::*;

#[pymodule]
fn _internal(_py: Python, m: &PyModule) -> PyResult<()> {
    // Register the python classes
    m.add_class::<PyClientOptions>()?;
    m.add_class::<PyObjectStore>()?;
    m.add_class::<PyPath>()?;
    m.add_class::<PyObjectMeta>()?;
    m.add_class::<PyListResult>()?;
    m.add_class::<ArrowFileSystemHandler>()?;
    m.add_class::<ObjectInputFile>()?;
    m.add_class::<ObjectOutputStream>()?;

    Ok(())
}
