use std::future::Future;

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectMeta, Result as ObjectStoreResult};
use pyo3::prelude::*;
use tokio::runtime::Runtime;

/// Utility to collect rust futures with GIL released
pub fn wait_for_future<F: Future>(py: Python, f: F) -> F::Output
where
    F: Send,
    F::Output: Send,
{
    let rt = Runtime::new().unwrap();
    py.allow_threads(|| rt.block_on(f))
}

/// List directory
pub async fn flatten_list_stream(
    storage: &DynObjectStore,
    prefix: Option<&Path>,
) -> ObjectStoreResult<Vec<ObjectMeta>> {
    storage
        .list(prefix)
        .await?
        .try_collect::<Vec<ObjectMeta>>()
        .await
}
