use std::future::Future;
use std::sync::Arc;

use futures::future::{join_all, BoxFuture, FutureExt};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{DynObjectStore, ListResult, ObjectMeta, Result as ObjectStoreResult};
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

pub async fn walk_tree(
    storage: Arc<DynObjectStore>,
    path: &Path,
    recursive: bool,
) -> ObjectStoreResult<ListResult> {
    list_with_delimiter_recursive(storage, [path.clone()], recursive).await
}

fn list_with_delimiter_recursive(
    storage: Arc<DynObjectStore>,
    paths: impl IntoIterator<Item = Path>,
    recursive: bool,
) -> BoxFuture<'static, ObjectStoreResult<ListResult>> {
    let mut tasks = vec![];
    for path in paths {
        let store = storage.clone();
        let prefix = path.clone();
        let handle =
            tokio::task::spawn(async move { store.list_with_delimiter(Some(&prefix)).await });
        tasks.push(handle);
    }

    async move {
        let mut results = join_all(tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .fold(
                ListResult {
                    common_prefixes: vec![],
                    objects: vec![],
                },
                |mut acc, res| {
                    acc.common_prefixes.extend(res.common_prefixes);
                    acc.objects.extend(res.objects);
                    acc
                },
            );

        if recursive && !results.common_prefixes.is_empty() {
            let more_result = list_with_delimiter_recursive(
                storage.clone(),
                results.common_prefixes.clone(),
                recursive,
            )
            .await?;
            results.common_prefixes.extend(more_result.common_prefixes);
            results.objects.extend(more_result.objects);
        }

        Ok(results)
    }
    .boxed()
}

pub async fn delete_dir(storage: &DynObjectStore, prefix: &Path) -> ObjectStoreResult<()> {
    // TODO batch delete would be really useful now...
    let mut stream = storage.list(Some(prefix)).await?;
    while let Some(maybe_meta) = stream.next().await {
        let meta = maybe_meta?;
        storage.delete(&meta.location).await?;
    }
    Ok(())
}

/// get bytes from a location
pub async fn get_bytes(storage: &DynObjectStore, path: &Path) -> ObjectStoreResult<Vec<u8>> {
    Ok(storage.get(path).await?.bytes().await?.into())
}
