from __future__ import annotations

from pathlib import Path

import pytest

from object_store import ObjectStore
from object_store import Path as ObjectStorePath


@pytest.fixture
def object_store(datadir: Path) -> tuple[ObjectStore, Path]:
    return ObjectStore(str(datadir)), datadir


def test_put_get_delete_list(object_store: tuple[ObjectStore, Path]):
    store, _ = object_store

    files = store.list()
    assert len(files) == 0

    expected_data = b"arbitrary data"
    location = ObjectStorePath("test_dir/test_file.json")
    store.put("test_dir/test_file.json", expected_data)

    files = store.list()
    assert len(files) == 1
    assert files[0].location == location

    files = store.list(ObjectStorePath("/"))
    assert len(files) == 1
    assert files[0].location == location

    result = store.list_with_delimiter()
    assert len(result.objects) == 0
    assert len(result.common_prefixes) == 1
    assert result.common_prefixes[0] == ObjectStorePath("test_dir")

    result = store.list_with_delimiter(ObjectStorePath("/"))
    assert len(result.objects) == 0
    assert len(result.common_prefixes) == 1
    assert result.common_prefixes[0] == ObjectStorePath("test_dir")

    files = store.list(ObjectStorePath("test_dir"))
    assert len(files) == 1
    assert files[0].location == location

    files = store.list(ObjectStorePath("something"))
    assert len(files) == 0

    data = store.get(location)
    assert data == expected_data

    range_result = store.get_range(location, 3, 4)
    assert range_result == expected_data[3:7]

    with pytest.raises(Exception):  # noqa: B017
        _ = store.get_range(location, 200, 100)

    head = store.head(location)
    assert head.location == location
    assert head.size == len(expected_data)

    store.delete(location)

    files = store.list()
    assert len(files) == 0

    with pytest.raises(FileNotFoundError):
        _ = store.get(location)

    with pytest.raises(FileNotFoundError):
        _ = store.head(location)


@pytest.mark.asyncio
async def test_put_get_delete_list_async(object_store: tuple[ObjectStore, Path]):
    store, _ = object_store

    files = await store.list_async()
    assert len(files) == 0

    expected_data = b"arbitrary data"
    location = ObjectStorePath("test_dir/test_file.json")
    store.put("test_dir/test_file.json", expected_data)

    files = await store.list_async()
    assert len(files) == 1
    assert files[0].location == location

    files = await store.list_async(ObjectStorePath("/"))
    assert len(files) == 1
    assert files[0].location == location

    result = await store.list_with_delimiter_async()
    assert len(result.objects) == 0
    assert len(result.common_prefixes) == 1
    assert result.common_prefixes[0] == ObjectStorePath("test_dir")

    result = await store.list_with_delimiter_async(ObjectStorePath("/"))
    assert len(result.objects) == 0
    assert len(result.common_prefixes) == 1
    assert result.common_prefixes[0] == ObjectStorePath("test_dir")

    files = await store.list_async(ObjectStorePath("test_dir"))
    assert len(files) == 1
    assert files[0].location == location

    files = await store.list_async(ObjectStorePath("something"))
    assert len(files) == 0

    data = await store.get_async(location)
    assert data == expected_data

    range_result = await store.get_range_async(location, 3, 4)
    assert range_result == expected_data[3:7]

    with pytest.raises(Exception):  # noqa: B017
        _ = await store.get_range_async(location, 200, 100)

    head = await store.head_async(location)
    assert head.location == location
    assert head.size == len(expected_data)

    await store.delete_async(location)

    files = await store.list_async()
    assert len(files) == 0

    with pytest.raises(FileNotFoundError):
        _ = await store.get_async(location)

    with pytest.raises(FileNotFoundError):
        _ = await store.head_async(location)


def test_rename_and_copy(object_store: tuple[ObjectStore, Path]):
    store, _ = object_store

    path1 = ObjectStorePath("test1")
    path2 = ObjectStorePath("test2")
    contents1 = b"cats"
    contents2 = b"dogs"

    # copy() make both objects identical
    store.put(path1, contents1)
    store.put(path2, contents2)
    store.copy(path1, path2)
    new_contents = store.get(path2)
    assert new_contents == contents1

    # rename() copies contents and deletes original
    store.put(path1, contents1)
    store.put(path2, contents2)
    store.rename(path1, path2)
    new_contents = store.get(path2)
    assert new_contents == contents1
    with pytest.raises(FileNotFoundError):
        store.get(path1)

    store.delete(path2)


@pytest.mark.asyncio
async def test_rename_and_copy_async(object_store: tuple[ObjectStore, Path]):
    store, _ = object_store

    path1 = ObjectStorePath("test1")
    path2 = ObjectStorePath("test2")
    contents1 = b"cats"
    contents2 = b"dogs"

    # copy() make both objects identical
    await store.put_async(path1, contents1)
    await store.put_async(path2, contents2)
    await store.copy_async(path1, path2)
    new_contents = await store.get_async(path2)
    assert new_contents == contents1

    # rename() copies contents and deletes original
    await store.put_async(path1, contents1)
    await store.put_async(path2, contents2)
    await store.rename_async(path1, path2)
    new_contents = await store.get_async(path2)
    assert new_contents == contents1
    with pytest.raises(FileNotFoundError):
        _ = await store.get_async(path1)

    await store.delete_async(path2)


@pytest.mark.asyncio
async def test_stream(object_store: tuple[ObjectStore, Path]):
    store, _ = object_store

    data = b"the quick brown fox jumps over the lazy dog," * 5000
    path = "big-data.txt"

    await store.put_async(path, data)

    pos = 0
    async for chunk in store.stream(path):
        size = len(chunk)
        assert chunk == data[pos : pos + size]
        pos += size

    assert pos == len(data)
