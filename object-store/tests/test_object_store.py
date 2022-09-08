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

    with pytest.raises(Exception):
        store.get_range(location, 200, 100)

    head = store.head(location)
    assert head.location == location
    assert head.size == len(expected_data)

    store.delete(location)

    files = store.list()
    assert len(files) == 0

    with pytest.raises(FileNotFoundError):
        store.get(location)

    with pytest.raises(FileNotFoundError):
        store.head(location)


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
