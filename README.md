# object-store-python

[![CI][ci-img]][ci-link]
[![code style: black][black-img]][black-link]
[![PyPI - Downloads][pypi-img]][pypi-link]

Python bindings and integrations for the excellent [`object_store`][object-store] crate.
The main idea is to provide a common interface to various storage backends including the
objects stores from most major cloud providers. The APIs are very focussed and taylored
towards modern cloud native applications by hiding away many features (and complexities)
encountered in full fledges file systems.

Among the included backend are:

- Amazon S3 and S3 compliant APIs
- Google Cloud Storage Buckets
- Azure Blob Gen1 and Gen2 accounts (including ADLS Gen2)
- local storage
- in-memory store

## Installation

The `object-store-python` package is available on PyPI and can be installed via

```sh
poetry add object-store-python
```

or using pip

```sh
pip install object-store-python
```

## Usage

The main [`ObjectStore`](#object-store-python) API mirrors the native [`object_store`][object-store]
implementation, with some slight adjustments for ease of use in python programs.

### `ObjectStore` api

```py
from object_store import ObjectStore, ObjectMeta

# we use an in-memory store for demonstration purposes.
# data will not be persisted and is not shared across store instances
store = ObjectStore("memory://")

store.put("data", b"some data")

data = store.get("data")
assert data == b"some data"

blobs = store.list()

meta: ObjectMeta = store.head("data")

range = store.get_range("data", start=0, length=4)
assert range == b"some"

store.copy("data", "copied")
copied = store.get("copied")
assert copied == data
```

### with `pyarrow`

```py
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from object_store import ArrowFileSystemHandler

table = pa.table({"a": range(10), "b": np.random.randn(10), "c": [1, 2] * 5})

base = Path.cwd()
store = fs.PyFileSystem(ArrowFileSystemHandler(str(base.absolute())))

pq.write_table(table.slice(0, 5), "data/data1.parquet", filesystem=store)
pq.write_table(table.slice(5, 10), "data/data2.parquet", filesystem=store)

dataset = ds.dataset("data", format="parquet", filesystem=store)
```

## Development

### Prerequisites

- [poetry](https://python-poetry.org/docs/)
- [Rust toolchain](https://www.rust-lang.org/tools/install)
- [just](https://github.com/casey/just#readme)

### Running tests

If you do not have [`just`](<(https://github.com/casey/just#readme)>) installed and do not wish to install it,
have a look at the [`justfile`](https://github.com/roeap/object-store-python/blob/main/justfile) to see the raw commands.

To set up the development environment, and install a dev version of the native package just run:

```sh
just init
```

This will also configure [`pre-commit`](https://pre-commit.com/) hooks in the repository.

To run the rust as well as python tests:

```sh
just test
```

[object-store]: https://crates.io/crates/object_store
[pypi-img]: https://img.shields.io/pypi/dm/object-store-python
[pypi-link]: https://pypi.org/project/object-store-python/
[ci-img]: https://github.com/roeap/object-store-python/actions/workflows/ci.yaml/badge.svg
[ci-link]: https://github.com/roeap/object-store-python/actions/workflows/ci.yaml
[black-img]: https://img.shields.io/badge/code%20style-black-000000.svg
[black-link]: https://github.com/psf/black
