# object-store-python

Recently the excellent [`object_store`](https://crates.io/crates/object_store) crate has been
[donated](https://www.influxdata.com/blog/rust-object-store-donation/) to the Apache Software Foundation.

## Prerequisites

- [poetry](https://python-poetry.org/docs/)
- [Rust toolchain](https://www.rust-lang.org/tools/install)
- [just](https://github.com/casey/just#readme)

## Development

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

## Usage

### `ObjectStore` api

### with `pyarrow`

```py
from pathlib import Path as PythonPath

import numpy as np
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from object_store import ArrowFileSystemHandler

table = pa.table({"a": range(10), "b": np.random.randn(10), "c": [1, 2] * 5})

base = PythonPath.cwd()
store = fs.PyFileSystem(ArrowFileSystemHandler(str(base.absolute())))

pq.write_table(table.slice(0, 5), "data/data1.parquet", filesystem=store)
pq.write_table(table.slice(5, 10), "data/data2.parquet", filesystem=store)

dataset = ds.dataset("data", format="parquet", filesystem=store)
```

## TODO

- [ ] walk tree in `get_file_info_selector`
- [ ] WHen to do abort with multi part writer
