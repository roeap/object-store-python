# Object Stores

```py
from object_store import ObjectStore

store = ObjectStore(".")

store.put("path", b"data")
loaded = store.get("path")

assert loaded == b"data"
```

## Integrations

### pyarrow

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
```

### Mlflow

### Dagster
