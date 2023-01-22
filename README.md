# object-store-python

[![CI][ci-img]][ci-link]
[![code style: black][black-img]][black-link]
![PyPI](https://img.shields.io/pypi/v/object-store-python)
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

### Configuration

As much as possible we aim to make access to various storage backends dependent
only on runtime configuration. The kind of service is always derived from the
url used to specifiy the storage location. Some basic configuration can also be
derived from the url string, dependent on the chosen url format.

```py
from object_store import ObjectStore

storage_options = {
    "azure_storage_account_name": "<my-account-name>",
    "azure_client_id": "<my-client-id>",
    "azure_client_secret": "<my-client-secret>",
    "azure_tenant_id": "<my-tenant-id>"
}

store = ObjectStore("az://<container-name>", storage_options)
```

We can provide the same configuration via the environment.

```py
import os
from object_store import ObjectStore

os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = "<my-account-name>"
os.environ["AZURE_CLIENT_ID"] = "<my-client-id>"
os.environ["AZURE_CLIENT_SECRET"] = "<my-client-secret>"
os.environ["AZURE_TENANT_ID"] = "<my-tenant-id>"

store = ObjectStore("az://<container-name>")
```

#### Azure

The recommended url format is `az://<container>/<path>` and Azure always requieres
`azure_storage_account_name` to be configured.

- [shared key][azure-key]
  - `azure_storage_account_key`
- [service principal][azure-ad]
  - `azure_client_id`
  - `azure_client_secret`
  - `azure_tenant_id`
- [shared access signature][azure-sas]
  - `azure_storage_sas_key` (as provided by StorageExplorer)
- bearer token
  - `azure_storage_token`
- [managed identity][azure-managed]
  - if using user assigned identity one of `azure_client_id`, `azure_object_id`, `azure_msi_resource_id`
  - `use_managed_identity`
- [workload identity][azure-workload]
  - `azure_client_id`
  - `azure_tenant_id`
  - `azure_federated_token_file`

#### S3

The recommended url format is `s3://<bucket>/<path>` S3 storage always requires a
region to be specified via one of `aws_region` or `aws_default_region`.

- [access key][aws-key]
  - `aws_access_key_id`
  - `aws_secret_access_key`
- [session token][aws-sts]
  - `aws_session_token`
- [imds instance metadata][aws-imds]
  - `aws_metadata_endpoint`
- [profile][aws-profile]
  - `aws_profile`

AWS supports [virtual hosting of buckets][aws-virtual], which can be configured by setting
`aws_virtual_hosted_style_request` to "true".

When an alternative implementation or a mocked service like localstack is used, the service
endpoint needs to be explicitly specified via `aws_endpoint`.

#### GCS

The recommended url format is `gs://<bucket>/<path>`.

- service account
  - `google_service_account`

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
[aws-virtual]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
[azure-managed]: https://learn.microsoft.com/en-gb/azure/app-service/overview-managed-identity
[azure-sas]: https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview
[azure-ad]: https://learn.microsoft.com/en-us/azure/storage/blobs/authorize-access-azure-active-directory
[azure-key]: https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key
[azure-workload]: https://learn.microsoft.com/en-us/azure/aks/workload-identity-overview
[aws-imds]: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html
[aws-profile]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2_instance-profiles.html
[aws-sts]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html
[aws-key]: https://docs.aws.amazon.com/accounts/latest/reference/credentials-access-keys-best-practices.html
