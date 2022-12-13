from __future__ import annotations

from io import BytesIO
from typing import List

# NOTE aliasing the imports with 'as' makes them public in the eyes
# of static code checkers. Thus we avoid listing them with __all__ = ...
from ._internal import ListResult as ListResult
from ._internal import ObjectMeta as ObjectMeta
from ._internal import ObjectStore as _ObjectStore
from ._internal import Path as Path

try:
    import importlib.metadata as importlib_metadata
except ImportError:
    import importlib_metadata  # type: ignore

__version__ = importlib_metadata.version(__name__)

PathLike = str | List[str] | Path
BytesLike = bytes | BytesIO

DELIMITER = "/"


def _as_path(raw: PathLike) -> Path:
    if isinstance(raw, str):
        return Path(raw)
    if isinstance(raw, list):
        return Path(DELIMITER.join(raw))
    if isinstance(raw, Path):
        return raw
    raise ValueError(f"Cannot convert type '{type(raw)}' to type Path.")


def _as_bytes(raw: BytesLike) -> bytes:
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, BytesIO):
        return raw.read()
    raise ValueError(f"Cannot convert type '{type(raw)}' to type bytes.")


class ObjectStore(_ObjectStore):
    """A uniform API for interacting with object storage services and local files.

    backed by the Rust object_store crate."""

    def head(self, location: PathLike) -> ObjectMeta:
        """Return the metadata for the specified location.

        Args:
            location (PathLike): path / key to storage location

        Returns:
            ObjectMeta: metadata for object at location
        """
        return super().head(_as_path(location))

    def get(self, location: PathLike) -> bytes:
        """Return the bytes that are stored at the specified location.

        Args:
            location (PathLike): path / key to storage location

        Returns:
            bytes: raw data stored in location
        """
        return super().get(_as_path(location))

    def get_range(self, location: PathLike, start: int, length: int) -> bytes:
        """Return the bytes that are stored at the specified location in the given byte range.

        Args:
            location (PathLike): path / key to storage location
            start (int): zero-based start index
            length (int): length of the byte range

        Returns:
            bytes: raw data range stored in location
        """
        return super().get_range(_as_path(location), start, length)

    def put(self, location: PathLike, bytes: BytesLike) -> None:
        """Save the provided bytes to the specified location.

        Args:
            location (PathLike): path / key to storage location
            bytes (BytesLike): data to be written to location
        """
        return super().put(_as_path(location), _as_bytes(bytes))

    def delete(self, location: PathLike) -> None:
        """Delete the object at the specified location.

        Args:
            location (PathLike): path / key to storage location
        """
        return super().delete(_as_path(location))

    def list(self, prefix: PathLike | None = None) -> list[ObjectMeta]:
        """List all the objects with the given prefix.

        Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix
        of `foo/bar/x` but not of `foo/bar_baz/x`.

        Args:
            prefix (PathLike | None, optional): path prefix to filter limit list results. Defaults to None.

        Returns:
            list[ObjectMeta]: ObjectMeta for all objects under the listed path
        """
        prefix_ = _as_path(prefix) if prefix else None
        return super().list(prefix_)

    def list_with_delimiter(self, prefix: PathLike | None = None) -> ListResult:
        """List objects with the given prefix and an implementation specific
        delimiter. Returns common prefixes (directories) in addition to object
        metadata.

        Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix
        of `foo/bar/x` but not of `foo/bar_baz/x`.

        Args:
            prefix (PathLike | None, optional): path prefix to filter limit list results. Defaults to None.

        Returns:
            list[ObjectMeta]: ObjectMeta for all objects under the listed path
        """
        prefix_ = _as_path(prefix) if prefix else None
        return super().list_with_delimiter(prefix_)

    def copy(self, src: PathLike, dst: PathLike) -> None:
        """Copy an object from one path to another in the same object store.

        If there exists an object at the destination, it will be overwritten.

        Args:
            src (PathLike): source path
            dst (PathLike): destination path
        """
        return super().copy(_as_path(src), _as_path(dst))

    def copy_if_not_exists(self, src: PathLike, dst: PathLike) -> None:
        """Copy an object from one path to another, only if destination is empty.

        Will return an error if the destination already has an object.

        Args:
            src (PathLike): source path
            dst (PathLike): destination path
        """
        return super().copy_if_not_exists(_as_path(src), _as_path(dst))

    def rename(self, src: PathLike, dst: PathLike) -> None:
        """Move an object from one path to another in the same object store.

        By default, this is implemented as a copy and then delete source. It may not
        check when deleting source that it was the same object that was originally copied.

        If there exists an object at the destination, it will be overwritten.

        Args:
            src (PathLike): source path
            dst (PathLike): destination path
        """
        return super().rename(_as_path(src), _as_path(dst))

    def rename_if_not_exists(self, src: PathLike, dst: PathLike) -> None:
        """Move an object from one path to another in the same object store.

        Will return an error if the destination already has an object.

        Args:
            src (PathLike): source path
            dst (PathLike): destination path
        """
        return super().rename_if_not_exists(_as_path(src), _as_path(dst))
