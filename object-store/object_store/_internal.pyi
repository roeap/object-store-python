class Path:
    def __init__(self, raw: str | list[str]) -> None: ...
    def child(self, part: str) -> Path: ...

class ObjectStore:
    def __init__(self, root: str) -> None: ...
    def get(self, location: Path) -> bytes:
        """Return the bytes that are stored at the specified location."""
    def get_range(self, location: Path, start: int, length: int) -> bytes:
        """Return the bytes that are stored at the specified location in the given byte range."""
    def put(self, location: Path, bytes: bytes) -> None:
        """Save the provided bytes to the specified location."""
    def list(self, prefix: Path | None) -> list[ObjectMeta]:
        """List all the objects with the given prefix.

        Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix
        of `foo/bar/x` but not of `foo/bar_baz/x`.
        """
    def head(self, location: Path) -> ObjectMeta:
        """Return the metadata for the specified location"""
    def list_with_delimiter(self, prefix: Path | None) -> ListResult:
        """List objects with the given prefix and an implementation specific
        delimiter. Returns common prefixes (directories) in addition to object
        metadata.

        Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix
        of `foo/bar/x` but not of `foo/bar_baz/x`.
        """
    def delete(self, location: Path) -> None:
        """Delete the object at the specified location."""
    def copy(self, src: Path, dst: Path) -> None:
        """Copy an object from one path to another in the same object store.

        If there exists an object at the destination, it will be overwritten.
        """
    def copy_if_not_exists(self, src: Path, dst: Path) -> None:
        """Copy an object from one path to another, only if destination is empty.

        Will return an error if the destination already has an object.
        """
    def rename(self, src: Path, dst: Path) -> None:
        """Move an object from one path to another in the same object store.

        By default, this is implemented as a copy and then delete source. It may not
        check when deleting source that it was the same object that was originally copied.

        If there exists an object at the destination, it will be overwritten.
        """
    def rename_if_not_exists(self, src: Path, dst: Path) -> None:
        """Move an object from one path to another in the same object store.

        Will return an error if the destination already has an object.
        """

class ObjectMeta:
    @property
    def size(self) -> int: ...
    @property
    def location(self) -> Path: ...
    @property
    def last_modified(self) -> int: ...

class ListResult:
    @property
    def common_prefixes(self) -> list[Path]: ...
    @property
    def objects(self) -> list[ObjectMeta]: ...
