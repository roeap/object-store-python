from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.fs as fs

class Path:
    def __init__(self, raw: str | list[str]) -> None: ...
    def child(self, part: str) -> Path: ...

class ObjectStore:
    """A uniform API for interacting with object storage services and local files."""

    def __init__(self, root: str, options: dict[str, str] | None = None) -> None: ...
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

class ObjectInputFile:
    @property
    def closed(self) -> bool: ...
    @property
    def mode(self) -> str: ...
    def isatty(self) -> bool: ...
    def readable(self) -> bool: ...
    def seekable(self) -> bool: ...
    def tell(self) -> int: ...
    def size(self) -> int: ...
    def seek(self, position: int, whence: int) -> int: ...
    def read(self, nbytes: int) -> bytes: ...

class ArrowFileSystemHandler:
    """Implementation of pyarrow.fs.FileSystemHandler for use with pyarrow.fs.PyFileSystem"""

    def __init__(self, root: str, options: dict[str, str] | None = None) -> None: ...
    def copy_file(self, src: str, dst: str) -> None:
        """Copy a file.

        If the destination exists and is a directory, an error is returned. Otherwise, it is replaced.
        """
    def create_dir(self, path: str, *, recursive: bool = True) -> None:
        """Create a directory and subdirectories.

        This function succeeds if the directory already exists.
        """
    def delete_dir(self, path: str) -> None:
        """Delete a directory and its contents, recursively."""
    def delete_file(self, path: str) -> None:
        """Delete a file."""
    def equals(self, other) -> bool: ...
    def delete_dir_contents(self, path: str, *, accept_root_dir: bool = False, missing_dir_ok: bool = False) -> None:
        """Delete a directory's contents, recursively.

        Like delete_dir, but doesn't delete the directory itself.
        """
    def get_file_info(self, paths: list[str]) -> list[fs.FileInfo]:
        """Get info for the given files.

        A non-existing or unreachable file returns a FileStat object and has a FileType of value NotFound.
        An exception indicates a truly exceptional condition (low-level I/O error, etc.).
        """
    def get_file_info_selector(
        self, base_dir: str, allow_not_found: bool = False, recursive: bool = False
    ) -> list[fs.FileInfo]:
        """Get info for the given files.

        A non-existing or unreachable file returns a FileStat object and has a FileType of value NotFound.
        An exception indicates a truly exceptional condition (low-level I/O error, etc.).
        """
    def move_file(self, src: str, dest: str) -> None:
        """Move / rename a file or directory.

        If the destination exists: - if it is a non-empty directory, an error is returned - otherwise,
        if it has the same type as the source, it is replaced - otherwise, behavior is
        unspecified (implementation-dependent).
        """
    def normalize_path(self, path: str) -> str:
        """Normalize filesystem path."""
    def open_input_file(self, path: str) -> ObjectInputFile:
        """Open an input file for random access reading."""

class ObjectMeta:
    """The metadata that describes an object."""

    @property
    def size(self) -> int:
        """The size in bytes of the object"""
    @property
    def location(self) -> Path:
        """The full path to the object"""
    @property
    def last_modified(self) -> int:
        """The last modified time"""

class ListResult:
    """Result of a list call that includes objects and prefixes (directories)"""

    @property
    def common_prefixes(self) -> list[Path]:
        """Prefixes that are common (like directories)"""
    @property
    def objects(self) -> list[ObjectMeta]:
        """Object metadata for the listing"""
