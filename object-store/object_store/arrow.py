import pyarrow as pa
import pyarrow.fs as fs

from ._internal import ArrowFileSystemHandler as _ArrowFileSystemHandler


# NOTE the order of inheritance is important to make sure the right methods are overwritten.
# _ArrowFileSystemHandler mus be the first element in the inherited classes, we need to also
# inherit form fs.FileSystemHandler to pass pyarrow's type checks.
class ArrowFileSystemHandler(_ArrowFileSystemHandler, fs.FileSystemHandler):
    def move(self, src: str, dest: str) -> None:
        return _ArrowFileSystemHandler.move_file(self, src, dest)

    def open_input_file(self, path: str) -> pa.PythonFile:
        file = _ArrowFileSystemHandler.open_input_file(self, path)
        return pa.PythonFile(file)

    def open_input_stream(self, path: str) -> pa.PythonFile:
        file = _ArrowFileSystemHandler.open_input_file(self, path)
        return pa.PythonFile(file)

    def get_file_info_selector(self, selector: fs.FileSelector) -> list[fs.FileInfo]:
        return _ArrowFileSystemHandler.get_file_info_selector(
            self, selector.base_dir, selector.allow_not_found, selector.recursive
        )
