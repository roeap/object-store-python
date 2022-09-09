from __future__ import annotations

from typing import List

import pyarrow.fs as pa_fs

from object_store._internal import ArrowFileSystem as _ArrowFileSystem
from object_store._internal import FileSelector as _FileSelector


class ArrowFileSystem(_ArrowFileSystem):
    def get_file_info(
        self, paths_or_selector: str | list[str] | pa_fs.FileSelector
    ) -> pa_fs.FileInfo | list[pa_fs.FileInfo]:
        is_list = True
        selectors = []
        if isinstance(paths_or_selector, str):
            selectors = [_FileSelector(paths_or_selector)]
            is_list = False
        if isinstance(paths_or_selector, List):
            selectors = [_FileSelector(path) for path in paths_or_selector]  # type: ignore

        result = super().get_file_info(selectors)

        if is_list:
            return result

        return result[0]
