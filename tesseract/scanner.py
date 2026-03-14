"""File system scanner - walks directories and collects metadata for deduplication."""

import os
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Generator, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class FileEntry:
    """Represents a single file's metadata for deduplication analysis."""
    path: Path
    relative_path: str
    size: int
    filename: str
    extension: str
    modified_time: float
    partial_hash: Optional[str] = None
    full_hash: Optional[str] = None
    group_id: Optional[str] = None
    is_master: bool = False

    @property
    def dedup_key(self) -> tuple:
        """
        Key for grouping potential duplicates.
        Files must match on size, filename, AND extension to be candidates.
        """
        return (self.size, self.filename, self.extension)


class FileScanner:
    """Scans a directory tree and yields FileEntry objects for each file."""

    def __init__(self, root_path: Path, exclude_patterns: Optional[List[str]] = None):
        self.root_path = Path(root_path).resolve()
        self.exclude_patterns = exclude_patterns or []
        if not self.root_path.is_dir():
            raise ValueError(f"Root path is not a directory: {self.root_path}")

    def _is_excluded(self, path: Path) -> bool:
        """Check if a path matches any exclusion pattern."""
        path_str = str(path)
        for pattern in self.exclude_patterns:
            if pattern in path_str:
                return True
        return False

    def scan(self) -> Generator[FileEntry, None, None]:
        """
        Walk directory tree and yield FileEntry for each regular file.
        Skips symlinks, inaccessible files, and excluded patterns.
        """
        for dirpath, dirnames, filenames in os.walk(self.root_path):
            # Remove excluded directories in-place to prevent descending
            dirnames[:] = [
                d for d in dirnames
                if not self._is_excluded(Path(dirpath) / d)
            ]

            for filename in filenames:
                filepath = Path(dirpath) / filename

                if self._is_excluded(filepath):
                    continue

                # Skip symlinks for safety
                if filepath.is_symlink():
                    logger.debug(f"Skipping symlink: {filepath}")
                    continue

                try:
                    stat = filepath.stat()
                    if not filepath.is_file():
                        continue
                    yield FileEntry(
                        path=filepath,
                        relative_path=str(filepath.relative_to(self.root_path)),
                        size=stat.st_size,
                        filename=filename,
                        extension=filepath.suffix.lower(),
                        modified_time=stat.st_mtime,
                    )
                except (OSError, PermissionError) as e:
                    logger.warning(f"Cannot access file {filepath}: {e}")

    def count_files(self) -> int:
        """Count total files without collecting full metadata. Useful for progress."""
        count = 0
        for dirpath, dirnames, filenames in os.walk(self.root_path):
            dirnames[:] = [
                d for d in dirnames
                if not self._is_excluded(Path(dirpath) / d)
            ]
            for filename in filenames:
                filepath = Path(dirpath) / filename
                if not self._is_excluded(filepath) and not filepath.is_symlink():
                    count += 1
        return count
