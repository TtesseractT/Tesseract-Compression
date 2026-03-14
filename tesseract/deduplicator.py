"""Multi-stage duplicate detection engine.

Deduplication strategy (ordered for maximum efficiency):
  1. Group by (size, filename, extension) — metadata only, zero disk I/O
  2. Compute partial hash (first+last 64KB) within each group — minimal I/O
  3. Compute full BLAKE3 for remaining candidates — full I/O, only for true candidates
  4. Files matching ALL criteria are confirmed true duplicates

A file is a "true duplicate" only if it matches on:
  - File size (bytes)
  - Filename (basename)
  - Extension (file type)
  - Content hash (BLAKE3 of full contents)
"""

import hashlib
import logging
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from .scanner import FileEntry
from .hasher import compute_partial_hash, compute_full_hash

logger = logging.getLogger(__name__)


@dataclass
class DuplicateGroup:
    """A group of identical files — one master copy, rest are duplicates."""
    group_id: str
    master: FileEntry
    duplicates: List[FileEntry]
    content_hash: str
    file_size: int
    filename: str
    extension: str

    @property
    def total_files(self) -> int:
        return 1 + len(self.duplicates)

    @property
    def space_savings(self) -> int:
        """Bytes saved by storing only the master copy."""
        return self.file_size * len(self.duplicates)


def _make_group_id(content_hash: str, filename: str, extension: str) -> str:
    """Create a unique group identifier from content + metadata."""
    raw = f"{content_hash}|{filename}|{extension}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _hash_partial_worker(filepath_str: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Worker function for parallel partial hashing. Must be top-level for pickling."""
    try:
        h = compute_partial_hash(Path(filepath_str))
        return (filepath_str, h, None)
    except Exception as e:
        return (filepath_str, None, str(e))


def _hash_full_worker(filepath_str: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Worker function for parallel full hashing. Must be top-level for pickling."""
    try:
        h = compute_full_hash(Path(filepath_str))
        return (filepath_str, h, None)
    except Exception as e:
        return (filepath_str, None, str(e))


class Deduplicator:
    """Multi-stage duplicate file detector with parallel hashing."""

    def __init__(self, workers: int = 4, progress_callback: Optional[Callable] = None):
        self.workers = max(1, workers)
        self.progress_callback = progress_callback or (lambda *a, **kw: None)

    def find_duplicates(self, entries: List[FileEntry], hash_cache=None) -> List[DuplicateGroup]:
        """
        Run the full multi-stage deduplication pipeline.
        Returns a list of DuplicateGroup objects (only groups with 2+ files).
        """
        if not entries:
            return []

        self._hash_cache = hash_cache

        # Stage 1: Group by metadata (size, filename, extension)
        logger.info("Stage 1: Grouping by metadata (size, filename, extension)...")
        meta_groups = self._group_by_metadata(entries)
        candidates = {k: v for k, v in meta_groups.items() if len(v) > 1}
        candidate_count = sum(len(v) for v in candidates.values())
        logger.info(f"  {len(candidates)} groups with {candidate_count} potential duplicates")

        if not candidates:
            return []

        # Stage 2: Partial hash within each metadata group
        logger.info("Stage 2: Computing partial hashes...")
        candidate_entries = [e for group in candidates.values() for e in group]
        self.progress_callback("phase", "partial_hashing", total=len(candidate_entries))
        self._compute_partial_hashes(candidate_entries)
        partial_groups = self._group_by_partial_hash(candidates)
        logger.info(f"  {len(partial_groups)} groups after partial hash filtering")

        if not partial_groups:
            return []

        # Stage 3: Full hash for remaining candidates
        logger.info("Stage 3: Computing full content hashes...")
        remaining_entries = [e for group in partial_groups.values() for e in group]
        self.progress_callback("phase", "full_hashing", total=len(remaining_entries))
        self._compute_full_hashes(remaining_entries)
        final_groups = self._group_by_full_hash(partial_groups)
        logger.info(f"  {len(final_groups)} confirmed duplicate groups")

        # Stage 4: Build DuplicateGroup objects
        result = []
        for group_key, entries_list in final_groups.items():
            if len(entries_list) < 2:
                continue

            master = entries_list[0]
            master.is_master = True
            group_id = _make_group_id(master.full_hash, master.filename, master.extension)
            master.group_id = group_id

            duplicates = entries_list[1:]
            for dup in duplicates:
                dup.is_master = False
                dup.group_id = group_id

            result.append(DuplicateGroup(
                group_id=group_id,
                master=master,
                duplicates=duplicates,
                content_hash=master.full_hash,
                file_size=master.size,
                filename=master.filename,
                extension=master.extension,
            ))

        total_savings = sum(g.space_savings for g in result)
        logger.info(
            f"Found {len(result)} duplicate groups, "
            f"potential space savings: {total_savings / (1024**3):.2f} GB"
        )
        return result

    # ── Stage helpers ──────────────────────────────────────────────

    def _group_by_metadata(self, entries: List[FileEntry]) -> Dict[tuple, List[FileEntry]]:
        """Group files by (size, filename, extension). No disk I/O."""
        groups: Dict[tuple, List[FileEntry]] = defaultdict(list)
        for entry in entries:
            # Skip zero-byte files — they're all "identical" by content
            # but deduplicating them saves nothing
            if entry.size == 0:
                continue
            groups[entry.dedup_key].append(entry)
            self.progress_callback("step", 1)
        return dict(groups)

    def _compute_partial_hashes(self, entries: List[FileEntry]):
        """Compute partial hashes in parallel, skipping entries already cached."""
        # Load from cache first
        need_hash = []
        for e in entries:
            if self._hash_cache:
                cached = self._hash_cache.get_partial(e.path)
                if cached:
                    e.partial_hash = cached
                    self.progress_callback("step", 1)
                    continue
            need_hash.append(e)

        if not need_hash:
            return

        need_hash.sort(key=lambda e: (str(e.path.parent).lower(), e.path.name.lower()))
        path_to_entry = {str(e.path): e for e in need_hash}
        batch_size = 1000
        with ProcessPoolExecutor(max_workers=self.workers) as executor:
            for start in range(0, len(need_hash), batch_size):
                batch = need_hash[start:start + batch_size]
                futures = {
                    executor.submit(_hash_partial_worker, str(e.path)): e
                    for e in batch
                }
                for future in as_completed(futures):
                    filepath_str, hash_val, error = future.result()
                    if error:
                        logger.warning(f"Partial hash failed for {filepath_str}: {error}")
                    elif filepath_str in path_to_entry:
                        path_to_entry[filepath_str].partial_hash = hash_val
                        if self._hash_cache:
                            self._hash_cache.put_partial(Path(filepath_str), hash_val)
                    self.progress_callback("step", 1)

    def _group_by_partial_hash(
        self, meta_groups: Dict[tuple, List[FileEntry]]
    ) -> Dict[tuple, List[FileEntry]]:
        """Refine metadata groups by partial hash — keeps only groups with 2+ matches."""
        result = {}
        for meta_key, entries in meta_groups.items():
            sub: Dict[tuple, List[FileEntry]] = defaultdict(list)
            for e in entries:
                if e.partial_hash:
                    sub[(meta_key, e.partial_hash)].append(e)
            for key, group in sub.items():
                if len(group) > 1:
                    result[key] = group
        return result

    def _compute_full_hashes(self, entries: List[FileEntry]):
        """Compute full BLAKE3 hashes in parallel, skipping entries already hashed (from cache)."""
        need_hash = [e for e in entries if not e.full_hash]
        # Mark already-hashed entries as done in progress bar
        already_done = len(entries) - len(need_hash)
        for _ in range(already_done):
            self.progress_callback("step", 1)
        if not need_hash:
            return
        need_hash.sort(key=lambda e: (str(e.path.parent).lower(), e.path.name.lower()))
        path_to_entry = {str(e.path): e for e in need_hash}
        batch_size = 500
        with ProcessPoolExecutor(max_workers=self.workers) as executor:
            for start in range(0, len(need_hash), batch_size):
                batch = need_hash[start:start + batch_size]
                futures = {
                    executor.submit(_hash_full_worker, str(e.path)): e
                    for e in batch
                }
                for future in as_completed(futures):
                    filepath_str, hash_val, error = future.result()
                    if error:
                        logger.warning(f"Full hash failed for {filepath_str}: {error}")
                    elif filepath_str in path_to_entry:
                        path_to_entry[filepath_str].full_hash = hash_val
                        if self._hash_cache:
                            self._hash_cache.put(Path(filepath_str), hash_val)
                    self.progress_callback("step", 1)

    def _group_by_full_hash(
        self, partial_groups: Dict[tuple, List[FileEntry]]
    ) -> Dict[tuple, List[FileEntry]]:
        """
        Final grouping by full content hash.
        Key is (content_hash, filename, extension, size) to ensure ALL criteria match.
        """
        final: Dict[tuple, List[FileEntry]] = defaultdict(list)
        for _, entries in partial_groups.items():
            for e in entries:
                if e.full_hash:
                    key = (e.full_hash, e.filename, e.extension, e.size)
                    final[key].append(e)
        return {k: v for k, v in final.items() if len(v) > 1}
