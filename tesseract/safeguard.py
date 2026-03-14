"""Failsafe staging system for Tesseract archive creation.

Ensures source files are NEVER modified, deleted, or corrupted during
the encoding process. Uses a multi-stage approach:

  1. PRE-FLIGHT: Verify all source files are readable and snapshot hashes
  2. STAGE: Compress each file into individual shard files in a staging directory
  3. VERIFY SHARDS: Re-read every shard from disk and check CRC32 integrity
  4. VERIFY SOURCE: Re-hash every source file to confirm nothing changed
  5. ASSEMBLE: Combine verified shards into a temporary archive file (.tmp)
  6. VERIFY ARCHIVE: Structurally validate the assembled archive
  7. FINALIZE: Atomically move .tmp -> final path (near-atomic on same filesystem)
  8. CLEANUP: Remove the staging directory

If ANY stage fails, the process stops immediately — source files remain
completely untouched, and only temporary work files need cleanup.

The staging directory and .tmp file are always on the same filesystem
as the output, ensuring the final rename is as close to atomic as possible.
"""

import json
import logging
import os
import shutil
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

STAGING_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB for streaming I/O


@dataclass
class ShardInfo:
    """Metadata for a single staged shard file."""
    shard_id: str
    shard_path: Path
    crc32: int          # CRC32 computed by RE-READING the shard from disk
    size: int           # Shard file size on disk (bytes)
    relative_path: str  # Source file this shard represents ("__solid__" for solid mode)
    original_hash: str  # SHA-256 of the original source file
    original_size: int  # Original source file size
    compressed_size: int  # Compressed data size (shard_size - block_header for normal mode)


class StagingArea:
    """Manages a temporary staging directory for safe, atomic archive creation.

    Shards are written to the staging directory during compression.
    After all shards are written, they are verified by re-reading from
    disk and checking CRC32 checksums. Only after full verification
    are shards assembled into the final archive.

    If the process is interrupted at any point:
      - Source files are untouched (they were only read, never written)
      - The staging directory contains partial work that can be safely deleted
      - No partial/corrupt archive exists at the final output path
    """

    def __init__(self, staging_dir: Path):
        self.staging_dir = staging_dir
        self.shards: Dict[str, ShardInfo] = {}
        self.index_path = staging_dir / "_index.json"

    def create(self):
        """Create the staging directory. Cleans up any remnants from prior runs."""
        if self.staging_dir.exists():
            shutil.rmtree(self.staging_dir)
        self.staging_dir.mkdir(parents=True)
        logger.debug(f"Created staging area: {self.staging_dir}")

    def shard_path(self, shard_id: str) -> Path:
        """Return the filesystem path for a shard file."""
        return self.staging_dir / f"{shard_id}.shard"

    def register_shard(
        self,
        shard_id: str,
        relative_path: str,
        original_hash: str,
        original_size: int,
        compressed_size: int,
    ) -> ShardInfo:
        """Register a shard after it's been written to disk.

        Re-reads the shard file from disk to compute its CRC32, ensuring
        we verify what actually hit the storage medium rather than trusting
        an in-memory buffer.
        """
        path = self.shard_path(shard_id)
        if not path.exists():
            raise RuntimeError(f"Shard file not found after write: {path}")
        crc, size = _compute_file_crc(path)
        info = ShardInfo(
            shard_id=shard_id,
            shard_path=path,
            crc32=crc,
            size=size,
            relative_path=relative_path,
            original_hash=original_hash,
            original_size=original_size,
            compressed_size=compressed_size,
        )
        self.shards[shard_id] = info
        return info

    def save_index(self):
        """Persist the shard index to disk as JSON."""
        self._save_index()

    def verify_shard(self, shard_id: str) -> bool:
        """Verify a shard's integrity by re-reading and checking CRC32."""
        info = self.shards.get(shard_id)
        if info is None or not info.shard_path.exists():
            return False
        crc, size = _compute_file_crc(info.shard_path)
        return crc == info.crc32 and size == info.size

    def verify_all_shards(self, progress_callback=None, workers=8) -> List[str]:
        """Verify every shard in parallel. Returns list of failed shard IDs (empty = all OK)."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        cb = progress_callback or (lambda *a, **kw: None)
        failed = []
        shard_ids = list(self.shards.keys())
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self.verify_shard, sid): sid for sid in shard_ids}
            for future in as_completed(futures):
                sid = futures[future]
                if not future.result():
                    failed.append(sid)
                cb("step", 1)
        return failed

    def stream_shard_to(self, shard_id: str, target_fh) -> int:
        """Stream a shard's contents into a target file handle.

        Returns the number of bytes written. Streams in chunks to avoid
        loading entire shards into memory (important for large files).
        """
        info = self.shards[shard_id]
        written = 0
        with open(info.shard_path, "rb") as src:
            while True:
                chunk = src.read(STAGING_CHUNK_SIZE)
                if not chunk:
                    break
                target_fh.write(chunk)
                written += len(chunk)
        return written

    def cleanup(self):
        """Remove the staging directory and all shard files."""
        if self.staging_dir.exists():
            shutil.rmtree(self.staging_dir, ignore_errors=True)
            logger.debug(f"Cleaned up staging area: {self.staging_dir}")

    def _save_index(self):
        """Persist the shard index to disk as JSON.

        This allows inspection/debugging if the process crashes, and makes
        the staging area self-describing.
        """
        index = {}
        for sid, info in self.shards.items():
            index[sid] = {
                "shard_path": str(info.shard_path),
                "crc32": info.crc32,
                "size": info.size,
                "relative_path": info.relative_path,
                "original_hash": info.original_hash,
                "original_size": info.original_size,
                "compressed_size": info.compressed_size,
            }
        self.index_path.write_text(json.dumps(index, indent=2))


# ── Source integrity verification ─────────────────────────────────

def preflight_check(
    entries: List,
    progress_callback: Optional[Callable] = None,
    workers: int = 8,
) -> Dict[str, str]:
    """Verify all source files are readable and snapshot their hashes.

    Called BEFORE compression begins. Returns a {relative_path: hash}
    dict that can later be compared to confirm source files are unchanged.

    Raises RuntimeError if any file cannot be read.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    progress_callback = progress_callback or (lambda *a, **kw: None)
    snapshot: Dict[str, str] = {}
    failed = []

    def _check_readable(entry):
        with open(entry.path, "rb") as f:
            f.read(1)
        return entry.relative_path, entry.full_hash or ""

    batch_size = 1000
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for start in range(0, len(entries), batch_size):
            batch = entries[start:start + batch_size]
            futures = {executor.submit(_check_readable, e): e for e in batch}
            for future in as_completed(futures):
                entry = futures[future]
                try:
                    rel_path, hash_val = future.result()
                    snapshot[rel_path] = hash_val
                except (OSError, PermissionError) as e:
                    failed.append(f"{entry.path}: {e}")
                progress_callback("step", 1)

    if failed:
        raise RuntimeError(
            f"Pre-flight check failed — cannot read {len(failed)} source file(s):\n"
            + "\n".join(f"  - {f}" for f in failed[:10])
        )

    return snapshot


def verify_source_unchanged(
    entries: List,
    snapshot: Dict[str, str],
    progress_callback: Optional[Callable] = None,
    workers: int = 8,
) -> List[str]:
    """Re-hash source files and compare to the pre-flight snapshot.

    Called AFTER compression but BEFORE assembly. If any source file's
    hash has changed, it means an external process modified the file
    during compression — we must abort to prevent data corruption.

    Returns a list of changed file relative paths (empty = all OK).
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed
    from .hasher import compute_full_hash

    progress_callback = progress_callback or (lambda *a, **kw: None)
    changed: List[str] = []

    # Filter to only entries that have a snapshot hash to compare
    to_verify = [(e, snapshot.get(e.relative_path, "")) for e in entries]
    to_verify = [(e, h) for e, h in to_verify if h]
    # Immediately tick entries with no snapshot
    skip_count = len(entries) - len(to_verify)
    for _ in range(skip_count):
        progress_callback("step", 1)

    if not to_verify:
        return changed

    batch_size = 500
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for start in range(0, len(to_verify), batch_size):
            batch = to_verify[start:start + batch_size]
            futures = {
                executor.submit(compute_full_hash, e.path): (e, expected)
                for e, expected in batch
            }
            for future in as_completed(futures):
                entry, expected = futures[future]
                try:
                    actual = future.result()
                    if actual != expected:
                        changed.append(entry.relative_path)
                        logger.error(
                            f"Source file CHANGED during compression: {entry.relative_path}"
                        )
                except (OSError, PermissionError):
                    changed.append(entry.relative_path)
                    logger.error(
                        f"Source file became UNREADABLE during compression: {entry.relative_path}"
                    )
                progress_callback("step", 1)

    return changed


# ── Utilities ─────────────────────────────────────────────────────

def _compute_file_crc(path: Path) -> Tuple[int, int]:
    """Compute CRC32 and file size by streaming. Returns (crc32, size)."""
    crc = 0
    size = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(STAGING_CHUNK_SIZE)
            if not chunk:
                break
            crc = zlib.crc32(chunk, crc)
            size += len(chunk)
    return crc & 0xFFFFFFFF, size
