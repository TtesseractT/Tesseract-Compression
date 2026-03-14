"""Tesseract archive encoder — creates .tesseract archives with deduplication.

The encoder:
  1. Scans the source directory for all files
  2. Detects duplicates using multi-stage content-aware matching
  3. Compresses unique files into verified staging shards
  4. Assembles verified shards into a .tesseract archive atomically
  5. Embeds a manifest describing how to reconstruct the full directory

Features:
  - FAILSAFE STAGING: Files compressed to shards, verified, then assembled
  - Normal mode: per-file compression blocks
  - Solid mode: single continuous compressed stream (better ratio)
  - AES-256-GCM encryption with password protection
  - Recovery records for archive repair
  - Archive comments
  - File permission preservation
  - Archive locking

CRITICAL SAFETY:
  - The encoder NEVER modifies or deletes source files.
  - All operations are read-only against the source directory.
  - Source file hashes are verified BEFORE and AFTER compression.
  - Shards are verified after write by re-reading from disk.
  - The final archive is written to a .tmp file and renamed atomically.
  - If ANY step fails, source files remain completely untouched.
"""

import logging
import os
import struct
import zstandard as zstd
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from .archive_format import (
    HEADER_SIZE,
    BLOCK_HEADER_SIZE,
    SOLID_HEADER_SIZE,
    MAGIC_FOOTER,
    FLAG_ENCRYPTED,
    FLAG_SOLID,
    FLAG_RECOVERY,
    FLAG_LOCKED,
    FLAG_PERMISSIONS,
    pack_header,
    pack_block_header,
    pack_solid_header,
)
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

from .deduplicator import Deduplicator, DuplicateGroup
from .hasher import compute_full_hash
from .hashcache import HashCache
from .manifest import Manifest
from .scanner import FileEntry, FileScanner

logger = logging.getLogger(__name__)

CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB streaming chunks
SMALL_FILE_THRESHOLD = 1 * 1024 * 1024  # 1 MB — files below this get fast compression
FAST_COMPRESSION_LEVEL = 3  # zstd level 3: still great ratio, ~50x faster than 19+


def _compress_shard_worker(
    source_path_str: str,
    shard_path_str: str,
    relative_path: str,
    full_hash: str,
    original_size: int,
    compression_level: int,
) -> Tuple[str, str, str, int, int, Optional[str]]:
    """Worker for parallel shard compression. Uses ThreadPoolExecutor (zstd releases the GIL).

    Adaptive compression: small files get FAST_COMPRESSION_LEVEL (high levels
    gain almost nothing on small data), large files get the requested level.

    Returns (shard_path, relative_path, full_hash, original_size, compressed_size, error).
    """
    try:
        # Adaptive level: high compression wastes CPU on small files
        effective_level = FAST_COMPRESSION_LEVEL if original_size < SMALL_FILE_THRESHOLD else compression_level
        cctx = zstd.ZstdCompressor(level=effective_level)
        compressor = cctx.compressobj()
        compressed_size = 0

        with open(shard_path_str, "wb") as shard_fh:
            # Write placeholder block header
            shard_fh.write(b"\x00" * BLOCK_HEADER_SIZE)

            with open(source_path_str, "rb") as src:
                while True:
                    chunk = src.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    compressed = compressor.compress(chunk)
                    if compressed:
                        shard_fh.write(compressed)
                        compressed_size += len(compressed)

            final = compressor.flush()
            if final:
                shard_fh.write(final)
                compressed_size += len(final)

            # Seek back and write actual block header
            current_pos = shard_fh.tell()
            shard_fh.seek(0)
            block_header = pack_block_header(
                content_hash=full_hash,
                original_size=original_size,
                compressed_size=compressed_size,
            )
            shard_fh.write(block_header)
            shard_fh.seek(current_pos)

        return (shard_path_str, relative_path, full_hash, original_size, compressed_size, None)
    except Exception as e:
        return (shard_path_str, relative_path, full_hash, original_size, 0, str(e))
DEFAULT_COMPRESSION_LEVEL = 19


class TesseractEncoder:
    """Creates .tesseract archive files with deduplication."""

    def __init__(
        self,
        workers: int = 4,
        compression_level: int = DEFAULT_COMPRESSION_LEVEL,
        progress_callback: Optional[Callable] = None,
        exclude_patterns: Optional[List[str]] = None,
        solid: bool = False,
        password: Optional[str] = None,
        recovery_percent: int = 0,
        comment: str = "",
        store_permissions: bool = False,
        lock: bool = False,
    ):
        self.workers = max(1, workers)
        self.compression_level = compression_level
        self.progress_callback = progress_callback or (lambda *a, **kw: None)
        self.exclude_patterns = exclude_patterns or []
        self.solid = solid
        self.password = password
        self.recovery_percent = max(0, min(30, recovery_percent))
        self.comment = comment
        self.store_permissions = store_permissions
        self.lock = lock

    def encode(self, source_dir: Path, output_path: Path) -> Manifest:
        """
        Encode a directory into a .tesseract archive using failsafe staging.

        The encoding process uses a multi-stage pipeline that ensures source
        files are never at risk. Files are compressed into staging shards,
        verified, source integrity is confirmed, then shards are assembled
        into the final archive atomically.

        Args:
            source_dir: Directory to compress/archive.
            output_path: Path for the output .tesseract file.

        Returns:
            The archive manifest with full metadata.
        """
        source_dir = Path(source_dir).resolve()
        output_path = Path(output_path).resolve()

        if not source_dir.is_dir():
            raise ValueError(f"Source directory does not exist: {source_dir}")
        if output_path.exists():
            raise FileExistsError(f"Output file already exists: {output_path}")
        if output_path.suffix != ".tesseract":
            output_path = output_path.with_suffix(".tesseract")

        output_path.parent.mkdir(parents=True, exist_ok=True)

        # ── Setup hash cache ──────────────────────────────────────
        cache_path = output_path.with_suffix(".hashcache")
        hash_cache = HashCache(cache_path)
        logger.info(f"Hash cache: {cache_path}")

        # ── Setup encryption ──────────────────────────────────────
        encryptor = None
        salt = b""
        password_check = b""
        if self.password:
            from .encryption import create_encryptor, compute_password_check
            encryptor, salt = create_encryptor(self.password)
            password_check = compute_password_check(self.password, salt)

        # ── Build flags ───────────────────────────────────────────
        flags = 0
        if self.password:
            flags |= FLAG_ENCRYPTED
        if self.solid:
            flags |= FLAG_SOLID
        if self.recovery_percent > 0:
            flags |= FLAG_RECOVERY
        if self.lock:
            flags |= FLAG_LOCKED
        if self.store_permissions:
            flags |= FLAG_PERMISSIONS

        # ── Phase 1: Scan ─────────────────────────────────────────
        logger.info(f"Scanning {source_dir}...")
        self.progress_callback("phase", "scanning", total=-1)
        scanner = FileScanner(source_dir, self.exclude_patterns)

        # Try to load scan from cache
        cached_scan = hash_cache.get_scan(str(source_dir))
        if cached_scan:
            all_entries = []
            stale = False
            for rec in cached_scan:
                p = Path(rec["path"])
                try:
                    stat = p.stat()
                    if stat.st_size != rec["size"] or stat.st_mtime != rec["mtime"]:
                        stale = True
                        break
                except OSError:
                    stale = True
                    break
                all_entries.append(FileEntry(
                    path=p,
                    relative_path=rec["relative_path"],
                    size=rec["size"],
                    filename=rec["filename"],
                    extension=rec["extension"],
                    modified_time=rec["mtime"],
                ))
                self.progress_callback("step", 1)
            if stale:
                logger.info("Scan cache stale, rescanning...")
                all_entries = []
                cached_scan = None

        if not cached_scan:
            all_entries = []
            for entry in scanner.scan():
                all_entries.append(entry)
                self.progress_callback("step", 1)
            # Save scan to cache
            scan_data = [
                {"path": str(e.path), "relative_path": e.relative_path,
                 "size": e.size, "filename": e.filename,
                 "extension": e.extension, "mtime": e.modified_time}
                for e in all_entries
            ]
            hash_cache.put_scan(str(source_dir), scan_data)

        logger.info(f"Found {len(all_entries)} files")

        if not all_entries:
            logger.warning("No files found in source directory")

        # ── Phase 1.5: Pre-populate hashes from cache ─────────────
        cached_count = 0
        for entry in all_entries:
            cached = hash_cache.get(entry.path)
            if cached:
                entry.full_hash = cached
                cached_count += 1
        if cached_count:
            logger.info(f"Loaded {cached_count} hashes from cache ({hash_cache.stats})")

        # ── Phase 2: Detect duplicates ────────────────────────────
        logger.info("Detecting duplicates...")
        self.progress_callback("phase", "deduplicating", total=len(all_entries))
        deduplicator = Deduplicator(
            workers=self.workers,
            progress_callback=self.progress_callback,
        )
        duplicate_groups = deduplicator.find_duplicates(all_entries, hash_cache=hash_cache)

        # ── Phase 3: Hash remaining unique files ──────────────────
        need_hash = [e for e in all_entries if not e.full_hash]
        logger.info(f"Hashing {len(need_hash)} unique files ({len(all_entries) - len(need_hash)} already hashed from dedup)...")
        self.progress_callback("phase", "hashing_unique", total=len(need_hash))
        self._hash_unique_files(need_hash, hash_cache=hash_cache)
        hash_cache.flush()
        logger.info(f"Hash cache: {hash_cache.stats}")

        # ── Phase 4: Determine which files to store ───────────────
        dup_non_masters = set()
        for group in duplicate_groups:
            for dup in group.duplicates:
                dup_non_masters.add(dup.relative_path)
        unique_entries = [e for e in all_entries if e.relative_path not in dup_non_masters]

        # ── Phase 5: Pre-flight integrity snapshot ────────────────
        logger.info("Pre-flight check: verifying all source files are readable...")
        self.progress_callback("phase", "preflight", total=len(all_entries))
        from .safeguard import StagingArea, preflight_check, verify_source_unchanged
        source_snapshot = preflight_check(all_entries, self.progress_callback, workers=self.workers)

        # ── Phase 6-11: Staged encode with full safety ────────────
        staging_dir = output_path.parent / f".{output_path.stem}_staging"
        staging = StagingArea(staging_dir)
        tmp_archive = output_path.parent / f".{output_path.name}.tmp"

        # Encode comment bytes
        comment_bytes = self.comment.encode("utf-8")[:65535] if self.comment else b""

        try:
            staging.create()

            # Clean up any leftover .tmp from a prior failed run
            if tmp_archive.exists():
                tmp_archive.unlink()

            # ── Phase 6: Stage shards ─────────────────────────────
            logger.info("Staging compressed shards...")
            self.progress_callback("phase", "staging", total=len(unique_entries))

            shard_compressed_sizes: Dict[str, int] = {}
            solid_offset_map: Dict[str, Tuple[int, int]] = {}

            if self.solid:
                # Solid mode: single shard with all files in one stream
                shard_id = "solid_stream"
                shard_file = staging.shard_path(shard_id)
                with open(shard_file, "wb") as shard_fh:
                    self._write_solid_stream(
                        shard_fh, unique_entries, solid_offset_map, encryptor
                    )
                staging.register_shard(
                    shard_id, "__solid__", "", 0,
                    compressed_size=shard_file.stat().st_size,
                )
            else:
                if encryptor:
                    # Encrypted mode: sequential (encryptor not picklable)
                    for i, entry in enumerate(unique_entries):
                        shard_id = f"shard_{i:06d}"
                        shard_file = staging.shard_path(shard_id)
                        try:
                            with open(shard_file, "wb") as shard_fh:
                                compressed_size = self._write_file_block(
                                    shard_fh, entry, encryptor
                                )
                            staging.register_shard(
                                shard_id, entry.relative_path,
                                entry.full_hash or "", entry.size,
                                compressed_size=compressed_size,
                            )
                            shard_compressed_sizes[entry.relative_path] = compressed_size
                            self.progress_callback("step", 1)
                        except (OSError, PermissionError) as e:
                            logger.error(f"Failed to stage {entry.relative_path}: {e}")
                            raise
                else:
                    # Normal mode: parallel shard compression
                    # ThreadPoolExecutor works because zstd releases the GIL during compression
                    batch_size = 2000
                    with ThreadPoolExecutor(max_workers=self.workers) as executor:
                        for start in range(0, len(unique_entries), batch_size):
                            batch = unique_entries[start:start + batch_size]
                            futures = {}
                            for j, entry in enumerate(batch):
                                idx = start + j
                                shard_id = f"shard_{idx:06d}"
                                shard_file = staging.shard_path(shard_id)
                                future = executor.submit(
                                    _compress_shard_worker,
                                    str(entry.path),
                                    str(shard_file),
                                    entry.relative_path,
                                    entry.full_hash or "",
                                    entry.size,
                                    self.compression_level,
                                )
                                futures[future] = (shard_id, entry)

                            for future in as_completed(futures):
                                shard_id, entry = futures[future]
                                shard_path_str, rel_path, fhash, orig_size, comp_size, error = future.result()
                                if error:
                                    logger.error(f"Failed to stage {rel_path}: {error}")
                                    raise RuntimeError(f"Shard compression failed for {rel_path}: {error}")
                                staging.register_shard(
                                    shard_id, rel_path, fhash, orig_size,
                                    compressed_size=comp_size,
                                )
                                shard_compressed_sizes[rel_path] = comp_size
                                self.progress_callback("step", 1)

            # ── Phase 7: Verify all shards ────────────────────────
            staging.save_index()
            logger.info("Verifying shard integrity (re-reading from disk)...")
            self.progress_callback("phase", "verifying_shards", total=len(staging.shards))
            failed_shards = staging.verify_all_shards(self.progress_callback, workers=self.workers)
            if failed_shards:
                raise RuntimeError(
                    f"Shard verification FAILED for {len(failed_shards)} shard(s). "
                    f"Possible disk corruption. Failed: {failed_shards}"
                )
            logger.info(f"All {len(staging.shards)} shards verified OK")

            # ── Phase 8: Verify source files unchanged ────────────
            logger.info("Verifying source files were not modified during compression...")
            self.progress_callback("phase", "verifying_source", total=len(all_entries))
            changed_files = verify_source_unchanged(
                all_entries, source_snapshot, self.progress_callback, workers=self.workers
            )
            if changed_files:
                raise RuntimeError(
                    f"SAFETY ABORT: {len(changed_files)} source file(s) were modified "
                    f"during compression. Changed: {changed_files}. "
                    f"Source files are untouched — staging will be cleaned up."
                )
            logger.info("All source files verified unchanged")

            # ── Phase 9: Assemble archive from shards ─────────────
            logger.info(f"Assembling archive to {output_path}...")
            self.progress_callback("phase", "assembling", total=len(unique_entries))

            offset_map: Dict[str, Tuple[int, int]] = {}

            with open(tmp_archive, "wb") as archive:
                # Write header placeholder
                archive.write(b"\x00" * HEADER_SIZE)

                # Write archive comment
                if comment_bytes:
                    archive.write(comment_bytes)

                data_start = archive.tell()

                if self.solid:
                    # Stream the solid shard into the archive
                    staging.stream_shard_to("solid_stream", archive)
                    offset_map = solid_offset_map
                else:
                    # Stream each file shard into the archive in order
                    for i, entry in enumerate(unique_entries):
                        shard_id = f"shard_{i:06d}"
                        block_offset = archive.tell()
                        staging.stream_shard_to(shard_id, archive)
                        offset_map[entry.relative_path] = (
                            block_offset,
                            shard_compressed_sizes[entry.relative_path],
                        )
                        self.progress_callback("step", 1)

                data_end = archive.tell()

                # Build manifest
                manifest = Manifest.build(
                    source_dir, all_entries, duplicate_groups,
                    comment=self.comment,
                    store_permissions=self.store_permissions,
                )
                manifest.is_encrypted = bool(self.password)
                manifest.is_solid = self.solid
                manifest.has_recovery = self.recovery_percent > 0
                manifest.is_locked = self.lock

                for rel_path, (offset, compressed_size) in offset_map.items():
                    if rel_path in manifest.files:
                        manifest.files[rel_path]["data_offset"] = offset
                        manifest.files[rel_path]["compressed_size"] = compressed_size

                # Propagate master offsets to duplicate entries
                for gid, ginfo in manifest.duplicate_groups.items():
                    master_path = ginfo["master"]
                    if master_path in offset_map:
                        master_offset, master_compressed = offset_map[master_path]
                        for dup_path in ginfo["duplicates"]:
                            if dup_path in manifest.files:
                                manifest.files[dup_path]["data_offset"] = master_offset
                                manifest.files[dup_path]["compressed_size"] = master_compressed

                # Write manifest (optionally encrypted)
                manifest_offset = archive.tell()
                manifest_data = manifest.to_json()
                if encryptor:
                    manifest_data = encryptor.encrypt(manifest_data)
                archive.write(manifest_data)

                # Write footer
                archive.write(MAGIC_FOOTER)

                # ── Recovery records ──────────────────────────────
                recovery_offset = 0
                recovery_size = 0
                if self.recovery_percent > 0:
                    logger.info("Generating recovery records...")
                    self.progress_callback("phase", "recovery", total=0)
                    from .recovery import generate_recovery_data

                    # Flush to ensure data is on disk before reading back
                    archive.flush()
                    recovery_offset = archive.tell()
                    rec = generate_recovery_data(
                        tmp_archive, data_start, data_end,
                        redundancy_percent=self.recovery_percent,
                    )
                    rec_data = rec.serialize()
                    archive.write(rec_data)
                    recovery_size = len(rec_data)

                # Update header with final offsets
                archive.seek(0)
                header = pack_header(
                    manifest_offset=manifest_offset,
                    manifest_compressed_size=len(manifest_data),
                    total_files=len(all_entries),
                    total_unique=len(unique_entries),
                    recovery_offset=recovery_offset,
                    recovery_size=recovery_size,
                    flags=flags,
                    encryption_salt=salt,
                    password_check=password_check,
                    comment_length=len(comment_bytes),
                )
                archive.write(header)

            # ── Phase 10: Verify assembled archive ────────────────
            logger.info("Verifying assembled archive...")
            self.progress_callback("phase", "verifying", total=0)
            self._verify_archive(tmp_archive, manifest, encryptor)

            # ── Phase 11: Finalize — atomic rename ────────────────
            logger.info("Finalizing archive...")
            self.progress_callback("phase", "finalizing", total=0)
            os.replace(tmp_archive, output_path)
            logger.info(f"Archive finalized at: {output_path}")

        finally:
            # ── Phase 12: Cleanup ─────────────────────────────────
            staging.cleanup()
            # Remove .tmp if assembly failed before rename
            if tmp_archive.exists():
                try:
                    tmp_archive.unlink()
                except OSError:
                    pass

        archive_size = output_path.stat().st_size
        mode_label = "solid" if self.solid else "normal"
        enc_label = ", encrypted" if self.password else ""
        rec_label = f", {self.recovery_percent}% recovery" if self.recovery_percent else ""
        logger.info(
            f"Archive created successfully ({mode_label}{enc_label}{rec_label}): {output_path}\n"
            f"  Total files:          {manifest.file_count}\n"
            f"  Unique files stored:  {manifest.unique_count}\n"
            f"  Duplicate groups:     {manifest.duplicate_group_count}\n"
            f"  Original size:        {manifest.total_original_size / (1024**3):.2f} GB\n"
            f"  Archive size:         {archive_size / (1024**3):.2f} GB\n"
            f"  Space savings (dedup):{manifest.space_savings / (1024**3):.2f} GB"
        )
        return manifest

    # ── Internal methods ──────────────────────────────────────────

    def _hash_unique_files(self, entries: List[FileEntry], hash_cache: HashCache = None):
        """Hash files that still need a full content hash, using parallel workers.

        Files are sorted by directory path to promote sequential disk reads.
        Results are persisted to the hash cache for crash resilience.
        """
        if not entries:
            return
        entries.sort(key=lambda e: (str(e.path.parent).lower(), e.path.name.lower()))
        batch_size = 500
        with ProcessPoolExecutor(max_workers=self.workers) as executor:
            for start in range(0, len(entries), batch_size):
                batch = entries[start:start + batch_size]
                futures = {
                    executor.submit(compute_full_hash, e.path): e
                    for e in batch
                }
                for future in as_completed(futures):
                    entry = futures[future]
                    try:
                        entry.full_hash = future.result()
                        if hash_cache:
                            hash_cache.put(entry.path, entry.full_hash)
                    except (OSError, PermissionError) as e:
                        logger.warning(f"Cannot hash {entry.path}: {e}")
                    self.progress_callback("step", 1)

    def _write_file_block(self, archive, entry: FileEntry, encryptor=None) -> int:
        """
        Write a single file's data block to the archive using streaming compression.
        Optionally encrypts the compressed data. Returns the compressed data size.
        """
        block_header_offset = archive.tell()
        archive.write(b"\x00" * BLOCK_HEADER_SIZE)

        # Stream-compress the file (adaptive level for small files)
        effective_level = FAST_COMPRESSION_LEVEL if entry.size < SMALL_FILE_THRESHOLD else self.compression_level
        cctx = zstd.ZstdCompressor(level=effective_level, threads=self.workers)
        compressor = cctx.compressobj()
        compressed_size = 0

        if encryptor:
            # Encrypt mode: compress fully first, then encrypt the result
            compressed_buf = bytearray()
            with open(entry.path, "rb") as src:
                while True:
                    chunk = src.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    compressed_buf.extend(compressor.compress(chunk))
            compressed_buf.extend(compressor.flush())

            encrypted = encryptor.encrypt(bytes(compressed_buf))
            archive.write(encrypted)
            compressed_size = len(encrypted)
        else:
            with open(entry.path, "rb") as src:
                while True:
                    chunk = src.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    compressed = compressor.compress(chunk)
                    if compressed:
                        archive.write(compressed)
                        compressed_size += len(compressed)

            final = compressor.flush()
            if final:
                archive.write(final)
                compressed_size += len(final)

        # Seek back and write actual block header
        current_pos = archive.tell()
        archive.seek(block_header_offset)
        block_header = pack_block_header(
            content_hash=entry.full_hash or "",
            original_size=entry.size,
            compressed_size=compressed_size,
        )
        archive.write(block_header)
        archive.seek(current_pos)

        return compressed_size

    def _write_solid_stream(self, archive, unique_entries, offset_map, encryptor=None):
        """
        Write all unique files as a single continuous compressed stream (solid mode).
        This typically achieves better compression for many similar files.
        """
        solid_header_offset = archive.tell()
        archive.write(b"\x00" * SOLID_HEADER_SIZE)

        cctx = zstd.ZstdCompressor(level=self.compression_level, threads=self.workers)
        compressor = cctx.compressobj()
        total_uncompressed = 0
        total_compressed = 0
        stream_offset = 0  # offset within the uncompressed stream

        if encryptor:
            # Encrypted solid: buffer all compressed data, encrypt entire stream at once.
            # Per-chunk encryption would misalign with decoder read boundaries.
            compressed_buf = bytearray()
            for entry in unique_entries:
                file_stream_offset = stream_offset
                with open(entry.path, "rb") as src:
                    while True:
                        chunk = src.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        compressed_buf.extend(compressor.compress(chunk))
                        total_uncompressed += len(chunk)
                        stream_offset += len(chunk)
                offset_map[entry.relative_path] = (file_stream_offset, entry.size)
                self.progress_callback("step", 1)

            compressed_buf.extend(compressor.flush())
            encrypted = encryptor.encrypt(bytes(compressed_buf))
            archive.write(encrypted)
            total_compressed = len(encrypted)
        else:
            for entry in unique_entries:
                file_stream_offset = stream_offset
                with open(entry.path, "rb") as src:
                    while True:
                        chunk = src.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        compressed = compressor.compress(chunk)
                        if compressed:
                            archive.write(compressed)
                            total_compressed += len(compressed)
                        total_uncompressed += len(chunk)
                        stream_offset += len(chunk)

                offset_map[entry.relative_path] = (file_stream_offset, entry.size)
                self.progress_callback("step", 1)

            # Flush compressor
            final = compressor.flush()
            if final:
                archive.write(final)
                total_compressed += len(final)

        # Write solid header
        current_pos = archive.tell()
        archive.seek(solid_header_offset)
        archive.write(pack_solid_header(total_uncompressed, total_compressed))
        archive.seek(current_pos)

        logger.info(
            f"Solid stream: {total_uncompressed} -> {total_compressed} bytes "
            f"({total_compressed/max(1,total_uncompressed)*100:.1f}%)"
        )

    def _verify_archive(self, archive_path: Path, manifest: Manifest, encryptor=None):
        """Verify the written archive is structurally valid."""
        file_size = archive_path.stat().st_size
        min_size = HEADER_SIZE + len(MAGIC_FOOTER)
        if file_size < min_size:
            raise RuntimeError(f"Archive too small: {file_size} bytes (minimum {min_size})")

        with open(archive_path, "rb") as f:
            from .archive_format import unpack_header
            header = unpack_header(f.read(HEADER_SIZE))

            # Skip comment
            if header.comment_length:
                f.read(header.comment_length)

            # Verify manifest is readable
            f.seek(header.manifest_offset)
            manifest_data = f.read(header.manifest_compressed_size)
            if header.is_encrypted and self.password:
                from .encryption import create_decryptor
                decryptor = create_decryptor(self.password, header.encryption_salt)
                manifest_data = decryptor.decrypt(manifest_data)
            recovered = Manifest.from_json(manifest_data)
            if recovered.file_count != manifest.file_count:
                raise RuntimeError(
                    f"Manifest file count mismatch: {recovered.file_count} vs {manifest.file_count}"
                )

            # Verify footer — seek past recovery if present
            if header.has_recovery and header.recovery_offset > 0:
                f.seek(header.recovery_offset - len(MAGIC_FOOTER))
            else:
                f.seek(-len(MAGIC_FOOTER), 2)

            # Find footer before recovery records
            f.seek(header.manifest_offset + header.manifest_compressed_size)
            footer_magic = f.read(len(MAGIC_FOOTER))
            if footer_magic != MAGIC_FOOTER:
                raise RuntimeError("Archive footer magic mismatch — file may be corrupt")

        logger.info("Archive verification passed")
