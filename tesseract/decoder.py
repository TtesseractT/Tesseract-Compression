"""Tesseract archive decoder — extracts .tesseract archives back to full directory trees.

The decoder:
  1. Reads the archive header and manifest
  2. Extracts all unique/master files from their data blocks (streaming)
  3. Copies master files to all duplicate locations
  4. Verifies every extracted file against its manifest hash

Features:
  - Normal and solid mode extraction
  - AES-256-GCM decryption with password
  - Selective extraction (specific files or glob patterns)
  - File permission restoration
  - Archive comment reading

CRITICAL SAFETY: The decoder verifies file integrity at every step.
Corrupt or mismatched files are detected and reported immediately.
"""

import fnmatch
import blake3
import logging
import os
import shutil
import zlib
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set

from .archive_format import (
    HEADER_SIZE,
    BLOCK_HEADER_SIZE,
    SOLID_HEADER_SIZE,
    MAGIC_FOOTER,
    unpack_header,
    unpack_block_header,
    unpack_solid_header,
)
from .manifest import Manifest

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB streaming chunks


class TesseractDecoder:
    """Extracts .tesseract archive files back to their original directory structure."""

    def __init__(
        self,
        workers: int = 4,
        verify: bool = True,
        overwrite: bool = False,
        password: Optional[str] = None,
        extract_patterns: Optional[List[str]] = None,
        progress_callback: Optional[Callable] = None,
    ):
        self.workers = max(1, workers)
        self.verify = verify
        self.overwrite = overwrite
        self.password = password
        self.extract_patterns = extract_patterns or []
        self.progress_callback = progress_callback or (lambda *a, **kw: None)

    def _should_extract(self, rel_path: str) -> bool:
        """Check if a file matches the extraction patterns (empty = extract all)."""
        if not self.extract_patterns:
            return True
        for pattern in self.extract_patterns:
            if fnmatch.fnmatch(rel_path, pattern):
                return True
            # Also match if pattern is a prefix (directory)
            if rel_path.startswith(pattern.rstrip("*").rstrip("/")):
                return True
        return False

    def _get_decryptor(self, header):
        """Create a decryptor from password and header salt, or raise if needed."""
        if not header.is_encrypted:
            return None
        if not self.password:
            raise ValueError("Archive is encrypted — password required (use -p / --password)")
        from .encryption import create_decryptor, verify_password_check
        if header.password_check and header.password_check != b"\x00" * 32:
            if not verify_password_check(self.password, header.encryption_salt, header.password_check):
                raise ValueError("Wrong password")
        return create_decryptor(self.password, header.encryption_salt)

    def decode(self, archive_path: Path, output_dir: Path) -> Manifest:
        """
        Extract a .tesseract archive to a directory.

        Args:
            archive_path: Path to the .tesseract archive.
            output_dir: Directory to extract files into.

        Returns:
            The archive manifest.
        """
        archive_path = Path(archive_path).resolve()
        output_dir = Path(output_dir).resolve()

        if not archive_path.is_file():
            raise ValueError(f"Archive file does not exist: {archive_path}")

        output_dir.mkdir(parents=True, exist_ok=True)

        with open(archive_path, "rb") as archive:
            # ── Read and validate header ──────────────────────────
            logger.info(f"Reading archive: {archive_path}")
            self.progress_callback("phase", "reading_header")

            header_data = archive.read(HEADER_SIZE)
            header = unpack_header(header_data)
            logger.info(
                f"Archive contains {header.total_files} files "
                f"({header.total_unique} unique)"
            )

            # Setup decryption if needed
            decryptor = self._get_decryptor(header)

            # Skip comment
            if header.comment_length:
                archive.read(header.comment_length)

            # ── Validate footer ───────────────────────────────────
            saved_pos = archive.tell()
            archive.seek(header.manifest_offset + header.manifest_compressed_size)
            footer = archive.read(len(MAGIC_FOOTER))
            if footer != MAGIC_FOOTER:
                raise RuntimeError("Archive footer is corrupt — file may be damaged")
            archive.seek(saved_pos)

            # ── Read manifest ─────────────────────────────────────
            logger.info("Reading manifest...")
            self.progress_callback("phase", "reading_manifest")

            archive.seek(header.manifest_offset)
            manifest_data = archive.read(header.manifest_compressed_size)
            if decryptor:
                manifest_data = decryptor.decrypt(manifest_data)
            manifest = Manifest.from_json(manifest_data)

            logger.info(
                f"  Files: {manifest.file_count}, "
                f"Unique: {manifest.unique_count}, "
                f"Duplicate groups: {manifest.duplicate_group_count}"
            )

            if manifest.comment:
                logger.info(f"  Comment: {manifest.comment}")

            # ── Extract files ─────────────────────────────────────
            logger.info("Extracting files...")
            self.progress_callback("phase", "extracting", total=manifest.unique_count)

            if header.is_solid:
                extracted = self._extract_solid(
                    archive, header, manifest, output_dir, decryptor
                )
            else:
                extracted = self._extract_normal(
                    archive, manifest, output_dir, decryptor
                )

            # ── Copy masters to duplicate locations ───────────────
            total_dups = sum(
                len(g["duplicates"]) for g in manifest.duplicate_groups.values()
            )
            logger.info("Restoring duplicates from master copies...")
            self.progress_callback("phase", "restoring_duplicates", total=total_dups)

            duplicates_restored = 0
            for gid, group_info in manifest.duplicate_groups.items():
                master_path = group_info["master"]
                if master_path not in extracted:
                    continue

                master_file = extracted[master_path]
                for dup_rel_path in group_info["duplicates"]:
                    if not self._should_extract(dup_rel_path):
                        continue

                    dup_output = output_dir / dup_rel_path

                    if dup_output.exists() and not self.overwrite:
                        raise FileExistsError(
                            f"Duplicate target already exists: {dup_output}"
                        )

                    dup_output.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(master_file), str(dup_output))
                    extracted[dup_rel_path] = dup_output

                    # Restore permissions on duplicate
                    if manifest.store_permissions:
                        self._restore_permissions(
                            dup_output, dup_rel_path, manifest
                        )

                    duplicates_restored += 1
                    self.progress_callback("step", 1)

            # ── Verification pass ─────────────────────────────────
            if self.verify:
                logger.info("Verifying extracted files...")
                self.progress_callback("phase", "verifying_extracted", total=len(extracted))
                self._verify_extracted(output_dir, manifest, extracted.keys())

        total_extracted = len(extracted)
        logger.info(
            f"Extraction complete: {total_extracted} files restored to {output_dir}"
        )
        return manifest

    def read_manifest(self, archive_path: Path) -> Manifest:
        """Read just the manifest from an archive without extracting."""
        archive_path = Path(archive_path).resolve()

        with open(archive_path, "rb") as archive:
            header = unpack_header(archive.read(HEADER_SIZE))
            decryptor = self._get_decryptor(header)
            archive.seek(header.manifest_offset)
            manifest_data = archive.read(header.manifest_compressed_size)
            if decryptor:
                manifest_data = decryptor.decrypt(manifest_data)
            return Manifest.from_json(manifest_data)

    def read_comment(self, archive_path: Path) -> str:
        """Read just the archive comment."""
        archive_path = Path(archive_path).resolve()
        with open(archive_path, "rb") as archive:
            header = unpack_header(archive.read(HEADER_SIZE))
            if header.comment_length:
                return archive.read(header.comment_length).decode("utf-8")
        return ""

    # ── Extraction engines ────────────────────────────────────────

    def _extract_normal(self, archive, manifest, output_dir, decryptor) -> Dict[str, Path]:
        """Extract files from normal (per-block) archive."""
        extracted: Dict[str, Path] = {}
        files_extracted = 0

        for rel_path, file_info in manifest.files.items():
            if file_info.get("group_id") and not file_info.get("is_master"):
                continue
            if not self._should_extract(rel_path):
                continue

            output_file = output_dir / rel_path

            if output_file.exists() and not self.overwrite:
                raise FileExistsError(
                    f"Output file already exists: {output_file}. "
                    f"Use --overwrite to replace."
                )

            output_file.parent.mkdir(parents=True, exist_ok=True)

            data_offset = file_info["data_offset"]
            self._extract_file_block(
                archive, data_offset, output_file, rel_path, decryptor
            )

            if manifest.store_permissions:
                self._restore_permissions(output_file, rel_path, manifest)

            extracted[rel_path] = output_file
            files_extracted += 1
            self.progress_callback("step", 1)

        return extracted

    def _extract_solid(self, archive, header, manifest, output_dir, decryptor) -> Dict[str, Path]:
        """Extract files from solid (continuous stream) archive."""
        extracted: Dict[str, Path] = {}

        # Read solid header
        data_offset = HEADER_SIZE + header.comment_length
        archive.seek(data_offset)
        solid_header_data = archive.read(SOLID_HEADER_SIZE)
        solid_header = unpack_solid_header(solid_header_data)

        # Decompress the entire solid stream
        if decryptor:
            # Encrypted solid: read entire stream, decrypt at once, then decompress
            encrypted_data = archive.read(solid_header.total_compressed)
            compressed_data = decryptor.decrypt(encrypted_data)
            decompressed_buf = bytearray(zlib.decompress(compressed_data))
        else:
            decompressor = zlib.decompressobj()
            remaining = solid_header.total_compressed
            decompressed_buf = bytearray()

            while remaining > 0:
                read_size = min(CHUNK_SIZE, remaining)
                chunk = archive.read(read_size)
                if not chunk:
                    break
                remaining -= len(chunk)
                decompressed_buf.extend(decompressor.decompress(chunk))

            try:
                final = decompressor.flush()
                if final:
                    decompressed_buf.extend(final)
            except zlib.error:
                pass

        # Extract individual files from the decompressed stream
        files_extracted = 0
        for rel_path, file_info in manifest.files.items():
            if file_info.get("group_id") and not file_info.get("is_master"):
                continue
            if not self._should_extract(rel_path):
                continue

            output_file = output_dir / rel_path

            if output_file.exists() and not self.overwrite:
                raise FileExistsError(f"Output file already exists: {output_file}")

            output_file.parent.mkdir(parents=True, exist_ok=True)

            # In solid mode, data_offset is the offset within the decompressed stream
            stream_offset = file_info["data_offset"]
            file_size = file_info["size"]
            file_data = bytes(decompressed_buf[stream_offset:stream_offset + file_size])

            # Verify hash
            if self.verify and file_info.get("content_hash"):
                actual_hash = blake3.blake3(file_data).hexdigest()
                if actual_hash != file_info["content_hash"]:
                    raise RuntimeError(
                        f"Hash mismatch for {rel_path}: "
                        f"expected {file_info['content_hash']}, got {actual_hash}"
                    )

            with open(output_file, "wb") as f:
                f.write(file_data)

            if manifest.store_permissions:
                self._restore_permissions(output_file, rel_path, manifest)

            extracted[rel_path] = output_file
            files_extracted += 1
            self.progress_callback("step", 1)

        return extracted

    # ── Internal methods ──────────────────────────────────────────

    def _extract_file_block(
        self, archive, data_offset: int, output_path: Path, rel_path: str,
        decryptor=None,
    ):
        """
        Extract a single file block from the archive using streaming decompression.
        Verifies content hash and size after extraction.
        """
        archive.seek(data_offset)

        # Read block header
        block_header_data = archive.read(BLOCK_HEADER_SIZE)
        block_header = unpack_block_header(block_header_data)

        if decryptor:
            # Encrypted mode: read entire block, decrypt, then decompress
            encrypted_data = archive.read(block_header.compressed_size)
            compressed_data = decryptor.decrypt(encrypted_data)
            decompressed = zlib.decompress(compressed_data)

            hasher = blake3.blake3(decompressed)

            with open(output_path, "wb") as out:
                out.write(decompressed)

            bytes_written = len(decompressed)
        else:
            # Streaming decompression
            decompressor = zlib.decompressobj()
            hasher = blake3.blake3()
            bytes_written = 0
            remaining_compressed = block_header.compressed_size

            with open(output_path, "wb") as out:
                while remaining_compressed > 0:
                    read_size = min(CHUNK_SIZE, remaining_compressed)
                    compressed_chunk = archive.read(read_size)
                    if not compressed_chunk:
                        break
                    remaining_compressed -= len(compressed_chunk)

                    try:
                        decompressed = decompressor.decompress(compressed_chunk)
                    except zlib.error as e:
                        out.close()
                        output_path.unlink(missing_ok=True)
                        raise RuntimeError(
                            f"Decompression error for {rel_path}: {e}"
                        )

                    if decompressed:
                        hasher.update(decompressed)
                        out.write(decompressed)
                        bytes_written += len(decompressed)

                try:
                    final = decompressor.flush()
                except zlib.error:
                    final = b""
                if final:
                    hasher.update(final)
                    out.write(final)
                    bytes_written += len(final)

        # Verify size
        if bytes_written != block_header.original_size:
            output_path.unlink(missing_ok=True)
            raise RuntimeError(
                f"Size mismatch for {rel_path}: "
                f"expected {block_header.original_size}, got {bytes_written}"
            )

        # Verify content hash
        if self.verify and block_header.content_hash:
            actual_hash = hasher.hexdigest()
            if actual_hash != block_header.content_hash:
                output_path.unlink(missing_ok=True)
                raise RuntimeError(
                    f"Hash mismatch for {rel_path}: "
                    f"expected {block_header.content_hash}, got {actual_hash}"
                )

    def _restore_permissions(self, file_path: Path, rel_path: str, manifest: Manifest):
        """Restore file permissions from manifest metadata."""
        file_info = manifest.files.get(rel_path, {})
        perms = file_info.get("permissions")
        if not perms:
            return
        try:
            mode = int(perms["mode"], 8)
            os.chmod(str(file_path), mode)
        except (ValueError, OSError, KeyError):
            pass

    def _verify_extracted(
        self, output_dir: Path, manifest: Manifest,
        extracted_paths: Optional[Set[str]] = None,
    ):
        """Verify all extracted files match their manifest entries."""
        errors = []
        verified = 0

        check_paths = extracted_paths or manifest.files.keys()

        for rel_path in check_paths:
            file_info = manifest.files.get(rel_path)
            if not file_info:
                continue

            output_file = output_dir / rel_path

            if not output_file.exists():
                errors.append(f"Missing file: {rel_path}")
                continue

            actual_size = output_file.stat().st_size
            expected_size = file_info["size"]
            if actual_size != expected_size:
                errors.append(
                    f"Size mismatch for {rel_path}: "
                    f"expected {expected_size}, got {actual_size}"
                )
                continue

            if file_info["content_hash"]:
                actual_hash = blake3.blake3(output_file.read_bytes()).hexdigest()
                if actual_hash != file_info["content_hash"]:
                    errors.append(
                        f"Hash mismatch for {rel_path}: "
                        f"expected {file_info['content_hash']}, got {actual_hash}"
                    )
                    continue

            verified += 1
            self.progress_callback("step", 1)

        if errors:
            error_msg = "Verification failed:\n" + "\n".join(f"  - {e}" for e in errors)
            raise RuntimeError(error_msg)

        logger.info(f"All {verified} files verified successfully")
