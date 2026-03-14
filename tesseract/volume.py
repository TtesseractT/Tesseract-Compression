"""Multi-volume archive splitting for Tesseract archives.

Splits a completed .tesseract archive into fixed-size volumes for
storage on size-limited media or easier transfers.

Volume naming: archive.tesseract.001, archive.tesseract.002, ...
The first volume contains a volume header with metadata to reassemble.

Volume header (prepended to first volume):
    magic(8) + total_volumes(4) + volume_size(8) + original_size(8)
    + original_sha256(64) = 92 bytes
"""

import hashlib
import logging
import struct
from pathlib import Path
from typing import Callable, List, Optional, Tuple

logger = logging.getLogger(__name__)

VOLUME_MAGIC = b"TSSVOL\x01\x00"
VOLUME_HEADER_FORMAT = "<8sIQ Q 64s"
VOLUME_HEADER_SIZE = struct.calcsize(VOLUME_HEADER_FORMAT)
DEFAULT_VOLUME_SIZE = 100 * 1024 * 1024  # 100 MB
CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB read/write chunks


def _volume_path(base_path: Path, volume_num: int) -> Path:
    """Generate volume filename: base.tesseract.001, .002, ..."""
    return base_path.parent / f"{base_path.name}.{volume_num:03d}"


def split_archive(
    archive_path: Path,
    volume_size: int = DEFAULT_VOLUME_SIZE,
    progress_callback: Optional[Callable] = None,
) -> List[Path]:
    """
    Split a .tesseract archive into fixed-size volumes.

    Args:
        archive_path: Path to the complete .tesseract archive.
        volume_size: Maximum size per volume in bytes.
        progress_callback: Optional callback(event, value).

    Returns:
        List of volume file paths created.
    """
    progress_callback = progress_callback or (lambda *a, **kw: None)
    archive_path = Path(archive_path).resolve()

    if not archive_path.is_file():
        raise ValueError(f"Archive not found: {archive_path}")

    original_size = archive_path.stat().st_size
    if volume_size < VOLUME_HEADER_SIZE + 1024:
        raise ValueError(f"Volume size too small (minimum {VOLUME_HEADER_SIZE + 1024} bytes)")

    # Compute SHA-256 of the original archive
    logger.info("Computing archive hash for volume integrity...")
    hasher = hashlib.sha256()
    with open(archive_path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
    original_hash = hasher.hexdigest()

    # Calculate volumes needed (first volume has smaller usable space due to header)
    first_volume_data = volume_size - VOLUME_HEADER_SIZE
    remaining_after_first = max(0, original_size - first_volume_data)
    extra_volumes = (remaining_after_first + volume_size - 1) // volume_size if remaining_after_first > 0 else 0
    total_volumes = 1 + extra_volumes

    logger.info(
        f"Splitting {original_size} bytes into {total_volumes} volumes "
        f"({volume_size} bytes each)"
    )

    volumes: List[Path] = []

    with open(archive_path, "rb") as src:
        for vol_num in range(1, total_volumes + 1):
            vol_path = _volume_path(archive_path, vol_num)
            volumes.append(vol_path)

            with open(vol_path, "wb") as vol:
                if vol_num == 1:
                    # Write volume header
                    hash_bytes = original_hash.encode("ascii").ljust(64, b"\x00")
                    vol_header = struct.pack(
                        VOLUME_HEADER_FORMAT,
                        VOLUME_MAGIC,
                        total_volumes,
                        volume_size,
                        original_size,
                        hash_bytes,
                    )
                    vol.write(vol_header)
                    bytes_for_this_vol = first_volume_data
                else:
                    bytes_for_this_vol = volume_size

                written = 0
                while written < bytes_for_this_vol:
                    to_read = min(CHUNK_SIZE, bytes_for_this_vol - written)
                    chunk = src.read(to_read)
                    if not chunk:
                        break
                    vol.write(chunk)
                    written += len(chunk)

            progress_callback("volume_written", vol_num)
            logger.info(f"  Volume {vol_num}/{total_volumes}: {vol_path.name}")

    logger.info(f"Split complete: {total_volumes} volumes created")
    return volumes


def join_volumes(
    first_volume_path: Path,
    output_path: Optional[Path] = None,
    progress_callback: Optional[Callable] = None,
) -> Path:
    """
    Reassemble a multi-volume archive back into a single .tesseract file.

    Args:
        first_volume_path: Path to the .001 volume file.
        output_path: Output path for reassembled archive. Defaults to original name.
        progress_callback: Optional callback(event, value).

    Returns:
        Path to the reassembled archive.
    """
    progress_callback = progress_callback or (lambda *a, **kw: None)
    first_volume_path = Path(first_volume_path).resolve()

    # Read volume header from first volume
    with open(first_volume_path, "rb") as f:
        header_data = f.read(VOLUME_HEADER_SIZE)

    magic, total_volumes, volume_size, original_size, raw_hash = struct.unpack(
        VOLUME_HEADER_FORMAT, header_data
    )

    if magic != VOLUME_MAGIC:
        raise ValueError("Not a Tesseract volume file (bad magic)")

    expected_hash = raw_hash.rstrip(b"\x00").decode("ascii")

    # Determine base path (strip .001)
    base_name = first_volume_path.name
    if base_name.endswith(".001"):
        base_name = base_name[:-4]
    base_path = first_volume_path.parent / base_name

    if output_path is None:
        output_path = base_path
    output_path = Path(output_path).resolve()

    if output_path.exists():
        raise FileExistsError(f"Output already exists: {output_path}")

    logger.info(f"Joining {total_volumes} volumes into {output_path}")

    # Verify all volumes exist
    for vol_num in range(1, total_volumes + 1):
        vol_path = _volume_path(base_path, vol_num)
        if not vol_path.exists():
            # Try from the first_volume_path's parent
            alt_path = _volume_path(first_volume_path.parent / base_name, vol_num)
            if not alt_path.exists():
                raise FileNotFoundError(f"Missing volume {vol_num}: {vol_path}")

    # Reassemble
    hasher = hashlib.sha256()
    with open(output_path, "wb") as out:
        for vol_num in range(1, total_volumes + 1):
            vol_path = _volume_path(base_path, vol_num)
            if not vol_path.exists():
                vol_path = _volume_path(first_volume_path.parent / base_name, vol_num)

            with open(vol_path, "rb") as vol:
                if vol_num == 1:
                    # Skip volume header
                    vol.read(VOLUME_HEADER_SIZE)

                while True:
                    chunk = vol.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    hasher.update(chunk)
                    out.write(chunk)

            progress_callback("volume_read", vol_num)
            logger.info(f"  Read volume {vol_num}/{total_volumes}")

    # Verify integrity
    actual_hash = hasher.hexdigest()
    actual_size = output_path.stat().st_size

    if actual_size != original_size:
        output_path.unlink()
        raise RuntimeError(
            f"Size mismatch: expected {original_size}, got {actual_size}"
        )

    if actual_hash != expected_hash:
        output_path.unlink()
        raise RuntimeError(
            f"Hash mismatch after join — volumes may be corrupt or out of order"
        )

    logger.info(f"Join complete: {output_path} ({actual_size} bytes, hash verified)")
    return output_path
