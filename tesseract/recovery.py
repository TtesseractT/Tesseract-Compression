"""Recovery records for Tesseract archives.

Implements Reed-Solomon-inspired XOR-based parity blocks that allow
repairing damaged archives. Recovery data is appended after the footer
and referenced by the header.

Strategy:
    - Divide the archive data region into fixed-size slices
    - Compute XOR parity across groups of N slices
    - Store parity blocks + metadata after the archive footer
    - On repair: detect damaged slices via CRC32, rebuild from parity

This can recover from single-slice corruption per parity group.
"""

import hashlib
import logging
import struct
import zlib
from pathlib import Path
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

RECOVERY_MAGIC = b"TSSR\x01\x00\x00\x00"  # 8 bytes
SLICE_SIZE = 512 * 1024  # 512 KB per slice
DEFAULT_REDUNDANCY = 5   # 5% recovery data by default
MAX_REDUNDANCY = 30      # max 30%


def _compute_slice_crc(data: bytes) -> int:
    """CRC32 for a slice of data."""
    return zlib.crc32(data) & 0xFFFFFFFF


class RecoveryRecord:
    """Stores recovery metadata and parity blocks."""

    def __init__(self):
        self.slice_size: int = SLICE_SIZE
        self.total_slices: int = 0
        self.parity_group_size: int = 0
        self.slice_crcs: List[int] = []
        self.parity_blocks: List[bytes] = []

    def serialize(self) -> bytes:
        """Serialize recovery record to bytes."""
        # Header: magic(8) + slice_size(4) + total_slices(4) + group_size(4) + parity_count(4)
        header = struct.pack(
            "<8sIIII",
            RECOVERY_MAGIC,
            self.slice_size,
            self.total_slices,
            self.parity_group_size,
            len(self.parity_blocks),
        )
        # CRC table: 4 bytes each
        crc_data = struct.pack(f"<{self.total_slices}I", *self.slice_crcs)
        # Parity blocks: each is slice_size bytes
        parity_data = b"".join(self.parity_blocks)
        # Total size footer for seeking
        total = len(header) + len(crc_data) + len(parity_data) + 8
        footer = struct.pack("<Q", total)
        return header + crc_data + parity_data + footer

    @classmethod
    def deserialize(cls, data: bytes) -> "RecoveryRecord":
        """Deserialize recovery record from bytes."""
        rec = cls()
        magic, rec.slice_size, rec.total_slices, rec.parity_group_size, parity_count = \
            struct.unpack("<8sIIII", data[:24])

        if magic != RECOVERY_MAGIC:
            raise ValueError("Invalid recovery record magic")

        offset = 24
        crc_size = rec.total_slices * 4
        rec.slice_crcs = list(struct.unpack(
            f"<{rec.total_slices}I", data[offset:offset + crc_size]
        ))
        offset += crc_size

        rec.parity_blocks = []
        for _ in range(parity_count):
            rec.parity_blocks.append(data[offset:offset + rec.slice_size])
            offset += rec.slice_size

        return rec


def generate_recovery_data(
    archive_path: Path,
    data_start: int,
    data_end: int,
    redundancy_percent: int = DEFAULT_REDUNDANCY,
) -> RecoveryRecord:
    """
    Generate recovery records for the data region of an archive.

    Args:
        archive_path: Path to the archive file.
        data_start: Byte offset where data blocks begin (after header).
        data_end: Byte offset where data blocks end (before manifest).
        redundancy_percent: Percentage of data size to use for recovery (1-30).

    Returns:
        RecoveryRecord with parity data.
    """
    redundancy_percent = max(1, min(MAX_REDUNDANCY, redundancy_percent))
    data_size = data_end - data_start

    if data_size <= 0:
        rec = RecoveryRecord()
        rec.total_slices = 0
        return rec

    total_slices = (data_size + SLICE_SIZE - 1) // SLICE_SIZE

    # Calculate parity group size from redundancy percentage
    # N data slices per 1 parity slice → redundancy ~ 1/N * 100
    target_parity_slices = max(1, (total_slices * redundancy_percent) // 100)
    parity_group_size = max(2, total_slices // target_parity_slices)

    rec = RecoveryRecord()
    rec.slice_size = SLICE_SIZE
    rec.total_slices = total_slices
    rec.parity_group_size = parity_group_size
    rec.slice_crcs = []
    rec.parity_blocks = []

    logger.info(
        f"Generating recovery data: {total_slices} slices, "
        f"group size {parity_group_size}, ~{redundancy_percent}% redundancy"
    )

    with open(archive_path, "rb") as f:
        # Read all slices and compute CRCs
        f.seek(data_start)
        slices: List[bytes] = []
        for i in range(total_slices):
            remaining = min(SLICE_SIZE, data_end - f.tell())
            slice_data = f.read(remaining)
            # Pad last slice to full size
            if len(slice_data) < SLICE_SIZE:
                slice_data = slice_data.ljust(SLICE_SIZE, b"\x00")
            slices.append(slice_data)
            rec.slice_crcs.append(_compute_slice_crc(slice_data))

        # Compute XOR parity for each group
        for group_start in range(0, total_slices, parity_group_size):
            group_end = min(group_start + parity_group_size, total_slices)
            parity = bytearray(SLICE_SIZE)
            for i in range(group_start, group_end):
                for j in range(len(slices[i])):
                    parity[j] ^= slices[i][j]
            rec.parity_blocks.append(bytes(parity))

    logger.info(f"Generated {len(rec.parity_blocks)} parity blocks")
    return rec


def repair_archive(
    archive_path: Path,
    data_start: int,
    data_end: int,
    recovery: RecoveryRecord,
) -> Tuple[int, int]:
    """
    Attempt to repair a damaged archive using its recovery records.

    Returns (slices_checked, slices_repaired).
    Raises RuntimeError if damage is beyond recovery capability.
    """
    data_size = data_end - data_start
    repaired = 0
    checked = 0
    unrecoverable = []

    with open(archive_path, "r+b") as f:
        # Read current slices and check CRCs
        f.seek(data_start)
        current_slices: List[bytes] = []
        damaged_indices: List[int] = []

        for i in range(recovery.total_slices):
            remaining = min(SLICE_SIZE, data_end - f.tell())
            slice_data = f.read(remaining)
            if len(slice_data) < SLICE_SIZE:
                slice_data = slice_data.ljust(SLICE_SIZE, b"\x00")
            current_slices.append(slice_data)

            actual_crc = _compute_slice_crc(slice_data)
            if actual_crc != recovery.slice_crcs[i]:
                damaged_indices.append(i)
                logger.warning(f"Damaged slice {i}: CRC {actual_crc:#010x} != {recovery.slice_crcs[i]:#010x}")
            checked += 1

        if not damaged_indices:
            logger.info("No damage detected — archive is intact")
            return checked, 0

        logger.info(f"Found {len(damaged_indices)} damaged slices, attempting repair...")

        # Try to repair each damaged slice using its parity group
        for damaged_idx in damaged_indices:
            group_idx = damaged_idx // recovery.parity_group_size
            group_start = group_idx * recovery.parity_group_size
            group_end = min(group_start + recovery.parity_group_size, recovery.total_slices)

            # Count damaged slices in this group
            group_damaged = [i for i in damaged_indices if group_start <= i < group_end]
            if len(group_damaged) > 1:
                unrecoverable.extend(group_damaged)
                continue

            if group_idx >= len(recovery.parity_blocks):
                unrecoverable.append(damaged_idx)
                continue

            # Reconstruct: parity XOR all_good_slices = damaged_slice
            rebuilt = bytearray(recovery.parity_blocks[group_idx])
            for i in range(group_start, group_end):
                if i != damaged_idx:
                    for j in range(SLICE_SIZE):
                        rebuilt[j] ^= current_slices[i][j]

            # Verify reconstructed slice CRC
            rebuilt_bytes = bytes(rebuilt)
            rebuilt_crc = _compute_slice_crc(rebuilt_bytes)
            if rebuilt_crc != recovery.slice_crcs[damaged_idx]:
                unrecoverable.append(damaged_idx)
                logger.error(f"Slice {damaged_idx}: reconstruction CRC mismatch")
                continue

            # Write repaired slice back
            write_offset = data_start + damaged_idx * SLICE_SIZE
            f.seek(write_offset)
            # Don't write padding beyond actual data
            actual_size = min(SLICE_SIZE, data_size - damaged_idx * SLICE_SIZE)
            f.write(rebuilt_bytes[:actual_size])
            repaired += 1
            logger.info(f"Repaired slice {damaged_idx}")

    if unrecoverable:
        unique_unrecoverable = sorted(set(unrecoverable))
        raise RuntimeError(
            f"Cannot repair {len(unique_unrecoverable)} slices "
            f"(multiple corruptions in same parity group): {unique_unrecoverable}"
        )

    logger.info(f"Repair complete: {checked} checked, {repaired} repaired")
    return checked, repaired
