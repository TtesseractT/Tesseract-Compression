"""Multi-stage file hashing for efficient duplicate detection.

Uses partial hashing (first+last 64KB) as a fast pre-filter,
then full BLAKE3 for definitive content verification.
BLAKE3 is ~6x faster than SHA-256 and parallelizes internally.
"""

import blake3
from pathlib import Path

CHUNK_SIZE = 4 * 1024 * 1024       # 4 MB read chunks (better for large files / NVMe)
PARTIAL_HASH_SIZE = 64 * 1024      # 64 KB for partial hashing


def compute_partial_hash(filepath: Path) -> str:
    """
    Compute a quick hash using the first and last 64KB of a file.
    Used as a fast pre-filter before full hashing to avoid unnecessary I/O.

    For files <= 128KB, the entire file is hashed.
    For larger files, only head + tail are hashed.
    """
    hasher = blake3.blake3()
    file_size = filepath.stat().st_size

    if file_size == 0:
        return hasher.hexdigest()

    with open(filepath, "rb") as f:
        head = f.read(min(PARTIAL_HASH_SIZE, file_size))
        hasher.update(head)

        if file_size > PARTIAL_HASH_SIZE * 2:
            f.seek(-PARTIAL_HASH_SIZE, 2)
            tail = f.read(PARTIAL_HASH_SIZE)
            hasher.update(tail)
        elif file_size > PARTIAL_HASH_SIZE:
            tail = f.read()
            hasher.update(tail)

    return hasher.hexdigest()


def compute_full_hash(filepath: Path) -> str:
    """Compute the full BLAKE3 hash of a file, streaming in 4MB chunks."""
    hasher = blake3.blake3()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def verify_file_hash(filepath: Path, expected_hash: str) -> bool:
    """Verify a file matches an expected BLAKE3 hash."""
    return compute_full_hash(filepath) == expected_hash
