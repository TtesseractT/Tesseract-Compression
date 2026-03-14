"""Binary format handling for .tesseract archives.

Archive Layout (v2):
    [HEADER: 120 bytes]
        magic (8B) | version (2B) | flags (2B)
        manifest_offset (8B) | manifest_compressed_size (8B)
        total_files (8B) | total_unique (8B)
        recovery_offset (8B) | recovery_size (8B)
        encryption_salt (32B) | password_check (32B)
        comment_length (2B) | reserved (2B)
    [ARCHIVE COMMENT: variable, 0-65535 bytes]
    [DATA BLOCKS]
        Normal mode — per-file blocks:
            [BLOCK HEADER: 80 bytes]
                content_hash (64B) | original_size (8B) | compressed_size (8B)
            [COMPRESSED DATA: variable size]
        Solid mode — single continuous block:
            [SOLID HEADER: 16 bytes]
                total_uncompressed (8B) | total_compressed (8B)
            [COMPRESSED STREAM: variable size]
    [MANIFEST]
        gzip-compressed JSON with all metadata and data offsets
    [FOOTER: 8 bytes]
        magic_footer (8B)
    [RECOVERY RECORDS: optional, variable size]

Header Flags (bitmask):
    FLAG_ENCRYPTED   = 0x0001  — AES-256-GCM encryption enabled
    FLAG_SOLID       = 0x0002  — Solid compression mode
    FLAG_RECOVERY    = 0x0004  — Recovery records appended
    FLAG_LOCKED      = 0x0008  — Archive is locked/finalized
    FLAG_PERMISSIONS = 0x0010  — File permissions stored in manifest
"""

import struct
from typing import NamedTuple

MAGIC_HEADER = b"TSSRACT\x02"
MAGIC_FOOTER = b"TSSRACTX"
FORMAT_VERSION = 2

# Feature flags
FLAG_ENCRYPTED   = 0x0001
FLAG_SOLID       = 0x0002
FLAG_RECOVERY    = 0x0004
FLAG_LOCKED      = 0x0008
FLAG_PERMISSIONS = 0x0010

# Header: magic(8) + version(2) + flags(2) + 6*uint64(48) + salt(32) + pwcheck(32) + comment_len(2) + reserved(2)
# = 8 + 2 + 2 + 48 + 32 + 32 + 2 + 2 = 128 bytes
HEADER_FORMAT = "<8sHH6Q32s32sHH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

# Block header: hash(64) + original_size(8) + compressed_size(8) = 80 bytes
BLOCK_HEADER_FORMAT = "<64sQQ"
BLOCK_HEADER_SIZE = struct.calcsize(BLOCK_HEADER_FORMAT)

# Solid header: total_uncompressed(8) + total_compressed(8) = 16 bytes
SOLID_HEADER_FORMAT = "<QQ"
SOLID_HEADER_SIZE = struct.calcsize(SOLID_HEADER_FORMAT)


class ArchiveHeader(NamedTuple):
    magic: bytes
    version: int
    flags: int
    manifest_offset: int
    manifest_compressed_size: int
    total_files: int
    total_unique: int
    recovery_offset: int
    recovery_size: int
    encryption_salt: bytes
    password_check: bytes
    comment_length: int
    reserved: int

    @property
    def is_encrypted(self) -> bool:
        return bool(self.flags & FLAG_ENCRYPTED)

    @property
    def is_solid(self) -> bool:
        return bool(self.flags & FLAG_SOLID)

    @property
    def has_recovery(self) -> bool:
        return bool(self.flags & FLAG_RECOVERY)

    @property
    def is_locked(self) -> bool:
        return bool(self.flags & FLAG_LOCKED)

    @property
    def has_permissions(self) -> bool:
        return bool(self.flags & FLAG_PERMISSIONS)


class BlockHeader(NamedTuple):
    content_hash: str
    original_size: int
    compressed_size: int


class SolidHeader(NamedTuple):
    total_uncompressed: int
    total_compressed: int


def pack_header(
    manifest_offset: int = 0,
    manifest_compressed_size: int = 0,
    total_files: int = 0,
    total_unique: int = 0,
    recovery_offset: int = 0,
    recovery_size: int = 0,
    flags: int = 0,
    encryption_salt: bytes = b"",
    password_check: bytes = b"",
    comment_length: int = 0,
) -> bytes:
    """Pack an archive header into bytes."""
    return struct.pack(
        HEADER_FORMAT,
        MAGIC_HEADER,
        FORMAT_VERSION,
        flags,
        manifest_offset,
        manifest_compressed_size,
        total_files,
        total_unique,
        recovery_offset,
        recovery_size,
        encryption_salt.ljust(32, b"\x00")[:32],
        password_check.ljust(32, b"\x00")[:32],
        comment_length,
        0,  # reserved
    )


def unpack_header(data: bytes) -> ArchiveHeader:
    """Unpack archive header from bytes. Validates magic and version."""
    if len(data) < HEADER_SIZE:
        raise ValueError(f"Header too short: {len(data)} bytes, need {HEADER_SIZE}")
    values = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
    header = ArchiveHeader(*values)
    if header.magic != MAGIC_HEADER:
        raise ValueError(f"Invalid archive: bad magic bytes {header.magic!r}")
    if header.version > FORMAT_VERSION:
        raise ValueError(f"Unsupported archive version: {header.version}")
    return header


def pack_block_header(content_hash: str, original_size: int, compressed_size: int) -> bytes:
    """Pack a data block header into bytes."""
    hash_bytes = content_hash.encode("ascii").ljust(64, b"\x00")
    return struct.pack(BLOCK_HEADER_FORMAT, hash_bytes, original_size, compressed_size)


def unpack_block_header(data: bytes) -> BlockHeader:
    """Unpack a data block header from bytes."""
    if len(data) < BLOCK_HEADER_SIZE:
        raise ValueError(f"Block header too short: {len(data)} bytes, need {BLOCK_HEADER_SIZE}")
    raw_hash, original_size, compressed_size = struct.unpack(
        BLOCK_HEADER_FORMAT, data[:BLOCK_HEADER_SIZE]
    )
    content_hash = raw_hash.rstrip(b"\x00").decode("ascii")
    return BlockHeader(content_hash, original_size, compressed_size)


def pack_solid_header(total_uncompressed: int, total_compressed: int) -> bytes:
    """Pack a solid-mode stream header into bytes."""
    return struct.pack(SOLID_HEADER_FORMAT, total_uncompressed, total_compressed)


def unpack_solid_header(data: bytes) -> SolidHeader:
    """Unpack a solid-mode stream header from bytes."""
    if len(data) < SOLID_HEADER_SIZE:
        raise ValueError(f"Solid header too short: {len(data)} bytes")
    return SolidHeader(*struct.unpack(SOLID_HEADER_FORMAT, data[:SOLID_HEADER_SIZE]))
