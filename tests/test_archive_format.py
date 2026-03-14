"""Tests for the archive_format module — binary header packing / unpacking."""

import struct

import pytest

from tesseract.archive_format import (
    HEADER_SIZE, BLOCK_HEADER_SIZE, SOLID_HEADER_SIZE,
    MAGIC_HEADER, MAGIC_FOOTER, FORMAT_VERSION,
    FLAG_ENCRYPTED, FLAG_SOLID, FLAG_RECOVERY, FLAG_LOCKED, FLAG_PERMISSIONS,
    pack_header, unpack_header,
    pack_block_header, unpack_block_header,
    pack_solid_header, unpack_solid_header,
    ArchiveHeader,
)


class TestHeaderPackUnpack:
    def test_roundtrip_defaults(self):
        packed = pack_header()
        header = unpack_header(packed)
        assert header.magic == MAGIC_HEADER
        assert header.version == FORMAT_VERSION
        assert header.flags == 0
        assert header.manifest_offset == 0
        assert header.total_files == 0

    def test_roundtrip_with_values(self):
        packed = pack_header(
            manifest_offset=1024,
            manifest_compressed_size=512,
            total_files=100,
            total_unique=80,
            recovery_offset=2048,
            recovery_size=256,
            flags=FLAG_ENCRYPTED | FLAG_SOLID | FLAG_RECOVERY,
            encryption_salt=b"\xaa" * 32,
            password_check=b"\xbb" * 32,
            comment_length=42,
        )
        h = unpack_header(packed)
        assert h.manifest_offset == 1024
        assert h.manifest_compressed_size == 512
        assert h.total_files == 100
        assert h.total_unique == 80
        assert h.recovery_offset == 2048
        assert h.recovery_size == 256
        assert h.is_encrypted is True
        assert h.is_solid is True
        assert h.has_recovery is True
        assert h.is_locked is False
        assert h.has_permissions is False
        assert h.comment_length == 42
        assert h.encryption_salt == b"\xaa" * 32
        assert h.password_check == b"\xbb" * 32

    def test_header_size_is_128(self):
        assert HEADER_SIZE == 128
        assert len(pack_header()) == 128

    def test_bad_magic_raises(self):
        data = b"\x00" * HEADER_SIZE
        with pytest.raises(ValueError, match="Invalid archive"):
            unpack_header(data)

    def test_short_data_raises(self):
        with pytest.raises(ValueError, match="Header too short"):
            unpack_header(b"short")

    def test_flag_properties(self):
        packed = pack_header(flags=FLAG_LOCKED | FLAG_PERMISSIONS)
        h = unpack_header(packed)
        assert h.is_locked is True
        assert h.has_permissions is True
        assert h.is_encrypted is False
        assert h.is_solid is False
        assert h.has_recovery is False


class TestBlockHeaderPackUnpack:
    def test_roundtrip(self):
        content_hash = "a" * 64
        packed = pack_block_header(content_hash, 1024, 512)
        bh = unpack_block_header(packed)
        assert bh.content_hash == content_hash
        assert bh.original_size == 1024
        assert bh.compressed_size == 512

    def test_block_header_size_is_80(self):
        assert BLOCK_HEADER_SIZE == 80
        assert len(pack_block_header("x" * 64, 0, 0)) == 80

    def test_short_data_raises(self):
        with pytest.raises(ValueError, match="Block header too short"):
            unpack_block_header(b"tiny")


class TestSolidHeaderPackUnpack:
    def test_roundtrip(self):
        packed = pack_solid_header(100_000, 50_000)
        sh = unpack_solid_header(packed)
        assert sh.total_uncompressed == 100_000
        assert sh.total_compressed == 50_000

    def test_solid_header_size_is_16(self):
        assert SOLID_HEADER_SIZE == 16

    def test_short_data_raises(self):
        with pytest.raises(ValueError):
            unpack_solid_header(b"\x00")
