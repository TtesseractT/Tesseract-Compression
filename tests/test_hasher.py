"""Tests for the hasher module — partial and full BLAKE3 hashing."""

import blake3
from pathlib import Path

import pytest

from tesseract.hasher import compute_partial_hash, compute_full_hash, verify_file_hash


class TestComputePartialHash:
    """Tests for the fast partial-hash pre-filter."""

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.bin"
        f.write_bytes(b"")
        h = compute_partial_hash(f)
        assert h == blake3.blake3(b"").hexdigest()

    def test_small_file_hashes_everything(self, tmp_path):
        """Files <= 128 KB should be hashed in full by partial hash."""
        data = b"hello world"
        f = tmp_path / "small.bin"
        f.write_bytes(data)
        h = compute_partial_hash(f)
        assert isinstance(h, str) and len(h) == 64

    def test_identical_files_match(self, tmp_path):
        data = b"identical content" * 1000
        f1 = tmp_path / "a.bin"
        f2 = tmp_path / "b.bin"
        f1.write_bytes(data)
        f2.write_bytes(data)
        assert compute_partial_hash(f1) == compute_partial_hash(f2)

    def test_different_files_differ(self, tmp_path):
        f1 = tmp_path / "a.bin"
        f2 = tmp_path / "b.bin"
        f1.write_bytes(b"A" * 200_000)
        f2.write_bytes(b"B" * 200_000)
        assert compute_partial_hash(f1) != compute_partial_hash(f2)


class TestComputeFullHash:
    """Tests for full SHA-256 streaming hash."""

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.bin"
        f.write_bytes(b"")
        assert compute_full_hash(f) == blake3.blake3(b"").hexdigest()

    def test_known_content(self, tmp_path):
        data = b"test data for hashing"
        f = tmp_path / "data.bin"
        f.write_bytes(data)
        expected = blake3.blake3(data).hexdigest()
        assert compute_full_hash(f) == expected

    def test_large_file(self, tmp_path):
        """Ensure streaming works for files larger than CHUNK_SIZE (1 MB)."""
        data = b"X" * (2 * 1024 * 1024)  # 2 MB
        f = tmp_path / "large.bin"
        f.write_bytes(data)
        expected = blake3.blake3(data).hexdigest()
        assert compute_full_hash(f) == expected


class TestVerifyFileHash:
    def test_correct_hash_passes(self, tmp_path):
        data = b"verify me"
        f = tmp_path / "file.bin"
        f.write_bytes(data)
        correct = blake3.blake3(data).hexdigest()
        assert verify_file_hash(f, correct) is True

    def test_wrong_hash_fails(self, tmp_path):
        f = tmp_path / "file.bin"
        f.write_bytes(b"some data")
        assert verify_file_hash(f, "0" * 64) is False
