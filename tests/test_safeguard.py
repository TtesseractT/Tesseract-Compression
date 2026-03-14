"""Tests for the safeguard module — staging, integrity verification, and failsafe encoding.

CRITICAL SAFETY TESTS:
  - Source files are NEVER modified, even during failures
  - Shard CRC verification catches corruption
  - Source modification during encoding is detected and aborted
  - Staging directory is cleaned up on success and failure
  - No partial/corrupt archive ever appears at the final output path
  - The final archive only materializes after ALL verification passes
"""

import blake3
import os
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from tesseract.encoder import TesseractEncoder
from tesseract.decoder import TesseractDecoder
from tesseract.safeguard import (
    StagingArea,
    ShardInfo,
    preflight_check,
    verify_source_unchanged,
    _compute_file_crc,
)
from tesseract.scanner import FileEntry


def _hash_all_files(directory: Path) -> dict:
    """Build a {relative_path: sha256_hash} dict for all files in a directory."""
    hashes = {}
    for root, _, files in os.walk(directory):
        for name in files:
            fpath = Path(root) / name
            rel = str(fpath.relative_to(directory))
            with open(fpath, "rb") as f:
                hashes[rel] = blake3.blake3(f.read()).hexdigest()
    return hashes


# ── StagingArea unit tests ────────────────────────────────────────

class TestStagingArea:
    def test_create_staging_dir(self, tmp_path):
        staging = StagingArea(tmp_path / "staging")
        staging.create()
        assert staging.staging_dir.exists()

    def test_create_cleans_prior(self, tmp_path):
        staging_dir = tmp_path / "staging"
        staging_dir.mkdir()
        (staging_dir / "old_junk.txt").write_text("leftover")
        staging = StagingArea(staging_dir)
        staging.create()
        assert not (staging_dir / "old_junk.txt").exists()

    def test_shard_path(self, tmp_path):
        staging = StagingArea(tmp_path / "staging")
        p = staging.shard_path("shard_000001")
        assert p.name == "shard_000001.shard"

    def test_register_shard_computes_crc(self, tmp_path):
        staging = StagingArea(tmp_path / "staging")
        staging.create()
        shard_path = staging.shard_path("test_shard")
        shard_path.write_bytes(b"test shard data")

        info = staging.register_shard(
            "test_shard", "file.txt", "abc123", 15, compressed_size=10,
        )
        assert info.crc32 != 0
        assert info.size == 15  # len(b"test shard data")
        assert info.relative_path == "file.txt"
        assert "test_shard" in staging.shards

    def test_register_missing_shard_raises(self, tmp_path):
        staging = StagingArea(tmp_path / "staging")
        staging.create()
        with pytest.raises(RuntimeError, match="not found"):
            staging.register_shard(
                "nonexistent", "file.txt", "abc", 0, compressed_size=0,
            )

    def test_verify_shard_passes(self, tmp_path):
        staging = StagingArea(tmp_path / "staging")
        staging.create()
        staging.shard_path("s1").write_bytes(b"good data")
        staging.register_shard("s1", "f.txt", "h", 9, compressed_size=5)
        assert staging.verify_shard("s1") is True

    def test_verify_shard_detects_corruption(self, tmp_path):
        staging = StagingArea(tmp_path / "staging")
        staging.create()
        staging.shard_path("s1").write_bytes(b"original data")
        staging.register_shard("s1", "f.txt", "h", 13, compressed_size=5)

        # Corrupt the shard after registration
        staging.shard_path("s1").write_bytes(b"TAMPERED data!")
        assert staging.verify_shard("s1") is False

    def test_verify_shard_detects_missing_file(self, tmp_path):
        staging = StagingArea(tmp_path / "staging")
        staging.create()
        staging.shard_path("s1").write_bytes(b"data")
        staging.register_shard("s1", "f.txt", "h", 4, compressed_size=2)

        # Delete the shard
        staging.shard_path("s1").unlink()
        assert staging.verify_shard("s1") is False

    def test_verify_all_shards(self, tmp_path):
        staging = StagingArea(tmp_path / "staging")
        staging.create()

        # Good shard
        staging.shard_path("s1").write_bytes(b"good")
        staging.register_shard("s1", "a.txt", "h1", 4, compressed_size=2)

        # Another good shard
        staging.shard_path("s2").write_bytes(b"also good")
        staging.register_shard("s2", "b.txt", "h2", 9, compressed_size=5)

        assert staging.verify_all_shards() == []

    def test_verify_all_detects_bad_shard(self, tmp_path):
        staging = StagingArea(tmp_path / "staging")
        staging.create()

        staging.shard_path("s1").write_bytes(b"fine")
        staging.register_shard("s1", "a.txt", "h1", 4, compressed_size=2)

        staging.shard_path("s2").write_bytes(b"will be tampered")
        staging.register_shard("s2", "b.txt", "h2", 16, compressed_size=10)

        # Corrupt s2
        staging.shard_path("s2").write_bytes(b"CORRUPTED!!!!!!")
        failed = staging.verify_all_shards()
        assert "s2" in failed
        assert "s1" not in failed

    def test_stream_shard_to(self, tmp_path):
        staging = StagingArea(tmp_path / "staging")
        staging.create()
        data = os.urandom(10_000)
        staging.shard_path("s1").write_bytes(data)
        staging.register_shard("s1", "f.txt", "h", len(data), compressed_size=len(data))

        output = tmp_path / "output.bin"
        with open(output, "wb") as fh:
            written = staging.stream_shard_to("s1", fh)
        assert written == len(data)
        assert output.read_bytes() == data

    def test_cleanup_removes_dir(self, tmp_path):
        staging = StagingArea(tmp_path / "staging")
        staging.create()
        staging.shard_path("s1").write_bytes(b"data")
        staging.cleanup()
        assert not staging.staging_dir.exists()

    def test_cleanup_idempotent(self, tmp_path):
        staging = StagingArea(tmp_path / "staging")
        staging.create()
        staging.cleanup()
        staging.cleanup()  # Should not raise

    def test_index_saved_to_disk(self, tmp_path):
        staging = StagingArea(tmp_path / "staging")
        staging.create()
        staging.shard_path("s1").write_bytes(b"data")
        staging.register_shard("s1", "f.txt", "hash123", 4, compressed_size=2)
        staging.save_index()
        assert staging.index_path.exists()
        import json
        index = json.loads(staging.index_path.read_text())
        assert "s1" in index
        assert index["s1"]["relative_path"] == "f.txt"


# ── Preflight & source verification tests ─────────────────────────

class TestPreflightCheck:
    def _make_entry(self, tmp_path, name, content):
        p = tmp_path / name
        p.write_bytes(content)
        h = blake3.blake3(content).hexdigest()
        return FileEntry(
            path=p, relative_path=name, size=len(content),
            filename=name, extension=Path(name).suffix,
            modified_time=p.stat().st_mtime, full_hash=h,
        )

    def test_preflight_passes(self, tmp_path):
        entries = [
            self._make_entry(tmp_path, "a.txt", b"hello"),
            self._make_entry(tmp_path, "b.txt", b"world"),
        ]
        snapshot = preflight_check(entries)
        assert len(snapshot) == 2
        assert snapshot["a.txt"] == blake3.blake3(b"hello").hexdigest()

    def test_preflight_fails_unreadable(self, tmp_path):
        entries = [self._make_entry(tmp_path, "a.txt", b"data")]
        # Delete the file to make it unreadable
        (tmp_path / "a.txt").unlink()
        with pytest.raises(RuntimeError, match="Pre-flight check failed"):
            preflight_check(entries)


class TestVerifySourceUnchanged:
    def _make_entry(self, tmp_path, name, content):
        p = tmp_path / name
        p.write_bytes(content)
        h = blake3.blake3(content).hexdigest()
        return FileEntry(
            path=p, relative_path=name, size=len(content),
            filename=name, extension=Path(name).suffix,
            modified_time=p.stat().st_mtime, full_hash=h,
        )

    def test_unchanged_files_pass(self, tmp_path):
        entries = [self._make_entry(tmp_path, "a.txt", b"original")]
        snapshot = {"a.txt": blake3.blake3(b"original").hexdigest()}
        changed = verify_source_unchanged(entries, snapshot)
        assert changed == []

    def test_modified_file_detected(self, tmp_path):
        entries = [self._make_entry(tmp_path, "a.txt", b"original")]
        snapshot = {"a.txt": blake3.blake3(b"original").hexdigest()}
        # Modify the file
        (tmp_path / "a.txt").write_bytes(b"MODIFIED!")
        changed = verify_source_unchanged(entries, snapshot)
        assert "a.txt" in changed

    def test_deleted_file_detected(self, tmp_path):
        entries = [self._make_entry(tmp_path, "a.txt", b"data")]
        snapshot = {"a.txt": blake3.blake3(b"data").hexdigest()}
        (tmp_path / "a.txt").unlink()
        changed = verify_source_unchanged(entries, snapshot)
        assert "a.txt" in changed


# ── File CRC utility tests ────────────────────────────────────────

class TestComputeFileCrc:
    def test_crc_deterministic(self, tmp_path):
        f = tmp_path / "data.bin"
        f.write_bytes(b"test content")
        crc1, size1 = _compute_file_crc(f)
        crc2, size2 = _compute_file_crc(f)
        assert crc1 == crc2
        assert size1 == size2 == 12

    def test_different_content_different_crc(self, tmp_path):
        f1 = tmp_path / "a.bin"
        f2 = tmp_path / "b.bin"
        f1.write_bytes(b"AAAA")
        f2.write_bytes(b"BBBB")
        assert _compute_file_crc(f1)[0] != _compute_file_crc(f2)[0]


# ── End-to-end staged encoder safety tests ────────────────────────

class TestStagedEncoderSafety:
    """Critical safety tests for the full staging pipeline."""

    def test_source_untouched_after_encode(self, sample_tree, tmp_path):
        """Source files must be completely unchanged after encoding."""
        before = _hash_all_files(sample_tree)

        archive = tmp_path / "out.tesseract"
        encoder = TesseractEncoder(workers=1)
        encoder.encode(sample_tree, archive)

        after = _hash_all_files(sample_tree)
        assert before == after, "Source files were modified during encoding!"

    def test_source_untouched_after_failed_encode(self, sample_tree, tmp_path):
        """Source files untouched even when encoding fails."""
        before = _hash_all_files(sample_tree)

        archive = tmp_path / "out.tesseract"
        archive.write_bytes(b"existing")  # Cause FileExistsError

        encoder = TesseractEncoder(workers=1)
        with pytest.raises(FileExistsError):
            encoder.encode(sample_tree, archive)

        after = _hash_all_files(sample_tree)
        assert before == after, "Source files changed by failed encode!"

    def test_staging_dir_cleaned_on_success(self, sample_tree, tmp_path):
        """Staging directory must be removed after successful encoding."""
        archive = tmp_path / "out.tesseract"
        encoder = TesseractEncoder(workers=1)
        encoder.encode(sample_tree, archive)

        # No staging directory should remain
        staging_dirs = list(tmp_path.glob(".*_staging"))
        assert staging_dirs == [], f"Staging dirs left behind: {staging_dirs}"

    def test_no_tmp_file_on_success(self, sample_tree, tmp_path):
        """No .tmp file should remain after successful encoding."""
        archive = tmp_path / "out.tesseract"
        encoder = TesseractEncoder(workers=1)
        encoder.encode(sample_tree, archive)

        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == [], f".tmp files left behind: {tmp_files}"

    def test_no_partial_archive_on_staging_failure(self, sample_tree, tmp_path):
        """If shard staging fails, no archive should exist at the output path."""
        archive = tmp_path / "out.tesseract"

        # Use encryption to force sequential _write_file_block path (picklable patching)
        encoder = TesseractEncoder(workers=1, password="test")
        original_write = encoder._write_file_block
        call_count = 0

        def failing_write(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                raise OSError("Simulated disk failure during staging")
            return original_write(*args, **kwargs)

        encoder._write_file_block = failing_write

        with pytest.raises(OSError, match="Simulated disk failure"):
            encoder.encode(sample_tree, archive)

        assert not archive.exists(), "Partial archive exists at output path after failure!"

    def test_full_roundtrip_with_staging(self, sample_tree, tmp_path):
        """Complete encode → decode cycle using staged pipeline."""
        archive = tmp_path / "staged.tesseract"
        encoder = TesseractEncoder(workers=1)
        encoder.encode(sample_tree, archive)

        output = tmp_path / "restored"
        decoder = TesseractDecoder(workers=1, verify=True)
        decoder.decode(archive, output)

        original = _hash_all_files(sample_tree)
        restored = _hash_all_files(output)
        assert original == restored

    def test_solid_roundtrip_with_staging(self, sample_tree, tmp_path):
        """Solid mode also uses staging safely."""
        archive = tmp_path / "solid_staged.tesseract"
        encoder = TesseractEncoder(workers=1, solid=True)
        encoder.encode(sample_tree, archive)

        output = tmp_path / "restored"
        decoder = TesseractDecoder(workers=1, verify=True)
        decoder.decode(archive, output)

        original = _hash_all_files(sample_tree)
        restored = _hash_all_files(output)
        assert original == restored

    def test_encrypted_roundtrip_with_staging(self, sample_tree, tmp_path):
        """Encrypted archive with staging safety."""
        archive = tmp_path / "enc_staged.tesseract"
        encoder = TesseractEncoder(workers=1, password="safe_pass!")
        encoder.encode(sample_tree, archive)

        output = tmp_path / "restored"
        decoder = TesseractDecoder(workers=1, password="safe_pass!", verify=True)
        decoder.decode(archive, output)

        original = _hash_all_files(sample_tree)
        restored = _hash_all_files(output)
        assert original == restored

    def test_staging_cleanup_on_source_change(self, sample_tree, tmp_path):
        """If source files change during compression, abort and clean up."""
        archive = tmp_path / "out.tesseract"

        encoder = TesseractEncoder(workers=1)

        # Monkey-patch verify_source_unchanged to simulate detection
        from tesseract import safeguard
        original_verify = safeguard.verify_source_unchanged

        def fake_verify(entries, snapshot, progress_callback=None, **kwargs):
            return ["docs/readme.txt"]  # Pretend this file changed

        with patch.object(safeguard, "verify_source_unchanged", fake_verify):
            with pytest.raises(RuntimeError, match="SAFETY ABORT"):
                encoder.encode(sample_tree, archive)

        assert not archive.exists(), "Archive should not exist after safety abort"

        # Staging should be cleaned up
        staging_dirs = list(tmp_path.glob(".*_staging"))
        assert staging_dirs == [], f"Staging not cleaned up: {staging_dirs}"

    def test_empty_directory_with_staging(self, tmp_path):
        """Empty directory should produce a valid archive via staging."""
        source = tmp_path / "empty_source"
        source.mkdir()
        archive = tmp_path / "empty.tesseract"
        encoder = TesseractEncoder(workers=1)
        manifest = encoder.encode(source, archive)
        assert manifest.file_count == 0
        assert archive.exists()
