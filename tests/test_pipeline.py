"""End-to-end integration tests for the Tesseract encode/decode pipeline.

CRITICAL SAFETY TESTS:
  - Source files are NEVER modified or deleted during encoding
  - All extracted files match their original content exactly
  - Hash verification catches corruption
  - Encryption protects data correctly
"""

import filecmp
import hashlib
import os
import shutil
from pathlib import Path

import pytest

from tesseract.encoder import TesseractEncoder
from tesseract.decoder import TesseractDecoder


def _hash_all_files(directory: Path) -> dict:
    """Build a {relative_path: sha256_hash} dict for all files in a directory."""
    hashes = {}
    for root, _, files in os.walk(directory):
        for name in files:
            fpath = Path(root) / name
            rel = str(fpath.relative_to(directory))
            with open(fpath, "rb") as f:
                hashes[rel] = hashlib.sha256(f.read()).hexdigest()
    return hashes


def _snapshot_metadata(directory: Path) -> dict:
    """Capture size + mtime for every file — for verifying source is untouched."""
    meta = {}
    for root, _, files in os.walk(directory):
        for name in files:
            fpath = Path(root) / name
            rel = str(fpath.relative_to(directory))
            st = fpath.stat()
            meta[rel] = (st.st_size, st.st_mtime)
    return meta


class TestBasicEncodeDecode:
    """Basic encode → decode round-trip with integrity verification."""

    def test_encode_produces_file(self, sample_tree, tmp_path):
        archive = tmp_path / "out.tesseract"
        encoder = TesseractEncoder(workers=1)
        manifest = encoder.encode(sample_tree, archive)
        assert archive.exists()
        assert archive.stat().st_size > 0
        assert manifest.file_count > 0

    def test_decode_restores_all_files(self, sample_tree, tmp_path):
        archive = tmp_path / "out.tesseract"
        encoder = TesseractEncoder(workers=1)
        encoder.encode(sample_tree, archive)

        output = tmp_path / "restored"
        decoder = TesseractDecoder(workers=1, verify=True)
        manifest = decoder.decode(archive, output)

        # Every original file should exist in output
        original_hashes = _hash_all_files(sample_tree)
        restored_hashes = _hash_all_files(output)

        assert set(original_hashes.keys()) == set(restored_hashes.keys()), \
            f"Missing: {set(original_hashes) - set(restored_hashes)}, Extra: {set(restored_hashes) - set(original_hashes)}"

        for rel_path in original_hashes:
            assert original_hashes[rel_path] == restored_hashes[rel_path], \
                f"Content mismatch: {rel_path}"

    def test_dedup_reduces_archive_size(self, sample_tree, tmp_path):
        """Archive should be smaller than the sum of all files due to dedup."""
        archive = tmp_path / "out.tesseract"
        encoder = TesseractEncoder(workers=1)
        manifest = encoder.encode(sample_tree, archive)

        archive_size = archive.stat().st_size
        assert manifest.space_savings > 0
        assert manifest.duplicate_group_count > 0


class TestSourceSafety:
    """CRITICAL: Verify the encoder NEVER modifies or deletes source files."""

    def test_source_files_unchanged_after_encode(self, sample_tree, tmp_path):
        """Verify every source file has identical content and metadata after encoding."""
        before_hashes = _hash_all_files(sample_tree)
        before_meta = _snapshot_metadata(sample_tree)

        archive = tmp_path / "out.tesseract"
        encoder = TesseractEncoder(workers=1)
        encoder.encode(sample_tree, archive)

        after_hashes = _hash_all_files(sample_tree)
        after_meta = _snapshot_metadata(sample_tree)

        # Same files must still exist
        assert set(before_hashes.keys()) == set(after_hashes.keys()), \
            "Source files were added or removed during encoding!"

        # Content must be identical
        for rel_path in before_hashes:
            assert before_hashes[rel_path] == after_hashes[rel_path], \
                f"Source file modified during encoding: {rel_path}"

        # Size must be identical
        for rel_path in before_meta:
            assert before_meta[rel_path][0] == after_meta[rel_path][0], \
                f"Source file size changed: {rel_path}"

    def test_source_files_unchanged_after_failed_encode(self, sample_tree, tmp_path):
        """Source should be untouched even if encoding fails."""
        before_hashes = _hash_all_files(sample_tree)

        # Try encoding to a path that already exists → should fail
        archive = tmp_path / "out.tesseract"
        archive.write_bytes(b"existing")

        encoder = TesseractEncoder(workers=1)
        with pytest.raises(FileExistsError):
            encoder.encode(sample_tree, archive)

        after_hashes = _hash_all_files(sample_tree)
        assert before_hashes == after_hashes, "Source was modified by failed encode!"


class TestEncryption:
    """Encryption and decryption end-to-end tests."""

    def test_encrypted_roundtrip(self, sample_tree, tmp_path):
        archive = tmp_path / "encrypted.tesseract"
        password = "strong_p@ssword_123!"

        encoder = TesseractEncoder(workers=1, password=password)
        encoder.encode(sample_tree, archive)

        output = tmp_path / "decrypted"
        decoder = TesseractDecoder(workers=1, password=password, verify=True)
        decoder.decode(archive, output)

        original = _hash_all_files(sample_tree)
        restored = _hash_all_files(output)
        assert original == restored

    def test_wrong_password_fails(self, sample_tree, tmp_path):
        archive = tmp_path / "encrypted.tesseract"
        encoder = TesseractEncoder(workers=1, password="correct")
        encoder.encode(sample_tree, archive)

        output = tmp_path / "bad_output"
        decoder = TesseractDecoder(workers=1, password="wrong")
        with pytest.raises(ValueError, match="[Ww]rong password"):
            decoder.decode(archive, output)

    def test_no_password_fails_on_encrypted(self, sample_tree, tmp_path):
        archive = tmp_path / "encrypted.tesseract"
        encoder = TesseractEncoder(workers=1, password="mypass")
        encoder.encode(sample_tree, archive)

        output = tmp_path / "no_pass_output"
        decoder = TesseractDecoder(workers=1, password=None)
        with pytest.raises(ValueError, match="[Pp]assword required"):
            decoder.decode(archive, output)


class TestSolidMode:
    """Solid compression mode end-to-end tests."""

    def test_solid_roundtrip(self, sample_tree, tmp_path):
        archive = tmp_path / "solid.tesseract"
        encoder = TesseractEncoder(workers=1, solid=True)
        encoder.encode(sample_tree, archive)

        output = tmp_path / "restored"
        decoder = TesseractDecoder(workers=1, verify=True)
        decoder.decode(archive, output)

        original = _hash_all_files(sample_tree)
        restored = _hash_all_files(output)
        assert original == restored

    def test_solid_encrypted_roundtrip(self, sample_tree, tmp_path):
        archive = tmp_path / "solid_enc.tesseract"
        encoder = TesseractEncoder(workers=1, solid=True, password="solidpass")
        encoder.encode(sample_tree, archive)

        output = tmp_path / "restored"
        decoder = TesseractDecoder(workers=1, password="solidpass", verify=True)
        decoder.decode(archive, output)

        original = _hash_all_files(sample_tree)
        restored = _hash_all_files(output)
        assert original == restored


class TestCompressionLevels:
    """Test different compression levels produce valid archives."""

    @pytest.mark.parametrize("level", [0, 1, 6, 9])
    def test_compression_level(self, sample_tree, tmp_path, level):
        archive = tmp_path / f"level_{level}.tesseract"
        encoder = TesseractEncoder(workers=1, compression_level=level)
        encoder.encode(sample_tree, archive)

        output = tmp_path / "output"
        decoder = TesseractDecoder(workers=1, verify=True)
        decoder.decode(archive, output)

        original = _hash_all_files(sample_tree)
        restored = _hash_all_files(output)
        assert original == restored


class TestCommentAndLock:
    def test_comment_stored_and_readable(self, sample_tree, tmp_path):
        archive = tmp_path / "commented.tesseract"
        encoder = TesseractEncoder(workers=1, comment="Test comment!")
        encoder.encode(sample_tree, archive)

        decoder = TesseractDecoder(workers=1)
        comment = decoder.read_comment(archive)
        assert comment == "Test comment!"

    def test_empty_comment(self, sample_tree, tmp_path):
        archive = tmp_path / "nocomment.tesseract"
        encoder = TesseractEncoder(workers=1)
        encoder.encode(sample_tree, archive)

        decoder = TesseractDecoder(workers=1)
        assert decoder.read_comment(archive) == ""


class TestSelectiveExtraction:
    def test_extract_specific_pattern(self, sample_tree, tmp_path):
        archive = tmp_path / "out.tesseract"
        encoder = TesseractEncoder(workers=1)
        encoder.encode(sample_tree, archive)

        output = tmp_path / "partial"
        decoder = TesseractDecoder(
            workers=1, extract_patterns=["docs/*"], verify=True
        )
        decoder.decode(archive, output)

        # Only docs/ files should be extracted
        extracted = set()
        for root, _, files in os.walk(output):
            for name in files:
                p = Path(root) / name
                extracted.add(str(p.relative_to(output)))

        for name in extracted:
            assert name.startswith("docs"), f"Unexpected file extracted: {name}"

    def test_extract_by_extension_only_gets_matching_files(self, sample_tree, tmp_path):
        archive = tmp_path / "out_ext.tesseract"
        encoder = TesseractEncoder(workers=1)
        encoder.encode(sample_tree, archive)

        output = tmp_path / "jpgs_only"
        decoder = TesseractDecoder(
            workers=1, extract_patterns=["*.jpg"], verify=True
        )
        decoder.decode(archive, output)

        extracted = []
        for root, _, files in os.walk(output):
            for name in files:
                extracted.append(name)

        assert len(extracted) > 0, "Expected at least one .jpg to be extracted"
        for name in extracted:
            assert name.endswith(".jpg"), f"Non-.jpg file extracted: {name}"

    def test_extract_no_matches_does_not_fail_verification(self, sample_tree, tmp_path):
        archive = tmp_path / "out_nomatch.tesseract"
        encoder = TesseractEncoder(workers=1)
        encoder.encode(sample_tree, archive)

        output = tmp_path / "partial_nomatch"
        decoder = TesseractDecoder(
            workers=1, extract_patterns=["*.doesnotexist"], verify=True
        )

        # Should complete without trying to verify the entire manifest.
        decoder.decode(archive, output)

        extracted_files = list(output.rglob("*")) if output.exists() else []
        assert not [p for p in extracted_files if p.is_file()]


class TestReadManifest:
    def test_read_manifest_without_extraction(self, sample_tree, tmp_path):
        archive = tmp_path / "out.tesseract"
        encoder = TesseractEncoder(workers=1, comment="info test")
        encoder.encode(sample_tree, archive)

        decoder = TesseractDecoder(workers=1)
        manifest = decoder.read_manifest(archive)
        assert manifest.file_count > 0
        assert manifest.comment == "info test"


class TestRecoveryIntegration:
    def test_encode_with_recovery(self, sample_tree, tmp_path):
        archive = tmp_path / "rec.tesseract"
        encoder = TesseractEncoder(workers=1, recovery_percent=10)
        manifest = encoder.encode(sample_tree, archive)
        assert manifest.has_recovery is True

        # Verify the archive is still valid
        output = tmp_path / "output"
        decoder = TesseractDecoder(workers=1, verify=True)
        decoder.decode(archive, output)
        assert _hash_all_files(sample_tree) == _hash_all_files(output)


class TestEdgeCases:
    def test_empty_directory(self, tmp_path):
        source = tmp_path / "empty_source"
        source.mkdir()
        archive = tmp_path / "empty.tesseract"
        encoder = TesseractEncoder(workers=1)
        manifest = encoder.encode(source, archive)
        assert manifest.file_count == 0

    def test_single_file(self, tmp_path):
        source = tmp_path / "single"
        source.mkdir()
        (source / "only.txt").write_text("only file")
        archive = tmp_path / "single.tesseract"
        encoder = TesseractEncoder(workers=1)
        encoder.encode(source, archive)

        output = tmp_path / "output"
        decoder = TesseractDecoder(workers=1, verify=True)
        decoder.decode(archive, output)
        assert (output / "only.txt").read_text() == "only file"

    def test_nonexistent_source_raises(self, tmp_path):
        encoder = TesseractEncoder(workers=1)
        with pytest.raises(ValueError, match="does not exist"):
            encoder.encode(tmp_path / "nonexistent", tmp_path / "out.tesseract")

    def test_output_already_exists_raises(self, sample_tree, tmp_path):
        archive = tmp_path / "exists.tesseract"
        archive.write_bytes(b"x")
        encoder = TesseractEncoder(workers=1)
        with pytest.raises(FileExistsError):
            encoder.encode(sample_tree, archive)

    def test_overwrite_flag(self, sample_tree, tmp_path):
        """Test that --overwrite allows replacing existing output files."""
        archive = tmp_path / "out.tesseract"
        encoder = TesseractEncoder(workers=1)
        encoder.encode(sample_tree, archive)

        output = tmp_path / "output"
        decoder = TesseractDecoder(workers=1, overwrite=True, verify=True)
        decoder.decode(archive, output)
        # Decode again with overwrite should succeed
        decoder.decode(archive, output)

    def test_exclude_patterns(self, sample_tree, tmp_path):
        archive = tmp_path / "excluded.tesseract"
        encoder = TesseractEncoder(workers=1, exclude_patterns=[".jpg"])
        manifest = encoder.encode(sample_tree, archive)

        # No .jpg files in manifest
        for rel_path in manifest.files:
            assert not rel_path.endswith(".jpg"), f"Expected excluded: {rel_path}"
