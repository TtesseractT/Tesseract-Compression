"""Tests for the volume module — multi-volume splitting and joining."""

import os
from pathlib import Path

import pytest

from tesseract.volume import (
    split_archive,
    join_volumes,
    VOLUME_HEADER_SIZE,
    VOLUME_MAGIC,
)


class TestSplitArchive:
    def test_split_small_file(self, tmp_path):
        archive = tmp_path / "test.tesseract"
        data = os.urandom(5000)
        archive.write_bytes(data)

        volumes = split_archive(archive, volume_size=2000 + VOLUME_HEADER_SIZE)
        assert len(volumes) >= 2
        for v in volumes:
            assert v.exists()

    def test_split_preserves_data(self, tmp_path):
        archive = tmp_path / "test.tesseract"
        data = os.urandom(10_000)
        archive.write_bytes(data)

        volumes = split_archive(archive, volume_size=3000 + VOLUME_HEADER_SIZE)
        assert len(volumes) >= 3

        # Join and verify
        output = tmp_path / "joined.tesseract"
        result = join_volumes(volumes[0], output_path=output)
        assert result.read_bytes() == data

    def test_single_volume_when_small(self, tmp_path):
        archive = tmp_path / "tiny.tesseract"
        archive.write_bytes(os.urandom(100))
        volumes = split_archive(archive, volume_size=1024 * 1024)
        assert len(volumes) == 1

    def test_volume_naming(self, tmp_path):
        archive = tmp_path / "test.tesseract"
        archive.write_bytes(os.urandom(5000))
        volumes = split_archive(archive, volume_size=2000 + VOLUME_HEADER_SIZE)
        for i, v in enumerate(volumes, 1):
            assert v.name.endswith(f".{i:03d}")

    def test_too_small_volume_raises(self, tmp_path):
        archive = tmp_path / "test.tesseract"
        archive.write_bytes(os.urandom(100))
        with pytest.raises(ValueError, match="too small"):
            split_archive(archive, volume_size=10)

    def test_missing_archive_raises(self, tmp_path):
        with pytest.raises(ValueError, match="not found"):
            split_archive(tmp_path / "nonexistent.tesseract")


class TestJoinVolumes:
    def test_roundtrip(self, tmp_path):
        archive = tmp_path / "test.tesseract"
        original_data = os.urandom(5000)
        archive.write_bytes(original_data)

        volumes = split_archive(archive, volume_size=2000 + VOLUME_HEADER_SIZE)

        output = tmp_path / "output.tesseract"
        result = join_volumes(volumes[0], output_path=output)
        assert result.read_bytes() == original_data

    def test_detects_corrupt_volume(self, tmp_path):
        archive = tmp_path / "test.tesseract"
        archive.write_bytes(os.urandom(5000))
        volumes = split_archive(archive, volume_size=2000 + VOLUME_HEADER_SIZE)

        # Corrupt a volume
        corrupted = volumes[1]
        data = bytearray(corrupted.read_bytes())
        data[10:20] = b"\xFF" * 10
        corrupted.write_bytes(bytes(data))

        output = tmp_path / "bad.tesseract"
        with pytest.raises(RuntimeError, match="[Hh]ash mismatch"):
            join_volumes(volumes[0], output_path=output)

    def test_missing_volume_raises(self, tmp_path):
        archive = tmp_path / "test.tesseract"
        archive.write_bytes(os.urandom(5000))
        volumes = split_archive(archive, volume_size=2000 + VOLUME_HEADER_SIZE)

        # Delete a middle volume
        volumes[1].unlink()

        output = tmp_path / "incomplete.tesseract"
        with pytest.raises(FileNotFoundError, match="Missing volume"):
            join_volumes(volumes[0], output_path=output)

    def test_output_exists_raises(self, tmp_path):
        archive = tmp_path / "test.tesseract"
        archive.write_bytes(os.urandom(1000))
        volumes = split_archive(archive, volume_size=5000 + VOLUME_HEADER_SIZE)

        output = tmp_path / "exists.tesseract"
        output.write_bytes(b"already here")

        with pytest.raises(FileExistsError):
            join_volumes(volumes[0], output_path=output)

    def test_bad_magic_raises(self, tmp_path):
        fake = tmp_path / "fake.001"
        fake.write_bytes(b"\x00" * (VOLUME_HEADER_SIZE + 100))
        with pytest.raises(ValueError, match="bad magic"):
            join_volumes(fake)
