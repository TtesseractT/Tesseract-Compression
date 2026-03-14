"""Tests for the recovery module — XOR parity generation and archive repair."""

import os
from pathlib import Path

import pytest

from tesseract.recovery import (
    RecoveryRecord,
    generate_recovery_data,
    repair_archive,
    SLICE_SIZE,
    _compute_slice_crc,
)


class TestRecoveryRecordSerialization:
    def test_roundtrip_empty(self):
        rec = RecoveryRecord()
        rec.total_slices = 0
        rec.parity_group_size = 0
        rec.slice_crcs = []
        rec.parity_blocks = []
        data = rec.serialize()
        rec2 = RecoveryRecord.deserialize(data)
        assert rec2.total_slices == 0

    def test_roundtrip_with_data(self):
        rec = RecoveryRecord()
        rec.slice_size = SLICE_SIZE
        rec.total_slices = 3
        rec.parity_group_size = 3
        rec.slice_crcs = [111, 222, 333]
        rec.parity_blocks = [b"\xAA" * SLICE_SIZE]
        data = rec.serialize()
        rec2 = RecoveryRecord.deserialize(data)
        assert rec2.total_slices == 3
        assert rec2.parity_group_size == 3
        assert rec2.slice_crcs == [111, 222, 333]
        assert len(rec2.parity_blocks) == 1

    def test_bad_magic_raises(self):
        with pytest.raises(ValueError, match="Invalid recovery"):
            RecoveryRecord.deserialize(b"\x00" * 100)


class TestGenerateRecoveryData:
    def test_generate_from_file(self, tmp_path):
        # Create a small "archive" file
        data = os.urandom(SLICE_SIZE * 4)  # 4 slices
        archive = tmp_path / "test.bin"
        archive.write_bytes(data)

        rec = generate_recovery_data(archive, 0, len(data), redundancy_percent=25)
        assert rec.total_slices == 4
        assert len(rec.slice_crcs) == 4
        assert len(rec.parity_blocks) > 0

    def test_generate_empty_data(self, tmp_path):
        archive = tmp_path / "empty.bin"
        archive.write_bytes(b"\x00" * 100)
        rec = generate_recovery_data(archive, 0, 0, redundancy_percent=5)
        assert rec.total_slices == 0

    def test_redundancy_clamped(self, tmp_path):
        data = os.urandom(SLICE_SIZE * 2)
        archive = tmp_path / "test.bin"
        archive.write_bytes(data)
        # Redundancy > 30 should be clamped
        rec = generate_recovery_data(archive, 0, len(data), redundancy_percent=50)
        assert rec is not None


class TestRepairArchive:
    def test_repair_single_slice_corruption(self, tmp_path):
        # Create test data
        data = os.urandom(SLICE_SIZE * 4)
        archive = tmp_path / "archive.bin"
        archive.write_bytes(data)

        # Generate recovery
        rec = generate_recovery_data(archive, 0, len(data), redundancy_percent=25)

        # Corrupt one slice (slice 1)
        corrupted = bytearray(data)
        corrupted[SLICE_SIZE:SLICE_SIZE + 100] = b"\xFF" * 100
        archive.write_bytes(bytes(corrupted))

        # Repair
        checked, repaired = repair_archive(archive, 0, len(data), rec)
        assert checked == 4
        assert repaired == 1

        # Verify repair
        repaired_data = archive.read_bytes()
        assert repaired_data == data

    def test_no_damage_detected(self, tmp_path):
        data = os.urandom(SLICE_SIZE * 2)
        archive = tmp_path / "clean.bin"
        archive.write_bytes(data)
        rec = generate_recovery_data(archive, 0, len(data), redundancy_percent=10)

        checked, repaired = repair_archive(archive, 0, len(data), rec)
        assert repaired == 0

    def test_multi_slice_same_group_fails(self, tmp_path):
        """Two corrupted slices in the same parity group cannot be recovered."""
        data = os.urandom(SLICE_SIZE * 4)
        archive = tmp_path / "archive.bin"
        archive.write_bytes(data)

        rec = generate_recovery_data(archive, 0, len(data), redundancy_percent=25)

        # Corrupt two slices in the same group
        corrupted = bytearray(data)
        # Assuming group_size groups all 4 together, corrupt 2
        corrupted[0:50] = b"\xFF" * 50
        corrupted[SLICE_SIZE:SLICE_SIZE + 50] = b"\xFF" * 50
        archive.write_bytes(bytes(corrupted))

        # Should either repair or raise depending on group size
        # With 25% redundancy on 4 slices, group_size = max(2, 4 // 1) = 4
        # So all 4 in one group with 2 corrupted → unrecoverable
        with pytest.raises(RuntimeError, match="Cannot repair"):
            repair_archive(archive, 0, len(data), rec)
