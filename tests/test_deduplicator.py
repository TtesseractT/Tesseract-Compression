"""Tests for the deduplicator module — multi-stage duplicate detection."""

import blake3
from pathlib import Path

import pytest

from tesseract.scanner import FileEntry
from tesseract.deduplicator import Deduplicator, DuplicateGroup, _make_group_id


class TestMakeGroupId:
    def test_deterministic(self):
        a = _make_group_id("abc", "file.txt", ".txt")
        b = _make_group_id("abc", "file.txt", ".txt")
        assert a == b

    def test_different_hash_differs(self):
        a = _make_group_id("abc", "file.txt", ".txt")
        b = _make_group_id("xyz", "file.txt", ".txt")
        assert a != b


class TestDeduplicator:
    def _make_entry(self, tmp_path, name, content, subdir=""):
        """Create a real file and return a FileEntry."""
        d = tmp_path / subdir if subdir else tmp_path
        d.mkdir(parents=True, exist_ok=True)
        p = d / name
        p.write_bytes(content)
        return FileEntry(
            path=p,
            relative_path=f"{subdir}/{name}" if subdir else name,
            size=p.stat().st_size,
            filename=name,
            extension=p.suffix.lower(),
            modified_time=p.stat().st_mtime,
        )

    def test_no_duplicates(self, tmp_path):
        entries = [
            self._make_entry(tmp_path, "a.txt", b"unique A"),
            self._make_entry(tmp_path, "b.txt", b"unique B"),
        ]
        dedup = Deduplicator(workers=1)
        groups = dedup.find_duplicates(entries)
        assert groups == []

    def test_detects_duplicates(self, tmp_path):
        content = b"identical content" * 100
        entries = [
            self._make_entry(tmp_path, "file.txt", content, "dir1"),
            self._make_entry(tmp_path, "file.txt", content, "dir2"),
        ]
        dedup = Deduplicator(workers=1)
        groups = dedup.find_duplicates(entries)
        assert len(groups) == 1
        g = groups[0]
        assert g.total_files == 2
        assert g.space_savings == len(content)

    def test_three_way_duplicate(self, tmp_path):
        content = b"triple" * 200
        entries = [
            self._make_entry(tmp_path, "data.bin", content, "a"),
            self._make_entry(tmp_path, "data.bin", content, "b"),
            self._make_entry(tmp_path, "data.bin", content, "c"),
        ]
        dedup = Deduplicator(workers=1)
        groups = dedup.find_duplicates(entries)
        assert len(groups) == 1
        assert groups[0].total_files == 3
        assert groups[0].space_savings == len(content) * 2

    def test_different_name_not_duplicate(self, tmp_path):
        """Files must have the same name to be grouped as duplicates."""
        content = b"same content"
        entries = [
            self._make_entry(tmp_path, "a.txt", content, "d1"),
            self._make_entry(tmp_path, "b.txt", content, "d2"),
        ]
        dedup = Deduplicator(workers=1)
        groups = dedup.find_duplicates(entries)
        assert groups == []

    def test_different_size_not_duplicate(self, tmp_path):
        entries = [
            self._make_entry(tmp_path, "f.txt", b"short", "d1"),
            self._make_entry(tmp_path, "f.txt", b"much longer content", "d2"),
        ]
        dedup = Deduplicator(workers=1)
        groups = dedup.find_duplicates(entries)
        assert groups == []

    def test_empty_input(self, tmp_path):
        dedup = Deduplicator(workers=1)
        groups = dedup.find_duplicates([])
        assert groups == []

    def test_single_file(self, tmp_path):
        entries = [self._make_entry(tmp_path, "solo.txt", b"only one")]
        dedup = Deduplicator(workers=1)
        groups = dedup.find_duplicates(entries)
        assert groups == []

    def test_master_has_full_hash(self, tmp_path):
        content = b"hash me" * 100
        entries = [
            self._make_entry(tmp_path, "f.bin", content, "x"),
            self._make_entry(tmp_path, "f.bin", content, "y"),
        ]
        dedup = Deduplicator(workers=1)
        groups = dedup.find_duplicates(entries)
        assert len(groups) == 1
        expected_hash = blake3.blake3(content).hexdigest()
        assert groups[0].content_hash == expected_hash
