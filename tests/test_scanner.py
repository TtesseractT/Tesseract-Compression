"""Tests for the scanner module — directory walking and metadata collection."""

import os
from pathlib import Path

import pytest

from tesseract.scanner import FileEntry, FileScanner


class TestFileEntry:
    def test_dedup_key(self):
        entry = FileEntry(
            path=Path("/tmp/a.txt"),
            relative_path="a.txt",
            size=100,
            filename="a.txt",
            extension=".txt",
            modified_time=1000.0,
        )
        assert entry.dedup_key == (100, "a.txt", ".txt")

    def test_defaults(self):
        entry = FileEntry(
            path=Path("/x"), relative_path="x", size=0,
            filename="x", extension="", modified_time=0.0,
        )
        assert entry.partial_hash is None
        assert entry.full_hash is None
        assert entry.group_id is None
        assert entry.is_master is False


class TestFileScanner:
    def test_scan_empty_dir(self, tmp_path):
        scanner = FileScanner(tmp_path)
        assert list(scanner.scan()) == []

    def test_scan_finds_files(self, tmp_path):
        (tmp_path / "a.txt").write_text("hello")
        (tmp_path / "b.txt").write_text("world")
        scanner = FileScanner(tmp_path)
        entries = list(scanner.scan())
        names = {e.filename for e in entries}
        assert names == {"a.txt", "b.txt"}

    def test_scan_recursive(self, tmp_path):
        sub = tmp_path / "sub" / "deep"
        sub.mkdir(parents=True)
        (sub / "deep.txt").write_text("deep file")
        scanner = FileScanner(tmp_path)
        entries = list(scanner.scan())
        assert len(entries) == 1
        assert entries[0].filename == "deep.txt"
        assert "sub" in entries[0].relative_path

    def test_excludes_patterns(self, tmp_path):
        (tmp_path / "keep.txt").write_text("keep")
        (tmp_path / "skip.log").write_text("skip")
        scanner = FileScanner(tmp_path, exclude_patterns=[".log"])
        entries = list(scanner.scan())
        assert len(entries) == 1
        assert entries[0].filename == "keep.txt"

    def test_excludes_directories(self, tmp_path):
        (tmp_path / "include").mkdir()
        (tmp_path / "include" / "yes.txt").write_text("yes")
        (tmp_path / "exclude_me").mkdir()
        (tmp_path / "exclude_me" / "no.txt").write_text("no")
        scanner = FileScanner(tmp_path, exclude_patterns=["exclude_me"])
        entries = list(scanner.scan())
        names = {e.filename for e in entries}
        assert names == {"yes.txt"}

    def test_skips_symlinks(self, tmp_path):
        real = tmp_path / "real.txt"
        real.write_text("real")
        link = tmp_path / "link.txt"
        try:
            link.symlink_to(real)
        except OSError:
            pytest.skip("Symlinks not supported on this OS/filesystem")
        scanner = FileScanner(tmp_path)
        entries = list(scanner.scan())
        names = {e.filename for e in entries}
        assert "link.txt" not in names
        assert "real.txt" in names

    def test_entry_metadata(self, tmp_path):
        data = b"some test data"
        f = tmp_path / "test.dat"
        f.write_bytes(data)
        scanner = FileScanner(tmp_path)
        entries = list(scanner.scan())
        assert len(entries) == 1
        e = entries[0]
        assert e.size == len(data)
        assert e.extension == ".dat"
        assert e.filename == "test.dat"
        assert e.modified_time > 0

    def test_invalid_root_raises(self, tmp_path):
        with pytest.raises(ValueError, match="not a directory"):
            FileScanner(tmp_path / "nonexistent")

    def test_count_files(self, tmp_path):
        for i in range(5):
            (tmp_path / f"file_{i}.txt").write_text(str(i))
        scanner = FileScanner(tmp_path)
        assert scanner.count_files() == 5
