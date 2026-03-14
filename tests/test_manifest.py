"""Tests for the manifest module — JSON serialization and metadata building."""

import json
import gzip
from pathlib import Path

import pytest

from tesseract.manifest import Manifest
from tesseract.scanner import FileEntry
from tesseract.deduplicator import DuplicateGroup


class TestManifestSerialization:
    def test_roundtrip_empty(self):
        m = Manifest()
        data = m.to_json()
        m2 = Manifest.from_json(data)
        assert m2.version == m.version
        assert m2.file_count == 0
        assert m2.files == {}

    def test_roundtrip_with_data(self):
        m = Manifest()
        m.version = 2
        m.file_count = 5
        m.unique_count = 3
        m.duplicate_group_count = 1
        m.total_original_size = 10000
        m.total_unique_size = 6000
        m.space_savings = 4000
        m.comment = "Test archive"
        m.is_encrypted = True
        m.is_solid = False
        m.has_recovery = True
        m.store_permissions = True
        m.is_locked = False
        m.files = {"data.txt": {"size": 100, "content_hash": "abc"}}

        data = m.to_json()
        m2 = Manifest.from_json(data)
        assert m2.file_count == 5
        assert m2.comment == "Test archive"
        assert m2.is_encrypted is True
        assert m2.has_recovery is True
        assert m2.store_permissions is True
        assert m2.files == {"data.txt": {"size": 100, "content_hash": "abc"}}

    def test_to_json_is_compressed(self):
        m = Manifest()
        m.comment = "x" * 1000
        data = m.to_json()
        # Should be gzip compressed
        assert data[:2] == b"\x1f\x8b"

    def test_backward_compatible_from_json(self):
        """Old manifests without new fields should still load."""
        old_data = {
            "version": 1,
            "file_count": 2,
            "files": {},
        }
        compressed = gzip.compress(json.dumps(old_data).encode("utf-8"))
        m = Manifest.from_json(compressed)
        assert m.version == 1
        assert m.comment == ""
        assert m.is_encrypted is False
        assert m.is_solid is False


class TestManifestBuild:
    def _make_entry(self, tmp_path, name, content, subdir=""):
        d = tmp_path / subdir if subdir else tmp_path
        d.mkdir(parents=True, exist_ok=True)
        p = d / name
        p.write_bytes(content)
        import hashlib
        h = hashlib.sha256(content).hexdigest()
        entry = FileEntry(
            path=p,
            relative_path=f"{subdir}/{name}" if subdir else name,
            size=len(content),
            filename=name,
            extension=p.suffix.lower(),
            modified_time=p.stat().st_mtime,
            full_hash=h,
        )
        return entry

    def test_build_no_duplicates(self, tmp_path):
        entries = [
            self._make_entry(tmp_path, "a.txt", b"AAA"),
            self._make_entry(tmp_path, "b.txt", b"BBB"),
        ]
        m = Manifest.build(tmp_path, entries, [])
        assert m.file_count == 2
        assert m.unique_count == 2
        assert m.duplicate_group_count == 0
        assert m.space_savings == 0

    def test_build_with_duplicates(self, tmp_path):
        content = b"dup content" * 10
        e1 = self._make_entry(tmp_path, "f.txt", content, "d1")
        e2 = self._make_entry(tmp_path, "f.txt", content, "d2")
        import hashlib
        ch = hashlib.sha256(content).hexdigest()
        e1.group_id = "g1"
        e1.is_master = True
        e2.group_id = "g1"
        e2.is_master = False
        group = DuplicateGroup(
            group_id="g1", master=e1, duplicates=[e2],
            content_hash=ch, file_size=len(content),
            filename="f.txt", extension=".txt",
        )
        m = Manifest.build(tmp_path, [e1, e2], [group])
        assert m.file_count == 2
        assert m.unique_count == 1
        assert m.duplicate_group_count == 1
        assert m.space_savings == len(content)

    def test_build_with_comment(self, tmp_path):
        m = Manifest.build(tmp_path, [], [], comment="Hello!")
        assert m.comment == "Hello!"

    def test_build_with_permissions(self, tmp_path):
        entries = [self._make_entry(tmp_path, "x.txt", b"perm test")]
        m = Manifest.build(tmp_path, entries, [], store_permissions=True)
        assert m.store_permissions is True
        info = m.files["x.txt"]
        assert "permissions" in info
        assert "mode" in info["permissions"]
