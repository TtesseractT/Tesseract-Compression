"""Archive manifest — describes the complete contents of a .tesseract archive.

The manifest is stored as gzip-compressed JSON inside the archive and contains:
  - Archive metadata (version, creation date, source path)
  - Per-file metadata (size, hash, offsets, group membership, permissions)
  - Duplicate group definitions (master path + list of duplicate paths)
  - Archive comment
  - Feature flags (encrypted, solid, recovery, permissions)
"""

import gzip
import json
import os
import stat
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from .scanner import FileEntry
from .deduplicator import DuplicateGroup


@dataclass
class Manifest:
    """Complete manifest for a .tesseract archive."""
    version: int = 2
    created: str = ""
    source_root: str = ""
    total_original_size: int = 0
    total_unique_size: int = 0
    file_count: int = 0
    unique_count: int = 0
    duplicate_group_count: int = 0
    space_savings: int = 0
    comment: str = ""
    is_encrypted: bool = False
    is_solid: bool = False
    has_recovery: bool = False
    store_permissions: bool = False
    is_locked: bool = False
    files: Dict[str, dict] = field(default_factory=dict)
    duplicate_groups: Dict[str, dict] = field(default_factory=dict)

    # Solid mode: maps rel_path -> (offset_in_stream, size) for extracting
    solid_offsets: Dict[str, dict] = field(default_factory=dict)

    def to_json(self) -> bytes:
        """Serialize manifest to gzip-compressed JSON bytes."""
        data = {
            "version": self.version,
            "created": self.created,
            "source_root": self.source_root,
            "total_original_size": self.total_original_size,
            "total_unique_size": self.total_unique_size,
            "file_count": self.file_count,
            "unique_count": self.unique_count,
            "duplicate_group_count": self.duplicate_group_count,
            "space_savings": self.space_savings,
            "comment": self.comment,
            "is_encrypted": self.is_encrypted,
            "is_solid": self.is_solid,
            "has_recovery": self.has_recovery,
            "store_permissions": self.store_permissions,
            "is_locked": self.is_locked,
            "files": self.files,
            "duplicate_groups": self.duplicate_groups,
            "solid_offsets": self.solid_offsets,
        }
        json_bytes = json.dumps(data, separators=(",", ":")).encode("utf-8")
        return gzip.compress(json_bytes)

    @classmethod
    def from_json(cls, compressed_data: bytes) -> "Manifest":
        """Deserialize from gzip-compressed JSON bytes."""
        json_bytes = gzip.decompress(compressed_data)
        data = json.loads(json_bytes)
        m = cls()
        m.version = data.get("version", 1)
        m.created = data.get("created", "")
        m.source_root = data.get("source_root", "")
        m.total_original_size = data.get("total_original_size", 0)
        m.total_unique_size = data.get("total_unique_size", 0)
        m.file_count = data.get("file_count", 0)
        m.unique_count = data.get("unique_count", 0)
        m.duplicate_group_count = data.get("duplicate_group_count", 0)
        m.space_savings = data.get("space_savings", 0)
        m.comment = data.get("comment", "")
        m.is_encrypted = data.get("is_encrypted", False)
        m.is_solid = data.get("is_solid", False)
        m.has_recovery = data.get("has_recovery", False)
        m.store_permissions = data.get("store_permissions", False)
        m.is_locked = data.get("is_locked", False)
        m.files = data.get("files", {})
        m.duplicate_groups = data.get("duplicate_groups", {})
        m.solid_offsets = data.get("solid_offsets", {})
        return m

    @classmethod
    def build(
        cls,
        source_root: Path,
        all_entries: List[FileEntry],
        duplicate_groups: List[DuplicateGroup],
        comment: str = "",
        store_permissions: bool = False,
    ) -> "Manifest":
        """Build a manifest from scan results and duplicate analysis."""
        manifest = cls()
        manifest.created = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        manifest.source_root = str(source_root)
        manifest.file_count = len(all_entries)
        manifest.comment = comment
        manifest.store_permissions = store_permissions

        # Build duplicate group lookup: relative_path -> (group_id, is_master)
        dup_lookup: Dict[str, tuple] = {}
        for group in duplicate_groups:
            gid = group.group_id
            manifest.duplicate_groups[gid] = {
                "master": group.master.relative_path,
                "duplicates": [d.relative_path for d in group.duplicates],
                "content_hash": group.content_hash,
                "size": group.file_size,
                "filename": group.filename,
                "extension": group.extension,
            }
            dup_lookup[group.master.relative_path] = (gid, True)
            for dup in group.duplicates:
                dup_lookup[dup.relative_path] = (gid, False)

        # Build file entries
        unique_paths = set()
        total_original = 0
        total_unique = 0

        for entry in all_entries:
            total_original += entry.size
            group_info = dup_lookup.get(entry.relative_path)
            group_id = group_info[0] if group_info else None
            is_master = group_info[1] if group_info else False
            is_stored = group_id is None or is_master

            if is_stored:
                unique_paths.add(entry.relative_path)
                total_unique += entry.size

            file_meta = {
                "size": entry.size,
                "content_hash": entry.full_hash or "",
                "filename": entry.filename,
                "extension": entry.extension,
                "modified_time": entry.modified_time,
                "group_id": group_id,
                "is_master": is_master if group_id else False,
                "data_offset": 0,
                "compressed_size": 0,
            }

            # Store file permissions if requested
            if store_permissions:
                try:
                    st = entry.path.stat()
                    file_meta["permissions"] = {
                        "mode": oct(st.st_mode),
                        "uid": getattr(st, "st_uid", 0),
                        "gid": getattr(st, "st_gid", 0),
                    }
                except OSError:
                    pass

            manifest.files[entry.relative_path] = file_meta

        manifest.total_original_size = total_original
        manifest.total_unique_size = total_unique
        manifest.unique_count = len(unique_paths)
        manifest.duplicate_group_count = len(duplicate_groups)
        manifest.space_savings = sum(g.space_savings for g in duplicate_groups)

        return manifest
