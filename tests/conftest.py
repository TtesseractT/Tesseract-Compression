"""Shared test fixtures for the Tesseract test suite."""

import os
import random
import string
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def tmp_dir(tmp_path):
    """Provide a clean temporary directory for test inputs."""
    return tmp_path / "source"


@pytest.fixture
def output_dir(tmp_path):
    """Provide a clean directory for extraction output."""
    d = tmp_path / "output"
    d.mkdir()
    return d


@pytest.fixture
def archive_path(tmp_path):
    """Provide a path for a .tesseract archive."""
    return tmp_path / "test.tesseract"


@pytest.fixture
def sample_tree(tmp_dir):
    """
    Create a realistic file tree with duplicates and varied content.

    Structure:
        docs/readme.txt          (unique)
        docs/notes.txt           (unique)
        photos/photo1.jpg        (original)
        photos/photo2.jpg        (unique)
        backup/photos/photo1.jpg (duplicate of photos/photo1.jpg)
        data/report.csv          (unique, larger)
        data/report_copy.csv     (duplicate of data/report.csv)
        empty/placeholder.txt    (empty file)
    """
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Unique files
    (tmp_dir / "docs").mkdir(parents=True)
    (tmp_dir / "docs" / "readme.txt").write_text("This is the readme file.\n" * 10)
    (tmp_dir / "docs" / "notes.txt").write_text("Some notes about the project.\n" * 5)

    # Original + duplicate photo
    (tmp_dir / "photos").mkdir()
    photo_data = bytes(range(256)) * 100  # 25.6 KB of binary-ish data
    (tmp_dir / "photos" / "photo1.jpg").write_bytes(photo_data)
    (tmp_dir / "photos" / "photo2.jpg").write_bytes(bytes(range(256)) * 50)  # Different

    # Duplicate in another directory (same name, same content, same size)
    (tmp_dir / "backup" / "photos").mkdir(parents=True)
    (tmp_dir / "backup" / "photos" / "photo1.jpg").write_bytes(photo_data)

    # Larger unique file + its duplicate
    (tmp_dir / "data").mkdir()
    csv_data = "id,name,value\n" + "".join(
        f"{i},item_{i},{random.randint(1, 1000)}\n" for i in range(500)
    )
    (tmp_dir / "data" / "report.csv").write_text(csv_data)
    (tmp_dir / "data" / "report_copy.csv").write_text(csv_data)

    # Empty file
    (tmp_dir / "empty").mkdir()
    (tmp_dir / "empty" / "placeholder.txt").write_bytes(b"")

    return tmp_dir


@pytest.fixture
def large_sample_tree(tmp_dir):
    """
    Create a larger file tree for stress/performance tests.
    50 files, some duplicates, various sizes.
    """
    tmp_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(42)  # Deterministic for reproducibility

    # Create 30 unique files
    for i in range(30):
        subdir = tmp_dir / f"dir_{i % 5}"
        subdir.mkdir(exist_ok=True)
        size = rng.randint(100, 50_000)
        data = rng.randbytes(size)
        (subdir / f"file_{i}.bin").write_bytes(data)

    # Create 20 duplicates of the first 10 files
    for i in range(20):
        src_idx = i % 10
        src_dir = tmp_dir / f"dir_{src_idx % 5}"
        src_file = src_dir / f"file_{src_idx}.bin"
        dst_dir = tmp_dir / f"dup_dir_{i % 3}"
        dst_dir.mkdir(exist_ok=True)
        # Must have same filename for dedup matching
        dst_file = dst_dir / f"file_{src_idx}.bin"
        if not dst_file.exists():
            dst_file.write_bytes(src_file.read_bytes())

    return tmp_dir
