"""Persistent cache using SQLite for crash-resilient encoding.

Caches file hashes and scan results keyed on (filepath, size, mtime_ns) so
that repeated or interrupted encoding runs skip already-completed work.
Auto-commits every 1000 entries for crash safety.
"""

import json
import logging
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class HashCache:
    """SQLite-backed cache for scan results, partial hashes, and full hashes."""

    def __init__(self, cache_path: Path):
        self._path = cache_path
        self._conn = sqlite3.connect(str(cache_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS hashes (
                filepath TEXT NOT NULL,
                size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                hash TEXT NOT NULL,
                PRIMARY KEY (filepath, size, mtime_ns)
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS partial_hashes (
                filepath TEXT NOT NULL,
                size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                hash TEXT NOT NULL,
                PRIMARY KEY (filepath, size, mtime_ns)
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS scan_cache (
                source_dir TEXT NOT NULL,
                entries_json TEXT NOT NULL,
                file_count INTEGER NOT NULL,
                PRIMARY KEY (source_dir)
            )
        """)
        self._conn.commit()
        self._pending = 0
        self._hits = 0
        self._misses = 0

    def get(self, filepath: Path) -> Optional[str]:
        """Look up a cached full hash. Returns None on miss or if file changed."""
        try:
            stat = filepath.stat()
        except OSError:
            return None
        row = self._conn.execute(
            "SELECT hash FROM hashes WHERE filepath=? AND size=? AND mtime_ns=?",
            (str(filepath), stat.st_size, stat.st_mtime_ns),
        ).fetchone()
        if row:
            self._hits += 1
            return row[0]
        self._misses += 1
        return None

    def put(self, filepath: Path, hash_val: str):
        """Store a full hash result. Auto-flushes every 1000 inserts."""
        try:
            stat = filepath.stat()
        except OSError:
            return
        self._conn.execute(
            "INSERT OR REPLACE INTO hashes (filepath, size, mtime_ns, hash) "
            "VALUES (?, ?, ?, ?)",
            (str(filepath), stat.st_size, stat.st_mtime_ns, hash_val),
        )
        self._pending += 1
        if self._pending >= 1000:
            self._conn.commit()
            self._pending = 0

    def get_partial(self, filepath: Path) -> Optional[str]:
        """Look up a cached partial hash."""
        try:
            stat = filepath.stat()
        except OSError:
            return None
        row = self._conn.execute(
            "SELECT hash FROM partial_hashes WHERE filepath=? AND size=? AND mtime_ns=?",
            (str(filepath), stat.st_size, stat.st_mtime_ns),
        ).fetchone()
        if row:
            self._hits += 1
            return row[0]
        self._misses += 1
        return None

    def put_partial(self, filepath: Path, hash_val: str):
        """Store a partial hash result."""
        try:
            stat = filepath.stat()
        except OSError:
            return
        self._conn.execute(
            "INSERT OR REPLACE INTO partial_hashes (filepath, size, mtime_ns, hash) "
            "VALUES (?, ?, ?, ?)",
            (str(filepath), stat.st_size, stat.st_mtime_ns, hash_val),
        )
        self._pending += 1
        if self._pending >= 1000:
            self._conn.commit()
            self._pending = 0

    def get_scan(self, source_dir: str) -> Optional[List[dict]]:
        """Load cached scan entries for a source directory."""
        row = self._conn.execute(
            "SELECT entries_json FROM scan_cache WHERE source_dir=?",
            (source_dir,),
        ).fetchone()
        if row:
            return json.loads(row[0])
        return None

    def put_scan(self, source_dir: str, entries: List[dict]):
        """Cache scan entries for a source directory."""
        self._conn.execute(
            "INSERT OR REPLACE INTO scan_cache (source_dir, entries_json, file_count) "
            "VALUES (?, ?, ?)",
            (source_dir, json.dumps(entries), len(entries)),
        )
        self._conn.commit()

    def flush(self):
        if self._pending > 0:
            self._conn.commit()
            self._pending = 0

    def close(self):
        self.flush()
        self._conn.close()

    @property
    def stats(self) -> str:
        return f"cache hits={self._hits}, misses={self._misses}"
