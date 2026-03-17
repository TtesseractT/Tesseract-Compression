<p align="center">
	<img src="LOGO.png" alt="Tesseract Compression System logo" width="220" />
</p>

<h1 align="center">Tesseract Compression System</h1>

<p align="center"><strong>v2.0.0</strong> — Deduplication-based archiver built for cold storage.</p>

Tesseract scans a directory, detects duplicate files using multi-stage content-aware matching, and compresses only unique data into a single `.tesseract` archive. Designed for archiving large drives (12TB+) where duplicate files waste significant space.

## Features

- **Content-aware deduplication** — 3-stage pipeline: metadata grouping → partial hash (64KB BLAKE3) → full BLAKE3
- **Zstandard compression** — modern zstd compression with adaptive levels (fast for small files, full level for large files)
- **Failsafe staged encoding** — files are compressed into verified multi-file shards (~500MB each) before atomic assembly; source files are never modified
- **BLAKE3 hashing** — cryptographically secure, ~6x faster than SHA-256, used everywhere
- **Persistent hash cache** — SQLite-backed cache for scan results, partial hashes, and full hashes; survives interruptions
- **AES-256-GCM encryption** — password-based with PBKDF2-HMAC-SHA256 (600K iterations)
- **Solid compression** — optional single continuous compressed stream for better ratios
- **Recovery records** — XOR parity-based self-repair (1-30% redundancy)
- **Multi-volume splitting** — split archives for FAT32 or media size limits
- **Archive comments** — embed text metadata in archives
- **File permission storage** — optional preservation of file permissions
- **Archive locking** — mark archives as finalized
- **Polished terminal UI** — rich-powered progress, cleaner summaries, and automatic fallback for plain terminals
- **Fully parallelized** — multi-threaded hashing, deduplication, compression, verification, and preflight checks

## Requirements

- Python ≥ 3.9
- `tqdm` ≥ 4.60.0
- `rich` ≥ 13.7.0
- `cryptography` ≥ 41.0.0
- `blake3` ≥ 0.3.0
- `zstandard` ≥ 0.19.0

## Installation

```bash
pip install -e .
```

For development (includes pytest):

```bash
pip install -e ".[dev]"
```

## Quick Start

```bash
# Compress a directory
tesseract encode "D:\MyFiles" "D:\Backups\myfiles.tesseract"

# Restore it
tesseract decode "D:\Backups\myfiles.tesseract" "D:\Restored"
```

---

## CLI Reference

### `tesseract encode`

Create a `.tesseract` archive from a directory.

The CLI now prefers a single live progress region with structured summaries instead of stacking multiple `tqdm` bars. Set the environment variable `TESSERACT_PLAIN=1` if you want plain terminal output.

```
tesseract encode <source> <output> [options]
```

| Flag | Short | Type | Default | Description |
|------|-------|------|---------|-------------|
| `--workers` | `-w` | int | CPU count - 1 | Number of CPU cores to use |
| `--compression-level` | `-c` | 1-22 | 9 | zstd compression level |
| `--exclude` | `-e` | str | — | Glob pattern to exclude (repeatable) |
| `--solid` | `-s` | flag | off | Solid compression mode (better ratio, slower random access) |
| `--password` | `-p` | str | — | Encrypt with this password |
| `--encrypt` | | flag | off | Encrypt (prompts for password securely) |
| `--recovery` | `-r` | 1-30 | 0 | Add recovery records (% of archive size) |
| `--comment` | `-m` | str | — | Embed a text comment in the archive |
| `--permissions` | | flag | off | Store file permissions |
| `--lock` | | flag | off | Mark archive as finalized |
| `--verbose` | `-v` | flag | off | Verbose logging |

**Examples:**

```bash
# Basic encode
tesseract encode "H:\" "X:\Backup\h_drive.tesseract"

# 30 threads, verbose
tesseract encode "H:\" "X:\Backup\h_drive.tesseract" -w 30 -v

# Encrypted with 5% recovery and a comment
tesseract encode "D:\Photos" "E:\archive.tesseract" --encrypt -r 5 -m "Photos backup 2026"

# Solid mode, max compression, exclude temp files
tesseract encode "D:\Projects" "E:\projects.tesseract" -s -c 22 -e "*.tmp" -e "node_modules" -e "__pycache__"
```

---

### `tesseract decode`

Extract a `.tesseract` archive to a directory.

```
tesseract decode <archive> <output> [options]
```

| Flag | Short | Type | Default | Description |
|------|-------|------|---------|-------------|
| `--workers` | `-w` | int | CPU count - 1 | Number of CPU cores |
| `--password` | `-p` | str | — | Password for encrypted archives |
| `--extract` | `-x` | str | — | Extract only files matching this glob (repeatable) |
| `--no-verify` | | flag | off | Skip post-extraction hash verification |
| `--overwrite` | | flag | off | Overwrite existing output files |
| `--verbose` | `-v` | flag | off | Verbose logging |

**Examples:**

```bash
# Basic decode
tesseract decode "E:\archive.tesseract" "D:\Restored"

# Encrypted archive
tesseract decode "E:\archive.tesseract" "D:\Restored" -p mypassword

# Extract only photos
tesseract decode "E:\archive.tesseract" "D:\Restored" -x "photos/*" -x "*.jpg"

# Overwrite existing files
tesseract decode "E:\archive.tesseract" "D:\Restored" --overwrite
```

---

### `tesseract info`

Display archive metadata without extracting.

```
tesseract info <archive> [options]
```

| Flag | Short | Type | Description |
|------|-------|------|-------------|
| `--password` | `-p` | str | Password for encrypted archives |
| `--list-files` | `-l` | flag | List all files in the archive |
| `--list-groups` | `-g` | flag | List duplicate groups |

---

### `tesseract verify`

Verify archive integrity without extracting.

```
tesseract verify <archive> [options]
```

| Flag | Short | Type | Description |
|------|-------|------|-------------|
| `--password` | `-p` | str | Password for encrypted archives |
| `--verbose` | `-v` | flag | Verbose logging |

---

### `tesseract split`

Split an archive into multi-volume parts.

```
tesseract split <archive> [options]
```

| Flag | Short | Type | Default | Description |
|------|-------|------|---------|-------------|
| `--size` | `-s` | int (MB) | 100 | Size of each volume in megabytes |

Output files are named `archive.001`, `archive.002`, etc.

---

### `tesseract join`

Reassemble multi-volume archive parts.

```
tesseract join <first_volume> [options]
```

| Flag | Short | Type | Description |
|------|-------|------|-------------|
| `--output` | `-o` | str | Output path (default: auto-named alongside volumes) |

---

### `tesseract repair`

Attempt to repair a damaged archive using embedded recovery records.

```
tesseract repair <archive> [options]
```

> Requires recovery records (`-r` flag during encode). Without them, repair is not possible.

---

### `tesseract comment`

Display the comment embedded in an archive.

```
tesseract comment <archive>
```

---

## How It Works

### Encoding Pipeline (Failsafe Staged)

1. **Scan** — recursively finds all files (cached in SQLite for subsequent runs)
2. **Deduplicate** — 3-stage content matching: metadata → partial BLAKE3 (64KB) → full BLAKE3
3. **Hash** — computes full BLAKE3 for all unique files (parallel, cached)
4. **Preflight** — verifies every source file is readable and snapshots hashes
5. **Stage shards** — compresses files into ~500MB multi-file shards (parallel with zstd)
6. **Verify shards** — re-reads every shard from disk and verifies CRC32 integrity
7. **Verify source** — re-checks all source files haven't changed since step 4
8. **Assemble** — streams verified shards into a `.tmp` archive file
9. **Verify archive** — validates the assembled archive structure and manifest
10. **Finalize** — atomic rename from `.tmp` to `.tesseract`
11. **Cleanup** — removes staging directory and any temp files

If **any step fails**, source files remain completely untouched. The staging directory and `.tmp` file are cleaned up automatically.

### Hash Cache

Tesseract maintains a `.hashcache` SQLite database alongside the output archive with three tables:
- **Full hashes** — keyed on (filepath, size, mtime_ns), auto-invalidated when files change
- **Partial hashes** — 64KB BLAKE3 for deduplication, also auto-invalidated
- **Scan cache** — directory scan results stored as JSON, validated against file metadata

On subsequent runs, cached hashes are loaded instantly — only new or modified files need to be re-hashed.

### Archive Format (v2)

```
┌─────────────────────────────┐
│ Header (128 bytes)          │  Magic, flags, offsets, salt, password check
├─────────────────────────────┤
│ Comment (variable)          │  UTF-8 text, up to 64KB
├─────────────────────────────┤
│ Data Blocks                 │  Compressed file data (normal or solid mode)
│   Normal: per-file blocks   │    80-byte block header + zstd stream [+ AES-GCM]
│   Solid: single stream      │    16-byte solid header + continuous zstd [+ AES-GCM]
├─────────────────────────────┤
│ Manifest (gzip JSON)        │  File metadata, dedup groups, offsets [+ AES-GCM]
├─────────────────────────────┤
│ Footer (8 bytes)            │  Magic bytes
├─────────────────────────────┤
│ Recovery Records (optional) │  XOR parity blocks for self-repair
└─────────────────────────────┘
```

## Testing

```bash
python -m pytest tests/ -q
```

142 tests covering the full pipeline including safety, encryption, recovery, deduplication, encoding/decoding roundtrips, and archive format validation.

## License

MIT License — see [LICENSE](LICENSE).

### Deduplication

Files are grouped by (size, extension) → partial hash (first + last 64KB) → full SHA-256. Only one copy of each unique file is stored. Duplicates reference the master copy's data offset in the manifest.

### Encryption

- AES-256-GCM authenticated encryption
- Key derived via PBKDF2-HMAC-SHA256 with 600,000 iterations
- Random 16-byte salt per archive
- Each encryption operation uses a unique nonce
- Manifest is also encrypted

### Recovery Records

- XOR parity computed over 512KB slices of the data region
- Configurable redundancy (1-30% of archive size)
- Can repair single-slice corruption per parity group

## Architecture

```
tesseract/
├── __init__.py          # Package exports, version
├── __main__.py          # Entry point (python -m tesseract)
├── cli.py               # CLI argument parsing and command handlers
├── encoder.py           # Staged encoding pipeline
├── decoder.py           # Archive extraction
├── safeguard.py         # Failsafe staging, CRC verification, preflight checks
├── scanner.py           # Recursive file discovery with exclusion patterns
├── deduplicator.py      # 3-stage duplicate detection
├── hasher.py            # SHA-256 partial and full hashing
├── manifest.py          # Archive manifest (gzip JSON)
├── archive_format.py    # Binary format v2 pack/unpack
├── encryption.py        # AES-256-GCM + PBKDF2 key derivation
├── recovery.py          # XOR parity recovery records
└── volume.py            # Multi-volume split/join
```

## Running Tests

```bash
pip install -e ".[dev]"
python -m pytest tests/ -v
```

142 tests covering all modules, pipeline roundtrips, encryption, recovery, staging safety, and edge cases.

## License

MIT
