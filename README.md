# Tesseract Compression System

**v2.0.0** — Deduplication-based archiver built for cold storage.

Tesseract scans a directory, detects duplicate files using multi-stage content-aware matching, and compresses only unique data into a single `.tesseract` archive. Designed for archiving large drives (12TB+) where duplicate files waste significant space.

## Features

- **Content-aware deduplication** — 3-stage pipeline: metadata grouping → partial hash (64KB) → full SHA-256
- **Failsafe staged encoding** — files are compressed into verified shards before atomic assembly; source files are never modified
- **AES-256-GCM encryption** — password-based with PBKDF2-HMAC-SHA256 (600K iterations)
- **Solid compression** — optional single continuous compressed stream for better ratios
- **Recovery records** — XOR parity-based self-repair (1-30% redundancy)
- **Multi-volume splitting** — split archives for FAT32 or media size limits
- **Archive comments** — embed text metadata in archives
- **File permission storage** — optional preservation of file permissions
- **Archive locking** — mark archives as finalized
- **Multi-threaded** — parallel hashing and deduplication

## Requirements

- Python ≥ 3.9
- `tqdm` ≥ 4.60.0
- `cryptography` ≥ 41.0.0

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

```
tesseract encode <source> <output> [options]
```

| Flag | Short | Type | Default | Description |
|------|-------|------|---------|-------------|
| `--workers` | `-w` | int | CPU count - 1 | Number of CPU cores to use |
| `--compression-level` | `-c` | 0-9 | 6 | zlib compression level (0 = none, 9 = max) |
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
tesseract encode "D:\Projects" "E:\projects.tesseract" -s -c 9 -e "*.tmp" -e "node_modules" -e "__pycache__"
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

**Examples:**

```bash
tesseract info "E:\archive.tesseract"
tesseract info "E:\archive.tesseract" -l          # list all files
tesseract info "E:\archive.tesseract" -g          # show duplicate groups
tesseract info "E:\encrypted.tesseract" -p mypass  # encrypted archive
```

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

**Examples:**

```bash
tesseract verify "E:\archive.tesseract"
tesseract verify "E:\encrypted.tesseract" -p mypass -v
```

---

### `tesseract split`

Split an archive into multi-volume parts.

```
tesseract split <archive> [options]
```

| Flag | Short | Type | Default | Description |
|------|-------|------|---------|-------------|
| `--size` | `-s` | int (MB) | 100 | Size of each volume in megabytes |

**Examples:**

```bash
# Split into 100MB volumes (default)
tesseract split "E:\large.tesseract"

# Split into 4GB volumes for FAT32
tesseract split "E:\large.tesseract" -s 4096
```

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

**Examples:**

```bash
# Join from first volume (auto-discovers .002, .003, etc.)
tesseract join "E:\large.001"

# Specify output path
tesseract join "E:\large.001" -o "D:\rejoined.tesseract"
```

---

### `tesseract repair`

Attempt to repair a damaged archive using embedded recovery records.

```
tesseract repair <archive> [options]
```

| Flag | Short | Type | Description |
|------|-------|------|-------------|
| `--verbose` | `-v` | flag | Verbose logging |

**Examples:**

```bash
tesseract repair "E:\damaged.tesseract"
tesseract repair "E:\damaged.tesseract" -v
```

> Requires recovery records (`-r` flag during encode). Without them, repair is not possible.

---

### `tesseract comment`

Display the comment embedded in an archive.

```
tesseract comment <archive>
```

**Example:**

```bash
tesseract comment "E:\archive.tesseract"
```

---

## How It Works

### Encoding Pipeline (Failsafe Staged)

1. **Scan** — recursively finds all files in the source directory
2. **Deduplicate** — 3-stage content matching detects duplicate files
3. **Hash** — computes full SHA-256 for all unique files
4. **Preflight** — snapshots CRC32 + size of every source file
5. **Stage shards** — compresses each unique file into a verified staging shard on disk
6. **Verify shards** — re-reads every shard from disk and verifies CRC32 integrity
7. **Verify source** — re-checks all source files haven't changed since step 4
8. **Assemble** — streams verified shards into a `.tmp` archive file
9. **Verify archive** — validates the assembled archive structure and manifest
10. **Finalize** — atomic rename from `.tmp` to `.tesseract`
11. **Cleanup** — removes staging directory and any temp files

If **any step fails**, source files remain completely untouched. The staging directory and `.tmp` file are cleaned up automatically.

### Archive Format (v2)

```
┌─────────────────────────────┐
│ Header (128 bytes)          │  Magic, flags, offsets, salt, password check
├─────────────────────────────┤
│ Comment (variable)          │  UTF-8 text, up to 64KB
├─────────────────────────────┤
│ Data Blocks                 │  Compressed file data (normal or solid mode)
│   Normal: per-file blocks   │    80-byte block header + zlib stream [+ AES-GCM]
│   Solid: single stream      │    16-byte solid header + continuous zlib [+ AES-GCM]
├─────────────────────────────┤
│ Manifest (gzip JSON)        │  File metadata, dedup groups, offsets [+ AES-GCM]
├─────────────────────────────┤
│ Footer (8 bytes)            │  Magic bytes
├─────────────────────────────┤
│ Recovery Records (optional) │  XOR parity blocks for self-repair
└─────────────────────────────┘
```

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
