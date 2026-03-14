"""Command-line interface for the Tesseract Compression System.

Usage:
    python -m tesseract encode <source_dir> <output.tesseract> [options]
    python -m tesseract decode <archive.tesseract> <output_dir> [options]
    python -m tesseract info <archive.tesseract>
    python -m tesseract verify <archive.tesseract>
    python -m tesseract split <archive.tesseract> [options]
    python -m tesseract join <first_volume.001> [options]
    python -m tesseract repair <archive.tesseract>
    python -m tesseract comment <archive.tesseract>
"""

import argparse
import getpass
import logging
import os
import sys
from pathlib import Path

from .encoder import TesseractEncoder
from .decoder import TesseractDecoder

logger = logging.getLogger("tesseract")

# Try to import tqdm for progress bars; fall back to basic logging
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


def _default_workers() -> int:
    """Default worker count: all CPUs minus 1, minimum 1."""
    cpu = os.cpu_count() or 4
    return max(1, cpu - 1)


def _prompt_password(confirm: bool = False) -> str:
    """Securely prompt for a password."""
    password = getpass.getpass("Password: ")
    if confirm:
        confirm_pw = getpass.getpass("Confirm password: ")
        if password != confirm_pw:
            print("Error: Passwords do not match", file=sys.stderr)
            sys.exit(1)
    return password


class ProgressTracker:
    """Bridges the progress_callback interface to tqdm or logging."""

    PHASE_LABELS = {
        "scanning": "Scanning files",
        "deduplicating": "Grouping by metadata",
        "partial_hashing": "Partial hashing (64KB)",
        "full_hashing": "Full BLAKE3 hashing",
        "hashing_unique": "Hashing unique files",
        "preflight": "Pre-flight verification",
        "staging": "Staging shards",
        "verifying_shards": "Verifying shards",
        "verifying_source": "Verifying source files",
        "assembling": "Assembling archive",
        "writing": "Writing archive",
        "verifying": "Verifying archive",
        "finalizing": "Finalizing",
        "recovery": "Recovery records",
        "reading_header": "Reading header",
        "reading_manifest": "Reading manifest",
        "extracting": "Extracting files",
        "restoring_duplicates": "Restoring duplicates",
        "verifying_extracted": "Verifying extracted files",
    }

    def __init__(self, use_tqdm: bool = True):
        self.use_tqdm = use_tqdm and HAS_TQDM
        self._bar = None
        self._current_phase = ""

    def __call__(self, event: str, value=None, total: int = 0):
        if event == "phase":
            self._set_phase(value, total)
        elif event == "step":
            if self._bar is not None:
                self._bar.update(value if value else 1)

    def _set_phase(self, phase: str, total: int = 0):
        if self._bar is not None:
            self._bar.close()
            self._bar = None
        self._current_phase = phase
        label = self.PHASE_LABELS.get(phase, phase)
        if self.use_tqdm and total > 0:
            self._bar = tqdm(
                total=total, desc=label, unit="file",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
            )
        elif self.use_tqdm and total == -1:
            # Counter mode: no known total, just count up
            self._bar = tqdm(
                desc=label, unit="file",
                bar_format="{desc}: {n_fmt} {unit} [{elapsed}]",
            )
        else:
            logger.info(f"{label}..." + (f" ({total} items)" if total > 0 else ""))

    def close(self):
        if self._bar is not None:
            self._bar.close()
            self._bar = None


# ── CLI Commands ──────────────────────────────────────────────────

def cmd_encode(args):
    """Create a .tesseract archive from a directory."""
    source = Path(args.source)
    output = Path(args.output)

    if not output.suffix:
        output = output.with_suffix(".tesseract")

    # Handle password
    password = None
    if args.password:
        password = args.password
    elif args.encrypt:
        password = _prompt_password(confirm=True)

    tracker = ProgressTracker(use_tqdm=True)

    encoder = TesseractEncoder(
        workers=args.workers or _default_workers(),
        compression_level=args.compression_level,
        progress_callback=tracker,
        exclude_patterns=args.exclude,
        solid=args.solid,
        password=password,
        recovery_percent=args.recovery or 0,
        comment=args.comment or "",
        store_permissions=args.permissions,
        lock=args.lock,
    )

    try:
        manifest = encoder.encode(source, output)
        tracker.close()
        print(f"\nArchive created: {output}")
        print(f"  Total files:       {manifest.file_count}")
        print(f"  Unique stored:     {manifest.unique_count}")
        print(f"  Duplicate groups:  {manifest.duplicate_group_count}")
        print(f"  Original size:     {_fmt_size(manifest.total_original_size)}")
        print(f"  Archive size:      {_fmt_size(output.stat().st_size)}")
        print(f"  Dedup savings:     {_fmt_size(manifest.space_savings)}")
        features = []
        if args.solid:
            features.append("solid")
        if password:
            features.append("encrypted")
        if args.recovery:
            features.append(f"{args.recovery}% recovery")
        if args.permissions:
            features.append("permissions")
        if args.lock:
            features.append("locked")
        if features:
            print(f"  Features:          {', '.join(features)}")
    except Exception as e:
        tracker.close()
        logger.error(f"Encoding failed: {e}")
        sys.exit(1)


def cmd_decode(args):
    """Extract a .tesseract archive to a directory."""
    archive = Path(args.archive)
    output = Path(args.output)

    # Handle password
    password = None
    if args.password:
        password = args.password
    else:
        # Check if archive is encrypted by reading header
        from .archive_format import unpack_header, HEADER_SIZE
        with open(archive, "rb") as f:
            header = unpack_header(f.read(HEADER_SIZE))
        if header.is_encrypted:
            password = _prompt_password(confirm=False)

    tracker = ProgressTracker(use_tqdm=True)

    decoder = TesseractDecoder(
        workers=args.workers or _default_workers(),
        verify=not args.no_verify,
        overwrite=args.overwrite,
        password=password,
        extract_patterns=args.extract or [],
        progress_callback=tracker,
    )

    try:
        manifest = decoder.decode(archive, output)
        tracker.close()
        print(f"\nExtracted to: {output}")
        print(f"  Total files restored: {manifest.file_count}")
    except Exception as e:
        tracker.close()
        logger.error(f"Decoding failed: {e}")
        sys.exit(1)


def cmd_info(args):
    """Display information about a .tesseract archive."""
    archive = Path(args.archive)

    # Handle password for encrypted archives
    password = None
    if args.password:
        password = args.password
    else:
        from .archive_format import unpack_header, HEADER_SIZE
        with open(archive, "rb") as f:
            header = unpack_header(f.read(HEADER_SIZE))
        if header.is_encrypted:
            password = _prompt_password(confirm=False)

    decoder = TesseractDecoder(password=password)
    try:
        manifest = decoder.read_manifest(archive)
    except Exception as e:
        logger.error(f"Cannot read archive: {e}")
        sys.exit(1)

    archive_size = archive.stat().st_size

    from .archive_format import unpack_header, HEADER_SIZE
    with open(archive, "rb") as f:
        header = unpack_header(f.read(HEADER_SIZE))

    print(f"Archive: {archive}")
    print(f"  Version:           {manifest.version}")
    print(f"  Created:           {manifest.created}")
    print(f"  Source:            {manifest.source_root}")
    print(f"  Total files:       {manifest.file_count}")
    print(f"  Unique stored:     {manifest.unique_count}")
    print(f"  Duplicate groups:  {manifest.duplicate_group_count}")
    print(f"  Original size:     {_fmt_size(manifest.total_original_size)}")
    print(f"  Unique data size:  {_fmt_size(manifest.total_unique_size)}")
    print(f"  Archive size:      {_fmt_size(archive_size)}")
    print(f"  Dedup savings:     {_fmt_size(manifest.space_savings)}")

    if manifest.total_original_size > 0:
        ratio = archive_size / manifest.total_original_size
        print(f"  Compression ratio: {ratio:.2%}")

    # Feature flags
    features = []
    if header.is_encrypted:
        features.append("encrypted (AES-256-GCM)")
    if header.is_solid:
        features.append("solid mode")
    if header.has_recovery:
        features.append("recovery records")
    if header.is_locked:
        features.append("locked")
    if header.has_permissions:
        features.append("permissions stored")
    if features:
        print(f"  Features:          {', '.join(features)}")

    if manifest.comment:
        print(f"  Comment:           {manifest.comment}")

    if args.list_files:
        print(f"\n  Files ({manifest.file_count}):")
        for rel_path, info in sorted(manifest.files.items()):
            marker = " [dup]" if info.get("group_id") and not info.get("is_master") else ""
            print(f"    {rel_path} ({_fmt_size(info['size'])}){marker}")

    if args.list_groups:
        print(f"\n  Duplicate Groups ({manifest.duplicate_group_count}):")
        for gid, ginfo in manifest.duplicate_groups.items():
            count = 1 + len(ginfo["duplicates"])
            savings = ginfo["size"] * len(ginfo["duplicates"])
            print(f"    [{ginfo['filename']}] {count} copies, saves {_fmt_size(savings)}")
            print(f"      Master: {ginfo['master']}")
            for dup in ginfo["duplicates"]:
                print(f"      Dup:    {dup}")


def cmd_verify(args):
    """Verify the integrity of a .tesseract archive."""
    archive = Path(args.archive)

    from .archive_format import unpack_header, HEADER_SIZE, MAGIC_FOOTER
    from .manifest import Manifest

    # Handle password
    password = args.password if hasattr(args, "password") else None

    try:
        with open(archive, "rb") as f:
            header = unpack_header(f.read(HEADER_SIZE))
            print(f"Header:   OK (v{header.version}, {header.total_files} files)")

            features = []
            if header.is_encrypted:
                features.append("encrypted")
            if header.is_solid:
                features.append("solid")
            if header.has_recovery:
                features.append("recovery")
            if header.is_locked:
                features.append("locked")
            if features:
                print(f"Features: {', '.join(features)}")

            # Skip comment
            if header.comment_length:
                comment = f.read(header.comment_length).decode("utf-8")
                print(f"Comment:  \"{comment}\"")

            # Check footer
            f.seek(header.manifest_offset + header.manifest_compressed_size)
            footer = f.read(len(MAGIC_FOOTER))
            if footer != MAGIC_FOOTER:
                print("Footer:   CORRUPT")
                sys.exit(1)
            print("Footer:   OK")

            # Check manifest
            f.seek(header.manifest_offset)
            manifest_data = f.read(header.manifest_compressed_size)
            if header.is_encrypted:
                if not password:
                    password = _prompt_password(confirm=False)
                from .encryption import create_decryptor
                decryptor = create_decryptor(password, header.encryption_salt)
                manifest_data = decryptor.decrypt(manifest_data)
            manifest = Manifest.from_json(manifest_data)
            print(f"Manifest: OK ({manifest.file_count} files)")

            # Verify data blocks (skip for solid mode — different structure)
            if not header.is_solid:
                from .archive_format import unpack_block_header, BLOCK_HEADER_SIZE
                errors = 0
                checked = 0

                for rel_path, file_info in manifest.files.items():
                    if file_info.get("group_id") and not file_info.get("is_master"):
                        continue

                    offset = file_info["data_offset"]
                    f.seek(offset)
                    bh_data = f.read(BLOCK_HEADER_SIZE)
                    bh = unpack_block_header(bh_data)

                    if bh.content_hash != file_info["content_hash"]:
                        print(f"  MISMATCH: {rel_path} (block hash != manifest hash)")
                        errors += 1
                    checked += 1

                if errors:
                    print(f"Blocks:   {errors} ERRORS in {checked} blocks")
                    sys.exit(1)
                print(f"Blocks:   OK ({checked} blocks verified)")
            else:
                print("Blocks:   Solid mode (stream integrity depends on decompression)")

            # Check recovery records
            if header.has_recovery and header.recovery_size > 0:
                print(f"Recovery: Present ({_fmt_size(header.recovery_size)})")
            else:
                print("Recovery: None")

        print("\nArchive integrity: PASSED")

    except Exception as e:
        logger.error(f"Verification failed: {e}")
        sys.exit(1)


def cmd_split(args):
    """Split a .tesseract archive into multi-volume parts."""
    archive = Path(args.archive)

    from .volume import split_archive

    volume_size = args.size * 1024 * 1024  # Convert MB to bytes

    tracker = ProgressTracker(use_tqdm=True)

    try:
        volumes = split_archive(
            archive, volume_size=volume_size, progress_callback=tracker,
        )
        tracker.close()
        print(f"\nSplit into {len(volumes)} volumes:")
        for v in volumes:
            print(f"  {v.name} ({_fmt_size(v.stat().st_size)})")
    except Exception as e:
        logger.error(f"Split failed: {e}")
        sys.exit(1)


def cmd_join(args):
    """Reassemble multi-volume archive parts."""
    first_vol = Path(args.first_volume)
    output = Path(args.output) if args.output else None

    from .volume import join_volumes

    tracker = ProgressTracker(use_tqdm=True)

    try:
        result = join_volumes(
            first_vol, output_path=output, progress_callback=tracker,
        )
        tracker.close()
        print(f"\nJoined archive: {result} ({_fmt_size(result.stat().st_size)})")
    except Exception as e:
        logger.error(f"Join failed: {e}")
        sys.exit(1)


def cmd_repair(args):
    """Attempt to repair a damaged archive using recovery records."""
    archive = Path(args.archive)

    from .archive_format import unpack_header, HEADER_SIZE
    from .recovery import RecoveryRecord, repair_archive

    try:
        with open(archive, "rb") as f:
            header = unpack_header(f.read(HEADER_SIZE))

        if not header.has_recovery or header.recovery_size == 0:
            print("Archive has no recovery records — repair not possible")
            sys.exit(1)

        # Read recovery record
        with open(archive, "rb") as f:
            f.seek(header.recovery_offset)
            rec_data = f.read(header.recovery_size)
        recovery = RecoveryRecord.deserialize(rec_data)

        data_start = HEADER_SIZE + header.comment_length
        data_end = header.manifest_offset

        print(f"Recovery records: {len(recovery.parity_blocks)} parity blocks")
        print(f"Data region: {_fmt_size(data_end - data_start)}")
        print("Scanning for damage...")

        checked, repaired = repair_archive(
            archive, data_start, data_end, recovery,
        )

        if repaired:
            print(f"\nRepair complete: {checked} slices checked, {repaired} repaired")
        else:
            print(f"\nNo damage found: {checked} slices checked, all OK")

    except RuntimeError as e:
        logger.error(f"Repair failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Repair failed: {e}")
        sys.exit(1)


def cmd_comment(args):
    """Display or set the archive comment."""
    archive = Path(args.archive)

    decoder = TesseractDecoder()
    comment = decoder.read_comment(archive)

    if comment:
        print(f"Comment: {comment}")
    else:
        print("No comment set")


# ── Utility ───────────────────────────────────────────────────────

def _fmt_size(size_bytes: int) -> str:
    """Format byte count as human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes / (1024**2):.1f} MB"
    elif size_bytes < 1024 ** 4:
        return f"{size_bytes / (1024**3):.2f} GB"
    else:
        return f"{size_bytes / (1024**4):.2f} TB"


def _parse_size(size_str: str) -> int:
    """Parse a size string like '100MB', '2GB' into megabytes."""
    size_str = size_str.strip().upper()
    multipliers = {"B": 1/(1024*1024), "KB": 1/1024, "MB": 1, "GB": 1024, "TB": 1024*1024}
    for suffix, mult in sorted(multipliers.items(), key=lambda x: -len(x[0])):
        if size_str.endswith(suffix):
            return int(float(size_str[:-len(suffix)].strip()) * mult)
    return int(size_str)


def main():
    """Main entry point for the tesseract CLI."""
    parser = argparse.ArgumentParser(
        prog="tesseract",
        description="Tesseract Compression System — Deduplication-based archiver for cold storage",
    )
    subparsers = parser.add_subparsers(dest="command")

    # ── encode ────────────────────────────────────────────────────
    enc = subparsers.add_parser("encode", help="Create a .tesseract archive")
    enc.add_argument("source", help="Source directory to archive")
    enc.add_argument("output", help="Output .tesseract file path")
    enc.add_argument(
        "-w", "--workers", type=int, default=None,
        help=f"Number of CPU cores to use (default: {_default_workers()})",
    )
    enc.add_argument(
        "-c", "--compression-level", type=int, default=19,
        choices=range(1, 23), metavar="1-22",
        help="zstd compression level (default: 19, max: 22)",
    )
    enc.add_argument(
        "-e", "--exclude", action="append", default=[],
        help="Pattern to exclude from archiving (can be repeated)",
    )
    enc.add_argument(
        "-s", "--solid", action="store_true",
        help="Solid compression mode (better ratio, slower random access)",
    )
    enc.add_argument(
        "-p", "--password", type=str, default=None, metavar="PASS",
        help="Encrypt archive with password (or use --encrypt to be prompted)",
    )
    enc.add_argument(
        "--encrypt", action="store_true",
        help="Encrypt archive (prompts for password securely)",
    )
    enc.add_argument(
        "-r", "--recovery", type=int, default=0, metavar="1-30",
        help="Add recovery records (percentage of archive size, 1-30%%)",
    )
    enc.add_argument(
        "-m", "--comment", type=str, default="",
        help="Add a text comment to the archive",
    )
    enc.add_argument(
        "--permissions", action="store_true",
        help="Store file permissions in archive",
    )
    enc.add_argument(
        "--lock", action="store_true",
        help="Lock archive (mark as finalized)",
    )
    enc.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")

    # ── decode ────────────────────────────────────────────────────
    dec = subparsers.add_parser("decode", help="Extract a .tesseract archive")
    dec.add_argument("archive", help=".tesseract archive file to extract")
    dec.add_argument("output", help="Output directory for extracted files")
    dec.add_argument("-w", "--workers", type=int, default=None, help="Number of CPU cores")
    dec.add_argument(
        "-p", "--password", type=str, default=None,
        help="Password for encrypted archives",
    )
    dec.add_argument(
        "-x", "--extract", action="append", default=[],
        help="Extract only files matching this glob pattern (can be repeated)",
    )
    dec.add_argument("--no-verify", action="store_true", help="Skip post-extraction verification")
    dec.add_argument("--overwrite", action="store_true", help="Overwrite existing output files")
    dec.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")

    # ── info ──────────────────────────────────────────────────────
    inf = subparsers.add_parser("info", help="Show archive information")
    inf.add_argument("archive", help=".tesseract archive file")
    inf.add_argument(
        "-p", "--password", type=str, default=None,
        help="Password for encrypted archives",
    )
    inf.add_argument("-l", "--list-files", action="store_true", help="List all files in archive")
    inf.add_argument("-g", "--list-groups", action="store_true", help="List duplicate groups")

    # ── verify ────────────────────────────────────────────────────
    ver = subparsers.add_parser("verify", help="Verify archive integrity")
    ver.add_argument("archive", help=".tesseract archive file")
    ver.add_argument(
        "-p", "--password", type=str, default=None,
        help="Password for encrypted archives",
    )
    ver.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")

    # ── split ─────────────────────────────────────────────────────
    spl = subparsers.add_parser("split", help="Split archive into multi-volume parts")
    spl.add_argument("archive", help=".tesseract archive to split")
    spl.add_argument(
        "-s", "--size", type=int, default=100, metavar="MB",
        help="Size of each volume in MB (default: 100)",
    )

    # ── join ──────────────────────────────────────────────────────
    joi = subparsers.add_parser("join", help="Reassemble multi-volume archive")
    joi.add_argument("first_volume", help="First volume file (.001)")
    joi.add_argument("-o", "--output", type=str, default=None, help="Output path for joined archive")

    # ── repair ────────────────────────────────────────────────────
    rep = subparsers.add_parser("repair", help="Repair damaged archive using recovery records")
    rep.add_argument("archive", help=".tesseract archive to repair")
    rep.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")

    # ── comment ───────────────────────────────────────────────────
    com = subparsers.add_parser("comment", help="Display archive comment")
    com.add_argument("archive", help=".tesseract archive file")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Set up logging
    level = logging.DEBUG if getattr(args, "verbose", False) else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    commands = {
        "encode": cmd_encode,
        "decode": cmd_decode,
        "info": cmd_info,
        "verify": cmd_verify,
        "split": cmd_split,
        "join": cmd_join,
        "repair": cmd_repair,
        "comment": cmd_comment,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
