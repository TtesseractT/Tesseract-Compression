"""Tesseract Compression System - Deduplication-based archiver for cold storage."""

__version__ = "2.0.0"

from .encoder import TesseractEncoder
from .decoder import TesseractDecoder
from .encryption import create_encryptor, create_decryptor
from .recovery import RecoveryRecord, generate_recovery_data, repair_archive
from .volume import split_archive, join_volumes
