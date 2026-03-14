"""AES-256-GCM encryption for Tesseract archives.

Provides authenticated encryption using AES-256-GCM with key derivation
from a user-supplied password via PBKDF2-HMAC-SHA256.

Encryption wraps around data blocks — each block gets its own random nonce
for maximum security. The manifest is also encrypted.

Layout of encrypted data:
    [SALT: 32 bytes] (only in header, used for key derivation)
    Per encrypted chunk:
        [NONCE: 12 bytes][TAG: 16 bytes][CIPHERTEXT: variable]
"""

import hashlib
import hmac
import os
import struct
from typing import Tuple

# AES-GCM constants
SALT_SIZE = 32
NONCE_SIZE = 12
TAG_SIZE = 16
KEY_SIZE = 32  # AES-256
KDF_ITERATIONS = 600_000  # OWASP recommended minimum for PBKDF2-HMAC-SHA256
ENCRYPTED_CHUNK_OVERHEAD = NONCE_SIZE + TAG_SIZE  # 28 bytes per chunk


def _derive_key(password: str, salt: bytes) -> bytes:
    """Derive a 256-bit key from password and salt using PBKDF2-HMAC-SHA256."""
    return hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        KDF_ITERATIONS,
        dklen=KEY_SIZE,
    )


def generate_salt() -> bytes:
    """Generate a cryptographically random salt."""
    return os.urandom(SALT_SIZE)


def create_encryptor(password: str, salt: bytes = None) -> Tuple["ChunkEncryptor", bytes]:
    """
    Create an encryptor from a password.
    Returns (encryptor, salt) — salt must be stored in the archive header.
    """
    if salt is None:
        salt = generate_salt()
    key = _derive_key(password, salt)
    return ChunkEncryptor(key), salt


def create_decryptor(password: str, salt: bytes) -> "ChunkDecryptor":
    """Create a decryptor from a password and the archive's salt."""
    key = _derive_key(password, salt)
    return ChunkDecryptor(key)


class ChunkEncryptor:
    """Encrypts individual data chunks with AES-256-GCM."""

    def __init__(self, key: bytes):
        self._key = key

    def encrypt(self, plaintext: bytes) -> bytes:
        """
        Encrypt a chunk of data. Returns nonce + tag + ciphertext.
        Each call uses a fresh random nonce.
        """
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        nonce = os.urandom(NONCE_SIZE)
        aesgcm = AESGCM(self._key)
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)
        # ciphertext from AESGCM includes the tag appended
        # Format: nonce(12) + ciphertext_with_tag(len+16)
        return nonce + ciphertext

    def encrypted_size(self, plaintext_size: int) -> int:
        """Calculate the encrypted output size for a given plaintext size."""
        return NONCE_SIZE + plaintext_size + TAG_SIZE


class ChunkDecryptor:
    """Decrypts individual data chunks with AES-256-GCM."""

    def __init__(self, key: bytes):
        self._key = key

    def decrypt(self, encrypted_data: bytes) -> bytes:
        """
        Decrypt a chunk. Input format: nonce(12) + ciphertext_with_tag.
        Raises ValueError on authentication failure (wrong password or tampered data).
        """
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        if len(encrypted_data) < NONCE_SIZE + TAG_SIZE:
            raise ValueError("Encrypted data too short")
        nonce = encrypted_data[:NONCE_SIZE]
        ciphertext_with_tag = encrypted_data[NONCE_SIZE:]
        aesgcm = AESGCM(self._key)
        try:
            return aesgcm.decrypt(nonce, ciphertext_with_tag, None)
        except Exception:
            raise ValueError(
                "Decryption failed — wrong password or data is corrupt"
            )


def compute_password_check(password: str, salt: bytes) -> bytes:
    """
    Compute a 32-byte check value that can verify a password without
    exposing the encryption key. Stored in the archive for fast rejection
    of wrong passwords before attempting full decryption.
    """
    check_key = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt + b"_password_check",
        KDF_ITERATIONS,
        dklen=32,
    )
    return check_key


def verify_password_check(password: str, salt: bytes, expected_check: bytes) -> bool:
    """Verify a password against the stored check value (constant-time)."""
    actual = compute_password_check(password, salt)
    return hmac.compare_digest(actual, expected_check)
