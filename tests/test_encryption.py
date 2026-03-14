"""Tests for the encryption module — AES-256-GCM encryption and key derivation."""

import os

import pytest

from tesseract.encryption import (
    create_encryptor,
    create_decryptor,
    generate_salt,
    compute_password_check,
    verify_password_check,
    ChunkEncryptor,
    ChunkDecryptor,
    SALT_SIZE,
    NONCE_SIZE,
    TAG_SIZE,
)


class TestKeyDerivation:
    def test_generate_salt_is_correct_size(self):
        salt = generate_salt()
        assert len(salt) == SALT_SIZE

    def test_generate_salt_is_random(self):
        a = generate_salt()
        b = generate_salt()
        assert a != b  # Extremely unlikely to collide

    def test_create_encryptor_returns_salt(self):
        enc, salt = create_encryptor("password123")
        assert isinstance(enc, ChunkEncryptor)
        assert len(salt) == SALT_SIZE

    def test_create_encryptor_with_provided_salt(self):
        salt = b"\x42" * SALT_SIZE
        enc, returned_salt = create_encryptor("pw", salt=salt)
        assert returned_salt == salt

    def test_create_decryptor(self):
        salt = generate_salt()
        dec = create_decryptor("password", salt)
        assert isinstance(dec, ChunkDecryptor)


class TestPasswordCheck:
    def test_correct_password_verifies(self):
        salt = generate_salt()
        check = compute_password_check("my_password", salt)
        assert len(check) == 32
        assert verify_password_check("my_password", salt, check) is True

    def test_wrong_password_fails(self):
        salt = generate_salt()
        check = compute_password_check("correct", salt)
        assert verify_password_check("wrong", salt, check) is False

    def test_different_salt_fails(self):
        salt1 = generate_salt()
        salt2 = generate_salt()
        check = compute_password_check("pass", salt1)
        assert verify_password_check("pass", salt2, check) is False


class TestEncryptDecrypt:
    def test_roundtrip_small(self):
        password = "test_password"
        enc, salt = create_encryptor(password)
        dec = create_decryptor(password, salt)

        plaintext = b"Hello, Tesseract!"
        encrypted = enc.encrypt(plaintext)
        decrypted = dec.decrypt(encrypted)
        assert decrypted == plaintext

    def test_roundtrip_large(self):
        password = "long-password-for-testing"
        enc, salt = create_encryptor(password)
        dec = create_decryptor(password, salt)

        plaintext = os.urandom(1024 * 1024)  # 1 MB
        encrypted = enc.encrypt(plaintext)
        decrypted = dec.decrypt(encrypted)
        assert decrypted == plaintext

    def test_encrypted_is_different(self):
        enc, salt = create_encryptor("pw")
        plaintext = b"secret data"
        encrypted = enc.encrypt(plaintext)
        assert encrypted != plaintext
        assert len(encrypted) > len(plaintext)  # nonce + tag overhead

    def test_encrypted_size(self):
        enc, _ = create_encryptor("pw")
        size = 1000
        assert enc.encrypted_size(size) == NONCE_SIZE + size + TAG_SIZE

    def test_each_encryption_different(self):
        """Each call should use a different nonce → different output."""
        enc, _ = create_encryptor("pw")
        plaintext = b"same data"
        e1 = enc.encrypt(plaintext)
        e2 = enc.encrypt(plaintext)
        assert e1 != e2  # Different nonces

    def test_wrong_password_raises(self):
        enc, salt = create_encryptor("correct")
        encrypted = enc.encrypt(b"sensitive")
        bad_dec = create_decryptor("wrong", salt)
        with pytest.raises(ValueError, match="Decryption failed"):
            bad_dec.decrypt(encrypted)

    def test_tampered_data_raises(self):
        enc, salt = create_encryptor("pw")
        encrypted = enc.encrypt(b"data")
        # Flip a byte in the ciphertext
        tampered = bytearray(encrypted)
        tampered[-5] ^= 0xFF
        dec = create_decryptor("pw", salt)
        with pytest.raises(ValueError, match="Decryption failed"):
            dec.decrypt(bytes(tampered))

    def test_empty_data(self):
        enc, salt = create_encryptor("pw")
        dec = create_decryptor("pw", salt)
        encrypted = enc.encrypt(b"")
        assert dec.decrypt(encrypted) == b""

    def test_too_short_raises(self):
        dec = create_decryptor("pw", generate_salt())
        with pytest.raises(ValueError, match="too short"):
            dec.decrypt(b"\x00" * 10)
