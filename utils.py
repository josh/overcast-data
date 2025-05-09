import base64
import logging
import os
import sys
from typing import NewType
from urllib.parse import urlparse

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

_RAISE_VALIDATION_ERRORS = "pytest" in sys.modules

logger = logging.getLogger("utils")


class URL(str):
    def __new__(cls, urlstring: str) -> "URL":
        try:
            components = urlparse(urlstring)
            if not components.scheme:
                raise ValueError(f"Invalid URL: {urlstring}")
        except ValueError as e:
            if _RAISE_VALIDATION_ERRORS:
                raise e
            else:
                logger.error(e)

        return str.__new__(cls, urlstring)


class HTTPURL(URL):
    def __new__(cls, urlstring: str) -> "HTTPURL":
        try:
            components = urlparse(urlstring)
            if components.scheme not in ["http", "https"]:
                raise ValueError(f"Invalid HTTP URL: '{urlstring}'")
        except ValueError as e:
            if _RAISE_VALIDATION_ERRORS:
                raise e
            else:
                logger.error(e)

        return str.__new__(cls, urlstring)


EncryptionKey = NewType("EncryptionKey", str)
Ciphertext = NewType("Ciphertext", str)


def encrypt(key: EncryptionKey, plaintext: str) -> Ciphertext:
    cipher, pkcs7 = _encryption_cipher(key)
    encryptor, padder = cipher.encryptor(), pkcs7.padder()
    plainbytes = plaintext.encode()
    padded_data = padder.update(plainbytes) + padder.finalize()
    cipherbytes = encryptor.update(padded_data) + encryptor.finalize()
    return Ciphertext(base64.b64encode(cipherbytes).decode("utf-8"))


def decrypt(key: EncryptionKey, ciphertext: Ciphertext) -> str:
    cipher, pkcs7 = _encryption_cipher(key)
    decryptor, unpadder = cipher.decryptor(), pkcs7.unpadder()
    cipherbytes = base64.b64decode(ciphertext)
    decrypted_padded = decryptor.update(cipherbytes) + decryptor.finalize()
    plainbytes = unpadder.update(decrypted_padded) + unpadder.finalize()
    return plainbytes.decode()


def environ_encryption_key() -> EncryptionKey | None:
    key = os.environ.get("ENCRYPTION_KEY", "")
    if not key:
        logger.warning("ENCRYPTION_KEY is not set")
        return None
    return EncryptionKey(key)


def generate_encryption_key() -> EncryptionKey:
    return EncryptionKey(base64.b64encode(os.urandom(32 + 16)).decode())


def _encryption_cipher(key: EncryptionKey) -> tuple[Cipher[modes.CBC], padding.PKCS7]:
    assert len(key) == 64
    key_data: bytes = base64.b64decode(key)
    assert len(key_data) == 48
    algorithm = algorithms.AES(key_data[0:32])
    mode = modes.CBC(key_data[32:48])
    padder = padding.PKCS7(algorithms.AES.block_size)
    cipher = Cipher(algorithm, mode)
    return cipher, padder
