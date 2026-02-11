import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def load_private_key_der() -> bytes:
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if not key_path:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH is not set")

    passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    password_bytes = passphrase.encode() if passphrase else None

    with open(key_path, "rb") as f:
        pkey = serialization.load_pem_private_key(
            f.read(),
            password=password_bytes,
            backend=default_backend(),
        )

    return pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )