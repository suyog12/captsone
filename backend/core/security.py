from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import bcrypt
from jose import JWTError, jwt

from backend.config import settings

# Password hashing and verification
def hash_password(plain: str) -> str:
    """
    Hash a plaintext password using bcrypt with a fresh random salt.
    Returns the hash as a UTF-8 string suitable for storage in
    recdash.users.password_hash.
    """
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(plain.encode("utf-8"), salt).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    """
    Check whether a plaintext password matches a stored bcrypt hash.

    Returns False on any failure (wrong password, malformed hash, etc.)
    rather than raising, so callers can treat it as a simple boolean.
    """
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except (ValueError, TypeError):
        return False

# JWT generation and decoding
def create_access_token(
    subject: str,
    extra_claims: Optional[dict[str, Any]] = None,
    expires_minutes: Optional[int] = None,
) -> str:
    """
    Create a signed JWT access token.

    The 'sub' claim holds the user identifier (we use the username string).
    Any extra claims (role, user_id) go in alongside.

    The token is signed with the secret in settings.jwt_secret_key.
    """
    if expires_minutes is None:
        expires_minutes = settings.jwt_expire_minutes

    now = datetime.now(timezone.utc)
    expire = now + timedelta(minutes=expires_minutes)

    claims: dict[str, Any] = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp()),
    }
    if extra_claims:
        claims.update(extra_claims)

    return jwt.encode(
        claims,
        settings.jwt_secret_key,
        algorithm=settings.jwt_algorithm,
    )


def decode_access_token(token: str) -> dict[str, Any]:
    """
    Decode and verify a JWT access token.

    Raises JWTError if the token is invalid, expired, or has the wrong
    signature. Caller should catch JWTError and return 401.
    """
    return jwt.decode(
        token,
        settings.jwt_secret_key,
        algorithms=[settings.jwt_algorithm],
    )


# Re-export so callers can catch the exception without importing jose
__all__ = [
    "hash_password",
    "verify_password",
    "create_access_token",
    "decode_access_token",
    "JWTError",
]
