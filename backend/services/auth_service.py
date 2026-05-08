from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.security import create_access_token, verify_password
from backend.config import settings
from backend.models import User


async def authenticate(
    db: AsyncSession,
    username: str,
    password: str,
) -> Optional[User]:
    """
    Look up the user by username and verify their password.
    Returns the User on success, None on failure (wrong username or password).
    """
    result = await db.execute(
        select(User).where(User.username == username)
    )
    user = result.scalar_one_or_none()
    if user is None:
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user


async def login(
    db: AsyncSession,
    username: str,
    password: str,
) -> Optional[tuple[User, str]]:
    """
    Full login flow: authenticate, update last_login_at, generate token.
    Returns (user, token) or None if credentials are invalid.
    """
    user = await authenticate(db, username, password)
    if user is None:
        return None
    # Update last_login_at. We commit inside the function because this
    # is a side effect of login that should persist even if the caller
    # does not commit explicitly.
    user.last_login_at = datetime.utcnow()
    await db.commit()
    await db.refresh(user)

    token = create_access_token(
        subject=user.username,
        extra_claims={"user_id": user.user_id, "role": user.role},
        expires_minutes=settings.jwt_expire_minutes,
    )
    return user, token
