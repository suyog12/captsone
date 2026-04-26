from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.core.dependencies import get_current_user
from backend.db.database import get_db
from backend.models import User
from backend.schemas.auth import (
    CurrentUserResponse,
    LoginResponse,
)
from backend.services import auth_service


router = APIRouter(prefix="/auth", tags=["auth"])


@router.post(
    "/login",
    response_model=LoginResponse,
    summary="Log in with username and password",
)
async def login(
    form: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db),
) -> LoginResponse:
    """
    Standard OAuth2 password flow.

    The /docs Authorize button uses this. From the React frontend you can
    POST a form-encoded body with `username` and `password` fields.
    """
    result = await auth_service.login(db, form.username, form.password)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user, token = result
    return LoginResponse(
        access_token=token,
        token_type="bearer",
        expires_in_minutes=settings.jwt_expire_minutes,
        user=CurrentUserResponse.model_validate(user),
    )


@router.get(
    "/me",
    response_model=CurrentUserResponse,
    summary="Return the current authenticated user",
)
async def me(
    current_user: User = Depends(get_current_user),
) -> CurrentUserResponse:
    """Return the user record corresponding to the bearer token."""
    return CurrentUserResponse.model_validate(current_user)
