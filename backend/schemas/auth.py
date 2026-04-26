from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    """Body of POST /auth/login when called as JSON."""

    username: str = Field(..., min_length=1, max_length=100)
    password: str = Field(..., min_length=1, max_length=200)


class LoginResponse(BaseModel):
    """Response from POST /auth/login. Compatible with OAuth2 password flow."""

    access_token: str
    token_type: str = "bearer"
    expires_in_minutes: int
    user: "CurrentUserResponse"


class TokenPayload(BaseModel):
    """Internal representation of decoded JWT claims."""

    sub: str            # username
    user_id: int
    role: str
    exp: int
    iat: int


class CurrentUserResponse(BaseModel):
    """User info returned from /auth/me and embedded in /auth/login."""

    user_id: int
    username: str
    role: str
    full_name: Optional[str] = None
    email: Optional[str] = None
    cust_id: Optional[int] = None

    model_config = {"from_attributes": True}


# Resolve the forward reference in LoginResponse
LoginResponse.model_rebuild()
