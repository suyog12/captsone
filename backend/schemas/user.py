from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, EmailStr, Field


class UserResponse(BaseModel):
    """Public user record returned by API."""
    user_id: int
    username: str
    role: str
    full_name: Optional[str] = None
    email: Optional[str] = None
    cust_id: Optional[int] = None
    is_active: bool = True
    created_at: Optional[datetime] = None
    last_login_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class AdminCreateRequest(BaseModel):
    """Body for POST /users/admins."""
    username: str = Field(min_length=3, max_length=100)
    password: str = Field(min_length=6, max_length=200)
    full_name: Optional[str] = Field(None, max_length=200)
    email: Optional[str] = Field(None, max_length=200)


class SellerCreateRequest(BaseModel):
    """Body for POST /users/sellers."""
    username: str = Field(min_length=3, max_length=100)
    password: str = Field(min_length=6, max_length=200)
    full_name: Optional[str] = Field(None, max_length=200)
    email: Optional[str] = Field(None, max_length=200)


MarketCode = Literal["PO", "LTC", "SC", "HC", "LC", "AC"]
SizeTier = Literal["new", "small", "mid", "large", "enterprise"]


class CustomerCreateRequest(BaseModel):
    """Body for POST /users/customers.
    assigned_seller_id is honored only when the caller is an admin. When a
    seller calls this endpoint, the new customer is always auto-assigned
    to that seller (the field is ignored).
    """
    username: str = Field(min_length=3, max_length=100)
    password: str = Field(min_length=6, max_length=200)
    full_name: Optional[str] = Field(None, max_length=200)
    email: Optional[str] = Field(None, max_length=200)
    customer_business_name: Optional[str] = Field(None, max_length=200)
    market_code: MarketCode = Field(...)
    size_tier: SizeTier = Field(...)
    specialty_code: Optional[str] = Field(None, max_length=20)
    assigned_seller_id: Optional[int] = Field(
        None,
        description="Optional. Admin-only field. Ignored when caller is a seller.",
    )


class PasswordChangeRequest(BaseModel):
    """Body for PATCH /users/me/password."""
    current_password: str = Field(min_length=1)
    new_password: str = Field(min_length=6, max_length=200)


class UserListResponse(BaseModel):
    """Response from GET /users."""
    total: int
    items: list[UserResponse]
