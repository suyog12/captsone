from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class CustomerResponse(BaseModel):
    """Full customer record returned by GET /customers/{cust_id} and /customers/me."""

    cust_id: int
    customer_name: Optional[str] = None
    specialty_code: Optional[str] = None
    market_code: Optional[str] = None
    segment: Optional[str] = None
    supplier_profile: Optional[str] = None
    status: Optional[str] = None
    archetype: Optional[str] = None
    assigned_seller_id: Optional[int] = None
    assigned_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    # Set by the router based on the caller's identity. Lets the frontend
    # decide whether to enable cart actions, claim, or read-only browse.
    is_assigned_to_me: Optional[bool] = None

    model_config = {"from_attributes": True}


class CustomerSearchResult(BaseModel):
    """A single hit in a customer search response."""

    cust_id: int
    customer_name: Optional[str] = None
    specialty_code: Optional[str] = None
    market_code: Optional[str] = None
    segment: Optional[str] = None
    status: Optional[str] = None
    archetype: Optional[str] = None
    assigned_seller_id: Optional[int] = None

    model_config = {"from_attributes": True}


# Customer record creation - no login

class CustomerRecordCreateRequest(BaseModel):
    """Minimal customer record - no user/login is created.

    Used by the seller workflow (auto-assigns to current seller) and
    by admins who want to create a customer without a login account.
    """

    customer_business_name: str = Field(
        ..., min_length=2, max_length=200,
        description="Business / account name shown in the customer roster.",
    )
    market_code: str = Field(
        ..., max_length=20,
        description="Market vertical code (PO, AC, IDN, LTC, HC, LC, SC, ...).",
    )
    size_tier: str = Field(
        ..., max_length=20,
        description="Account size tier (new, small, mid, large, enterprise).",
    )
    specialty_code: Optional[str] = Field(
        None, max_length=20,
        description=(
            "Provider specialty code (FP, IM, PD, ...). Optional; defaults "
            "to None for accounts without a single dominant specialty."
        ),
    )
    # Admin-only field: leave None when seller is creating
    assigned_seller_id: Optional[int] = Field(
        None,
        description=(
            "Admins can pin the new customer to a specific seller. "
            "Sellers MUST leave this null - it auto-resolves to their own "
            "user_id. Sellers passing a non-null value get a 403."
        ),
    )
