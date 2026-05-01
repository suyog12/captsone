from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


# Single customer record - returned by GET /customers/{cust_id} and /customers/me

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


# Customer search/filter result - returned by /customers/search and /customers/filter

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

    # True when there is a dashboard login (User row) tied to this cust_id.
    # Populated by the router after the DB read. Defaults to False so the
    # field is always present in the JSON response.
    has_user_account: bool = False

    model_config = {"from_attributes": True}


class CustomerListResponse(BaseModel):
    """
    Paginated customer list. Returned by /customers/filter so the
    frontend can render real pagination ('Page 3 of 47') without
    fetching every customer.

    'total' is the total number of customers matching the filter set
    across all pages. 'limit' and 'offset' echo the request parameters.
    """

    total: int = Field(..., description="Total customers matching the filters across all pages")
    limit: int = Field(..., description="Page size used for this response")
    offset: int = Field(..., description="Offset used for this response (0-based)")
    items: List[CustomerSearchResult] = Field(..., description="The customers on this page")


# Customer record creation - no login

class CustomerRecordCreateRequest(BaseModel):
    """Minimal customer record - no user/login is created.

    Used by the seller workflow (auto-assigns to current seller) and
    by admins who want to create a customer without a login account.
    """

    customer_business_name: str = Field(
        ..., min_length=2, max_length=200,
        description="Display name for the customer (clinic, hospital, practice, etc.)"
    )
    market_code: str = Field(
        ..., min_length=2, max_length=10,
        description="Market code: PO, SC, LTC, AC, HC, LC, OTHER"
    )
    size_tier: str = Field(
        ..., min_length=2, max_length=20,
        description="Size tier: new, small, mid, large, enterprise"
    )
    specialty_code: Optional[str] = Field(
        None, max_length=10,
        description="Optional specialty code (FP, IM, GS, etc.)"
    )
    assigned_seller_id: Optional[int] = Field(
        None,
        description="Admin only - explicit seller assignment. Sellers must omit this; they are auto-assigned to themselves."
    )
