from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field


class RecommendationItem(BaseModel):
    """A single recommendation. Same shape for precomputed and cold-start."""
    rank: int
    item_id: int
    description: Optional[str] = None
    family: Optional[str] = None
    category: Optional[str] = None
    primary_signal: str
    rec_purpose: Optional[str] = None
    pitch_reason: Optional[str] = None
    confidence_tier: Optional[str] = None
    is_mckesson_brand: bool = False
    is_private_brand: bool = False
    median_unit_price: Optional[Decimal] = None
    peer_adoption_rate: Optional[float] = None
    specialty_match: Optional[str] = None
    units_in_stock: Optional[int] = None


class RecommendationsResponse(BaseModel):
    """Response wrapper for the top-N endpoints."""

    cust_id: int
    customer_segment: Optional[str] = None
    customer_specialty: Optional[str] = None
    recommendation_source: Literal["precomputed", "cold_start"]
    n_results: int
    recommendations: list[RecommendationItem]


# Reject
REJECTION_REASON_CODES = {
    "not_relevant",
    "already_have",
    "out_of_stock",
    "price_too_high",
    "wrong_size_or_spec",
    "different_brand",
    "bad_timing",
    "wrong_recommendation",
    "other",
}


class RejectRecommendationRequest(BaseModel):
    """Body for POST /recommendations/reject - seller marks a rec as not useful."""
    cust_id: int = Field(..., description="Customer the rec was for")
    item_id: int = Field(..., description="Product being rejected")
    primary_signal: Optional[str] = Field(None, description="Signal that produced the rec")
    rec_purpose: Optional[str] = Field(None, description="Purpose tag of the rec")
    reason_code: str = Field(..., description="Quick-pick code, e.g. 'not_relevant'")
    reason_note: Optional[str] = Field(
        None, max_length=2000, description="Optional free-text note"
    )


class RejectRecommendationResponse(BaseModel):
    """Returned on successful rejection."""
    event_id: int
    cust_id: int
    item_id: int
    outcome: Literal["rejected"]
    reason_code: str
    rejected_at: datetime


# Cart helper
class CartHelperRequest(BaseModel):
    """Request body for POST /recommendations/cart-helper.

    Two ways to call this endpoint:

    1. Pass cust_id only. The endpoint will read the customer's active
       in_cart items from Postgres and use those.
       Example: {"cust_id": 13479955}

    2. Pass cust_id AND cart_items. The endpoint will use the provided
       list directly (this is the original behavior, kept for backward
       compatibility and for use cases where the frontend is tracking
       cart state in memory rather than persisting it).
       Example: {"cust_id": 13479955, "cart_items": [21969, 486974]}

    If cart_items is provided and non-empty, it takes precedence over
    whatever is in Postgres.
    """
    cust_id: int = Field(..., description="The customer whose cart this is")
    cart_items: Optional[list[int]] = Field(
        default=None,
        max_length=50,
        description=(
            "Optional list of item IDs currently in the cart. If omitted, "
            "the endpoint reads the customer's active in_cart items from "
            "Postgres. If provided, must contain 1 to 50 item IDs."
        ),
    )


class CartComplement(BaseModel):
    """One cart-complement suggestion (from product_cooccurrence)."""
    trigger_item_id: int
    trigger_description: Optional[str] = None
    item_id: int
    description: Optional[str] = None
    family: Optional[str] = None
    category: Optional[str] = None
    lift: float
    support: Optional[float] = None
    confidence: Optional[float] = None
    is_mckesson_brand: bool = False
    median_unit_price: Optional[Decimal] = None
    units_in_stock: Optional[int] = None
    pitch_reason: Optional[str] = None


class PrivateBrandUpgrade(BaseModel):
    """One private-brand upgrade suggestion."""
    cart_item_id: int
    cart_item_description: Optional[str] = None
    pb_item_id: int
    pb_description: Optional[str] = None
    family: Optional[str] = None
    category: Optional[str] = None
    estimated_savings_pct: Optional[float] = None
    median_unit_price: Optional[Decimal] = None
    units_in_stock: Optional[int] = None
    pitch_reason: Optional[str] = None


class MedlineConversion(BaseModel):
    """One Medline-to-McKesson conversion suggestion."""
    medline_item_id: int
    medline_description: Optional[str] = None
    mckesson_item_id: int
    mckesson_description: Optional[str] = None
    family: Optional[str] = None
    category: Optional[str] = None
    estimated_savings_pct: Optional[float] = None
    median_unit_price: Optional[Decimal] = None
    units_in_stock: Optional[int] = None
    pitch_reason: Optional[str] = None


class CartHelperResponse(BaseModel):
    """Response from POST /recommendations/cart-helper."""
    cust_id: int
    cart_size: int
    cart_source: Literal["request_body", "postgres_cart", "empty"] = Field(
        "request_body",
        description=(
            "Where the cart contents came from. "
            "'request_body' means the caller passed cart_items explicitly. "
            "'postgres_cart' means the endpoint read the customer's active "
            "in_cart items from the cart_items table. "
            "'empty' means the customer has no items in cart and none were passed."
        ),
    )
    cart_complements: list[CartComplement]
    private_brand_upgrades: list[PrivateBrandUpgrade]
    medline_conversions: list[MedlineConversion]


# Purchase history
class PurchaseLine(BaseModel):
    """One line from recdash.purchase_history (with product description joined in)."""
    purchase_id: int
    item_id: int
    description: Optional[str] = None
    family: Optional[str] = None
    category: Optional[str] = None
    quantity: int
    unit_price: Optional[Decimal] = None
    sold_at: datetime

    model_config = {"from_attributes": True}


class PurchaseHistoryResponse(BaseModel):
    """Response wrapper for GET /customers/{cust_id}/history."""
    cust_id: int
    total_lines: int
    returned: int
    items: list[PurchaseLine]
