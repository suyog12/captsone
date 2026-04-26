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


# ---- Cart helper ----

class CartHelperRequest(BaseModel):
    """Request body for POST /recommendations/cart-helper."""

    cust_id: int = Field(..., description="The customer whose cart this is")
    cart_items: list[int] = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Item IDs currently in the cart",
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
    cart_complements: list[CartComplement]
    private_brand_upgrades: list[PrivateBrandUpgrade]
    medline_conversions: list[MedlineConversion]


# ---- Purchase history ----

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
