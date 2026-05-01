"""
Append the contents of this file to the bottom of
backend/schemas/stats.py - they reference CodeAndDisplay which is
already defined in that file.
"""

from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


# /admin/stats/engine-effectiveness


class EngineSignalFunnelRow(BaseModel):
    """One signal's funnel: adds, sold, rejected, with derived rates."""
    signal: "CodeAndDisplay"  # forward reference - exists in stats.py
    cart_adds: int = Field(..., description="Cart adds attributed to this signal.")
    sold: int = Field(..., description="Of those adds, how many became sold cart lines.")
    not_sold_cart: int = Field(..., description="Adds that the seller marked not_sold (cart-side decline).")
    rejected: int = Field(..., description="Recommendations explicitly rejected via the reject flow.")
    engaged: int = Field(..., description="adds + rejected = total engagement events for this signal.")
    conversion_rate_pct: float = Field(..., description="sold / cart_adds * 100.")
    acceptance_rate_pct: float = Field(..., description="cart_adds / engaged * 100.")
    rejection_rate_pct: float = Field(..., description="rejected / engaged * 100.")
    revenue: Decimal = Field(..., description="Revenue from sold rows for this signal.")


class EngineEffectivenessTotals(BaseModel):
    """Top-line aggregate across all signals."""
    cart_adds: int
    sold: int
    not_sold_cart: int
    rejected: int
    engaged: int
    conversion_rate_pct: float
    acceptance_rate_pct: float
    rejection_rate_pct: float
    revenue: Decimal


class EngineRejectionReasonRow(BaseModel):
    """How many times each reason_code was used."""
    code: str
    count: int


class EngineEffectivenessResponse(BaseModel):
    """Funnel of engine performance for the admin overview."""
    totals: EngineEffectivenessTotals
    by_signal: list[EngineSignalFunnelRow]
    by_reason: list[EngineRejectionReasonRow]
