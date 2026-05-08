from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field


# Allowed values for the source field on add-to-cart.
# These match the CHECK constraint on cart_items.source in the database.
CartSource = Literal[
    "manual",
    "recommendation_peer_gap",
    "recommendation_lapsed",
    "recommendation_replenishment",
    "recommendation_cart_complement",
    "recommendation_pb_upgrade",
    "recommendation_medline_conversion",
    "recommendation_item_similarity",
    "recommendation_popularity",
]

CartStatus = Literal["in_cart", "sold", "not_sold"]


# Add to 

class AddToCartRequest(BaseModel):
    """Body for POST /customers/{cust_id}/cart."""

    item_id: int = Field(..., description="Product ID to add.")
    quantity: int = Field(..., gt=0, description="Quantity to add (positive integer).")
    source: CartSource = Field(
        "manual",
        description="Where this add came from. Use 'manual' if not from a recommendation.",
    )


# Update quantity
class UpdateCartQuantityRequest(BaseModel):
    """Body for PATCH /cart/{cart_item_id}."""

    quantity: int = Field(..., gt=0, description="New quantity (positive integer).")


# Update status (mark sold or not_sold)
class UpdateCartStatusRequest(BaseModel):
    """Body for PATCH /cart/{cart_item_id}/status."""

    status: Literal["sold", "not_sold"] = Field(
        ...,
        description="Target status. Use POST /cart/{id}/checkout to mark sold AND write purchase history.",
    )


# Single cart line response
class CartLine(BaseModel):
    """One row of cart_items, hydrated with product description and stock."""

    cart_item_id: int
    cust_id: int
    item_id: int
    description: Optional[str] = None
    family: Optional[str] = None
    category: Optional[str] = None
    quantity: int
    unit_price_at_add: Optional[Decimal] = None
    line_total: Optional[Decimal] = Field(
        None,
        description="quantity * unit_price_at_add, computed at response time.",
    )
    added_by_user_id: int
    added_by_username: Optional[str] = None
    added_by_role: str
    source: str
    status: str
    added_at: datetime
    resolved_at: Optional[datetime] = None
    resolved_by_user_id: Optional[int] = None
    units_in_stock: Optional[int] = None


# Cart view response
class CartViewResponse(BaseModel):
    """Response from GET /customers/{cust_id}/cart and GET /cart/me."""

    cust_id: int
    customer_name: Optional[str] = None
    status_filter: str = Field(
        "in_cart",
        description="Which status was filtered. 'all' if status filtering was disabled.",
    )
    total_items: int = Field(..., description="Number of distinct cart_items rows returned.")
    total_quantity: int = Field(..., description="Sum of quantity across returned items.")
    estimated_total: Optional[Decimal] = Field(
        None,
        description="Sum of quantity * unit_price_at_add across returned items, when prices are available.",
    )
    items: list[CartLine]


# Single update response
class CartLineResponse(BaseModel):
    """Response from POST/PATCH/DELETE /cart endpoints. Returns the affected line."""

    item: CartLine
    message: Optional[str] = None


# Checkout response
class CartCheckoutResponse(BaseModel):
    """Response from POST /cart/{cart_item_id}/checkout."""

    cart_item_id: int
    purchase_id: int = Field(..., description="ID of the new row in purchase_history.")
    cust_id: int
    item_id: int
    quantity: int
    unit_price: Decimal
    line_total: Decimal
    sold_at: datetime
    sold_by_seller_id: Optional[int] = None
    message: str
