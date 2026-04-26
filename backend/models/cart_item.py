from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from backend.models.base import Base


_VALID_SOURCES = (
    "manual",
    "recommendation_peer_gap",
    "recommendation_lapsed",
    "recommendation_replenishment",
    "recommendation_cart_complement",
    "recommendation_pb_upgrade",
    "recommendation_medline_conversion",
    "recommendation_item_similarity",
    "recommendation_popularity",
)

_VALID_STATUSES = ("in_cart", "sold", "not_sold")
_VALID_ROLES = ("seller", "customer")


class CartItem(Base):
    __tablename__ = "cart_items"
    __table_args__ = (
        CheckConstraint("quantity > 0", name="cart_items_qty_positive"),
        CheckConstraint(
            f"added_by_role IN {_VALID_ROLES!r}".replace("'", "'"),
            name="cart_items_role_check",
        ),
        CheckConstraint(
            f"source IN {_VALID_SOURCES!r}",
            name="cart_items_source_check",
        ),
        CheckConstraint(
            f"status IN {_VALID_STATUSES!r}",
            name="cart_items_status_check",
        ),
    )

    cart_item_id:        Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cust_id:             Mapped[int] = mapped_column(
        BigInteger, ForeignKey("customers.cust_id"), nullable=False
    )
    item_id:             Mapped[int] = mapped_column(
        BigInteger, ForeignKey("products.item_id"), nullable=False
    )
    quantity:            Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price_at_add:   Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    added_by_user_id:    Mapped[int] = mapped_column(
        Integer, ForeignKey("users.user_id"), nullable=False
    )
    added_by_role:       Mapped[str] = mapped_column(String(20), nullable=False)
    source:              Mapped[str] = mapped_column(String(40), default="manual")
    status:              Mapped[str] = mapped_column(String(20), default="in_cart")
    added_at:            Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    resolved_at:         Mapped[Optional[datetime]] = mapped_column(DateTime)
    resolved_by_user_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.user_id")
    )

    def __repr__(self) -> str:
        return (
            f"<CartItem(cart_item_id={self.cart_item_id}, "
            f"cust_id={self.cust_id}, item_id={self.item_id}, "
            f"qty={self.quantity}, status={self.status!r})>"
        )
