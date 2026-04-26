from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, Numeric
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from backend.models.base import Base


class PurchaseHistory(Base):
    __tablename__ = "purchase_history"

    purchase_id:        Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cust_id:            Mapped[int] = mapped_column(
        BigInteger, ForeignKey("customers.cust_id"), nullable=False
    )
    item_id:            Mapped[int] = mapped_column(
        BigInteger, ForeignKey("products.item_id"), nullable=False
    )
    quantity:           Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price:         Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    sold_by_seller_id:  Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.user_id")
    )
    sold_at:            Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    cart_item_id:       Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("cart_items.cart_item_id")
    )

    def __repr__(self) -> str:
        return (
            f"<PurchaseHistory(purchase_id={self.purchase_id}, "
            f"cust_id={self.cust_id}, item_id={self.item_id}, qty={self.quantity})>"
        )
