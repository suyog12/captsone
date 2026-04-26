from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from backend.models.base import Base


class Customer(Base):
    __tablename__ = "customers"

    cust_id:             Mapped[int] = mapped_column(BigInteger, primary_key=True)
    customer_name:       Mapped[Optional[str]] = mapped_column(String(200))
    specialty_code:      Mapped[Optional[str]] = mapped_column(String(20))
    market_code:         Mapped[Optional[str]] = mapped_column(String(20))
    segment:             Mapped[Optional[str]] = mapped_column(String(50))
    supplier_profile:    Mapped[Optional[str]] = mapped_column(String(50))
    assigned_seller_id:  Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.user_id")
    )
    assigned_at:         Mapped[Optional[datetime]] = mapped_column(DateTime)
    created_at:          Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    def __repr__(self) -> str:
        return (
            f"<Customer(cust_id={self.cust_id}, segment={self.segment!r}, "
            f"market={self.market_code!r})>"
        )
