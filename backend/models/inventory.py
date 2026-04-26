from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, Integer
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from backend.models.base import Base


class Inventory(Base):
    __tablename__ = "inventory"
    __table_args__ = (
        CheckConstraint("units_available >= 0", name="inventory_units_nonneg"),
    )

    item_id:          Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("products.item_id", ondelete="CASCADE"),
        primary_key=True,
    )
    units_available:  Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_updated:     Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    last_updated_by:  Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.user_id")
    )

    def __repr__(self) -> str:
        return (
            f"<Inventory(item_id={self.item_id}, "
            f"units_available={self.units_available})>"
        )
