from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import BigInteger, Boolean, DateTime, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from backend.models.base import Base


class Product(Base):
    __tablename__ = "products"

    item_id:           Mapped[int] = mapped_column(BigInteger, primary_key=True)
    description:       Mapped[Optional[str]] = mapped_column(String(500))
    family:            Mapped[Optional[str]] = mapped_column(String(200))
    category:          Mapped[Optional[str]] = mapped_column(String(200))
    is_private_brand:  Mapped[bool] = mapped_column(Boolean, default=False)
    unit_price:        Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    supplier:          Mapped[Optional[str]] = mapped_column(String(200))
    pack_size:         Mapped[Optional[str]] = mapped_column(String(100))
    image_url:         Mapped[Optional[str]] = mapped_column(String(500))
    created_at:        Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    def __repr__(self) -> str:
        return (
            f"<Product(item_id={self.item_id}, "
            f"description={self.description[:30] if self.description else ''!r}, "
            f"is_private_brand={self.is_private_brand})>"
        )
