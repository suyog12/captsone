from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from backend.models.base import Base


class CustomerAssignmentHistory(Base):
    __tablename__ = "customer_assignment_history"

    history_id:         Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    cust_id:            Mapped[int] = mapped_column(
        BigInteger, ForeignKey("customers.cust_id"), nullable=False
    )
    previous_seller_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.user_id"), nullable=True
    )
    new_seller_id:      Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.user_id"), nullable=True
    )
    changed_by_user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.user_id"), nullable=False
    )
    change_reason:      Mapped[str] = mapped_column(String(50), nullable=False)
    notes:              Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    changed_at:         Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<CustomerAssignmentHistory(cust_id={self.cust_id}, "
            f"prev={self.previous_seller_id}, new={self.new_seller_id}, "
            f"reason={self.change_reason!r})>"
        )
