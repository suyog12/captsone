from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from backend.models.base import Base


class RecommendationEvent(Base):
    __tablename__ = "recommendation_events"
    __table_args__ = (
        CheckConstraint(
            "outcome IN ('pending', 'purchased', 'rejected')",
            name="rec_events_outcome_check",
        ),
    )

    event_id:          Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cust_id:           Mapped[int] = mapped_column(BigInteger, nullable=False)
    item_id:           Mapped[int] = mapped_column(BigInteger, nullable=False)
    signal:            Mapped[Optional[str]] = mapped_column(String(40))
    rank:              Mapped[Optional[int]] = mapped_column(Integer)
    shown_to_user_id:  Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.user_id")
    )
    shown_at:          Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    outcome:           Mapped[str] = mapped_column(String(20), default="pending")
    resolved_at:       Mapped[Optional[datetime]] = mapped_column(DateTime)

    def __repr__(self) -> str:
        return (
            f"<RecommendationEvent(event_id={self.event_id}, "
            f"cust_id={self.cust_id}, item_id={self.item_id}, "
            f"signal={self.signal!r}, outcome={self.outcome!r})>"
        )
