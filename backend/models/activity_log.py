from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from backend.models.base import Base


class ActivityLog(Base):
    __tablename__ = "activity_log"

    log_id:       Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id:      Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.user_id")
    )
    action:       Mapped[str] = mapped_column(String(50), nullable=False)
    entity_type:  Mapped[Optional[str]] = mapped_column(String(30))
    entity_id:    Mapped[Optional[int]] = mapped_column(BigInteger)
    details:      Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB)
    created_at:   Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    def __repr__(self) -> str:
        return (
            f"<ActivityLog(log_id={self.log_id}, "
            f"user_id={self.user_id}, action={self.action!r})>"
        )
