from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Boolean, CheckConstraint, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from backend.models.base import Base


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint(
            "role IN ('admin', 'seller', 'customer')",
            name="users_role_check",
        ),
    )

    user_id:        Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username:       Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    password_hash:  Mapped[str] = mapped_column(String(255), nullable=False)
    role:           Mapped[str] = mapped_column(String(20), nullable=False)
    full_name:      Mapped[Optional[str]] = mapped_column(String(200))
    email:          Mapped[Optional[str]] = mapped_column(String(200))
    cust_id:        Mapped[Optional[int]] = mapped_column(BigInteger)
    is_active:      Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at:     Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    last_login_at:  Mapped[Optional[datetime]] = mapped_column(DateTime)

    def __repr__(self) -> str:
        return (
            f"<User(user_id={self.user_id}, username={self.username!r}, "
            f"role={self.role!r}, is_active={self.is_active})>"
        )
