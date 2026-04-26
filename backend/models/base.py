from __future__ import annotations

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

from backend.config import settings


class Base(DeclarativeBase):
    """Base class for all ORM models. Bound to the recdash schema."""

    metadata = MetaData(schema=settings.postgres_schema)
