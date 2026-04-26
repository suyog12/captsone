from __future__ import annotations

from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.config import settings


# Async engine. echo=False because we don't want every SQL query in the logs;
# echo=True is useful when debugging a specific issue.
engine = create_async_engine(
    settings.async_database_url,
    echo=False,
    pool_pre_ping=True,           # check connections are alive before using them
    pool_size=5,                  # 5 concurrent DB connections is plenty for capstone
    max_overflow=10,              # can grow to 15 under load
    # Set the search_path on every new connection so we don't need to qualify
    # every table reference. The ORM models also have schema='recdash' set on
    # the metadata so this is belt and suspenders.
    connect_args={
        "server_settings": {
            "search_path": f"{settings.postgres_schema},public",
        },
    },
)


# Session factory. expire_on_commit=False is important for FastAPI because
# objects stay usable after commit (default behaviour would invalidate them).
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency that yields an async DB session.

    The session is automatically closed when the request finishes,
    rolled back on exceptions, and committed only if the route handler
    explicitly calls db.commit().
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
