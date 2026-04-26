from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.db.database import get_db
from backend.db.parquet_store import parquet_health_check


router = APIRouter(prefix="/health", tags=["health"])


@router.get("", summary="App is alive")
async def health() -> dict[str, Any]:
    """The simplest health check. Returns ok if the FastAPI app is running."""
    return {
        "ok": True,
        "service": settings.api_title,
        "version": settings.api_version,
    }


@router.get("/db", summary="Postgres is reachable")
async def health_db(db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    """
    Verifies the async SQLAlchemy connection works.

    Runs three checks: SELECT 1 (round-trip), confirms the search_path is
    pointed at recdash, and counts the tables in the schema.
    """
    try:
        # 1. round trip
        one = (await db.execute(text("SELECT 1"))).scalar_one()

        # 2. search path
        search_path = (await db.execute(text("SHOW search_path"))).scalar_one()

        # 3. table count in recdash schema
        n_tables = (await db.execute(text(
            f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = '{settings.postgres_schema}'"
        ))).scalar_one()

        return {
            "ok": True,
            "postgres_round_trip": int(one),
            "search_path": search_path,
            "schema": settings.postgres_schema,
            "tables_in_schema": int(n_tables),
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connectivity failed: {exc}",
        )


@router.get("/parquet", summary="DuckDB can read parquet files")
async def health_parquet() -> dict[str, Any]:
    """
    Verifies DuckDB can open the recommendations parquet file.

    Returns row count and the column names of the first row.
    """
    result = parquet_health_check()
    if not result.get("ok"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=result,
        )
    return result
