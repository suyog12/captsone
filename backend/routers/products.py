"""
Product catalog endpoints.

  GET /products              - Browse the catalog with filters and pagination
  GET /products/filters      - Available filter options (families, categories, suppliers)

All authenticated roles can access.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.dependencies import get_current_user
from backend.db.database import get_db
from backend.models import User
from backend.schemas.product import (
    ProductBrowseResponse,
    ProductFiltersResponse,
    ProductRow,
)
from backend.services import product_service


router = APIRouter(prefix="/products", tags=["products"])


@router.get(
    "",
    response_model=ProductBrowseResponse,
    summary="Browse the product catalog with filters and pagination",
)
async def browse_products(
    q: Optional[str] = Query(None, description="Search across description, family, category, supplier"),
    family: Optional[str] = Query(None, description="Exact match on family"),
    category: Optional[str] = Query(None, description="Exact match on category"),
    supplier: Optional[str] = Query(None, description="Exact match on supplier"),
    is_private_brand: Optional[bool] = Query(None, description="Filter for McKesson private-brand items"),
    in_stock_only: bool = Query(False, description="Only return items with units_in_stock > 0"),
    limit: int = Query(24, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> ProductBrowseResponse:
    rows, total = await product_service.browse(
        db,
        q=q,
        family=family,
        category=category,
        supplier=supplier,
        is_private_brand=is_private_brand,
        in_stock_only=in_stock_only,
        limit=limit,
        offset=offset,
    )
    return ProductBrowseResponse(
        items=[ProductRow(**r) for r in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/filters",
    response_model=ProductFiltersResponse,
    summary="Available filter options for the catalog browse UI",
)
async def product_filters(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> ProductFiltersResponse:
    options = await product_service.get_filter_options(db)
    return ProductFiltersResponse(**options)