"""
Product catalog service.

Joins recdash.products with recdash.inventory so each row carries the
current stock count along with description, family, category, supplier,
private-brand flag, and price. Used by the catalog browse endpoint.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import or_, select, func
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models import Product, Inventory


DEFAULT_LIMIT = 24
MAX_LIMIT = 100


async def browse(
    db: AsyncSession,
    *,
    q: Optional[str] = None,
    family: Optional[str] = None,
    category: Optional[str] = None,
    supplier: Optional[str] = None,
    is_private_brand: Optional[bool] = None,
    in_stock_only: bool = False,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """
    Browse the catalog with filters.

    Returns (rows, total_count).
    """
    limit = min(max(limit, 1), MAX_LIMIT)
    offset = max(offset, 0)

    base = (
        select(
            Product.item_id,
            Product.description,
            Product.family,
            Product.category,
            Product.supplier,
            Product.is_private_brand,
            Product.unit_price,
            Product.pack_size,
            Product.image_url,
            func.coalesce(Inventory.units_available, 0).label("units_in_stock"),
        )
        .select_from(Product)
        .outerjoin(Inventory, Inventory.item_id == Product.item_id)
    )

    if q:
        q_clean = q.strip()
        if q_clean.isdigit():
            base = base.where(Product.item_id == int(q_clean))
        else:
            pattern = f"%{q_clean}%"
            base = base.where(
                or_(
                    Product.description.ilike(pattern),
                    Product.family.ilike(pattern),
                    Product.category.ilike(pattern),
                    Product.supplier.ilike(pattern),
                )
            )

    if family:
        base = base.where(Product.family == family)
    if category:
        base = base.where(Product.category == category)
    if supplier:
        base = base.where(Product.supplier == supplier)
    if is_private_brand is not None:
        base = base.where(Product.is_private_brand == is_private_brand)
    if in_stock_only:
        base = base.where(func.coalesce(Inventory.units_available, 0) > 0)

    count_stmt = select(func.count()).select_from(base.subquery())
    total_result = await db.execute(count_stmt)
    total = int(total_result.scalar() or 0)

    page_stmt = base.order_by(Product.description).limit(limit).offset(offset)
    rows_result = await db.execute(page_stmt)
    rows = []
    for row in rows_result.all():
        rows.append({
            "item_id": row.item_id,
            "description": row.description,
            "family": row.family,
            "category": row.category,
            "supplier": row.supplier,
            "is_private_brand": bool(row.is_private_brand) if row.is_private_brand is not None else False,
            "unit_price": float(row.unit_price) if row.unit_price is not None else None,
            "pack_size": row.pack_size,
            "image_url": row.image_url,
            "units_in_stock": int(row.units_in_stock or 0),
        })

    return rows, total


async def get_filter_options(db: AsyncSession) -> dict:
    """
    Distinct families, categories, and suppliers for the filter dropdowns.
    """
    families_stmt = select(Product.family).where(Product.family.is_not(None)).distinct().order_by(Product.family)
    categories_stmt = select(Product.category).where(Product.category.is_not(None)).distinct().order_by(Product.category)
    suppliers_stmt = select(Product.supplier).where(Product.supplier.is_not(None)).distinct().order_by(Product.supplier)

    families = [r[0] for r in (await db.execute(families_stmt)).all() if r[0]]
    categories = [r[0] for r in (await db.execute(categories_stmt)).all() if r[0]]
    suppliers = [r[0] for r in (await db.execute(suppliers_stmt)).all() if r[0]]

    return {
        "families": families,
        "categories": categories,
        "suppliers": suppliers,
    }