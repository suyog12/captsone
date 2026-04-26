from __future__ import annotations

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models import Product, PurchaseHistory


async def list_for_customer(
    db: AsyncSession,
    cust_id: int,
    *,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """
    Return (rows, total_count) where each row is a dict with purchase fields
    plus product description / family / category.
    """
    # Count
    total = (
        await db.execute(
            select(func.count())
            .select_from(PurchaseHistory)
            .where(PurchaseHistory.cust_id == cust_id)
        )
    ).scalar_one()

    # Page
    stmt = (
        select(
            PurchaseHistory.purchase_id,
            PurchaseHistory.item_id,
            PurchaseHistory.quantity,
            PurchaseHistory.unit_price,
            PurchaseHistory.sold_at,
            Product.description,
            Product.family,
            Product.category,
        )
        .join(Product, Product.item_id == PurchaseHistory.item_id, isouter=True)
        .where(PurchaseHistory.cust_id == cust_id)
        .order_by(desc(PurchaseHistory.sold_at))
        .limit(limit)
        .offset(offset)
    )
    rows = (await db.execute(stmt)).all()

    out: list[dict] = []
    for r in rows:
        out.append({
            "purchase_id": int(r[0]),
            "item_id": int(r[1]),
            "quantity": int(r[2]),
            "unit_price": r[3],
            "sold_at": r[4],
            "description": r[5],
            "family": r[6],
            "category": r[7],
        })

    return out, int(total)
