from __future__ import annotations

from typing import Optional

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models import Customer


DEFAULT_SEARCH_LIMIT = 25
MAX_SEARCH_LIMIT = 500


async def get_by_id(db: AsyncSession, cust_id: int) -> Optional[Customer]:
    """Fetch one customer by cust_id, or None if not found."""
    return await db.get(Customer, cust_id)


async def search(
    db: AsyncSession,
    query: str,
    limit: int = DEFAULT_SEARCH_LIMIT,
    seller_id: Optional[int] = None,
) -> list[Customer]:
    """
    Search customers by id, market code, specialty code, or segment substring.

    If seller_id is given, the result is filtered to customers assigned
    to that seller (used by the seller view). If seller_id is None, all
    customers are searchable (admin view).
    """
    # Clamp limit to a safe upper bound
    limit = min(max(limit, 1), MAX_SEARCH_LIMIT)
    q = (query or "").strip()
    if not q:
        return []

    stmt = select(Customer)

    # Numeric: exact match on cust_id
    if q.isdigit():
        try:
            cid = int(q)
            stmt = stmt.where(Customer.cust_id == cid)
        except ValueError:
            return []
    else:
        # Text: match against market_code, specialty_code, or segment
        upper_q = q.upper()
        like_q = f"%{q}%"
        stmt = stmt.where(
            or_(
                Customer.market_code == upper_q,
                Customer.specialty_code == upper_q,
                Customer.segment.ilike(like_q),
            )
        )

    # Optional scoping for sellers
    if seller_id is not None:
        stmt = stmt.where(Customer.assigned_seller_id == seller_id)

    stmt = stmt.limit(limit)

    result = await db.execute(stmt)
    return list(result.scalars().all())


async def search_by_filters(
    db: AsyncSession,
    *,
    segment: Optional[str] = None,
    status: Optional[str] = None,
    archetype: Optional[str] = None,
    market_code: Optional[str] = None,
    specialty_code: Optional[str] = None,
    seller_id: Optional[int] = None,
    limit: int = DEFAULT_SEARCH_LIMIT,
    offset: int = 0,
) -> list[Customer]:
    """
    Filtered customer browsing for the dashboard.

    All filters are optional and combined with AND. Useful for queries like:
      - all churned customers (status='churned_warm')
      - all surgery centers (archetype='surgery_center')
      - declining LTC customers (status='declining_warm', segment='LTC_large')

    For sellers, results are auto-scoped to their assigned customers via
    seller_id parameter.
    """
    limit = min(max(limit, 1), MAX_SEARCH_LIMIT)

    stmt = select(Customer)

    if status is not None:
        stmt = stmt.where(Customer.status == status)
    if archetype is not None:
        stmt = stmt.where(Customer.archetype == archetype)
    if segment is not None:
        stmt = stmt.where(Customer.segment == segment)
    if market_code is not None:
        stmt = stmt.where(Customer.market_code == market_code.upper())
    if specialty_code is not None:
        stmt = stmt.where(Customer.specialty_code == specialty_code.upper())
    if seller_id is not None:
        stmt = stmt.where(Customer.assigned_seller_id == seller_id)

    stmt = stmt.order_by(Customer.cust_id).limit(limit).offset(offset)

    result = await db.execute(stmt)
    return list(result.scalars().all())
