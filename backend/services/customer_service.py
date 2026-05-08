from __future__ import annotations

from typing import Optional

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models import Customer, User


DEFAULT_SEARCH_LIMIT = 25


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
    Search customers by id, market code, specialty code, segment,
    or business name substring.

    If seller_id is given, the result is filtered to customers assigned
    to that seller (used by the seller view). If seller_id is None, all
    customers are searchable (admin view).
    """
    limit = max(int(limit or DEFAULT_SEARCH_LIMIT), 1)
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
        upper_q = q.upper()
        like_q = f"%{q}%"
        stmt = stmt.where(
            or_(
                Customer.market_code == upper_q,
                Customer.specialty_code == upper_q,
                Customer.segment.ilike(like_q),
                Customer.customer_name.ilike(like_q),
            )
        )

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
    account_status: Optional[str] = None,
    limit: int = DEFAULT_SEARCH_LIMIT,
    offset: int = 0,
) -> list[Customer]:
    """
    Filtered customer browsing.
    All filters combine with AND.
    account_status valid values:
      - None or 'all' (default, no account filter)
      - 'users'    - only customers WITH a dashboard login
      - 'no_users' - only customers WITHOUT a dashboard login
    """
    limit = max(int(limit or DEFAULT_SEARCH_LIMIT), 1)
    offset = max(int(offset or 0), 0)

    stmt = _apply_filters(
        select(Customer),
        segment=segment,
        status=status,
        archetype=archetype,
        market_code=market_code,
        specialty_code=specialty_code,
        seller_id=seller_id,
        account_status=account_status,
    )
    stmt = stmt.order_by(Customer.cust_id).limit(limit).offset(offset)

    result = await db.execute(stmt)
    return list(result.scalars().all())


async def count_by_filters(
    db: AsyncSession,
    *,
    segment: Optional[str] = None,
    status: Optional[str] = None,
    archetype: Optional[str] = None,
    market_code: Optional[str] = None,
    specialty_code: Optional[str] = None,
    seller_id: Optional[int] = None,
    account_status: Optional[str] = None,
) -> int:
    """
    Total count of customers matching the same filter set used by
    search_by_filters. Drives the pagination footer ('Page 3 of 47').
    """
    stmt = _apply_filters(
        select(func.count(Customer.cust_id)),
        segment=segment,
        status=status,
        archetype=archetype,
        market_code=market_code,
        specialty_code=specialty_code,
        seller_id=seller_id,
        account_status=account_status,
    )
    result = await db.execute(stmt)
    return int(result.scalar_one() or 0)


async def get_user_account_map(
    db: AsyncSession,
    cust_ids: list[int],
) -> dict[int, bool]:
    """
    For a list of cust_ids, return a dict mapping cust_id -> True if
    a user account exists, False otherwise. Used to populate the
    has_user_account flag on CustomerSearchResult rows.

    Returns empty dict on empty input. Called once per page (not once
    per row) so the cost is one extra round-trip per page.
    """
    if not cust_ids:
        return {}

    stmt = select(User.cust_id).where(
        User.cust_id.is_not(None),
        User.cust_id.in_(cust_ids),
    )
    result = await db.execute(stmt)
    linked = {int(r) for r in result.scalars().all() if r is not None}

    return {int(cid): (int(cid) in linked) for cid in cust_ids}


# Internal: shared filter builder
def _apply_filters(
    stmt,
    *,
    segment: Optional[str],
    status: Optional[str],
    archetype: Optional[str],
    market_code: Optional[str],
    specialty_code: Optional[str],
    seller_id: Optional[int],
    account_status: Optional[str],
):
    """Apply the same WHERE clauses to either a select() of Customer
    rows or a select(func.count(...)). Keeps search and count
    perfectly in sync."""
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

    if account_status in ("users", "no_users"):
        sub = select(User.cust_id).where(User.cust_id.is_not(None))
        if account_status == "users":
            stmt = stmt.where(Customer.cust_id.in_(sub))
        else:
            stmt = stmt.where(Customer.cust_id.not_in(sub))

    return stmt
