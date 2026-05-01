from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models import Customer
from backend.services import assignment_service


async def _next_cust_id(db: AsyncSession) -> int:
    """Find max(cust_id) from customers and return +1.

    Mirrors the helper in user_service.py so this module is self-contained.
    Both implementations stay in sync because they share the same Customer
    model and Postgres sequence semantics.
    """
    result = await db.execute(select(func.max(Customer.cust_id)))
    max_id = result.scalar_one()
    return int(max_id or 0) + 1


async def create_customer_record_only(
    db: AsyncSession,
    *,
    customer_business_name: str,
    market_code: str,
    size_tier: str,
    specialty_code: Optional[str] = None,
    assigned_seller_id: Optional[int] = None,
    actor_user_id: int,
) -> Customer:
    """Create only a recdash.customers row, no User login.

    Parameters
    ----------
    db : AsyncSession
        Open SQLAlchemy session. The function commits before returning.
    customer_business_name : str
        Display name for the new customer (e.g. "Sunrise Pediatrics").
    market_code : str
        Market vertical code such as PO, AC, IDN, LTC, HC, LC, SC.
    size_tier : str
        Account size tier such as new, small, mid, large, enterprise.
    specialty_code : Optional[str]
        Provider specialty code (FP, IM, PD, ...). May be None.
    assigned_seller_id : Optional[int]
        Seller user_id to assign the customer to. None leaves the customer
        unassigned. When set, an assignment_history row is written with
        the standard auto_assign_on_create reason code.
    actor_user_id : int
        user_id of the user making the call. Recorded as changed_by_user_id
        in the assignment_history row, so the audit trail distinguishes
        "seller created their own customer" (actor == assigned_seller_id)
        from "admin created and assigned to a seller" (actor != seller).

    Returns
    -------
    Customer
        The freshly inserted customer row, refreshed from the database.
    """
    next_cust_id = await _next_cust_id(db)

    segment = None
    if market_code and size_tier:
        segment = f"{market_code}_{size_tier}"

    customer = Customer(
        cust_id=next_cust_id,
        customer_name=customer_business_name,
        specialty_code=specialty_code,
        market_code=market_code,
        segment=segment,
        supplier_profile=None,
        assigned_seller_id=assigned_seller_id,
        assigned_at=datetime.utcnow() if assigned_seller_id is not None else None,
    )
    db.add(customer)
    await db.flush()

    if assigned_seller_id is not None:
        if assigned_seller_id == actor_user_id:
            notes = (
                f"Customer record created (no login) by user_id={actor_user_id}. "
                f"Auto-assigned to seller_id={assigned_seller_id}."
            )
        else:
            notes = (
                f"Customer record created (no login) by user_id={actor_user_id}. "
                f"Assigned to seller_id={assigned_seller_id}."
            )
        await assignment_service._record_history(
            db,
            cust_id=next_cust_id,
            previous_seller_id=None,
            new_seller_id=assigned_seller_id,
            changed_by_user_id=actor_user_id,
            change_reason="auto_assign_on_create",
            notes=notes,
        )

    await db.commit()
    await db.refresh(customer)
    return customer