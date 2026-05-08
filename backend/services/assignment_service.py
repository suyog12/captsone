from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from backend.models import Customer, CustomerAssignmentHistory, User


# Helpers
async def _get_seller_or_raise(
    db: AsyncSession, seller_id: int
) -> User:
    """Verify a user_id is a real, active seller. Raise ValueError if not."""
    user = await db.get(User, seller_id)
    if user is None:
        raise ValueError(f"User {seller_id} not found.")
    if user.role != "seller":
        raise ValueError(
            f"User {seller_id} ({user.username!r}) is not a seller; role={user.role!r}."
        )
    if not user.is_active:
        raise ValueError(
            f"Seller {seller_id} ({user.username!r}) is deactivated; "
            f"reactivate before assigning customers."
        )
    return user


async def _record_history(
    db: AsyncSession,
    *,
    cust_id: int,
    previous_seller_id: Optional[int],
    new_seller_id: Optional[int],
    changed_by_user_id: int,
    change_reason: str,
    notes: Optional[str] = None,
) -> CustomerAssignmentHistory:
    """Insert one row into customer_assignment_history. Caller commits."""
    row = CustomerAssignmentHistory(
        cust_id=cust_id,
        previous_seller_id=previous_seller_id,
        new_seller_id=new_seller_id,
        changed_by_user_id=changed_by_user_id,
        change_reason=change_reason,
        notes=notes,
    )
    db.add(row)
    await db.flush()  # populate row.history_id
    return row


# Single assignment / reassignment / unassignment (admin)
async def assign_or_reassign(
    db: AsyncSession,
    *,
    cust_id: int,
    new_seller_id: Optional[int],
    changed_by: User,
    notes: Optional[str] = None,
) -> CustomerAssignmentHistory:
    """
    Admin operation: set customers.assigned_seller_id to new_seller_id
    (or NULL to unassign), and write a history row.

    new_seller_id semantics:
        int  -> assign or reassign
        None -> unassign

    Determines change_reason automatically based on previous and new state.
    """
    if changed_by.role != "admin":
        raise PermissionError("Only admins can perform direct assignments.")

    customer = await db.get(Customer, cust_id)
    if customer is None:
        raise ValueError(f"Customer {cust_id} not found.")

    # Validate target seller (if given)
    if new_seller_id is not None:
        await _get_seller_or_raise(db, new_seller_id)

    previous_seller_id = customer.assigned_seller_id

    # No-op short-circuit (still writes history with a skip note)
    if previous_seller_id == new_seller_id:
        raise ValueError(
            "Customer is already in the requested assignment state; no change made."
        )

    # Decide reason
    if previous_seller_id is None and new_seller_id is not None:
        reason = "admin_assign"
    elif previous_seller_id is not None and new_seller_id is None:
        reason = "admin_unassign"
    else:
        reason = "admin_reassign"

    # Apply
    customer.assigned_seller_id = new_seller_id
    customer.assigned_at = datetime.utcnow() if new_seller_id is not None else None

    history = await _record_history(
        db,
        cust_id=cust_id,
        previous_seller_id=previous_seller_id,
        new_seller_id=new_seller_id,
        changed_by_user_id=changed_by.user_id,
        change_reason=reason,
        notes=notes,
    )
    await db.commit()
    await db.refresh(history)
    return history


# Seller self-claim of unassigned customer
async def seller_claim(
    db: AsyncSession,
    *,
    cust_id: int,
    seller: User,
    notes: Optional[str] = None,
) -> CustomerAssignmentHistory:
    """
    Seller operation: claim an unassigned customer for themselves.

    Requirements:
      - seller must have role='seller' and is_active=true
      - customer must exist
      - customer must currently be unassigned (assigned_seller_id IS NULL)
    """
    if seller.role != "seller":
        raise PermissionError("Only sellers can claim customers.")
    if not seller.is_active:
        raise PermissionError("Deactivated sellers cannot claim customers.")

    customer = await db.get(Customer, cust_id)
    if customer is None:
        raise ValueError(f"Customer {cust_id} not found.")

    if customer.assigned_seller_id is not None:
        raise ValueError(
            f"Customer {cust_id} is already assigned to seller "
            f"{customer.assigned_seller_id}. Ask an admin to reassign."
        )

    customer.assigned_seller_id = seller.user_id
    customer.assigned_at = datetime.utcnow()

    history = await _record_history(
        db,
        cust_id=cust_id,
        previous_seller_id=None,
        new_seller_id=seller.user_id,
        changed_by_user_id=seller.user_id,
        change_reason="seller_claim",
        notes=notes,
    )
    await db.commit()
    await db.refresh(history)
    return history


# Bulk admin assignment
async def bulk_assign(
    db: AsyncSession,
    *,
    seller_id: int,
    cust_ids: list[int],
    changed_by: User,
    notes: Optional[str] = None,
) -> tuple[int, int, dict[int, str]]:
    """
    Admin operation: assign every customer in cust_ids to seller_id.

    Returns:
        (assigned_count, skipped_count, skipped_reasons)

    A customer is skipped if:
      - the customer doesn't exist (reason: 'not_found')
      - the customer is already assigned to that exact seller (reason: 'already_assigned')

    Customers already assigned to a DIFFERENT seller are reassigned (counted
    as assigned, with an 'admin_reassign' history reason).
    """
    if changed_by.role != "admin":
        raise PermissionError("Only admins can perform bulk assignments.")

    # Validate target seller once
    await _get_seller_or_raise(db, seller_id)

    assigned = 0
    skipped: dict[int, str] = {}

    for cid in cust_ids:
        customer = await db.get(Customer, cid)
        if customer is None:
            skipped[cid] = "not_found"
            continue
        if customer.assigned_seller_id == seller_id:
            skipped[cid] = "already_assigned"
            continue

        previous = customer.assigned_seller_id
        reason = "admin_assign" if previous is None else "admin_reassign"

        customer.assigned_seller_id = seller_id
        customer.assigned_at = datetime.utcnow()

        await _record_history(
            db,
            cust_id=cid,
            previous_seller_id=previous,
            new_seller_id=seller_id,
            changed_by_user_id=changed_by.user_id,
            change_reason=reason,
            notes=notes,
        )
        assigned += 1

    await db.commit()
    return assigned, len(skipped), skipped


# Auto-unassign on seller deactivation
async def auto_unassign_for_seller_deactivation(
    db: AsyncSession,
    *,
    deactivated_seller_id: int,
    changed_by: User,
) -> int:
    """
    Called when an admin deactivates a seller. All customers currently
    assigned to that seller have their assigned_seller_id cleared, and a
    history row is written for each with reason 'seller_deactivated'.

    Returns the number of customers that were unassigned.

    This function does NOT commit; the caller (the deactivation flow)
    commits as part of the same transaction that marks the user inactive.
    """
    result = await db.execute(
        select(Customer).where(Customer.assigned_seller_id == deactivated_seller_id)
    )
    customers = list(result.scalars().all())

    for customer in customers:
        previous = customer.assigned_seller_id
        customer.assigned_seller_id = None
        customer.assigned_at = None
        await _record_history(
            db,
            cust_id=customer.cust_id,
            previous_seller_id=previous,
            new_seller_id=None,
            changed_by_user_id=changed_by.user_id,
            change_reason="seller_deactivated",
            notes=f"Auto-unassign triggered by deactivation of user_id={deactivated_seller_id}",
        )

    return len(customers)


# Read: list customers for a seller
async def list_for_seller(
    db: AsyncSession,
    *,
    seller_id: int,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[Customer], int]:
    """Return (customers, total_count) currently assigned to seller_id."""
    total = (
        await db.execute(
            select(func.count())
            .select_from(Customer)
            .where(Customer.assigned_seller_id == seller_id)
        )
    ).scalar_one()

    rows = (
        await db.execute(
            select(Customer)
            .where(Customer.assigned_seller_id == seller_id)
            .order_by(desc(Customer.assigned_at), Customer.cust_id)
            .limit(limit)
            .offset(offset)
        )
    ).scalars().all()

    return list(rows), int(total)


# Read: assignment history for a customer
async def list_history(
    db: AsyncSession,
    *,
    cust_id: int,
    limit: int = 100,
) -> tuple[list[dict], int]:
    """
    Return (rows, total_count) for the assignment history of one customer.

    Each row is a dict including the resolved usernames for the
    previous_seller_id, new_seller_id, and changed_by_user_id columns.
    """
    PrevUser = aliased(User)
    NewUser = aliased(User)
    ChangedBy = aliased(User)

    total = (
        await db.execute(
            select(func.count())
            .select_from(CustomerAssignmentHistory)
            .where(CustomerAssignmentHistory.cust_id == cust_id)
        )
    ).scalar_one()

    stmt = (
        select(
            CustomerAssignmentHistory.history_id,
            CustomerAssignmentHistory.cust_id,
            CustomerAssignmentHistory.previous_seller_id,
            PrevUser.username.label("previous_seller_username"),
            CustomerAssignmentHistory.new_seller_id,
            NewUser.username.label("new_seller_username"),
            CustomerAssignmentHistory.changed_by_user_id,
            ChangedBy.username.label("changed_by_username"),
            CustomerAssignmentHistory.change_reason,
            CustomerAssignmentHistory.notes,
            CustomerAssignmentHistory.changed_at,
        )
        .select_from(CustomerAssignmentHistory)
        .join(PrevUser, PrevUser.user_id == CustomerAssignmentHistory.previous_seller_id, isouter=True)
        .join(NewUser, NewUser.user_id == CustomerAssignmentHistory.new_seller_id, isouter=True)
        .join(ChangedBy, ChangedBy.user_id == CustomerAssignmentHistory.changed_by_user_id, isouter=True)
        .where(CustomerAssignmentHistory.cust_id == cust_id)
        .order_by(desc(CustomerAssignmentHistory.changed_at))
        .limit(limit)
    )

    rows = (await db.execute(stmt)).all()

    out: list[dict] = []
    for r in rows:
        out.append({
            "history_id": int(r[0]),
            "cust_id": int(r[1]),
            "previous_seller_id": int(r[2]) if r[2] is not None else None,
            "previous_seller_username": r[3],
            "new_seller_id": int(r[4]) if r[4] is not None else None,
            "new_seller_username": r[5],
            "changed_by_user_id": int(r[6]),
            "changed_by_username": r[7],
            "change_reason": r[8],
            "notes": r[9],
            "changed_at": r[10],
        })

    return out, int(total)
