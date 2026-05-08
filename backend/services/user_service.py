from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import desc, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.security import hash_password, verify_password
from backend.models import Customer, User
from backend.services import assignment_service


# Create
async def create_admin(
    db: AsyncSession,
    *,
    username: str,
    password: str,
    full_name: Optional[str] = None,
    email: Optional[str] = None,
) -> User:
    return await _create_user(
        db,
        username=username,
        password=password,
        role="admin",
        full_name=full_name,
        email=email,
        cust_id=None,
    )


async def create_seller(
    db: AsyncSession,
    *,
    username: str,
    password: str,
    full_name: Optional[str] = None,
    email: Optional[str] = None,
) -> User:
    return await _create_user(
        db,
        username=username,
        password=password,
        role="seller",
        full_name=full_name,
        email=email,
        cust_id=None,
    )


async def create_customer(
    db: AsyncSession,
    *,
    username: str,
    password: str,
    full_name: Optional[str] = None,
    email: Optional[str] = None,
    customer_business_name: Optional[str] = None,
    market_code: Optional[str] = None,
    size_tier: Optional[str] = None,
    specialty_code: Optional[str] = None,
    assigned_seller_id: Optional[int] = None,
) -> User:
    """Create a customer login AND a recdash.customers record.

    If assigned_seller_id is provided, the customer is automatically
    assigned to that seller (used when a seller creates a customer through
    the API, the seller's own user_id is passed in here).
    """
    # Build the customer row first to get a fresh cust_id
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

    # Now create the user with cust_id pointing at the new customer row
    user = await _create_user(
        db,
        username=username,
        password=password,
        role="customer",
        full_name=full_name,
        email=email,
        cust_id=next_cust_id,
        commit=False,
    )

    # If auto-assigned, write a history row
    if assigned_seller_id is not None:
        await assignment_service._record_history(
            db,
            cust_id=next_cust_id,
            previous_seller_id=None,
            new_seller_id=assigned_seller_id,
            changed_by_user_id=assigned_seller_id,
            change_reason="auto_assign_on_create",
            notes=f"Auto-assigned at customer creation by user_id={assigned_seller_id}",
        )

    await db.commit()
    await db.refresh(user)
    return user


async def _create_user(
    db: AsyncSession,
    *,
    username: str,
    password: str,
    role: str,
    full_name: Optional[str] = None,
    email: Optional[str] = None,
    cust_id: Optional[int] = None,
    commit: bool = True,
) -> User:
    user = User(
        username=username,
        password_hash=hash_password(password),
        role=role,
        full_name=full_name,
        email=email,
        cust_id=cust_id,
        is_active=True,
    )
    db.add(user)
    try:
        if commit:
            await db.commit()
            await db.refresh(user)
        else:
            await db.flush()
    except IntegrityError as e:
        await db.rollback()
        raise ValueError(f"Username {username!r} is already taken.") from e
    return user


async def _next_cust_id(db: AsyncSession) -> int:
    """Find max(cust_id) from customers and return +1."""
    result = await db.execute(select(func.max(Customer.cust_id)))
    max_id = result.scalar_one()
    return int(max_id or 0) + 1


# Read
async def get_by_id(db: AsyncSession, user_id: int) -> Optional[User]:
    return await db.get(User, user_id)


async def get_by_username(db: AsyncSession, username: str) -> Optional[User]:
    result = await db.execute(select(User).where(User.username == username))
    return result.scalar_one_or_none()


async def list_users(
    db: AsyncSession,
    *,
    role: Optional[str] = None,
    is_active: Optional[bool] = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[User], int]:
    base = select(User)
    if role is not None:
        base = base.where(User.role == role)
    if is_active is not None:
        base = base.where(User.is_active == is_active)

    total = (
        await db.execute(
            select(func.count()).select_from(base.subquery())
        )
    ).scalar_one()

    result = await db.execute(
        base.order_by(User.user_id).limit(limit).offset(offset)
    )
    return list(result.scalars().all()), int(total)


# Password change
async def change_password(
    db: AsyncSession,
    user: User,
    *,
    current_password: str,
    new_password: str,
) -> bool:
    if not verify_password(current_password, user.password_hash):
        return False
    user.password_hash = hash_password(new_password)
    await db.commit()
    return True


# Soft delete (deactivate) / reactivate
async def deactivate(
    db: AsyncSession,
    user_id: int,
    *,
    changed_by: User,
) -> tuple[Optional[User], int]:
    """
    Soft-delete a user (set is_active=False).

    If the user is a seller, also auto-unassigns all their customers
    (sets customers.assigned_seller_id to NULL and writes history).

    Returns (user, customers_unassigned_count). For non-sellers the count
    is always 0.
    """
    user = await db.get(User, user_id)
    if user is None:
        return None, 0

    customers_unassigned = 0

    if user.role == "seller" and user.is_active:
        customers_unassigned = await assignment_service.auto_unassign_for_seller_deactivation(
            db,
            deactivated_seller_id=user_id,
            changed_by=changed_by,
        )

    user.is_active = False
    await db.commit()
    await db.refresh(user)
    return user, customers_unassigned


async def reactivate(db: AsyncSession, user_id: int) -> Optional[User]:
    user = await db.get(User, user_id)
    if user is None:
        return None
    user.is_active = True
    await db.commit()
    await db.refresh(user)
    return user

# Create customer record only - no login (used by POST /customers/record)
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

    Used when a seller adds a customer via the dashboard (auto-assigned to
    that seller) or when an admin creates a record without a login.
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
        status="cold_start",
        archetype="other",
        assigned_seller_id=assigned_seller_id,
        assigned_at=datetime.utcnow() if assigned_seller_id else None,
        created_at=datetime.utcnow(),
    )
    db.add(customer)
    await db.flush()

    if assigned_seller_id is not None:
        await assignment_service._record_history(
            db,
            cust_id=next_cust_id,
            previous_seller_id=None,
            new_seller_id=assigned_seller_id,
            change_reason="customer_created",
            changed_by_user_id=actor_user_id,
            notes=None,
        )

    await db.commit()
    await db.refresh(customer)
    return customer


# BEGIN APPENDED 20260501-043628
"""
Append this function to the END of backend/services/user_service.py.

It reuses _create_user() and the Customer/User imports already present
in that file. Don't duplicate any imports.

Used by the new POST /users/customers/{cust_id}/login endpoint.
"""

# Append below this line into user_service.py

async def attach_login_to_customer(
    db: AsyncSession,
    *,
    cust_id: int,
    username: str,
    password: str,
    full_name: Optional[str] = None,
    email: Optional[str] = None,
) -> User:
    """Attach a dashboard login (User row) to an EXISTING customer record.

    Used by POST /users/customers/{cust_id}/login. Differs from
    create_customer() in that it does NOT create a new customer record -
    the customer must already exist and must not already have a login.

    Raises:
        ValueError("Customer {cust_id} not found.")
        ValueError("Customer {cust_id} already has a dashboard login.")
        ValueError("Username {username!r} is already taken.")
    """
    # Confirm the customer exists
    customer = await db.get(Customer, cust_id)
    if customer is None:
        raise ValueError(f"Customer {cust_id} not found.")

    # Confirm the customer does not already have a login
    stmt = select(User).where(User.cust_id == cust_id).limit(1)
    existing = (await db.execute(stmt)).scalar_one_or_none()
    if existing is not None:
        raise ValueError(f"Customer {cust_id} already has a dashboard login.")

    # Reuse the standard user creation path. _create_user handles password
    # hashing and the username-already-taken case.
    user = await _create_user(
        db,
        username=username,
        password=password,
        role="customer",
        full_name=full_name,
        email=email,
        cust_id=cust_id,
        commit=True,
    )
    return user
