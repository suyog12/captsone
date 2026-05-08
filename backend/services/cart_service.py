from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models import (
    CartItem,
    Customer,
    Inventory,
    Product,
    PurchaseHistory,
    User,
)


# Add to cart
async def add_item(
    db: AsyncSession,
    *,
    cust_id: int,
    item_id: int,
    quantity: int,
    source: str,
    user: User,
) -> CartItem:
    """
    Add an item to the customer's cart. If the same item is already in_cart
    for this customer, bump its quantity instead of creating a duplicate row.

    Caller is responsible for verifying that `user` is allowed to act on
    this customer's cart.
    """
    # Verify the customer exists
    customer = await db.get(Customer, cust_id)
    if customer is None:
        raise ValueError(f"Customer {cust_id} not found.")

    # Verify the product exists and capture current price
    product = await db.get(Product, item_id)
    if product is None:
        raise ValueError(f"Product {item_id} not found.")

    # Determine added_by_role from the user's role
    if user.role == "seller":
        added_by_role = "seller"
    elif user.role == "customer":
        added_by_role = "customer"
    elif user.role == "admin":
        added_by_role = "seller"
    else:
        raise ValueError(f"Unrecognised user role: {user.role!r}")

    # Look for an existing in_cart row for the same (cust_id, item_id)
    existing_stmt = (
        select(CartItem)
        .where(CartItem.cust_id == cust_id)
        .where(CartItem.item_id == item_id)
        .where(CartItem.status == "in_cart")
    )
    existing = (await db.execute(existing_stmt)).scalar_one_or_none()

    if existing is not None:
        # Bump quantity on the existing row
        existing.quantity = int(existing.quantity) + int(quantity)
        await db.commit()
        await db.refresh(existing)
        return existing

    # Otherwise, create a new row
    cart_item = CartItem(
        cust_id=cust_id,
        item_id=item_id,
        quantity=int(quantity),
        unit_price_at_add=product.unit_price,
        added_by_user_id=user.user_id,
        added_by_role=added_by_role,
        source=source,
        status="in_cart",
    )
    db.add(cart_item)
    await db.commit()
    await db.refresh(cart_item)
    return cart_item


# List cart for a customer
async def list_for_customer(
    db: AsyncSession,
    *,
    cust_id: int,
    status_filter: Optional[str] = "in_cart",
    limit: int = 200,
) -> tuple[list[dict], dict]:
    """
    Return (rows, totals) for the customer's cart.

    status_filter:
        'in_cart' (default) -> only active cart lines
        'sold'              -> only sold lines
        'not_sold'          -> only abandoned lines
        None or 'all'       -> everything

    rows is a list of dicts with all CartItem fields plus product
    description, family, category, the actor's username, and current
    inventory level.

    totals is a dict with total_items, total_quantity, estimated_total.
    """
    stmt = (
        select(
            CartItem.cart_item_id,
            CartItem.cust_id,
            CartItem.item_id,
            CartItem.quantity,
            CartItem.unit_price_at_add,
            CartItem.added_by_user_id,
            User.username,
            CartItem.added_by_role,
            CartItem.source,
            CartItem.status,
            CartItem.added_at,
            CartItem.resolved_at,
            CartItem.resolved_by_user_id,
            Product.description,
            Product.family,
            Product.category,
            Inventory.units_available,
        )
        .select_from(CartItem)
        .join(Product, Product.item_id == CartItem.item_id, isouter=True)
        .join(Inventory, Inventory.item_id == CartItem.item_id, isouter=True)
        .join(User, User.user_id == CartItem.added_by_user_id, isouter=True)
        .where(CartItem.cust_id == cust_id)
        .order_by(desc(CartItem.added_at))
        .limit(limit)
    )

    if status_filter and status_filter != "all":
        stmt = stmt.where(CartItem.status == status_filter)

    raw_rows = (await db.execute(stmt)).all()

    rows: list[dict] = []
    total_items = 0
    total_quantity = 0
    estimated_total: Decimal = Decimal("0")
    have_any_price = False

    for r in raw_rows:
        qty = int(r[3])
        price = r[4]  # Decimal or None
        line_total: Optional[Decimal] = None
        if price is not None:
            line_total = Decimal(price) * qty
            estimated_total += line_total
            have_any_price = True

        rows.append({
            "cart_item_id": int(r[0]),
            "cust_id": int(r[1]),
            "item_id": int(r[2]),
            "quantity": qty,
            "unit_price_at_add": price,
            "line_total": line_total,
            "added_by_user_id": int(r[5]),
            "added_by_username": r[6],
            "added_by_role": r[7],
            "source": r[8],
            "status": r[9],
            "added_at": r[10],
            "resolved_at": r[11],
            "resolved_by_user_id": int(r[12]) if r[12] is not None else None,
            "description": r[13],
            "family": r[14],
            "category": r[15],
            "units_in_stock": int(r[16]) if r[16] is not None else None,
        })

        total_items += 1
        total_quantity += qty

    totals = {
        "total_items": total_items,
        "total_quantity": total_quantity,
        "estimated_total": estimated_total if have_any_price else None,
    }
    return rows, totals


# Get a single cart_item by id (with hydration)
async def get_hydrated(
    db: AsyncSession, cart_item_id: int
) -> Optional[dict]:
    """Return one cart line as a hydrated dict, or None if not found."""
    stmt = (
        select(
            CartItem.cart_item_id,
            CartItem.cust_id,
            CartItem.item_id,
            CartItem.quantity,
            CartItem.unit_price_at_add,
            CartItem.added_by_user_id,
            User.username,
            CartItem.added_by_role,
            CartItem.source,
            CartItem.status,
            CartItem.added_at,
            CartItem.resolved_at,
            CartItem.resolved_by_user_id,
            Product.description,
            Product.family,
            Product.category,
            Inventory.units_available,
        )
        .select_from(CartItem)
        .join(Product, Product.item_id == CartItem.item_id, isouter=True)
        .join(Inventory, Inventory.item_id == CartItem.item_id, isouter=True)
        .join(User, User.user_id == CartItem.added_by_user_id, isouter=True)
        .where(CartItem.cart_item_id == cart_item_id)
    )
    r = (await db.execute(stmt)).one_or_none()
    if r is None:
        return None

    qty = int(r[3])
    price = r[4]
    line_total = Decimal(price) * qty if price is not None else None

    return {
        "cart_item_id": int(r[0]),
        "cust_id": int(r[1]),
        "item_id": int(r[2]),
        "quantity": qty,
        "unit_price_at_add": price,
        "line_total": line_total,
        "added_by_user_id": int(r[5]),
        "added_by_username": r[6],
        "added_by_role": r[7],
        "source": r[8],
        "status": r[9],
        "added_at": r[10],
        "resolved_at": r[11],
        "resolved_by_user_id": int(r[12]) if r[12] is not None else None,
        "description": r[13],
        "family": r[14],
        "category": r[15],
        "units_in_stock": int(r[16]) if r[16] is not None else None,
    }


# Update quantity
async def update_quantity(
    db: AsyncSession,
    *,
    cart_item_id: int,
    new_quantity: int,
) -> CartItem:
    """Change the quantity on an in_cart line. Reject if not in_cart."""
    if new_quantity <= 0:
        raise ValueError("Quantity must be a positive integer.")

    cart_item = await db.get(CartItem, cart_item_id)
    if cart_item is None:
        raise ValueError(f"Cart item {cart_item_id} not found.")
    if cart_item.status != "in_cart":
        raise ValueError(
            f"Cannot modify quantity on a {cart_item.status!r} line. "
            f"Only 'in_cart' lines can be edited."
        )

    cart_item.quantity = int(new_quantity)
    await db.commit()
    await db.refresh(cart_item)
    return cart_item


# Update status (sold / not_sold) without purchase_history write
async def update_status(
    db: AsyncSession,
    *,
    cart_item_id: int,
    new_status: str,
    user: User,
) -> CartItem:
    """
    Flip status to 'sold' or 'not_sold'. Records the resolved_at timestamp
    and the resolving user. Does NOT write to purchase_history. Use
    checkout_item for that.
    """
    if new_status not in ("sold", "not_sold"):
        raise ValueError(
            f"Invalid status {new_status!r}. Must be 'sold' or 'not_sold'."
        )

    cart_item = await db.get(CartItem, cart_item_id)
    if cart_item is None:
        raise ValueError(f"Cart item {cart_item_id} not found.")
    if cart_item.status != "in_cart":
        raise ValueError(
            f"Cart item {cart_item_id} is already {cart_item.status!r}; "
            "cannot change status again."
        )

    cart_item.status = new_status
    cart_item.resolved_at = datetime.utcnow()
    cart_item.resolved_by_user_id = user.user_id
    await db.commit()
    await db.refresh(cart_item)
    return cart_item


# Delete a cart line
async def delete_item(
    db: AsyncSession,
    *,
    cart_item_id: int,
) -> bool:
    """
    Hard-delete an in_cart line. Returns True if deleted, False if not found.
    Refuses to delete sold or not_sold lines (those are kept for audit).
    """
    cart_item = await db.get(CartItem, cart_item_id)
    if cart_item is None:
        return False
    if cart_item.status != "in_cart":
        raise ValueError(
            f"Cannot delete a {cart_item.status!r} line. "
            "Only in_cart lines can be deleted."
        )

    await db.delete(cart_item)
    await db.commit()
    return True


# Checkout: mark sold AND write purchase_history
async def checkout_item(
    db: AsyncSession,
    *,
    cart_item_id: int,
    user: User,
) -> dict:
    """
    Atomic: flip cart_items.status from 'in_cart' to 'sold' AND insert a
    new row into purchase_history with the same item_id, cust_id, qty,
    and unit_price (using the price at add).

    Returns a dict describing the new purchase row.

    Either both writes commit, or neither does.
    """
    cart_item = await db.get(CartItem, cart_item_id)
    if cart_item is None:
        raise ValueError(f"Cart item {cart_item_id} not found.")
    if cart_item.status != "in_cart":
        raise ValueError(
            f"Cart item {cart_item_id} is already {cart_item.status!r}; "
            "cannot check out a line that's already resolved."
        )

    if user.role in ("seller", "admin"):
        sold_by = user.user_id
    else:
        sold_by = None

    # Insert the purchase_history row
    purchase = PurchaseHistory(
        cust_id=cart_item.cust_id,
        item_id=cart_item.item_id,
        quantity=int(cart_item.quantity),
        unit_price=cart_item.unit_price_at_add,
        sold_by_seller_id=sold_by,
        sold_at=datetime.utcnow(),
        cart_item_id=cart_item.cart_item_id,
    )
    db.add(purchase)

    # Flip the cart status
    cart_item.status = "sold"
    cart_item.resolved_at = datetime.utcnow()
    cart_item.resolved_by_user_id = user.user_id

    # Single commit covers both writes
    await db.commit()
    await db.refresh(purchase)
    await db.refresh(cart_item)

    qty = int(cart_item.quantity)
    price = cart_item.unit_price_at_add or Decimal("0")
    line_total = Decimal(price) * qty

    return {
        "cart_item_id": cart_item.cart_item_id,
        "purchase_id": purchase.purchase_id,
        "cust_id": cart_item.cust_id,
        "item_id": cart_item.item_id,
        "quantity": qty,
        "unit_price": price,
        "line_total": line_total,
        "sold_at": purchase.sold_at,
        "sold_by_seller_id": sold_by,
    }
