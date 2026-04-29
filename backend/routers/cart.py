from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.dependencies import get_current_user
from backend.db.database import get_db
from backend.models import CartItem, Customer, User
from backend.schemas.cart import (
    AddToCartRequest,
    CartCheckoutResponse,
    CartLine,
    CartLineResponse,
    CartViewResponse,
    UpdateCartQuantityRequest,
    UpdateCartStatusRequest,
)
from backend.services import cart_service


router = APIRouter(tags=["cart"])


# Auth helpers

async def _verify_customer_access(
    db: AsyncSession, user: User, cust_id: int
) -> Customer:
    """
    Raise HTTP 403/404 if the caller cannot act on this customer's cart.
    Returns the Customer record on success.
    """
    customer = await db.get(Customer, cust_id)
    if customer is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Customer {cust_id} not found.",
        )

    if user.role == "admin":
        return customer

    if user.role == "seller":
        if customer.assigned_seller_id != user.user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="This customer is not assigned to you.",
            )
        return customer

    if user.role == "customer":
        if int(user.cust_id or 0) != cust_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Customers can only access their own cart.",
            )
        return customer

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Unrecognised role: {user.role}",
    )


async def _verify_cart_item_access(
    db: AsyncSession, user: User, cart_item_id: int
) -> CartItem:
    """
    Raise HTTP 403/404 if the caller cannot act on this cart_item.
    Returns the CartItem on success.
    """
    cart_item = await db.get(CartItem, cart_item_id)
    if cart_item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cart item {cart_item_id} not found.",
        )

    # Resolve the customer so we can check assignment / ownership
    await _verify_customer_access(db, user, cart_item.cust_id)
    return cart_item


# POST /customers/{cust_id}/cart

@router.post(
    "/customers/{cust_id}/cart",
    response_model=CartLineResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add an item to a customer's cart (admin, assigned seller, or self-customer).",
)
async def add_to_cart(
    cust_id: int,
    body: AddToCartRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CartLineResponse:
    await _verify_customer_access(db, user, cust_id)

    try:
        await cart_service.add_item(
            db,
            cust_id=cust_id,
            item_id=body.item_id,
            quantity=body.quantity,
            source=body.source,
            user=user,
        )
    except ValueError as e:
        msg = str(e)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=400, detail=msg)

    # Hydrate the affected line for the response
    # (find the matching in_cart line for this customer + item_id)
    rows, _ = await cart_service.list_for_customer(
        db, cust_id=cust_id, status_filter="in_cart"
    )
    for r in rows:
        if r["item_id"] == body.item_id:
            return CartLineResponse(
                item=CartLine(**r),
                message=f"Item {body.item_id} added to cart for customer {cust_id}.",
            )

    # Fallback (shouldn't hit)
    raise HTTPException(
        status_code=500,
        detail="Cart line was added but could not be retrieved.",
    )


# GET /customers/{cust_id}/cart  (active cart only)

@router.get(
    "/customers/{cust_id}/cart",
    response_model=CartViewResponse,
    summary="View a customer's active cart (only items with status='in_cart').",
)
async def view_cart(
    cust_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CartViewResponse:
    customer = await _verify_customer_access(db, user, cust_id)

    rows, totals = await cart_service.list_for_customer(
        db, cust_id=cust_id, status_filter="in_cart"
    )

    return CartViewResponse(
        cust_id=cust_id,
        customer_name=customer.customer_name,
        status_filter="in_cart",
        total_items=totals["total_items"],
        total_quantity=totals["total_quantity"],
        estimated_total=totals["estimated_total"],
        items=[CartLine(**r) for r in rows],
    )


# GET /customers/{cust_id}/cart/history  (all statuses)

@router.get(
    "/customers/{cust_id}/cart/history",
    response_model=CartViewResponse,
    summary="View a customer's full cart history (all statuses, optionally filtered).",
)
async def view_cart_history(
    cust_id: int,
    status_filter: str = Query(
        "all",
        pattern="^(in_cart|sold|not_sold|all)$",
        description="Filter by status. Use 'all' for everything.",
    ),
    limit: int = Query(200, ge=1, le=500),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CartViewResponse:
    customer = await _verify_customer_access(db, user, cust_id)

    rows, totals = await cart_service.list_for_customer(
        db, cust_id=cust_id, status_filter=status_filter, limit=limit
    )

    return CartViewResponse(
        cust_id=cust_id,
        customer_name=customer.customer_name,
        status_filter=status_filter,
        total_items=totals["total_items"],
        total_quantity=totals["total_quantity"],
        estimated_total=totals["estimated_total"],
        items=[CartLine(**r) for r in rows],
    )


# GET /cart/me  (customer convenience)

@router.get(
    "/cart/me",
    response_model=CartViewResponse,
    summary="Convenience: the logged-in customer's own active cart.",
)
async def view_my_cart(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CartViewResponse:
    if user.role != "customer":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This endpoint is for customers. "
                   "Sellers/admins should use GET /customers/{cust_id}/cart.",
        )
    if user.cust_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="This customer account is not linked to a cust_id.",
        )

    customer = await db.get(Customer, user.cust_id)
    rows, totals = await cart_service.list_for_customer(
        db, cust_id=user.cust_id, status_filter="in_cart"
    )

    return CartViewResponse(
        cust_id=user.cust_id,
        customer_name=customer.customer_name if customer else None,
        status_filter="in_cart",
        total_items=totals["total_items"],
        total_quantity=totals["total_quantity"],
        estimated_total=totals["estimated_total"],
        items=[CartLine(**r) for r in rows],
    )


# PATCH /cart/{cart_item_id}  (update quantity)

@router.patch(
    "/cart/{cart_item_id}",
    response_model=CartLineResponse,
    summary="Update the quantity on an in_cart line.",
)
async def update_quantity(
    cart_item_id: int,
    body: UpdateCartQuantityRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CartLineResponse:
    await _verify_cart_item_access(db, user, cart_item_id)

    try:
        await cart_service.update_quantity(
            db, cart_item_id=cart_item_id, new_quantity=body.quantity
        )
    except ValueError as e:
        msg = str(e)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=400, detail=msg)

    hydrated = await cart_service.get_hydrated(db, cart_item_id)
    return CartLineResponse(
        item=CartLine(**hydrated),
        message=f"Quantity updated to {body.quantity}.",
    )


# PATCH /cart/{cart_item_id}/status  (mark sold or not_sold)

@router.patch(
    "/cart/{cart_item_id}/status",
    response_model=CartLineResponse,
    summary="Mark a cart line as sold or not_sold (no purchase_history write). "
            "Use POST /cart/{id}/checkout instead to also record the sale.",
)
async def update_status(
    cart_item_id: int,
    body: UpdateCartStatusRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CartLineResponse:
    await _verify_cart_item_access(db, user, cart_item_id)

    try:
        await cart_service.update_status(
            db,
            cart_item_id=cart_item_id,
            new_status=body.status,
            user=user,
        )
    except ValueError as e:
        msg = str(e)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=400, detail=msg)

    hydrated = await cart_service.get_hydrated(db, cart_item_id)
    return CartLineResponse(
        item=CartLine(**hydrated),
        message=f"Cart line {cart_item_id} marked {body.status!r}.",
    )


# DELETE /cart/{cart_item_id}

@router.delete(
    "/cart/{cart_item_id}",
    status_code=status.HTTP_200_OK,
    summary="Remove an in_cart line. Sold/not_sold lines cannot be deleted.",
)
async def delete_cart_item(
    cart_item_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    await _verify_cart_item_access(db, user, cart_item_id)

    try:
        deleted = await cart_service.delete_item(db, cart_item_id=cart_item_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not deleted:
        raise HTTPException(
            status_code=404,
            detail=f"Cart item {cart_item_id} not found.",
        )

    return {
        "deleted": True,
        "cart_item_id": cart_item_id,
        "message": f"Cart item {cart_item_id} removed.",
    }


# POST /cart/{cart_item_id}/checkout

@router.post(
    "/cart/{cart_item_id}/checkout",
    response_model=CartCheckoutResponse,
    summary="Mark a cart line as sold AND write a corresponding row to purchase_history.",
)
async def checkout(
    cart_item_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CartCheckoutResponse:
    await _verify_cart_item_access(db, user, cart_item_id)

    try:
        result = await cart_service.checkout_item(
            db, cart_item_id=cart_item_id, user=user
        )
    except ValueError as e:
        msg = str(e)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=400, detail=msg)

    return CartCheckoutResponse(
        cart_item_id=result["cart_item_id"],
        purchase_id=result["purchase_id"],
        cust_id=result["cust_id"],
        item_id=result["item_id"],
        quantity=result["quantity"],
        unit_price=result["unit_price"],
        line_total=result["line_total"],
        sold_at=result["sold_at"],
        sold_by_seller_id=result["sold_by_seller_id"],
        message=(
            f"Cart item {cart_item_id} checked out. "
            f"Purchase {result['purchase_id']} written for customer {result['cust_id']}."
        ),
    )
