from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.dependencies import get_current_user
from backend.db.database import get_db
from backend.models import Customer, User
from backend.schemas.recommendation import PurchaseHistoryResponse, PurchaseLine
from backend.services import purchase_service


router = APIRouter(tags=["customers"])


@router.get(
    "/customers/{cust_id}/history",
    response_model=PurchaseHistoryResponse,
    summary="Customer's recent purchase history",
)
async def customer_history(
    cust_id: int,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> PurchaseHistoryResponse:
    # Authorisation
    if user.role == "admin":
        pass
    elif user.role == "seller":
        customer = await db.get(Customer, cust_id)
        if customer is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Customer {cust_id} not found.",
            )
        if customer.assigned_seller_id != user.user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="This customer is not assigned to you.",
            )
    elif user.role == "customer":
        if int(user.cust_id or 0) != cust_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Customers can only view their own purchase history.",
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Unrecognised role: {user.role}",
        )

    rows, total = await purchase_service.list_for_customer(
        db, cust_id, limit=limit, offset=offset
    )
    items = [
        PurchaseLine(
            purchase_id=r["purchase_id"],
            item_id=r["item_id"],
            description=r["description"],
            family=r["family"],
            category=r["category"],
            quantity=r["quantity"],
            unit_price=r["unit_price"],
            sold_at=r["sold_at"],
        )
        for r in rows
    ]
    return PurchaseHistoryResponse(
        cust_id=cust_id,
        total_lines=total,
        returned=len(items),
        items=items,
    )
