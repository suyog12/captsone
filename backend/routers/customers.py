from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.dependencies import (
    get_current_user,
    require_seller_or_admin,
)
from backend.db.database import get_db
from backend.models import User
from backend.schemas.customer import CustomerResponse, CustomerSearchResult
from backend.services import customer_service


router = APIRouter(prefix="/customers", tags=["customers"])


@router.get(
    "/search",
    response_model=list[CustomerSearchResult],
    summary="Search customers by id, market, specialty, or segment",
)
async def search_customers(
    q: str = Query(..., min_length=1, max_length=50, description="Search text"),
    limit: int = Query(25, ge=1, le=100),
    user: User = Depends(require_seller_or_admin),
    db: AsyncSession = Depends(get_db),
) -> list[CustomerSearchResult]:
    """
    Search customers.

    For admin: searches the entire customer base.
    For seller: searches only customers assigned to this seller.

    Search modes:
      - Numeric input -> exact cust_id match
      - 1 to 6 letters uppercase -> market_code or specialty_code exact match
      - Longer text -> ILIKE substring match against segment
    """
    seller_id = user.user_id if user.role == "seller" else None
    rows = await customer_service.search(db, q, limit=limit, seller_id=seller_id)
    return [CustomerSearchResult.model_validate(r) for r in rows]


@router.get(
    "/me",
    response_model=CustomerResponse,
    summary="Customer's own record",
)
async def get_my_customer_record(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CustomerResponse:
    """
    Return the customer record linked to the current user.

    Only available to users with role='customer' and a populated cust_id.
    """
    if user.role != "customer":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This endpoint is for customer users only.",
        )
    if user.cust_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No customer record is linked to this user.",
        )
    customer = await customer_service.get_by_id(db, user.cust_id)
    if customer is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Customer {user.cust_id} not found.",
        )
    return CustomerResponse.model_validate(customer)


@router.get(
    "/{cust_id}",
    response_model=CustomerResponse,
    summary="Get one customer by cust_id",
)
async def get_customer(
    cust_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CustomerResponse:
    """
    Look up one customer.

    Access rules:
      - admin can see any customer
      - seller can see only customers assigned to them
      - customer can see only themselves
    """
    customer = await customer_service.get_by_id(db, cust_id)
    if customer is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Customer {cust_id} not found.",
        )

    # Authorisation
    if user.role == "admin":
        pass  # admins see everyone
    elif user.role == "seller":
        if customer.assigned_seller_id != user.user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="This customer is not assigned to you.",
            )
    elif user.role == "customer":
        if user.cust_id != cust_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Customers can only access their own record.",
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Unrecognised role: {user.role}",
        )

    return CustomerResponse.model_validate(customer)
