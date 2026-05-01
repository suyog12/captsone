from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.dependencies import get_current_user
from backend.db.database import get_db
from backend.models import User
from backend.schemas.assignment import (
    AssignmentChangeRequest,
    AssignmentChangeResponse,
    AssignmentHistoryEntry,
    AssignmentHistoryResponse,
    BulkAssignRequest,
    BulkAssignResponse,
    ClaimRequest,
    SellerCustomerListResponse,
)
from backend.schemas.customer import CustomerSearchResult
from backend.services import assignment_service, customer_service


router = APIRouter(tags=["assignments"])


# Admin: assign / reassign / unassign one customer

@router.patch(
    "/customers/{cust_id}/assignment",
    response_model=AssignmentChangeResponse,
    summary="Admin: assign, reassign, or unassign a customer (pass seller_id=null to unassign)",
)
async def change_assignment(
    cust_id: int,
    body: AssignmentChangeRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> AssignmentChangeResponse:
    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can directly change assignments. "
                   "Sellers must use POST /customers/{id}/claim for unassigned customers.",
        )

    try:
        history = await assignment_service.assign_or_reassign(
            db,
            cust_id=cust_id,
            new_seller_id=body.seller_id,
            changed_by=user,
            notes=body.notes,
        )
    except ValueError as e:
        msg = str(e)
        # 404 when the entity does not exist
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg)
        # 400 for business-rule failures (already assigned, no-op, deactivated seller, wrong role)
        raise HTTPException(status_code=400, detail=msg)

    return AssignmentChangeResponse(
        cust_id=history.cust_id,
        previous_seller_id=history.previous_seller_id,
        new_seller_id=history.new_seller_id,
        change_reason=history.change_reason,
        changed_by_user_id=history.changed_by_user_id,
        changed_at=history.changed_at,
        history_id=history.history_id,
    )


# Seller: claim an unassigned customer

@router.post(
    "/customers/{cust_id}/claim",
    response_model=AssignmentChangeResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Seller: claim an unassigned customer for yourself",
)
async def claim_customer(
    cust_id: int,
    body: ClaimRequest = ClaimRequest(),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> AssignmentChangeResponse:
    if user.role != "seller":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only sellers can claim customers. "
                   "Admins should use PATCH /customers/{id}/assignment instead.",
        )

    try:
        history = await assignment_service.seller_claim(
            db,
            cust_id=cust_id,
            seller=user,
            notes=body.notes,
        )
    except ValueError as e:
        msg = str(e)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=400, detail=msg)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))

    return AssignmentChangeResponse(
        cust_id=history.cust_id,
        previous_seller_id=history.previous_seller_id,
        new_seller_id=history.new_seller_id,
        change_reason=history.change_reason,
        changed_by_user_id=history.changed_by_user_id,
        changed_at=history.changed_at,
        history_id=history.history_id,
    )


# Admin: bulk-assign many customers to one seller

@router.post(
    "/customers/assignments/bulk",
    response_model=BulkAssignResponse,
    summary="Admin: assign a list of customers to one seller in a single call",
)
async def bulk_assign(
    body: BulkAssignRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> BulkAssignResponse:
    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can bulk-assign customers.",
        )

    try:
        assigned, skipped, skipped_reasons = await assignment_service.bulk_assign(
            db,
            seller_id=body.seller_id,
            cust_ids=body.cust_ids,
            changed_by=user,
            notes=body.notes,
        )
    except ValueError as e:
        msg = str(e)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=400, detail=msg)

    return BulkAssignResponse(
        seller_id=body.seller_id,
        requested_count=len(body.cust_ids),
        assigned_count=assigned,
        skipped_count=skipped,
        skipped_reasons=skipped_reasons,
    )


# Read: assignment history for one customer

@router.get(
    "/customers/{cust_id}/assignment-history",
    response_model=AssignmentHistoryResponse,
    summary="Admin: full audit trail of who-was-assigned-to-whom for this customer",
)
async def assignment_history(
    cust_id: int,
    limit: int = Query(100, ge=1, le=500),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> AssignmentHistoryResponse:
    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can view assignment history.",
        )

    rows, total = await assignment_service.list_history(
        db, cust_id=cust_id, limit=limit
    )

    items = [AssignmentHistoryEntry(**r) for r in rows]
    return AssignmentHistoryResponse(
        cust_id=cust_id,
        total_changes=total,
        items=items,
    )


# Read: seller's own customers

@router.get(
    "/sellers/me/customers",
    response_model=SellerCustomerListResponse,
    summary="Convenience: the logged-in seller's own customer list",
)
async def list_my_customers(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> SellerCustomerListResponse:
    if user.role != "seller":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This endpoint is for sellers only. Admins should use /sellers/{id}/customers.",
        )

    customers, total = await assignment_service.list_for_seller(
        db, seller_id=user.user_id, limit=limit, offset=offset
    )

    # Hydrate has_user_account flag in one round-trip
    cust_ids = [c.cust_id for c in customers]
    user_map = await customer_service.get_user_account_map(db, cust_ids)
    items = []
    for c in customers:
        item = CustomerSearchResult.model_validate(c)
        item.has_user_account = bool(user_map.get(c.cust_id, False))
        items.append(item.model_dump())

    return SellerCustomerListResponse(
        seller_id=user.user_id,
        seller_username=user.username,
        total=total,
        items=items,
    )


# Read: list customers for a seller (admin-or-self)

@router.get(
    "/sellers/{user_id}/customers",
    response_model=SellerCustomerListResponse,
    summary="List a seller's customers. Admin sees any seller, sellers see their own only.",
)
async def list_seller_customers(
    user_id: int,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> SellerCustomerListResponse:
    if user.role == "admin":
        pass
    elif user.role == "seller":
        if user.user_id != user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Sellers can only view their own customer list.",
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"This endpoint is not available to {user.role!r} users.",
        )

    # Resolve target seller
    target = await db.get(User, user_id)
    if target is None or target.role != "seller":
        raise HTTPException(
            status_code=404,
            detail=f"User {user_id} is not a seller.",
        )

    customers, total = await assignment_service.list_for_seller(
        db, seller_id=user_id, limit=limit, offset=offset
    )

    # Hydrate has_user_account flag in one round-trip
    cust_ids = [c.cust_id for c in customers]
    user_map = await customer_service.get_user_account_map(db, cust_ids)
    items = []
    for c in customers:
        item = CustomerSearchResult.model_validate(c)
        item.has_user_account = bool(user_map.get(c.cust_id, False))
        items.append(item.model_dump())

    return SellerCustomerListResponse(
        seller_id=user_id,
        seller_username=target.username,
        total=total,
        items=items,
    )