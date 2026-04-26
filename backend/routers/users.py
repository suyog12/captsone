from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.dependencies import (
    get_current_user,
    require_admin,
    require_seller_or_admin,
)
from backend.db.database import get_db
from backend.models import User
from backend.schemas.assignment import SellerDeactivationResponse
from backend.schemas.user import (
    AdminCreateRequest,
    CustomerCreateRequest,
    PasswordChangeRequest,
    SellerCreateRequest,
    UserListResponse,
    UserResponse,
)
from backend.services import user_service


router = APIRouter(prefix="/users", tags=["users"])


# Create users

@router.post(
    "/admins",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create an admin account (admin only)",
)
async def create_admin(
    body: AdminCreateRequest,
    user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> UserResponse:
    try:
        new_user = await user_service.create_admin(
            db,
            username=body.username,
            password=body.password,
            full_name=body.full_name,
            email=body.email,
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    return UserResponse.model_validate(new_user)


@router.post(
    "/sellers",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a seller account (admin only)",
)
async def create_seller(
    body: SellerCreateRequest,
    user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> UserResponse:
    try:
        new_user = await user_service.create_seller(
            db,
            username=body.username,
            password=body.password,
            full_name=body.full_name,
            email=body.email,
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    return UserResponse.model_validate(new_user)


@router.post(
    "/customers",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a customer account and customer record (admin or seller)",
)
async def create_customer(
    body: CustomerCreateRequest,
    user: User = Depends(require_seller_or_admin),
    db: AsyncSession = Depends(get_db),
) -> UserResponse:
    """
    If a seller calls this, the new customer is auto-assigned to that
    seller. Admins can pass an explicit seller via assigned_seller_id in
    the body or omit it to leave the customer unassigned.
    """
    # If the caller is a seller, force auto-assignment to themselves
    if user.role == "seller":
        assigned_seller_id = user.user_id
    else:
        assigned_seller_id = body.assigned_seller_id  # admin choice, may be None

    try:
        new_user = await user_service.create_customer(
            db,
            username=body.username,
            password=body.password,
            full_name=body.full_name,
            email=body.email,
            customer_business_name=body.customer_business_name,
            market_code=body.market_code,
            size_tier=body.size_tier,
            specialty_code=body.specialty_code,
            assigned_seller_id=assigned_seller_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    return UserResponse.model_validate(new_user)


# Read users

@router.get(
    "",
    response_model=UserListResponse,
    summary="List users (admin only, paginated)",
)
async def list_users(
    role: Optional[str] = Query(None, pattern="^(admin|seller|customer)$"),
    is_active: Optional[bool] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> UserListResponse:
    rows, total = await user_service.list_users(
        db,
        role=role,
        is_active=is_active,
        limit=limit,
        offset=offset,
    )
    return UserListResponse(
        total=total,
        items=[UserResponse.model_validate(r) for r in rows],
    )


@router.get(
    "/{user_id}",
    response_model=UserResponse,
    summary="Get one user by ID (admin only)",
)
async def get_user(
    user_id: int,
    user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> UserResponse:
    target = await user_service.get_by_id(db, user_id)
    if target is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User {user_id} not found.",
        )
    return UserResponse.model_validate(target)


# Password change

@router.patch(
    "/me/password",
    response_model=UserResponse,
    summary="Change your own password",
)
async def change_my_password(
    body: PasswordChangeRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> UserResponse:
    ok = await user_service.change_password(
        db,
        user,
        current_password=body.current_password,
        new_password=body.new_password,
    )
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect.",
        )
    return UserResponse.model_validate(user)


# Soft delete (deactivate) / reactivate

@router.delete(
    "/{user_id}",
    response_model=SellerDeactivationResponse,
    summary="Deactivate a user (admin only). Sellers' customers are auto-unassigned.",
)
async def deactivate_user(
    user_id: int,
    user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> SellerDeactivationResponse:
    if user.user_id == user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You cannot deactivate your own account.",
        )

    result, customers_unassigned = await user_service.deactivate(
        db, user_id, changed_by=user
    )
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User {user_id} not found.",
        )

    if result.role == "seller" and customers_unassigned > 0:
        message = (
            f"Seller {result.username!r} deactivated. "
            f"{customers_unassigned} customer(s) auto-unassigned and now "
            f"available for reassignment."
        )
    elif result.role == "seller":
        message = f"Seller {result.username!r} deactivated. No customers were assigned to them."
    else:
        message = f"User {result.username!r} deactivated."

    return SellerDeactivationResponse(
        user_id=result.user_id,
        username=result.username,
        role=result.role,
        is_active=result.is_active,
        customers_unassigned=customers_unassigned,
        message=message,
    )


@router.post(
    "/{user_id}/reactivate",
    response_model=UserResponse,
    summary="Reactivate a deactivated user (admin only)",
)
async def reactivate_user(
    user_id: int,
    user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> UserResponse:
    result = await user_service.reactivate(db, user_id)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User {user_id} not found.",
        )
    return UserResponse.model_validate(result)
