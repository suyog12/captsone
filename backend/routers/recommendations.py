from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.dependencies import get_current_user
from backend.db.database import get_db
from backend.models import Customer, RecommendationEvent, User
from backend.schemas.recommendation import (
    CartHelperRequest,
    CartHelperResponse,
    RecommendationsResponse,
    REJECTION_REASON_CODES,
    RejectRecommendationRequest,
    RejectRecommendationResponse,
)
from backend.services import recommendation_service


router = APIRouter(prefix="/recommendations", tags=["recommendations"])

# /recommendations/me  -  current customer's recs
@router.get(
    "/me",
    response_model=RecommendationsResponse,
    summary="Top 10 recommendations for the logged-in customer",
)
async def my_recommendations(
    n: int = Query(10, ge=1, le=20),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> RecommendationsResponse:
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
    source, items, segment, specialty = await recommendation_service.get_recommendations(
        db, int(user.cust_id), n=n
    )
    return RecommendationsResponse(
        cust_id=int(user.cust_id),
        customer_segment=segment,
        customer_specialty=specialty,
        recommendation_source=source,
        n_results=len(items),
        recommendations=items,
    )

# /recommendations/customer/{cust_id}
@router.get(
    "/customer/{cust_id}",
    response_model=RecommendationsResponse,
    summary="Top 10 recommendations for a specific customer (admin or assigned seller)",
)
async def customer_recommendations(
    cust_id: int,
    n: int = Query(10, ge=1, le=20),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> RecommendationsResponse:
    # Authorisation
    if user.role == "admin":
        pass  # admins see anyone
    elif user.role == "seller":
        # Verify the customer is assigned to this seller
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
                detail="Customers can only view their own recommendations.",
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Unrecognised role: {user.role}",
        )

    source, items, segment, specialty = await recommendation_service.get_recommendations(
        db, int(cust_id), n=n
    )
    return RecommendationsResponse(
        cust_id=int(cust_id),
        customer_segment=segment,
        customer_specialty=specialty,
        recommendation_source=source,
        n_results=len(items),
        recommendations=items,
    )


# Reject
@router.post(
    "/reject",
    response_model=RejectRecommendationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Seller rejects a recommendation with a reason (logs feedback for later analysis)",
)
async def reject_recommendation(
    body: RejectRecommendationRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> RejectRecommendationResponse:
    # Sellers only - this is a seller workflow tool
    if user.role != "seller":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only sellers can reject recommendations.",
        )

    # Verify the customer is assigned to this seller
    customer = await db.get(Customer, body.cust_id)
    if customer is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Customer {body.cust_id} not found.",
        )
    if customer.assigned_seller_id != user.user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This customer is not assigned to you.",
        )

    # Normalize and validate the reason_code
    reason_code = (body.reason_code or "").strip().lower()
    if reason_code not in REJECTION_REASON_CODES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Unknown reason_code {reason_code!r}. Allowed: "
                f"{sorted(REJECTION_REASON_CODES)}"
            ),
        )

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    existing = await db.execute(
        select(RecommendationEvent)
        .where(RecommendationEvent.cust_id == body.cust_id)
        .where(RecommendationEvent.item_id == body.item_id)
        .where(RecommendationEvent.outcome == "pending")
        .order_by(RecommendationEvent.shown_at.desc())
        .limit(1)
    )
    event = existing.scalar_one_or_none()
    if event is not None:
        event.outcome = "rejected"
        event.resolved_at = now
        event.rejected_by_user_id = user.user_id
        event.rejection_reason_code = reason_code
        event.rejection_reason_note = body.reason_note
        if body.primary_signal and not event.signal:
            event.signal = body.primary_signal
        if body.rec_purpose and not event.rec_purpose:
            event.rec_purpose = body.rec_purpose
    else:
        event = RecommendationEvent(
            cust_id=body.cust_id,
            item_id=body.item_id,
            signal=body.primary_signal,
            rec_purpose=body.rec_purpose,
            shown_to_user_id=user.user_id,
            shown_at=now,
            outcome="rejected",
            resolved_at=now,
            rejected_by_user_id=user.user_id,
            rejection_reason_code=reason_code,
            rejection_reason_note=body.reason_note,
        )
        db.add(event)

    await db.commit()
    await db.refresh(event)

    return RejectRecommendationResponse(
        event_id=event.event_id,
        cust_id=event.cust_id,
        item_id=event.item_id,
        outcome="rejected",
        reason_code=reason_code,
        rejected_at=event.resolved_at or now,
    )


# /recommendations/cart-helper
@router.post(
    "/cart-helper",
    response_model=CartHelperResponse,
    summary="Live cart suggestions: complements, PB upgrades, Medline conversions",
)
async def cart_helper(
    body: CartHelperRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CartHelperResponse:
    """
    Take a list of cart item_ids and return live suggestions.

    The seller calls this when looking at their customer's cart, or the
    customer calls it for their own cart.
    """
    cust_id = int(body.cust_id)

    # Authorisation: same rules as customer detail
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
                detail="Customers can only run cart helper for their own cart.",
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Unrecognised role: {user.role}",
        )

    complements, pb_upgrades, conversions, effective_items, cart_source = (
        await recommendation_service.get_cart_helper(
            db, cust_id, body.cart_items
        )
    )
    return CartHelperResponse(
        cust_id=cust_id,
        cart_size=len(effective_items),
        cart_source=cart_source,
        cart_complements=complements,
        private_brand_upgrades=pb_upgrades,
        medline_conversions=conversions,
    )
