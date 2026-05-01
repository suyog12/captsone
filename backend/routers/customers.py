"""
Customer endpoints.

  GET  /customers/search?q=...&limit=...   - search customers (admin/seller)
  GET  /customers/filter?...&scope=...     - filter customers (admin/seller)
  POST /customers/record                   - create customer record only (no login)
  GET  /customers/me                       - current customer's own record
  GET  /customers/{cust_id}                - one customer's full details

Access control:

  search   : admin sees everyone; seller sees only their assigned customers
  filter   : admin sees everyone; seller sees their assigned customers by
             default. Seller may pass scope=all to see the whole customer
             base (read-only browsing for the platform-wide customer tab).
  record   : admin or seller. Seller auto-assigns to themselves; admin can
             pass an explicit assigned_seller_id (or leave null).
  me       : customer role only; returns the customer linked to their user
  {cust_id}: admin sees anyone; seller can READ any customer (response
             includes is_assigned_to_me so the UI can gate cart actions);
             customer sees only self
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.dependencies import (
    get_current_user,
    require_seller_or_admin,
)
from backend.db.database import get_db
from backend.models import User
from backend.schemas.customer import (
    CustomerRecordCreateRequest,
    CustomerResponse,
    CustomerSearchResult,
)
from backend.services import customer_service, user_service


router = APIRouter(prefix="/customers", tags=["customers"])


@router.get(
    "/search",
    response_model=list[CustomerSearchResult],
    summary="Search customers by id, market, specialty, or segment",
)
async def search_customers(
    q: str = Query(..., min_length=1, max_length=50, description="Search text"),
    limit: int = Query(25, ge=1, le=500),
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
    "/filter",
    response_model=list[CustomerSearchResult],
    summary="Filter customers by segment, status, archetype, market, or specialty",
)
async def filter_customers(
    segment:        Optional[str] = Query(None, description="e.g. 'PO_large'"),
    status:         Optional[str] = Query(None, description="cold_start | stable_warm | declining_warm | churned_warm"),
    archetype:      Optional[str] = Query(None, description="e.g. 'surgery_center', 'primary_care'"),
    market_code:    Optional[str] = Query(None, description="e.g. 'PO', 'SC', 'LTC'"),
    specialty_code: Optional[str] = Query(None, description="e.g. 'FP', 'GS'"),
    scope:          str = Query("mine", description="'mine' = only assigned (default for sellers); 'all' = entire customer base"),
    limit:          int = Query(25, ge=1, le=500),
    offset:         int = Query(0, ge=0),
    user: User = Depends(require_seller_or_admin),
    db: AsyncSession = Depends(get_db),
) -> list[CustomerSearchResult]:
    """
    Filtered customer browsing for the dashboard.

    All filters are optional and combine with AND.

    Scope semantics:
      - admin: scope is ignored, admin always sees the full customer base
      - seller, scope='mine' (default): only customers assigned to this seller
      - seller, scope='all': every customer in the system (read-only browse)

    Common queries:
      GET /customers/filter?status=churned_warm
      GET /customers/filter?archetype=surgery_center&segment=SC_large
      GET /customers/filter?scope=all&status=stable_warm   (seller browse mode)
    """
    # Validate scope value.
    # NOTE: the local "status" query parameter shadows the imported
    # fastapi.status module here, so use the integer literal 400.
    if scope not in ("mine", "all"):
        raise HTTPException(
            status_code=400,
            detail="scope must be 'mine' or 'all'.",
        )

    # Resolve effective seller filter
    if user.role == "admin":
        # Admin always sees everyone, scope is ignored
        effective_seller_id: Optional[int] = None
    elif user.role == "seller":
        # Seller toggles between their book and the full base
        effective_seller_id = user.user_id if scope == "mine" else None
    else:
        # require_seller_or_admin guarantees this branch is unreachable
        raise HTTPException(status_code=403, detail="Forbidden")

    rows = await customer_service.search_by_filters(
        db,
        segment=segment,
        status=status,
        archetype=archetype,
        market_code=market_code,
        specialty_code=specialty_code,
        seller_id=effective_seller_id,
        limit=limit,
        offset=offset,
    )
    return [CustomerSearchResult.model_validate(r) for r in rows]


# Create customer record only - no login
@router.post(
    "/record",
    response_model=CustomerResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a customer record (no login). Sellers auto-assign to themselves.",
)
async def create_customer_record(
    body: CustomerRecordCreateRequest,
    user: User = Depends(require_seller_or_admin),
    db: AsyncSession = Depends(get_db),
) -> CustomerResponse:
    """
    Creates a Customer row in recdash.customers without a User login.
    Useful for sellers adding new accounts they're already in contact
    with - they don't need to invent a password and the customer can be
    issued a login later by an admin.

    Auto-assignment rules:
      - Seller calling this endpoint: assigned_seller_id is forced to
        user.user_id. Any non-null value in the body is rejected with 403.
      - Admin: respects body.assigned_seller_id (may be None for an
        unassigned customer).
    """
    if user.role == "seller":
        if (
            body.assigned_seller_id is not None
            and body.assigned_seller_id != user.user_id
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "Sellers cannot assign customers to another seller. "
                    "Leave assigned_seller_id blank - it will be set to "
                    "your user_id automatically."
                ),
            )
        assigned_seller_id = user.user_id
    else:
        # admin
        assigned_seller_id = body.assigned_seller_id

    try:
        customer = await user_service.create_customer_record_only(
            db,
            customer_business_name=body.customer_business_name,
            market_code=body.market_code,
            size_tier=body.size_tier,
            specialty_code=body.specialty_code,
            assigned_seller_id=assigned_seller_id,
            actor_user_id=user.user_id,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(e)
        )

    resp = CustomerResponse.model_validate(customer)
    resp.is_assigned_to_me = (
        assigned_seller_id is not None and assigned_seller_id == user.user_id
    )
    return resp


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
            detail="Linked customer record not found.",
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
    Fetch a customer record.

    Access:
      - admin: any customer
      - seller: any customer (read-only browse). The response includes
        is_assigned_to_me so the UI can decide whether to allow cart
        actions, recommendations, claim, or read-only view
      - customer: only their own record (cust_id must match)
    """
    if user.role == "customer":
        if user.cust_id != cust_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Customers may only view their own record.",
            )

    customer = await customer_service.get_by_id(db, cust_id)
    if customer is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Customer {cust_id} not found.",
        )

    response = CustomerResponse.model_validate(customer)

    # For sellers, annotate whether this customer belongs to them so the
    # frontend can show the right action set without an extra round-trip
    if user.role == "seller":
        response.is_assigned_to_me = (
            customer.assigned_seller_id == user.user_id
        )
    elif user.role == "admin":
        # Admins see all customers as their own for action purposes
        response.is_assigned_to_me = True
    else:
        response.is_assigned_to_me = True

    return response
