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
    CustomerListResponse,
    CustomerRecordCreateRequest,
    CustomerResponse,
    CustomerSearchResult,
)
from backend.services import (
    assignment_service,
    customer_service,
    user_service,
)


router = APIRouter(tags=["customers"])


# GET /customers/search
@router.get(
    "/customers/search",
    response_model=list[CustomerSearchResult],
    summary="Search customers by id, market, specialty, segment, or business name",
)
async def search_customers(
    q: str = Query(..., min_length=1, max_length=50, description="Search text"),
    limit: int = Query(25, ge=1),
    scope: str = Query("mine", description="'mine' = seller's assigned customers (default for sellers); 'all' = entire customer base"),
    user: User = Depends(require_seller_or_admin),
    db: AsyncSession = Depends(get_db),
) -> list[CustomerSearchResult]:
    """
    Search customers.

    Scope semantics:
      - admin: scope is ignored, admin always searches the full base.
      - seller, scope='mine' (default): only customers assigned to this seller.
      - seller, scope='all': search the entire customer base
        (read-only browse; mirrors /customers/filter?scope=all).

    Search modes:
      - Numeric input -> exact cust_id match
      - 1 to 6 letters uppercase -> market_code or specialty_code exact match
      - Longer text -> ILIKE substring match against segment OR customer_name
        (so newly-created records with a populated business name are also
        searchable by name)
    """
    if scope not in ("mine", "all"):
        raise HTTPException(
            status_code=400,
            detail="scope must be 'mine' or 'all'.",
        )

    if user.role == "admin":
        effective_seller_id: Optional[int] = None
    elif user.role == "seller":
        effective_seller_id = user.user_id if scope == "mine" else None
    else:
        raise HTTPException(status_code=403, detail="Forbidden")

    rows = await customer_service.search(
        db, query=q, limit=limit, seller_id=effective_seller_id,
    )

    # Hydrate has_user_account in one round-trip for the whole page
    cust_ids = [r.cust_id for r in rows]
    user_map = await customer_service.get_user_account_map(db, cust_ids)

    out = []
    for r in rows:
        item = CustomerSearchResult.model_validate(r)
        item.has_user_account = bool(user_map.get(r.cust_id, False))
        out.append(item)
    return out


# GET /customers/filter
@router.get(
    "/customers/filter",
    response_model=CustomerListResponse,
    summary="Filter customers by segment, status, archetype, market, specialty, or account status",
)
async def filter_customers(
    segment:        Optional[str] = Query(None, description="e.g. 'PO_large'"),
    status_filter:  Optional[str] = Query(None, alias="status", description="cold_start | stable_warm | declining_warm | churned_warm"),
    archetype:      Optional[str] = Query(None, description="e.g. 'surgery_center', 'primary_care'"),
    market_code:    Optional[str] = Query(None, description="e.g. 'PO', 'SC', 'LTC'"),
    specialty_code: Optional[str] = Query(None, description="e.g. 'FP', 'GS'"),
    account_status: Optional[str] = Query(None, description="all | users | no_users"),
    scope:          str = Query("mine", description="'mine' = only assigned (default for sellers); 'all' = entire customer base"),
    limit:          int = Query(25, ge=1),
    offset:         int = Query(0, ge=0),
    user: User = Depends(require_seller_or_admin),
    db: AsyncSession = Depends(get_db),
) -> CustomerListResponse:
    """
    Filtered customer browsing for the dashboard.

    All filters are optional and combine with AND.

    Scope semantics:
      - admin: scope is ignored, admin always sees the full customer base
      - seller, scope='mine' (default): only customers assigned to this seller
      - seller, scope='all': every customer in the system (read-only browse)

    Account status semantics:
      - omitted or 'all': do not filter by account status
      - 'users':   only customers WITH a dashboard login
      - 'no_users': only customers WITHOUT a dashboard login

    Common queries:
      GET /customers/filter?status=churned_warm
      GET /customers/filter?archetype=surgery_center&segment=SC_large
      GET /customers/filter?account_status=no_users   (onboarding queue)
      GET /customers/filter?scope=all&status=stable_warm   (seller browse mode)

    Response shape: { total, limit, offset, items: [...] }. The 'total'
    field is the count across all pages (used for 'Page 3 of 47' UI).
    """
    # Validate scope value (use 400 literal because 'status_filter' alias
    # avoids shadowing fastapi.status; 400 is also explicit)
    if scope not in ("mine", "all"):
        raise HTTPException(
            status_code=400,
            detail="scope must be 'mine' or 'all'.",
        )

    # Validate account_status value
    if account_status is not None and account_status not in ("all", "users", "no_users"):
        raise HTTPException(
            status_code=400,
            detail="account_status must be 'all', 'users', or 'no_users'.",
        )
    if account_status == "all":
        account_status = None  # Treat 'all' as no filter

    # Resolve effective seller filter
    if user.role == "admin":
        effective_seller_id: Optional[int] = None
    elif user.role == "seller":
        effective_seller_id = user.user_id if scope == "mine" else None
    else:
        # require_seller_or_admin guarantees this branch is unreachable
        raise HTTPException(status_code=403, detail="Forbidden")

    rows = await customer_service.search_by_filters(
        db,
        segment=segment,
        status=status_filter,
        archetype=archetype,
        market_code=market_code,
        specialty_code=specialty_code,
        seller_id=effective_seller_id,
        account_status=account_status,
        limit=limit,
        offset=offset,
    )

    total = await customer_service.count_by_filters(
        db,
        segment=segment,
        status=status_filter,
        archetype=archetype,
        market_code=market_code,
        specialty_code=specialty_code,
        seller_id=effective_seller_id,
        account_status=account_status,
    )

    # Hydrate has_user_account
    cust_ids = [r.cust_id for r in rows]
    user_map = await customer_service.get_user_account_map(db, cust_ids)

    items: list[CustomerSearchResult] = []
    for r in rows:
        item = CustomerSearchResult.model_validate(r)
        item.has_user_account = bool(user_map.get(r.cust_id, False))
        items.append(item)

    return CustomerListResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=items,
    )


# POST /customers/record - create customer record only, no login
@router.post(
    "/customers/record",
    response_model=CustomerResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a customer record (no login). Sellers auto-assign; admins may set assigned_seller_id.",
)
async def create_customer_record(
    body: CustomerRecordCreateRequest,
    user: User = Depends(require_seller_or_admin),
    db: AsyncSession = Depends(get_db),
) -> CustomerResponse:
    # Sellers cannot pin a record to another seller
    if user.role == "seller" and body.assigned_seller_id is not None and body.assigned_seller_id != user.user_id:
        raise HTTPException(
            status_code=403,
            detail="Sellers cannot assign customers to another seller. Omit assigned_seller_id to auto-assign.",
        )

    # Compute effective assignment
    if user.role == "seller":
        effective_assignee = user.user_id  # Sellers always self-assign
    else:
        effective_assignee = body.assigned_seller_id  # Admins may pass null

    customer = await user_service.create_customer_record_only(
        db,
        customer_business_name=body.customer_business_name,
        market_code=body.market_code,
        size_tier=body.size_tier,
        specialty_code=body.specialty_code,
        assigned_seller_id=effective_assignee,
        actor_user_id=user.user_id,
    )

    response = CustomerResponse.model_validate(customer)
    if user.role == "seller":
        response.is_assigned_to_me = (customer.assigned_seller_id == user.user_id)
    else:
        response.is_assigned_to_me = True
    return response


# GET /customers/me - the logged-in customer's own record
@router.get(
    "/customers/me",
    response_model=CustomerResponse,
    summary="Get the logged-in customer's own record",
)
async def get_my_customer_record(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CustomerResponse:
    if user.role != "customer":
        raise HTTPException(
            status_code=403,
            detail="/me endpoints are for customers only.",
        )
    if user.cust_id is None:
        raise HTTPException(
            status_code=404,
            detail="This account is not linked to a customer record.",
        )

    customer = await customer_service.get_by_id(db, user.cust_id)
    if customer is None:
        raise HTTPException(
            status_code=404,
            detail=f"Customer {user.cust_id} not found.",
        )

    response = CustomerResponse.model_validate(customer)
    response.is_assigned_to_me = None  # Not meaningful for self-view
    return response


# GET /customers/{cust_id}
@router.get(
    "/customers/{cust_id}",
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

    # is_assigned_to_me flag
    if user.role == "seller":
        response.is_assigned_to_me = (
            customer.assigned_seller_id == user.user_id
        )
    elif user.role == "admin":
        response.is_assigned_to_me = True
    else:
        response.is_assigned_to_me = True

    return response