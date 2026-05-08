from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.dependencies import get_current_user
from backend.db.database import get_db
from backend.models import Customer, User
from backend.schemas.stats import (
    ConversionBySignalResponse,
    CustomerStatsResponse,
    EngineEffectivenessResponse,
    OverviewResponse,
    RecentSalesResponse,
    SalesTrendResponse,
    SegmentDistributionResponse,
    SellerStatsResponse,
    TopCustomersResponse,
    TopSellersResponse,
)
from backend.services import stats_service


router = APIRouter(tags=["stats"])



# Auth helpers
def _require_admin(user: User) -> None:
    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin role required.",
        )


def _require_seller(user: User) -> User:
    if user.role != "seller":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Seller role required.",
        )
    return user

@router.get(
    "/admin/stats/overview",
    response_model=OverviewResponse,
    summary="Admin dashboard KPI numbers (population, products, sales, carts).",
)
async def admin_overview(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> OverviewResponse:
    _require_admin(user)
    data = await stats_service.get_overview(db)
    return OverviewResponse(**data)

@router.get(
    "/admin/stats/sales-trend",
    response_model=SalesTrendResponse,
    summary="Time-series of sales aggregations across all customers.",
)
async def admin_sales_trend(
    granularity: Literal["daily", "weekly", "monthly"] = Query(
        "daily",
        description="Bucket size for the time series.",
    ),
    range: Literal["7d", "30d", "90d", "180d", "1y", "all"] = Query(
        "90d",
        description="How far back to include.",
    ),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> SalesTrendResponse:
    _require_admin(user)
    data = await stats_service.get_sales_trend(db, granularity, range)
    return SalesTrendResponse(**data)

@router.get(
    "/admin/stats/conversion-by-signal",
    response_model=ConversionBySignalResponse,
    summary="For each cart_items.source value, how many converted to sales (admin scope).",
)
async def admin_conversion_by_signal(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> ConversionBySignalResponse:
    _require_admin(user)
    data = await stats_service.get_conversion_by_signal(db, seller_id=None)
    return ConversionBySignalResponse(**data)

@router.get(
    "/admin/stats/segment-distribution",
    response_model=SegmentDistributionResponse,
    summary="Customer counts per segment (full population from Parquet).",
)
async def admin_segment_distribution(
    user: User = Depends(get_current_user),
) -> SegmentDistributionResponse:
    _require_admin(user)
    data = await stats_service.get_segment_distribution()
    return SegmentDistributionResponse(**data)

@router.get(
    "/admin/stats/top-sellers",
    response_model=TopSellersResponse,
    summary="Leaderboard of sellers ranked by revenue.",
)
async def admin_top_sellers(
    limit: int = Query(10, ge=1, le=50),
    range: Literal["7d", "30d", "90d", "180d", "1y", "all"] = Query("all"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> TopSellersResponse:
    _require_admin(user)
    data = await stats_service.get_top_sellers(db, limit=limit, range_str=range)
    return TopSellersResponse(**data)

@router.get(
    "/admin/stats/recent-sales",
    response_model=RecentSalesResponse,
    summary="Live activity feed of recent sales with recommendation attribution.",
)
async def admin_recent_sales(
    limit: int = Query(50, ge=1, le=200),
    since: Optional[datetime] = Query(
        None,
        description="Optional ISO timestamp; only return sales after this time.",
    ),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> RecentSalesResponse:
    _require_admin(user)
    data = await stats_service.get_recent_sales(db, limit=limit, since=since)
    return RecentSalesResponse(**data)

@router.get(
    "/customers/{cust_id}/stats",
    response_model=CustomerStatsResponse,
    summary="Per-customer drilldown: KPIs, revenue trend, top products, top families.",
)
async def customer_stats(
    cust_id: int,
    range: Literal["7d", "30d", "90d", "180d", "1y", "all"] = Query("90d"),
    granularity: Literal["daily", "weekly", "monthly"] = Query("daily"),
    top_products: int = Query(10, ge=1, le=50),
    top_families: int = Query(5, ge=1, le=20),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CustomerStatsResponse:
    # Authorization: admin (any), seller (only assigned), customer (only self)
    if user.role == "admin":
        pass
    elif user.role == "seller":
        customer = await db.get(Customer, cust_id)
        if customer is None:
            raise HTTPException(
                status_code=404,
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
                detail="Customers can only view their own stats.",
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Authentication required.",
        )

    try:
        data = await stats_service.get_customer_stats(
            db,
            cust_id,
            range_str=range,
            granularity=granularity,
            top_products_limit=top_products,
            top_families_limit=top_families,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return CustomerStatsResponse(**data)

@router.get(
    "/sellers/me/stats",
    response_model=SellerStatsResponse,
    summary="Logged-in seller's own performance: KPIs, trend, top products, top families.",
)
async def my_seller_stats(
    range: Literal["7d", "30d", "90d", "180d", "1y", "all"] = Query("90d"),
    granularity: Literal["daily", "weekly", "monthly"] = Query("daily"),
    top_products: int = Query(10, ge=1, le=50),
    top_families: int = Query(5, ge=1, le=20),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> SellerStatsResponse:
    seller = _require_seller(user)

    try:
        data = await stats_service.get_seller_stats(
            db,
            seller.user_id,
            range_str=range,
            granularity=granularity,
            top_products_limit=top_products,
            top_families_limit=top_families,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return SellerStatsResponse(**data)

@router.get(
    "/sellers/me/conversion-by-signal",
    response_model=ConversionBySignalResponse,
    summary="Logged-in seller's own conversion rate per recommendation signal.",
)
async def my_seller_conversion_by_signal(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> ConversionBySignalResponse:
    seller = _require_seller(user)
    data = await stats_service.get_conversion_by_signal(
        db, seller_id=seller.user_id
    )
    return ConversionBySignalResponse(**data)

@router.get(
    "/admin/stats/top-customers",
    response_model=TopCustomersResponse,
    summary="Highest-revenue customers ranked by total spend.",
)
async def admin_top_customers(
    limit: int = Query(10, ge=1, le=50),
    range: Literal["7d", "30d", "90d", "180d", "1y", "all"] = Query("all"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> TopCustomersResponse:
    _require_admin(user)
    data = await stats_service.get_top_customers(db, limit=limit, range_str=range)
    return TopCustomersResponse(**data)

@router.get(
    "/admin/stats/engine-effectiveness",
    response_model=EngineEffectivenessResponse,
    summary="Recommendation engine funnel: adds, sold, rejected per signal.",
)
async def admin_engine_effectiveness(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> EngineEffectivenessResponse:
    _require_admin(user)
    data = await stats_service.get_engine_effectiveness(db)
    return EngineEffectivenessResponse(**data)


from backend.schemas.stats import ChurnFunnelResponse

@router.get(
    "/admin/stats/churn-funnel",
    response_model=ChurnFunnelResponse,
    summary="Customer lifecycle distribution: counts and percentages by status.",
)
async def admin_churn_funnel(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> ChurnFunnelResponse:
    _require_admin(user)
    data = await stats_service.get_churn_funnel(db)
    return ChurnFunnelResponse(**data)
