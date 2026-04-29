from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Literal, Optional

import pandas as pd
from sqlalchemy import (
    Date,
    Integer,
    String,
    cast,
    case,
    func,
    select,
    text,
)
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.display_names import (
    is_recommendation_source,
    market_display,
    segment_display,
    size_display,
    source_display,
    specialty_display,
    SOURCE_DISPLAY,
)
from backend.db.parquet_store import (
    CUSTOMER_SEGMENTS_FILE,
    MERGED_DATASET_FILE,
    duckdb_query,
)
from backend.models import (
    CartItem,
    Customer,
    Inventory,
    Product,
    PurchaseHistory,
    User,
)


GranularityType = Literal["daily", "weekly", "monthly"]
RangeType = Literal["7d", "30d", "90d", "180d", "1y", "all"]


# Helpers

def _resolve_range(range_str: str) -> tuple[Optional[date], date, str]:
    """Convert '30d' -> (today-30d, today, 'last 30 days')."""
    today = date.today()
    if range_str == "7d":
        return today - timedelta(days=7), today, "last 7 days"
    if range_str == "30d":
        return today - timedelta(days=30), today, "last 30 days"
    if range_str == "90d":
        return today - timedelta(days=90), today, "last 90 days"
    if range_str == "180d":
        return today - timedelta(days=180), today, "last 180 days"
    if range_str == "1y":
        return today - timedelta(days=365), today, "last 365 days"
    if range_str == "all":
        return None, today, "all time"
    raise ValueError(
        f"Unknown range {range_str!r}. Use 7d, 30d, 90d, 180d, 1y, or all."
    )


def _bucket_expr(granularity: str):
    """Return a SQL expression that groups purchase_history.sold_at into buckets.

    Uses date_trunc which is supported by Postgres natively.
    """
    if granularity == "daily":
        return func.date_trunc("day", PurchaseHistory.sold_at)
    if granularity == "weekly":
        return func.date_trunc("week", PurchaseHistory.sold_at)
    if granularity == "monthly":
        return func.date_trunc("month", PurchaseHistory.sold_at)
    raise ValueError(f"Unknown granularity {granularity!r}")


def _format_bucket(bucket_start: datetime, granularity: str) -> str:
    """Render a bucket boundary as a label."""
    d = bucket_start.date() if isinstance(bucket_start, datetime) else bucket_start
    if granularity == "daily":
        return d.isoformat()
    if granularity == "weekly":
        iso = d.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"
    if granularity == "monthly":
        return d.strftime("%Y-%m")
    return str(d)


def _split_segment_code(seg_code: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Return (market, size) from 'PO_large' -> ('PO', 'large')."""
    if not seg_code:
        return None, None
    parts = seg_code.split("_", 1)
    if len(parts) != 2:
        return seg_code, None
    return parts[0], parts[1]


# /admin/stats/overview

async def get_overview(db: AsyncSession) -> dict:
    """Return KPI numbers for the admin dashboard overview."""

    # --- Customer population (Parquet) ---
    try:
        cust_df = duckdb_query(
            "SELECT COUNT(*) AS total FROM read_parquet(?)",
            [str(CUSTOMER_SEGMENTS_FILE)],
        )
        total_customers = int(cust_df.iloc[0]["total"]) if not cust_df.empty else 0
    except Exception:
        total_customers = 0

    # --- Active accounts (Postgres users with role='customer') ---
    active_accounts_q = await db.execute(
        select(func.count(User.user_id)).where(
            User.role == "customer",
            User.is_active == True,  # noqa: E712
        )
    )
    active_accounts = int(active_accounts_q.scalar() or 0)

    # --- New customer accounts in last 30 days ---
    # We count User records (login accounts) created in the last 30 days,
    # not Customer records, because the customers table was bulk-imported
    # with created_at = now, so it doesn't reflect real signup dates.
    # Customer USER accounts are created one-at-a-time via the API and have
    # accurate created_at timestamps.
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    new_q = await db.execute(
        select(func.count(User.user_id)).where(
            User.role == "customer",
            User.created_at >= thirty_days_ago,
        )
    )
    new_customers_30d = int(new_q.scalar() or 0)

    # --- Products ---
    total_products_q = await db.execute(select(func.count(Product.item_id)))
    total_products = int(total_products_q.scalar() or 0)

    pb_products_q = await db.execute(
        select(func.count(Product.item_id)).where(
            Product.is_private_brand == True  # noqa: E712
        )
    )
    pb_products = int(pb_products_q.scalar() or 0)
    pb_pct = (pb_products / total_products * 100.0) if total_products > 0 else 0.0

    # --- Sales in last 7/30/90 days ---
    sales_7d = await _aggregate_sales_period(db, days=7, label="last 7 days")
    sales_30d = await _aggregate_sales_period(db, days=30, label="last 30 days")
    sales_90d = await _aggregate_sales_period(db, days=90, label="last 90 days")

    # --- Active carts ---
    cart_q = await db.execute(
        select(
            func.count(CartItem.cart_item_id),
            func.count(func.distinct(CartItem.cust_id)),
        ).where(CartItem.status == "in_cart")
    )
    cart_row = cart_q.one()
    active_carts = int(cart_row[0] or 0)
    distinct_carts = int(cart_row[1] or 0)

    return {
        "customer_population": {
            "total_customers": total_customers,
            "active_accounts": active_accounts,
            "new_customers_this_month": new_customers_30d,
        },
        "products": {
            "total_products": total_products,
            "private_brand_products": pb_products,
            "private_brand_pct": round(pb_pct, 2),
        },
        "sales_last_7_days": sales_7d,
        "sales_last_30_days": sales_30d,
        "sales_last_90_days": sales_90d,
        "carts": {
            "active_carts": active_carts,
            "distinct_customers_with_active_cart": distinct_carts,
        },
        "generated_at": datetime.utcnow(),
    }


async def _aggregate_sales_period(
    db: AsyncSession, days: int, label: str
) -> dict:
    """Aggregate purchase_history over the last N days."""
    since = datetime.utcnow() - timedelta(days=days)

    revenue_expr = func.coalesce(
        func.sum(PurchaseHistory.quantity * PurchaseHistory.unit_price), 0
    )
    qty_expr = func.coalesce(func.sum(PurchaseHistory.quantity), 0)

    q = await db.execute(
        select(
            func.count(PurchaseHistory.purchase_id),
            revenue_expr,
            qty_expr,
            func.count(func.distinct(PurchaseHistory.cust_id)),
            func.count(func.distinct(PurchaseHistory.sold_by_seller_id)),
        ).where(PurchaseHistory.sold_at >= since)
    )
    row = q.one()

    return {
        "period_label": label,
        "transactions": int(row[0] or 0),
        "revenue": Decimal(str(row[1] or 0)),
        "total_quantity": int(row[2] or 0),
        "distinct_customers": int(row[3] or 0),
        "distinct_sellers": int(row[4] or 0),
    }


# /admin/stats/sales-trend

async def get_sales_trend(
    db: AsyncSession,
    granularity: GranularityType,
    range_str: RangeType,
) -> dict:
    """Time-series of sales aggregations."""
    range_start, range_end, range_label = _resolve_range(range_str)

    bucket = _bucket_expr(granularity).label("bucket")
    revenue_expr = func.coalesce(
        func.sum(PurchaseHistory.quantity * PurchaseHistory.unit_price), 0
    ).label("revenue")
    qty_expr = func.coalesce(func.sum(PurchaseHistory.quantity), 0).label("qty")

    stmt = select(
        bucket,
        func.count(PurchaseHistory.purchase_id).label("orders"),
        revenue_expr,
        qty_expr,
        func.count(func.distinct(PurchaseHistory.cust_id)).label("custs"),
    ).group_by(bucket).order_by(bucket)

    if range_start is not None:
        stmt = stmt.where(PurchaseHistory.sold_at >= range_start)

    rows = (await db.execute(stmt)).all()

    buckets = []
    for r in rows:
        b_start = r[0]
        if isinstance(b_start, datetime):
            b_date = b_start.date()
        else:
            b_date = b_start
        buckets.append({
            "bucket": _format_bucket(b_start, granularity),
            "bucket_start": b_date,
            "revenue": Decimal(str(r[2] or 0)),
            "order_count": int(r[1] or 0),
            "quantity": int(r[3] or 0),
            "distinct_customers": int(r[4] or 0),
        })

    return {
        "granularity": granularity,
        "range_label": range_label,
        "range_start": range_start or date(1970, 1, 1),
        "range_end": range_end,
        "total_buckets": len(buckets),
        "buckets": buckets,
    }


# /admin/stats/conversion-by-signal
# /sellers/me/conversion-by-signal

async def get_conversion_by_signal(
    db: AsyncSession,
    *,
    seller_id: Optional[int] = None,
) -> dict:
    """For each cart_items.source value, how many converted to sales.

    seller_id: when provided, filters to carts of customers assigned to
               that seller (so it shows that seller's conversion).
               When None, returns aggregate across all signals (admin view).
    """
    revenue_expr = func.coalesce(
        func.sum(
            case(
                (CartItem.status == "sold",
                 CartItem.quantity * CartItem.unit_price_at_add),
                else_=0,
            )
        ),
        0,
    ).label("revenue")

    qty_expr = func.coalesce(
        func.sum(
            case(
                (CartItem.status == "sold", CartItem.quantity),
                else_=0,
            )
        ),
        0,
    ).label("qty")

    sold_count = func.sum(case((CartItem.status == "sold", 1), else_=0)).label("sold")
    abandon_count = func.sum(
        case(
            ((CartItem.status == "not_sold") | (CartItem.status == "in_cart"), 1),
            else_=0,
        )
    ).label("abandons")

    stmt = select(
        CartItem.source,
        func.count(CartItem.cart_item_id).label("adds"),
        sold_count,
        abandon_count,
        revenue_expr,
        qty_expr,
    ).group_by(CartItem.source).order_by(CartItem.source)

    if seller_id is not None:
        stmt = (
            stmt.join(Customer, Customer.cust_id == CartItem.cust_id)
            .where(Customer.assigned_seller_id == seller_id)
        )

    raw = (await db.execute(stmt)).all()

    rows = []
    total_adds = 0
    total_checkouts = 0
    total_revenue = Decimal("0")
    for r in raw:
        src = r[0] or "manual"
        adds = int(r[1] or 0)
        sold = int(r[2] or 0)
        abandons = int(r[3] or 0)
        rev = Decimal(str(r[4] or 0))
        qty = int(r[5] or 0)
        rate = round((sold / adds * 100.0), 2) if adds > 0 else 0.0

        rows.append({
            "source": {
                "code": src,
                "display_name": source_display(src),
            },
            "cart_adds": adds,
            "checkouts": sold,
            "abandons": abandons,
            "conversion_rate_pct": rate,
            "revenue_generated": rev,
            "quantity_sold": qty,
        })
        total_adds += adds
        total_checkouts += sold
        total_revenue += rev

    overall_rate = (
        round((total_checkouts / total_adds * 100.0), 2) if total_adds > 0 else 0.0
    )

    return {
        "scope": "seller" if seller_id is not None else "all",
        "seller_id": seller_id,
        "rows": rows,
        "overall_conversion_rate_pct": overall_rate,
        "total_cart_adds": total_adds,
        "total_checkouts": total_checkouts,
        "total_revenue": total_revenue,
    }


# /admin/stats/segment-distribution

async def get_segment_distribution() -> dict:
    """Customer counts per segment (from Parquet, full population)."""
    try:
        df = duckdb_query(
            """
            SELECT
              segment AS segment_code,
              COUNT(*) AS customer_count
            FROM read_parquet(?)
            WHERE segment IS NOT NULL AND segment != ''
            GROUP BY segment
            ORDER BY customer_count DESC
            """,
            [str(CUSTOMER_SEGMENTS_FILE)],
        )
    except Exception:
        df = pd.DataFrame()

    total = int(df["customer_count"].sum()) if not df.empty else 0
    rows = []
    for _, r in df.iterrows():
        seg_code = str(r["segment_code"])
        market_code, size_code = _split_segment_code(seg_code)
        count = int(r["customer_count"])
        pct = (count / total * 100.0) if total > 0 else 0.0
        rows.append({
            "segment_code": seg_code,
            "segment_display": segment_display(seg_code),
            "market": {
                "code": market_code or "",
                "display_name": market_display(market_code) or "",
            },
            "size": {
                "code": size_code or "",
                "display_name": size_display(size_code) or "",
            },
            "customer_count": count,
            "pct_of_total": round(pct, 2),
        })

    return {
        "total_customers": total,
        "rows": rows,
    }


# /admin/stats/top-sellers

async def get_top_sellers(
    db: AsyncSession,
    *,
    limit: int = 10,
    range_str: RangeType = "all",
) -> dict:
    """Leaderboard of sellers ranked by revenue."""
    range_start, range_end, range_label = _resolve_range(range_str)

    revenue_expr = func.coalesce(
        func.sum(PurchaseHistory.quantity * PurchaseHistory.unit_price), 0
    ).label("revenue")
    qty_expr = func.coalesce(func.sum(PurchaseHistory.quantity), 0).label("qty")
    sales_expr = func.count(PurchaseHistory.purchase_id).label("sales")

    stmt = (
        select(
            User.user_id,
            User.username,
            User.full_name,
            sales_expr,
            revenue_expr,
            qty_expr,
        )
        .join(PurchaseHistory, PurchaseHistory.sold_by_seller_id == User.user_id)
        .where(User.role == "seller")
        .group_by(User.user_id, User.username, User.full_name)
        .order_by(revenue_expr.desc())
        .limit(limit)
    )
    if range_start is not None:
        stmt = stmt.where(PurchaseHistory.sold_at >= range_start)

    raw = (await db.execute(stmt)).all()

    # Pull customer-managed counts in one shot
    cust_counts_q = await db.execute(
        select(
            Customer.assigned_seller_id,
            func.count(Customer.cust_id),
        )
        .where(Customer.assigned_seller_id.is_not(None))
        .group_by(Customer.assigned_seller_id)
    )
    cust_count_map = {int(sid): int(cnt) for sid, cnt in cust_counts_q.all()}

    rows = []
    for r in raw:
        sid = int(r[0])
        sales = int(r[3] or 0)
        revenue = Decimal(str(r[4] or 0))
        qty = int(r[5] or 0)
        avg_order_val = (revenue / sales) if sales > 0 else Decimal("0")
        rows.append({
            "seller_id": sid,
            "seller_username": r[1],
            "seller_full_name": r[2],
            "customers_managed": cust_count_map.get(sid, 0),
            "total_sales": sales,
            "total_revenue": revenue,
            "total_quantity_sold": qty,
            "avg_order_value": avg_order_val.quantize(Decimal("0.01")),
        })

    return {
        "rows": rows,
        "range_label": range_label,
        "range_start": range_start,
        "range_end": range_end,
    }


# /admin/stats/recent-sales

async def get_recent_sales(
    db: AsyncSession,
    *,
    limit: int = 50,
    since: Optional[datetime] = None,
) -> dict:
    """Recent sales feed with optional time filter."""

    stmt = (
        select(
            PurchaseHistory.purchase_id,
            PurchaseHistory.sold_at,
            PurchaseHistory.cust_id,
            Customer.customer_name,
            PurchaseHistory.item_id,
            Product.description,
            Product.family,
            Product.category,
            PurchaseHistory.quantity,
            PurchaseHistory.unit_price,
            PurchaseHistory.sold_by_seller_id,
            User.username,
            PurchaseHistory.cart_item_id,
            CartItem.source,
        )
        .select_from(PurchaseHistory)
        .join(Customer, Customer.cust_id == PurchaseHistory.cust_id, isouter=True)
        .join(Product, Product.item_id == PurchaseHistory.item_id, isouter=True)
        .join(User, User.user_id == PurchaseHistory.sold_by_seller_id, isouter=True)
        .join(CartItem, CartItem.cart_item_id == PurchaseHistory.cart_item_id, isouter=True)
        .order_by(PurchaseHistory.sold_at.desc())
        .limit(limit)
    )

    if since is not None:
        stmt = stmt.where(PurchaseHistory.sold_at >= since)

    raw = (await db.execute(stmt)).all()

    rows = []
    for r in raw:
        qty = int(r[8])
        price = r[9]
        line_total = Decimal(price) * qty if price is not None else None
        source = r[13]
        from_rec = is_recommendation_source(source)
        rec_block = None
        if from_rec:
            rec_block = {
                "code": source,
                "display_name": source_display(source),
            }

        rows.append({
            "purchase_id": int(r[0]),
            "sold_at": r[1],
            "cust_id": int(r[2]),
            "customer_name": r[3],
            "item_id": int(r[4]),
            "item_description": r[5],
            "family": r[6],
            "category": r[7],
            "quantity": qty,
            "unit_price": price,
            "line_total": line_total,
            "sold_by_seller_id": int(r[10]) if r[10] is not None else None,
            "sold_by_seller_username": r[11],
            "cart_item_id": int(r[12]) if r[12] is not None else None,
            "from_recommendation": from_rec,
            "recommendation_source": rec_block,
        })

    return {
        "rows": rows,
        "returned": len(rows),
        "limit_used": limit,
        "since_used": since,
    }


# /customers/{cust_id}/stats

async def get_customer_stats(
    db: AsyncSession,
    cust_id: int,
    *,
    range_str: RangeType = "90d",
    granularity: GranularityType = "daily",
    top_products_limit: int = 10,
    top_families_limit: int = 5,
) -> dict:
    """Per-customer drilldown: header, summary, trend, top products, top families."""
    range_start, range_end, range_label = _resolve_range(range_str)

    # --- Header ---
    customer = await db.get(Customer, cust_id)
    if customer is None:
        raise ValueError(f"Customer {cust_id} not found.")

    # Resolve segment / market / size from the segment string if present
    seg_code = customer.segment
    market_code, size_code = _split_segment_code(seg_code)

    # Resolve assigned seller username
    seller_username = None
    if customer.assigned_seller_id is not None:
        seller_q = await db.execute(
            select(User.username).where(User.user_id == customer.assigned_seller_id)
        )
        seller_username = seller_q.scalar_one_or_none()

    # Specialty code lives in customer.specialty_code
    specialty_code = customer.specialty_code

    header = {
        "cust_id": cust_id,
        "customer_name": customer.customer_name,
        "market": {
            "code": market_code or "",
            "display_name": market_display(market_code) or "",
        } if market_code else None,
        "size": {
            "code": size_code or "",
            "display_name": size_display(size_code) or "",
        } if size_code else None,
        "segment_code": seg_code,
        "segment_display": segment_display(seg_code),
        "specialty": {
            "code": specialty_code,
            "display_name": specialty_display(specialty_code) or specialty_code,
        } if specialty_code else None,
        "assigned_seller_id": customer.assigned_seller_id,
        "assigned_seller_username": seller_username,
    }

    # --- Summary (across all-time, not range-limited) ---
    rev_expr = func.coalesce(
        func.sum(PurchaseHistory.quantity * PurchaseHistory.unit_price), 0
    )
    qty_expr = func.coalesce(func.sum(PurchaseHistory.quantity), 0)
    summary_q = await db.execute(
        select(
            func.count(PurchaseHistory.purchase_id),
            rev_expr,
            qty_expr,
            func.count(func.distinct(PurchaseHistory.item_id)),
            func.min(PurchaseHistory.sold_at),
            func.max(PurchaseHistory.sold_at),
        ).where(PurchaseHistory.cust_id == cust_id)
    )
    s = summary_q.one()
    total_orders = int(s[0] or 0)
    total_revenue = Decimal(str(s[1] or 0))
    total_qty = int(s[2] or 0)
    distinct_products = int(s[3] or 0)
    first_dt = s[4]
    last_dt = s[5]
    avg_order = (
        (total_revenue / total_orders).quantize(Decimal("0.01"))
        if total_orders > 0 else Decimal("0")
    )

    summary = {
        "total_orders": total_orders,
        "total_revenue": total_revenue,
        "total_items_purchased": total_qty,
        "distinct_products_purchased": distinct_products,
        "first_order_date": first_dt.date() if first_dt else None,
        "last_order_date": last_dt.date() if last_dt else None,
        "avg_order_value": avg_order,
    }

    has_data = total_orders > 0

    # --- Trend (range-limited) ---
    bucket = _bucket_expr(granularity).label("bucket")
    trend_stmt = (
        select(
            bucket,
            func.count(PurchaseHistory.purchase_id),
            func.coalesce(
                func.sum(PurchaseHistory.quantity * PurchaseHistory.unit_price), 0
            ),
            func.coalesce(func.sum(PurchaseHistory.quantity), 0),
        )
        .where(PurchaseHistory.cust_id == cust_id)
        .group_by(bucket)
        .order_by(bucket)
    )
    if range_start is not None:
        trend_stmt = trend_stmt.where(PurchaseHistory.sold_at >= range_start)

    trend_rows = (await db.execute(trend_stmt)).all()
    trend = []
    for r in trend_rows:
        b_start = r[0]
        b_date = b_start.date() if isinstance(b_start, datetime) else b_start
        trend.append({
            "bucket": _format_bucket(b_start, granularity),
            "bucket_start": b_date,
            "revenue": Decimal(str(r[2] or 0)),
            "order_count": int(r[1] or 0),
            "quantity": int(r[3] or 0),
        })

    # --- Top products (all-time for this customer) ---
    rev_p = func.coalesce(
        func.sum(PurchaseHistory.quantity * PurchaseHistory.unit_price), 0
    ).label("rev")
    qty_p = func.coalesce(func.sum(PurchaseHistory.quantity), 0).label("qty")
    cnt_p = func.count(PurchaseHistory.purchase_id).label("cnt")

    top_p_stmt = (
        select(
            PurchaseHistory.item_id,
            Product.description,
            Product.family,
            Product.category,
            Product.is_private_brand,
            cnt_p,
            rev_p,
            qty_p,
        )
        .join(Product, Product.item_id == PurchaseHistory.item_id, isouter=True)
        .where(PurchaseHistory.cust_id == cust_id)
        .group_by(
            PurchaseHistory.item_id,
            Product.description,
            Product.family,
            Product.category,
            Product.is_private_brand,
        )
        .order_by(rev_p.desc())
        .limit(top_products_limit)
    )
    top_p_rows = (await db.execute(top_p_stmt)).all()
    top_products = []
    for r in top_p_rows:
        top_products.append({
            "item_id": int(r[0]),
            "description": r[1],
            "family": r[2],
            "category": r[3],
            "is_private_brand": bool(r[4]) if r[4] is not None else False,
            "revenue": Decimal(str(r[6] or 0)),
            "quantity_sold": int(r[7] or 0),
            "order_count": int(r[5] or 0),
        })

    # --- Top families ---
    rev_f = func.coalesce(
        func.sum(PurchaseHistory.quantity * PurchaseHistory.unit_price), 0
    ).label("rev")
    qty_f = func.coalesce(func.sum(PurchaseHistory.quantity), 0).label("qty")
    cnt_f = func.count(PurchaseHistory.purchase_id).label("cnt")

    top_f_stmt = (
        select(Product.family, cnt_f, rev_f, qty_f)
        .join(Product, Product.item_id == PurchaseHistory.item_id, isouter=True)
        .where(PurchaseHistory.cust_id == cust_id)
        .where(Product.family.is_not(None))
        .group_by(Product.family)
        .order_by(rev_f.desc())
        .limit(top_families_limit)
    )
    top_f_rows = (await db.execute(top_f_stmt)).all()
    top_families = []
    for r in top_f_rows:
        fam_revenue = Decimal(str(r[2] or 0))
        pct = float(fam_revenue / total_revenue * 100) if total_revenue > 0 else 0.0
        top_families.append({
            "family": r[0],
            "revenue": fam_revenue,
            "quantity_sold": int(r[3] or 0),
            "order_count": int(r[1] or 0),
            "pct_of_total_revenue": round(pct, 2),
        })

    return {
        "header": header,
        "has_data": has_data,
        "summary": summary,
        "range_label": range_label,
        "range_start": range_start or (first_dt.date() if first_dt else date(1970, 1, 1)),
        "range_end": range_end,
        "granularity": granularity,
        "trend": trend,
        "top_products": top_products,
        "top_families": top_families,
    }


# /sellers/me/stats

async def get_seller_stats(
    db: AsyncSession,
    seller_id: int,
    *,
    range_str: RangeType = "90d",
    granularity: GranularityType = "daily",
    top_products_limit: int = 10,
    top_families_limit: int = 5,
) -> dict:
    """Aggregate stats for everything a seller has sold (across all assigned customers)."""
    range_start, range_end, range_label = _resolve_range(range_str)

    # Header info: seller details + how many customers managed
    seller = await db.get(User, seller_id)
    if seller is None or seller.role != "seller":
        raise ValueError(f"Seller {seller_id} not found.")

    cust_count_q = await db.execute(
        select(func.count(Customer.cust_id)).where(
            Customer.assigned_seller_id == seller_id
        )
    )
    customers_managed = int(cust_count_q.scalar() or 0)

    # Summary
    rev_expr = func.coalesce(
        func.sum(PurchaseHistory.quantity * PurchaseHistory.unit_price), 0
    )
    qty_expr = func.coalesce(func.sum(PurchaseHistory.quantity), 0)
    summary_q = await db.execute(
        select(
            func.count(PurchaseHistory.purchase_id),
            rev_expr,
            qty_expr,
            func.count(func.distinct(PurchaseHistory.item_id)),
            func.min(PurchaseHistory.sold_at),
            func.max(PurchaseHistory.sold_at),
        ).where(PurchaseHistory.sold_by_seller_id == seller_id)
    )
    s = summary_q.one()
    total_orders = int(s[0] or 0)
    total_revenue = Decimal(str(s[1] or 0))
    total_qty = int(s[2] or 0)
    distinct_products = int(s[3] or 0)
    first_dt = s[4]
    last_dt = s[5]
    avg_order = (
        (total_revenue / total_orders).quantize(Decimal("0.01"))
        if total_orders > 0 else Decimal("0")
    )

    summary = {
        "total_orders": total_orders,
        "total_revenue": total_revenue,
        "total_items_purchased": total_qty,
        "distinct_products_purchased": distinct_products,
        "first_order_date": first_dt.date() if first_dt else None,
        "last_order_date": last_dt.date() if last_dt else None,
        "avg_order_value": avg_order,
    }
    has_data = total_orders > 0

    # Trend
    bucket = _bucket_expr(granularity).label("bucket")
    trend_stmt = (
        select(
            bucket,
            func.count(PurchaseHistory.purchase_id),
            func.coalesce(
                func.sum(PurchaseHistory.quantity * PurchaseHistory.unit_price), 0
            ),
            func.coalesce(func.sum(PurchaseHistory.quantity), 0),
        )
        .where(PurchaseHistory.sold_by_seller_id == seller_id)
        .group_by(bucket)
        .order_by(bucket)
    )
    if range_start is not None:
        trend_stmt = trend_stmt.where(PurchaseHistory.sold_at >= range_start)

    trend_rows = (await db.execute(trend_stmt)).all()
    trend = []
    for r in trend_rows:
        b_start = r[0]
        b_date = b_start.date() if isinstance(b_start, datetime) else b_start
        trend.append({
            "bucket": _format_bucket(b_start, granularity),
            "bucket_start": b_date,
            "revenue": Decimal(str(r[2] or 0)),
            "order_count": int(r[1] or 0),
            "quantity": int(r[3] or 0),
        })

    # Top products sold by this seller
    rev_p = func.coalesce(
        func.sum(PurchaseHistory.quantity * PurchaseHistory.unit_price), 0
    ).label("rev")
    qty_p = func.coalesce(func.sum(PurchaseHistory.quantity), 0).label("qty")
    cnt_p = func.count(PurchaseHistory.purchase_id).label("cnt")

    top_p_stmt = (
        select(
            PurchaseHistory.item_id,
            Product.description,
            Product.family,
            Product.category,
            Product.is_private_brand,
            cnt_p,
            rev_p,
            qty_p,
        )
        .join(Product, Product.item_id == PurchaseHistory.item_id, isouter=True)
        .where(PurchaseHistory.sold_by_seller_id == seller_id)
        .group_by(
            PurchaseHistory.item_id,
            Product.description,
            Product.family,
            Product.category,
            Product.is_private_brand,
        )
        .order_by(rev_p.desc())
        .limit(top_products_limit)
    )
    top_p_rows = (await db.execute(top_p_stmt)).all()
    top_products = []
    for r in top_p_rows:
        top_products.append({
            "item_id": int(r[0]),
            "description": r[1],
            "family": r[2],
            "category": r[3],
            "is_private_brand": bool(r[4]) if r[4] is not None else False,
            "revenue": Decimal(str(r[6] or 0)),
            "quantity_sold": int(r[7] or 0),
            "order_count": int(r[5] or 0),
        })

    # Top families sold by this seller
    rev_f = func.coalesce(
        func.sum(PurchaseHistory.quantity * PurchaseHistory.unit_price), 0
    ).label("rev")
    qty_f = func.coalesce(func.sum(PurchaseHistory.quantity), 0).label("qty")
    cnt_f = func.count(PurchaseHistory.purchase_id).label("cnt")

    top_f_stmt = (
        select(Product.family, cnt_f, rev_f, qty_f)
        .join(Product, Product.item_id == PurchaseHistory.item_id, isouter=True)
        .where(PurchaseHistory.sold_by_seller_id == seller_id)
        .where(Product.family.is_not(None))
        .group_by(Product.family)
        .order_by(rev_f.desc())
        .limit(top_families_limit)
    )
    top_f_rows = (await db.execute(top_f_stmt)).all()
    top_families = []
    for r in top_f_rows:
        fam_revenue = Decimal(str(r[2] or 0))
        pct = float(fam_revenue / total_revenue * 100) if total_revenue > 0 else 0.0
        top_families.append({
            "family": r[0],
            "revenue": fam_revenue,
            "quantity_sold": int(r[3] or 0),
            "order_count": int(r[1] or 0),
            "pct_of_total_revenue": round(pct, 2),
        })

    return {
        "seller_id": seller_id,
        "seller_username": seller.username,
        "seller_full_name": seller.full_name,
        "customers_managed": customers_managed,
        "has_data": has_data,
        "summary": summary,
        "range_label": range_label,
        "range_start": range_start or (first_dt.date() if first_dt else date(1970, 1, 1)),
        "range_end": range_end,
        "granularity": granularity,
        "trend": trend,
        "top_products": top_products,
        "top_families": top_families,
    }