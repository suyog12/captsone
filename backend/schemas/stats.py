from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field



# Common helpers


class CodeAndDisplay(BaseModel):
    """A code value plus its human-readable display name.

    Used everywhere a response includes a code (segment, market, specialty,
    recommendation source) so the frontend never has to maintain its own
    code -> name mapping.
    """
    code: str
    display_name: str



# /admin/stats/overview


class CustomerPopulationBlock(BaseModel):
    """Counts split between analytical population and active operational accounts."""
    total_customers: int = Field(..., description="All customers in the analytical Parquet file.")
    active_accounts: int = Field(..., description="Customers with login accounts in Postgres.")
    new_customers_this_month: int = Field(..., description="Customers created in the last 30 days.")


class ProductsBlock(BaseModel):
    total_products: int
    private_brand_products: int
    private_brand_pct: float


class SalesPeriodBlock(BaseModel):
    """Aggregations for a defined time period."""
    period_label: str = Field(..., description="Human-readable label for the period (e.g. 'last 30 days').")
    transactions: int = Field(..., description="Number of purchase_history rows in the period.")
    revenue: Decimal = Field(..., description="Sum of quantity * unit_price across the period.")
    total_quantity: int = Field(..., description="Sum of quantity across the period.")
    distinct_customers: int = Field(..., description="Distinct customer IDs in the period.")
    distinct_sellers: int = Field(..., description="Distinct seller IDs in the period (excludes nulls).")


class CartsBlock(BaseModel):
    active_carts: int = Field(..., description="Cart_items rows with status='in_cart'.")
    distinct_customers_with_active_cart: int


class OverviewResponse(BaseModel):
    customer_population: CustomerPopulationBlock
    products: ProductsBlock
    sales_last_7_days: SalesPeriodBlock
    sales_last_30_days: SalesPeriodBlock
    sales_last_90_days: SalesPeriodBlock
    carts: CartsBlock
    generated_at: datetime



# /admin/stats/sales-trend


class SalesTrendBucket(BaseModel):
    """One time bucket of sales aggregations."""
    bucket: str = Field(..., description="Bucket label. For daily: 'YYYY-MM-DD'. For weekly: 'YYYY-Www'. For monthly: 'YYYY-MM'.")
    bucket_start: date = Field(..., description="First day in the bucket.")
    revenue: Decimal
    order_count: int = Field(..., description="Number of distinct purchase_history rows in this bucket.")
    quantity: int = Field(..., description="Sum of quantity in this bucket.")
    distinct_customers: int


class SalesTrendResponse(BaseModel):
    granularity: Literal["daily", "weekly", "monthly"]
    range_label: str = Field(..., description="Human-readable date range, e.g. 'last 90 days'.")
    range_start: date
    range_end: date
    total_buckets: int
    buckets: list[SalesTrendBucket]



# /admin/stats/conversion-by-signal
# /sellers/me/conversion-by-signal


class SignalConversionRow(BaseModel):
    """Conversion stats for one cart_items.source value."""
    source: CodeAndDisplay
    cart_adds: int = Field(..., description="Total cart_items rows with this source.")
    checkouts: int = Field(..., description="Of those cart_adds, how many ended up in purchase_history (status='sold').")
    abandons: int = Field(..., description="Of those cart_adds, how many ended up not_sold or are still in_cart.")
    conversion_rate_pct: float = Field(..., description="checkouts / cart_adds * 100, or 0 if cart_adds is 0.")
    revenue_generated: Decimal = Field(..., description="Sum of revenue from the checked-out lines.")
    quantity_sold: int = Field(..., description="Sum of quantity from the checked-out lines.")


class ConversionBySignalResponse(BaseModel):
    scope: Literal["all", "seller"] = Field(..., description="'all' for admin endpoint, 'seller' for seller-scoped.")
    seller_id: Optional[int] = Field(None, description="Set when scope='seller'.")
    rows: list[SignalConversionRow]
    overall_conversion_rate_pct: float = Field(..., description="Aggregated across all signals.")
    total_cart_adds: int
    total_checkouts: int
    total_revenue: Decimal



# /admin/stats/segment-distribution


class SegmentDistributionRow(BaseModel):
    """One slice of the customer-segment pie chart."""
    segment_code: str = Field(..., description="Combined market + size code, e.g. 'PO_large'.")
    segment_display: str = Field(..., description="'Physician Office, Large'.")
    market: CodeAndDisplay
    size: CodeAndDisplay
    customer_count: int
    pct_of_total: float


class SegmentDistributionResponse(BaseModel):
    total_customers: int
    rows: list[SegmentDistributionRow]



# /admin/stats/top-sellers


class TopSellerRow(BaseModel):
    seller_id: int
    seller_username: str
    seller_full_name: Optional[str] = None
    customers_managed: int
    total_sales: int = Field(..., description="Number of purchase_history rows where sold_by_seller_id matches.")
    total_revenue: Decimal
    total_quantity_sold: int
    avg_order_value: Decimal = Field(..., description="total_revenue / total_sales, or 0 if no sales.")


class TopSellersResponse(BaseModel):
    rows: list[TopSellerRow]
    range_label: str
    range_start: Optional[date] = None
    range_end: Optional[date] = None



# /admin/stats/recent-sales


class RecentSaleRow(BaseModel):
    """One row in the live sales feed."""
    purchase_id: int
    sold_at: datetime
    cust_id: int
    customer_name: Optional[str] = None
    item_id: int
    item_description: Optional[str] = None
    family: Optional[str] = None
    category: Optional[str] = None
    quantity: int
    unit_price: Optional[Decimal] = None
    line_total: Optional[Decimal] = None
    sold_by_seller_id: Optional[int] = None
    sold_by_seller_username: Optional[str] = None
    cart_item_id: Optional[int] = None
    from_recommendation: bool = Field(..., description="True if this sale traces back to a cart line tagged with a recommendation source.")
    recommendation_source: Optional[CodeAndDisplay] = Field(
        None,
        description="The recommendation source that drove the cart add, when from_recommendation is True.",
    )


class RecentSalesResponse(BaseModel):
    rows: list[RecentSaleRow]
    returned: int
    limit_used: int
    since_used: Optional[datetime] = None



# /customers/{cust_id}/stats
# /sellers/me/stats


class CustomerStatsHeader(BaseModel):
    cust_id: int
    customer_name: Optional[str] = None
    market: Optional[CodeAndDisplay] = None
    size: Optional[CodeAndDisplay] = None
    segment_code: Optional[str] = None
    segment_display: Optional[str] = None
    specialty: Optional[CodeAndDisplay] = None
    assigned_seller_id: Optional[int] = None
    assigned_seller_username: Optional[str] = None


class CustomerStatsSummary(BaseModel):
    total_orders: int
    total_revenue: Decimal
    total_items_purchased: int = Field(..., description="Sum of quantity across all orders.")
    distinct_products_purchased: int
    first_order_date: Optional[date] = None
    last_order_date: Optional[date] = None
    avg_order_value: Decimal


class CustomerTrendBucket(BaseModel):
    bucket: str
    bucket_start: date
    revenue: Decimal
    order_count: int
    quantity: int


class CustomerTopProductRow(BaseModel):
    item_id: int
    description: Optional[str] = None
    family: Optional[str] = None
    category: Optional[str] = None
    is_private_brand: bool = False
    revenue: Decimal
    quantity_sold: int
    order_count: int = Field(..., description="Number of distinct purchase rows including this item.")


class CustomerTopFamilyRow(BaseModel):
    family: str
    revenue: Decimal
    quantity_sold: int
    order_count: int
    pct_of_total_revenue: float


class CustomerStatsResponse(BaseModel):
    """Per-customer drilldown.

    Empty when the customer has no rows in purchase_history. Frontend should
    handle empty arrays and zero summaries gracefully.
    """
    header: CustomerStatsHeader
    has_data: bool = Field(..., description="False when the customer has zero purchase_history rows.")
    summary: CustomerStatsSummary
    range_label: str
    range_start: date
    range_end: date
    granularity: Literal["daily", "weekly", "monthly"]
    trend: list[CustomerTrendBucket]
    top_products: list[CustomerTopProductRow]
    top_families: list[CustomerTopFamilyRow]


class SellerStatsResponse(BaseModel):
    """Same shape as customer drilldown but scoped to all customers a seller manages."""
    seller_id: int
    seller_username: str
    seller_full_name: Optional[str] = None
    customers_managed: int
    has_data: bool
    summary: CustomerStatsSummary
    range_label: str
    range_start: date
    range_end: date
    granularity: Literal["daily", "weekly", "monthly"]
    trend: list[CustomerTrendBucket]
    top_products: list[CustomerTopProductRow]
    top_families: list[CustomerTopFamilyRow]
