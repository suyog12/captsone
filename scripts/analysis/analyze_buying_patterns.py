from __future__ import annotations

import sys
import time
import warnings
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


# Paths

ROOT         = Path(__file__).resolve().parent.parent.parent
DATA_CLEAN   = ROOT / "data_clean"
MERGED_FILE  = DATA_CLEAN / "serving"  / "merged_dataset.parquet"
FEATURE_FILE = DATA_CLEAN / "features" / "customer_features.parquet"
SEG_FILE     = DATA_CLEAN / "serving"  / "precomputed" / "customer_segments.parquet"

OUT_PRECOMP  = DATA_CLEAN / "serving"  / "precomputed"
OUT_ANALYSIS = DATA_CLEAN / "analysis"

PATTERNS_OUT          = OUT_PRECOMP  / "customer_patterns.parquet"
LAPSED_OUT            = OUT_PRECOMP  / "customer_lapsed_products.parquet"
SEGMENT_CADENCE_OUT   = OUT_PRECOMP  / "product_segment_cadence.parquet"
REPLENISHMENT_OUT     = OUT_PRECOMP  / "customer_replenishment_candidates.parquet"
PATTERNS_XLSX         = OUT_ANALYSIS / "customer_patterns_analysis.xlsx"


# Configuration

FISCAL_YEARS         = ("FY2425", "FY2526")
LAPSED_WINDOW_MONTHS = 6      # Bought in FY2425 but not in last 6 months = lapsed

# Cadence classification (days between orders)
CADENCE_FREQUENT    = 14   # <= 14 days between orders
CADENCE_REGULAR     = 45   # 14-45 days
# > 45 days = occasional

# Phase 6: Replenishment signal config
# These are tunable thresholds. For production, these would be calibrated
# against business outcomes (conversion lift, customer feedback, etc.).
# Documented as initial defaults based on retail recsys conventions.

# Activity window: how recent counts as "currently buying"
PEER_ACTIVITY_WINDOW_MONTHS = 6

# A (segment, product) pair is "alive" when at least this fraction of historical
# buyers in the segment bought it in the last PEER_ACTIVITY_WINDOW_MONTHS.
# 0.30 = 30% of historical buyers still active = product still healthy in segment
PEER_ALIVE_THRESHOLD = 0.30

# A customer is "due for reorder" when their days-since-last is at least this
# multiple of the segment's median reorder interval.
# 1.5x = customer is 50% past the segment's typical reorder cadence
REPLENISHMENT_OVERDUE_FACTOR = 1.5

# Minimum buyers in a segment for the segment cadence to be reliable.
# Below this, peer signal is too noisy to trust.
MIN_SEGMENT_BUYERS_FOR_CADENCE = 10

# Minimum repeat buyers (those with 2+ purchases) to compute a segment cadence
# at all. Without enough repeat buyers, we don't know the cadence.
MIN_SEGMENT_REPEAT_BUYERS = 5

# Phase 6 fix: minimum spend gate for "declining" classification.
# Customers with very small historical spend can be flagged as declining
# from noise (e.g. $10 historical -> $4 recent). Require a meaningful baseline.
MIN_HISTORICAL_SPEND_FOR_DECLINING = 100.0

# Phase 6 fix: Medline is no longer excluded from buying-pattern analysis.
# Medline products are eligible to be recommended at priority 3 (after
# McKesson Brand and other brands), and we need their transaction data
# to compute reorder cadence and lapsed signals correctly.
# The Medline-conversion logic (priority swap) is handled in
# recommendation_factors.py via the medline_conversion signal.
EXCLUDED_FAMILIES  = {"Fee", "Unknown", "NaN", "nan", ""}


# Logging

def _s(title: str) -> None:
    print(f"\n{'-' * 64}\n  {title}\n{'-' * 64}", flush=True)


def _log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def _pq(p: Path) -> str:
    return "'" + p.as_posix() + "'"


# Step 1: Load transactions with date

def load_customer_transactions(con: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    _s("Step 1: Loading customer transaction data")
    t0 = time.time()

    if not MERGED_FILE.exists():
        print(f"\nFATAL: merged_dataset.parquet not found", file=sys.stderr)
        sys.exit(1)

    desc = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({_pq(MERGED_FILE)}) LIMIT 0"
    ).df()
    available = set(desc["column_name"].tolist())

    family_col = next(
        (c for c in ["PROD_FMLY_LVL1_DSC", "PROD_FMLY_LVL1_CD"]
         if c in available), None
    )
    has_date_parts = all(
        c in available for c in ["order_year", "order_month", "order_day"]
    )

    _log(f"Product family column : {family_col}")
    _log(f"Date parts available  : {has_date_parts}")
    _log(f"Medline filter        : DISABLED (Phase 6 - Medline kept for cadence/lapsed analysis)")

    if not has_date_parts:
        print("\nFATAL: need order_year, order_month, order_day columns",
              file=sys.stderr)
        sys.exit(1)

    max_date_row = con.execute(f"""
        SELECT MAX(MAKE_DATE(order_year, order_month, order_day)) AS max_dt,
               MIN(MAKE_DATE(order_year, order_month, order_day)) AS min_dt
        FROM read_parquet({_pq(MERGED_FILE)})
    """).df()
    max_date = str(max_date_row["max_dt"].iloc[0])
    min_date = str(max_date_row["min_dt"].iloc[0])
    _log(f"Date range: {min_date} to {max_date}")

    fy_sql = ", ".join(f"'{fy}'" for fy in FISCAL_YEARS)
    family_filter = ""
    if family_col:
        excl_fams = ", ".join(f"'{f}'" for f in sorted(EXCLUDED_FAMILIES))
        family_filter = f"AND COALESCE({family_col}, 'Unknown') NOT IN ({excl_fams})"

    _log("Building filtered transaction table (Medline included)...")
    con.execute(f"""
        CREATE TEMPORARY TABLE filtered_txn AS
        SELECT
            CAST(DIM_CUST_CURR_ID AS BIGINT) AS cust_id,
            CAST(DIM_ITEM_E1_CURR_ID AS BIGINT) AS item_id,
            MAKE_DATE(order_year, order_month, order_day) AS order_date,
            fiscal_year,
            ORDR_QTY,
            UNIT_SLS_AMT
        FROM read_parquet({_pq(MERGED_FILE)})
        WHERE UNIT_SLS_AMT > 0
          AND fiscal_year IN ({fy_sql})
          AND DIM_CUST_CURR_ID IS NOT NULL
          AND DIM_ITEM_E1_CURR_ID IS NOT NULL
          {family_filter}
    """)

    n_txn = con.execute("SELECT COUNT(*) FROM filtered_txn").fetchone()[0]
    _log(f"  Filtered transactions: {n_txn:,}")
    _log(f"Step 1 done in {time.time()-t0:.1f}s")

    return max_date, min_date


# Step 2: Compute per-customer order cadence

def compute_order_cadence(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    _s("Step 2: Computing per-customer order cadence")
    t0 = time.time()

    _log("Finding unique order dates per customer...")
    con.execute("""
        CREATE TEMPORARY TABLE customer_dates AS
        SELECT DISTINCT cust_id, order_date
        FROM filtered_txn
    """)

    _log("Computing cadence metrics...")
    cadence = con.execute("""
        SELECT
            cust_id,
            COUNT(*) AS n_order_dates,
            MIN(order_date) AS first_order_date,
            MAX(order_date) AS last_order_date,
            CAST(MAX(order_date) AS DATE) - CAST(MIN(order_date) AS DATE) AS active_span_days
        FROM customer_dates
        GROUP BY cust_id
    """).df()

    cadence["cust_id"] = cadence["cust_id"].astype("int64")
    cadence["first_order_date"] = pd.to_datetime(cadence["first_order_date"])
    cadence["last_order_date"]  = pd.to_datetime(cadence["last_order_date"])

    cadence["avg_days_between_orders"] = np.where(
        cadence["n_order_dates"] > 1,
        cadence["active_span_days"] / (cadence["n_order_dates"] - 1),
        -1.0
    )

    def classify_cadence(avg_gap: float) -> str:
        if avg_gap < 0:
            return "single_order"
        elif avg_gap <= CADENCE_FREQUENT:
            return "frequent"
        elif avg_gap <= CADENCE_REGULAR:
            return "regular"
        else:
            return "occasional"

    cadence["order_cadence_tier"] = cadence["avg_days_between_orders"].apply(classify_cadence)

    _log(f"  Customers with cadence data: {len(cadence):,}")
    _log(f"  Cadence tier distribution:")
    for tier, n in cadence["order_cadence_tier"].value_counts().items():
        pct = n / len(cadence) * 100
        _log(f"    {tier:<15}  {n:>8,} customers  ({pct:.1f}%)")

    _log(f"Step 2 done in {time.time()-t0:.1f}s")
    return cadence


# Step 3: Activity trends - declining/churned detection

def compute_activity_trends(con: duckdb.DuckDBPyConnection,
                             max_date: str) -> pd.DataFrame:
    _s("Step 3: Computing per-customer activity trends")
    t0 = time.time()

    max_dt = pd.to_datetime(max_date)
    # Phase 6 fix: use real-month math, not 30-day approximation
    cutoff_6mo = (max_dt - pd.DateOffset(months=6)).strftime("%Y-%m-%d")
    cutoff_3mo = (max_dt - pd.DateOffset(months=3)).strftime("%Y-%m-%d")

    _log(f"  Max date: {max_date}")
    _log(f"  Cutoff 6mo: {cutoff_6mo}")
    _log(f"  Cutoff 3mo: {cutoff_3mo}")

    _log("Computing recent vs historical spend...")
    trends = con.execute(f"""
        SELECT
            cust_id,
            COUNT(DISTINCT item_id) AS total_unique_products,
            SUM(UNIT_SLS_AMT) AS total_spend_all_time,
            COUNT(DISTINCT CASE WHEN order_date >= DATE '{cutoff_6mo}' THEN item_id END)
                AS unique_products_last_6mo,
            SUM(CASE WHEN order_date >= DATE '{cutoff_6mo}' THEN UNIT_SLS_AMT ELSE 0 END)
                AS spend_last_6mo,
            COUNT(DISTINCT CASE WHEN order_date >= DATE '{cutoff_3mo}' THEN item_id END)
                AS unique_products_last_3mo,
            SUM(CASE WHEN order_date >= DATE '{cutoff_3mo}' THEN UNIT_SLS_AMT ELSE 0 END)
                AS spend_last_3mo,
            COUNT(DISTINCT CASE WHEN order_date < DATE '{cutoff_6mo}' THEN item_id END)
                AS unique_products_historical,
            SUM(CASE WHEN order_date < DATE '{cutoff_6mo}' THEN UNIT_SLS_AMT ELSE 0 END)
                AS spend_historical
        FROM filtered_txn
        GROUP BY cust_id
    """).df()

    trends["cust_id"] = trends["cust_id"].astype("int64")

    recent_monthly = trends["spend_last_6mo"] / 6.0
    historical_monthly = trends["spend_historical"] / 18.0
    has_historical = trends["spend_historical"] > 0
    # Phase 6 fix: gate declining classification on minimum historical spend
    # to avoid flagging noise from very-low-spend customers
    has_meaningful_history = trends["spend_historical"] >= MIN_HISTORICAL_SPEND_FOR_DECLINING

    # Churned: had history, zero recent
    trends["is_churned"] = (
        has_historical & (trends["spend_last_6mo"] == 0)
    ).astype(int)

    # Declining: meaningful history, has recent, but recent monthly < 50% historical monthly
    trends["is_declining"] = (
        has_meaningful_history &
        (trends["spend_last_6mo"] > 0) &
        (recent_monthly < historical_monthly * 0.5)
    ).astype(int)

    # Sanity check: mutually exclusive
    overlap = ((trends["is_churned"] == 1) & (trends["is_declining"] == 1)).sum()
    if overlap > 0:
        _log(f"  WARNING: {overlap} customers flagged both churned and declining (logic bug)")

    _log(f"  Customers with activity data: {len(trends):,}")
    _log(f"  Activity status breakdown:")
    n_churned   = int(trends["is_churned"].sum())
    n_declining = int(trends["is_declining"].sum())
    n_stable    = len(trends) - n_churned - n_declining
    _log(f"    Churned   (had history, zero recent spend): "
         f"{n_churned:,} ({n_churned/len(trends)*100:.1f}%)")
    _log(f"    Declining (recent < 50% of historical, hist >= ${MIN_HISTORICAL_SPEND_FOR_DECLINING:.0f}): "
         f"{n_declining:,} ({n_declining/len(trends)*100:.1f}%)")
    _log(f"    Stable    (rest)                          : "
         f"{n_stable:,} ({n_stable/len(trends)*100:.1f}%)")

    _log(f"Step 3 done in {time.time()-t0:.1f}s")
    return trends


# Step 4: Find lapsed products per customer

def find_lapsed_products(con: duckdb.DuckDBPyConnection,
                          max_date: str) -> pd.DataFrame:
    _s("Step 4: Finding lapsed products per customer")
    t0 = time.time()

    max_dt = pd.to_datetime(max_date)
    # Phase 6 fix: real 6-month math
    lapsed_cutoff = (max_dt - pd.DateOffset(months=LAPSED_WINDOW_MONTHS)).strftime("%Y-%m-%d")
    _log(f"  Lapsed cutoff: {lapsed_cutoff} ({LAPSED_WINDOW_MONTHS} months before max date)")

    _log("Computing per customer-product last order date and historical spend...")
    con.execute("""
        CREATE TEMPORARY TABLE cust_product_history AS
        SELECT
            cust_id,
            item_id,
            MAX(order_date) AS last_order_date,
            MIN(order_date) AS first_order_date,
            COUNT(*) AS n_lines,
            COUNT(DISTINCT order_date) AS n_purchases,
            SUM(ORDR_QTY) AS total_qty,
            SUM(UNIT_SLS_AMT) AS total_spend
        FROM filtered_txn
        GROUP BY cust_id, item_id
    """)

    n_pairs = con.execute(
        "SELECT COUNT(*) FROM cust_product_history"
    ).fetchone()[0]
    _log(f"  Customer-product pairs: {n_pairs:,}")

    _log(f"Filtering to products bought in FY2425 but not after {lapsed_cutoff}...")
    lapsed = con.execute(f"""
        SELECT
            h.cust_id,
            h.item_id,
            h.last_order_date,
            h.first_order_date,
            h.n_lines,
            h.total_qty,
            h.total_spend,
            CAST(DATE '{max_date}' AS DATE) - CAST(h.last_order_date AS DATE) AS days_since_last
        FROM cust_product_history h
        WHERE h.last_order_date < DATE '{lapsed_cutoff}'
          AND EXISTS (
              SELECT 1 FROM filtered_txn t
              WHERE t.cust_id = h.cust_id
                AND t.item_id = h.item_id
                AND t.fiscal_year = 'FY2425'
          )
    """).df()

    _log(f"  Lapsed customer-product pairs: {len(lapsed):,}")
    _log(f"  Customers with at least one lapsed product: {lapsed['cust_id'].nunique():,}")

    lapsed["cust_id"] = lapsed["cust_id"].astype("int64")
    lapsed["item_id"] = lapsed["item_id"].astype("int64")
    lapsed["last_order_date"]  = pd.to_datetime(lapsed["last_order_date"])
    lapsed["first_order_date"] = pd.to_datetime(lapsed["first_order_date"])
    lapsed["n_lines"]    = lapsed["n_lines"].astype("int32")
    lapsed["total_qty"]  = lapsed["total_qty"].astype("float32")
    lapsed["total_spend"] = lapsed["total_spend"].astype("float32")
    lapsed["days_since_last"] = lapsed["days_since_last"].astype("int32")

    _log(f"Step 4 done in {time.time()-t0:.1f}s")
    return lapsed


# Step 5: Per-customer portfolio metrics

def compute_portfolio_metrics(con: duckdb.DuckDBPyConnection,
                               lapsed_df: pd.DataFrame) -> pd.DataFrame:
    _s("Step 5: Computing per-customer portfolio metrics")
    t0 = time.time()

    _log("Counting active vs lapsed products per customer...")

    lapsed_counts = lapsed_df.groupby("cust_id").size().rename("n_lapsed_products").reset_index()

    total_counts = con.execute("""
        SELECT
            cust_id,
            COUNT(DISTINCT item_id) AS n_unique_products_total
        FROM filtered_txn
        GROUP BY cust_id
    """).df()
    total_counts["cust_id"] = total_counts["cust_id"].astype("int64")

    portfolio = total_counts.merge(lapsed_counts, on="cust_id", how="left")
    portfolio["n_lapsed_products"] = portfolio["n_lapsed_products"].fillna(0).astype("int32")
    portfolio["n_active_products"] = (
        portfolio["n_unique_products_total"] - portfolio["n_lapsed_products"]
    )

    portfolio["lapsed_rate"] = (
        portfolio["n_lapsed_products"] / portfolio["n_unique_products_total"].replace(0, 1)
    )

    _log(f"  Customers with portfolio data: {len(portfolio):,}")
    _log(f"  Distribution of unique products per customer:")
    vals = portfolio["n_unique_products_total"]
    _log(f"    p10={vals.quantile(0.10):.0f}  median={vals.quantile(0.50):.0f}  p90={vals.quantile(0.90):.0f}")
    _log(f"  Distribution of lapsed rate:")
    vals = portfolio["lapsed_rate"]
    _log(f"    p10={vals.quantile(0.10):.2f}  median={vals.quantile(0.50):.2f}  p90={vals.quantile(0.90):.2f}")

    _log(f"Step 5 done in {time.time()-t0:.1f}s")
    return portfolio


# Step 6 (NEW Phase 6): Per-(segment x product) reorder cadence and peer activity
# This is the foundation of peer-validated replenishment. For each
# (segment, product) pair we compute:
#   - The median reorder interval among repeat buyers in that segment
#   - The fraction of historical buyers still active in the last 6 months
# A product is "alive in segment" when the active-buyer fraction is above
# PEER_ALIVE_THRESHOLD. Products that have died off in a segment never
# trigger replenishment recs - we don't push customers to reorder products
# their peers have collectively abandoned.

def compute_segment_product_cadence(
    con: duckdb.DuckDBPyConnection,
    max_date: str,
) -> pd.DataFrame:
    _s("Step 6: Computing (segment x product) reorder cadence + peer activity")
    t0 = time.time()

    if not SEG_FILE.exists():
        _log(f"  WARNING: {SEG_FILE} not found - cannot compute segment cadence")
        return pd.DataFrame()

    max_dt = pd.to_datetime(max_date)
    activity_cutoff = (max_dt - pd.DateOffset(months=PEER_ACTIVITY_WINDOW_MONTHS)).strftime("%Y-%m-%d")
    _log(f"  Peer-activity cutoff: {activity_cutoff} (last {PEER_ACTIVITY_WINDOW_MONTHS} months)")
    _log(f"  Alive threshold:      {PEER_ALIVE_THRESHOLD:.0%} of historical buyers active recently")
    _log(f"  Min segment buyers:   {MIN_SEGMENT_BUYERS_FOR_CADENCE}")
    _log(f"  Min repeat buyers:    {MIN_SEGMENT_REPEAT_BUYERS}")

    # Load segment lookup
    _log("Loading customer_segments for segment lookup...")
    segs = pd.read_parquet(SEG_FILE, columns=[
        "DIM_CUST_CURR_ID", "segment", "size_tier", "mkt_cd_clean"
    ])
    segs["DIM_CUST_CURR_ID"] = segs["DIM_CUST_CURR_ID"].astype("int64")
    segs = segs.dropna(subset=["segment"])
    _log(f"  Customers with segment: {len(segs):,}")

    # Register the segment lookup as a DuckDB view so we can join in SQL
    con.register("segs_df", segs)

    # Per (cust, item) - n_purchases, last_order_date
    _log("Computing per (cust, item) purchase counts and dates...")
    con.execute("""
        CREATE OR REPLACE TEMPORARY TABLE cust_item_summary AS
        SELECT
            t.cust_id,
            t.item_id,
            COUNT(DISTINCT t.order_date) AS n_purchases,
            MIN(t.order_date) AS first_order_date,
            MAX(t.order_date) AS last_order_date,
            CAST(MAX(t.order_date) AS DATE) - CAST(MIN(t.order_date) AS DATE) AS span_days
        FROM filtered_txn t
        GROUP BY t.cust_id, t.item_id
    """)

    # Per-(cust, item) personal cadence (only meaningful for n_purchases >= 2)
    # Average gap = span_days / (n_purchases - 1) when n_purchases >= 2
    _log("Computing per (cust, item) personal cadence...")
    con.execute("""
        CREATE OR REPLACE TEMPORARY TABLE cust_item_cadence AS
        SELECT
            cust_id,
            item_id,
            n_purchases,
            last_order_date,
            span_days,
            CASE
                WHEN n_purchases >= 2 THEN CAST(span_days AS DOUBLE) / (n_purchases - 1)
                ELSE NULL
            END AS personal_avg_days_between
        FROM cust_item_summary
    """)

    # Join with segments and aggregate to (segment x item)
    _log("Aggregating to (segment x item) - peer activity and median cadence...")
    seg_cadence = con.execute(f"""
        WITH joined AS (
            SELECT
                c.cust_id,
                c.item_id,
                c.n_purchases,
                c.last_order_date,
                c.personal_avg_days_between,
                s.segment,
                s.size_tier,
                s.mkt_cd_clean
            FROM cust_item_cadence c
            INNER JOIN segs_df s
              ON c.cust_id = s.DIM_CUST_CURR_ID
        )
        SELECT
            item_id,
            segment,
            ANY_VALUE(size_tier) AS size_tier,
            ANY_VALUE(mkt_cd_clean) AS mkt_cd_clean,
            COUNT(*) AS n_buyers_segment,
            SUM(CASE WHEN n_purchases >= 2 THEN 1 ELSE 0 END) AS n_buyers_with_2plus,
            SUM(CASE WHEN last_order_date >= DATE '{activity_cutoff}' THEN 1 ELSE 0 END)
                AS n_buyers_active_recent,
            -- Median requires the right syntax in DuckDB
            MEDIAN(personal_avg_days_between) FILTER (WHERE personal_avg_days_between IS NOT NULL)
                AS median_days_between_segment,
            AVG(personal_avg_days_between) FILTER (WHERE personal_avg_days_between IS NOT NULL)
                AS mean_days_between_segment
        FROM joined
        GROUP BY item_id, segment
    """).df()

    # Compute alive flag and types
    seg_cadence["item_id"]                 = seg_cadence["item_id"].astype("int64")
    seg_cadence["n_buyers_segment"]        = seg_cadence["n_buyers_segment"].astype("int32")
    seg_cadence["n_buyers_with_2plus"]     = seg_cadence["n_buyers_with_2plus"].astype("int32")
    seg_cadence["n_buyers_active_recent"]  = seg_cadence["n_buyers_active_recent"].astype("int32")
    seg_cadence["peer_activity_rate"] = (
        seg_cadence["n_buyers_active_recent"] / seg_cadence["n_buyers_segment"].replace(0, 1)
    ).astype("float32")

    # "Alive" = enough total buyers AND enough repeat buyers AND active fraction above threshold
    seg_cadence["is_alive"] = (
        (seg_cadence["n_buyers_segment"]    >= MIN_SEGMENT_BUYERS_FOR_CADENCE) &
        (seg_cadence["n_buyers_with_2plus"] >= MIN_SEGMENT_REPEAT_BUYERS) &
        (seg_cadence["peer_activity_rate"]  >= PEER_ALIVE_THRESHOLD)
    ).astype("int8")

    # Cadence reliability flag - the median is meaningful only with enough repeat buyers
    seg_cadence["cadence_is_reliable"] = (
        seg_cadence["n_buyers_with_2plus"] >= MIN_SEGMENT_REPEAT_BUYERS
    ).astype("int8")

    seg_cadence["median_days_between_segment"] = (
        seg_cadence["median_days_between_segment"].astype("float32")
    )
    seg_cadence["mean_days_between_segment"] = (
        seg_cadence["mean_days_between_segment"].astype("float32")
    )

    _log(f"  (segment x product) pairs computed: {len(seg_cadence):,}")
    n_alive = int(seg_cadence["is_alive"].sum())
    n_reliable = int(seg_cadence["cadence_is_reliable"].sum())
    _log(f"  Alive in segment    : {n_alive:,} ({n_alive/len(seg_cadence)*100:.1f}%)")
    _log(f"  Reliable cadence    : {n_reliable:,} ({n_reliable/len(seg_cadence)*100:.1f}%)")
    _log(f"  Cadence stats (alive + reliable only):")
    alive_reliable = seg_cadence[
        (seg_cadence["is_alive"] == 1) &
        (seg_cadence["cadence_is_reliable"] == 1)
    ]
    if len(alive_reliable) > 0:
        v = alive_reliable["median_days_between_segment"].dropna()
        if len(v) > 0:
            _log(f"    median cadence: p10={v.quantile(0.10):.0f}d  "
                 f"median={v.quantile(0.50):.0f}d  p90={v.quantile(0.90):.0f}d")

    _log(f"Step 6 done in {time.time()-t0:.1f}s")
    return seg_cadence


# Step 7 (NEW Phase 6): Identify replenishment candidates
# For each customer, find products where:
#   - The customer has bought it at least once (i.e. it's in their history)
#   - The product is "alive" in the customer's segment
#   - The customer's days_since_last >= REPLENISHMENT_OVERDUE_FACTOR * segment median cadence
# These are the products to recommend as "you should reorder."

def find_replenishment_candidates(
    con: duckdb.DuckDBPyConnection,
    seg_cadence: pd.DataFrame,
    max_date: str,
) -> pd.DataFrame:
    _s("Step 7: Identifying customer-product replenishment candidates")
    t0 = time.time()

    if len(seg_cadence) == 0:
        _log("  No segment cadence data - skipping")
        return pd.DataFrame()

    if not SEG_FILE.exists():
        _log("  No customer segments - skipping")
        return pd.DataFrame()

    _log(f"  Overdue factor: {REPLENISHMENT_OVERDUE_FACTOR}x segment cadence")

    # Restrict segment cadence to alive + reliable only
    alive_cadence = seg_cadence[
        (seg_cadence["is_alive"] == 1) &
        (seg_cadence["cadence_is_reliable"] == 1)
    ][["item_id", "segment", "median_days_between_segment",
       "peer_activity_rate", "n_buyers_segment"]].copy()
    _log(f"  Alive + reliable (segment x product) pairs: {len(alive_cadence):,}")

    if len(alive_cadence) == 0:
        return pd.DataFrame()

    # Load customer segments for the join
    segs = pd.read_parquet(SEG_FILE, columns=[
        "DIM_CUST_CURR_ID", "segment", "size_tier", "mkt_cd_clean"
    ])
    segs = segs.rename(columns={"DIM_CUST_CURR_ID": "cust_id"})
    segs["cust_id"] = segs["cust_id"].astype("int64")
    segs = segs.dropna(subset=["segment"])

    # Per (cust, item) - last_order_date and n_purchases (already computed in Step 6)
    cust_item = con.execute("""
        SELECT
            cust_id,
            item_id,
            n_purchases,
            last_order_date,
            personal_avg_days_between
        FROM cust_item_cadence
    """).df()
    cust_item["cust_id"]         = cust_item["cust_id"].astype("int64")
    cust_item["item_id"]         = cust_item["item_id"].astype("int64")
    cust_item["last_order_date"] = pd.to_datetime(cust_item["last_order_date"])

    _log(f"  (cust, item) history rows: {len(cust_item):,}")

    # Join: customer -> their segment -> alive (segment, item) cadence
    cust_with_seg = cust_item.merge(segs[["cust_id", "segment"]], on="cust_id", how="inner")
    candidates = cust_with_seg.merge(
        alive_cadence, on=["item_id", "segment"], how="inner"
    )
    _log(f"  After joining customer history with alive segment cadence: {len(candidates):,}")

    if len(candidates) == 0:
        return pd.DataFrame()

    # Compute days_since_last and overdue_ratio
    max_dt = pd.to_datetime(max_date)
    candidates["days_since_last"] = (
        (max_dt - candidates["last_order_date"]).dt.days.astype("int32")
    )
    candidates["overdue_ratio"] = (
        candidates["days_since_last"].astype("float32") /
        candidates["median_days_between_segment"].clip(lower=1.0)
    ).astype("float32")

    # The replenishment candidate flag
    candidates["is_replenishment_candidate"] = (
        candidates["overdue_ratio"] >= REPLENISHMENT_OVERDUE_FACTOR
    ).astype("int8")

    n_due = int(candidates["is_replenishment_candidate"].sum())
    _log(f"  Replenishment candidates (overdue + alive): {n_due:,} "
         f"({n_due/len(candidates)*100:.1f}% of joined rows)")

    # Filter to candidates only
    out = candidates[candidates["is_replenishment_candidate"] == 1].copy()

    if len(out) == 0:
        _log("  No replenishment candidates found")
        return pd.DataFrame()

    _log(f"  Customers with at least one replenishment candidate: "
         f"{out['cust_id'].nunique():,}")
    _log(f"  Distribution of overdue_ratio:")
    v = out["overdue_ratio"]
    _log(f"    p10={v.quantile(0.10):.2f}x  median={v.quantile(0.50):.2f}x  "
         f"p90={v.quantile(0.90):.2f}x  max={v.max():.2f}x")
    _log(f"  Distribution of segment median cadence:")
    v = out["median_days_between_segment"]
    _log(f"    p10={v.quantile(0.10):.0f}d  median={v.quantile(0.50):.0f}d  "
         f"p90={v.quantile(0.90):.0f}d")

    # Final dtypes
    out["n_purchases"]                  = out["n_purchases"].astype("int32")
    out["personal_avg_days_between"]    = out["personal_avg_days_between"].astype("float32")
    out["median_days_between_segment"]  = out["median_days_between_segment"].astype("float32")
    out["peer_activity_rate"]           = out["peer_activity_rate"].astype("float32")
    out["n_buyers_segment"]             = out["n_buyers_segment"].astype("int32")

    _log(f"Step 7 done in {time.time()-t0:.1f}s")
    return out


# Step 8: Combine all metrics

def build_customer_patterns(
    cadence: pd.DataFrame,
    trends: pd.DataFrame,
    portfolio: pd.DataFrame,
) -> pd.DataFrame:
    _s("Step 8: Combining into customer_patterns dataframe")

    _log("Merging cadence + trends + portfolio...")
    patterns = cadence.merge(trends, on="cust_id", how="outer")
    patterns = patterns.merge(portfolio, on="cust_id", how="outer")

    int_cols = ["n_order_dates", "active_span_days",
                "total_unique_products", "unique_products_last_6mo",
                "unique_products_last_3mo", "unique_products_historical",
                "n_lapsed_products", "n_active_products",
                "n_unique_products_total", "is_declining", "is_churned"]
    for c in int_cols:
        if c in patterns.columns:
            patterns[c] = patterns[c].fillna(0).astype("int32")

    float_cols = ["avg_days_between_orders", "total_spend_all_time",
                  "spend_last_6mo", "spend_last_3mo", "spend_historical",
                  "lapsed_rate"]
    for c in float_cols:
        if c in patterns.columns:
            patterns[c] = patterns[c].fillna(0.0).astype("float32")

    if "order_cadence_tier" in patterns.columns:
        patterns["order_cadence_tier"] = patterns["order_cadence_tier"].fillna("no_data")

    patterns["is_cold_start"] = (
        (patterns["n_unique_products_total"] < 5) |
        (patterns["order_cadence_tier"] == "single_order") |
        (patterns["order_cadence_tier"] == "no_data")
    ).astype("int32")

    patterns["is_single_order_customer"] = (
        patterns["order_cadence_tier"] == "single_order"
    ).astype("int32")

    patterns["is_cold_start"]            = patterns["is_cold_start"].astype("int32")
    patterns["is_single_order_customer"] = patterns["is_single_order_customer"].astype("int32")

    if SEG_FILE.exists():
        _log("Joining customer_segments info...")
        segs = pd.read_parquet(SEG_FILE, columns=[
            "DIM_CUST_CURR_ID", "segment", "size_tier", "mkt_cd_clean"
        ])
        segs = segs.rename(columns={"DIM_CUST_CURR_ID": "cust_id"})
        segs["cust_id"] = segs["cust_id"].astype("int64")
        patterns = patterns.merge(segs, on="cust_id", how="left")

    patterns = patterns.rename(columns={"cust_id": "DIM_CUST_CURR_ID"})

    _log(f"  Final customer_patterns rows: {len(patterns):,}")
    _log(f"  Columns: {len(patterns.columns)}")

    return patterns


# Step 9: Save outputs

def save_outputs(
    patterns: pd.DataFrame,
    lapsed: pd.DataFrame,
    seg_cadence: pd.DataFrame,
    replenishment: pd.DataFrame,
) -> None:
    _s("Step 9: Saving outputs")

    OUT_PRECOMP.mkdir(parents=True, exist_ok=True)
    OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

    # customer_patterns.parquet
    patterns.to_parquet(PATTERNS_OUT, index=False)
    size_mb = PATTERNS_OUT.stat().st_size / (1024 * 1024)
    _log(f"Saved: {PATTERNS_OUT.relative_to(ROOT)}  ({size_mb:.1f} MB, "
         f"{len(patterns):,} customers, {len(patterns.columns)} cols)")

    # customer_lapsed_products.parquet
    lapsed_save = lapsed.rename(columns={
        "cust_id": "DIM_CUST_CURR_ID",
        "item_id": "DIM_ITEM_E1_CURR_ID",
    })
    lapsed_save.to_parquet(LAPSED_OUT, index=False)
    size_mb = LAPSED_OUT.stat().st_size / (1024 * 1024)
    _log(f"Saved: {LAPSED_OUT.relative_to(ROOT)}  ({size_mb:.1f} MB, "
         f"{len(lapsed_save):,} lapsed pairs)")

    # NEW Phase 6: product_segment_cadence.parquet
    if len(seg_cadence) > 0:
        seg_save = seg_cadence.rename(columns={"item_id": "DIM_ITEM_E1_CURR_ID"})
        seg_save.to_parquet(SEGMENT_CADENCE_OUT, index=False)
        size_mb = SEGMENT_CADENCE_OUT.stat().st_size / (1024 * 1024)
        _log(f"Saved: {SEGMENT_CADENCE_OUT.relative_to(ROOT)}  ({size_mb:.1f} MB, "
             f"{len(seg_save):,} (segment x product) pairs)")
    else:
        _log(f"  Skipped: {SEGMENT_CADENCE_OUT.name} (no data)")

    # NEW Phase 6: customer_replenishment_candidates.parquet
    if len(replenishment) > 0:
        repl_save = replenishment.rename(columns={
            "cust_id": "DIM_CUST_CURR_ID",
            "item_id": "DIM_ITEM_E1_CURR_ID",
        })
        repl_save.to_parquet(REPLENISHMENT_OUT, index=False)
        size_mb = REPLENISHMENT_OUT.stat().st_size / (1024 * 1024)
        _log(f"Saved: {REPLENISHMENT_OUT.relative_to(ROOT)}  ({size_mb:.1f} MB, "
             f"{len(repl_save):,} replenishment candidates)")
    else:
        _log(f"  Skipped: {REPLENISHMENT_OUT.name} (no data)")

    # Sample xlsx for inspection
    sample = patterns.nlargest(500, "total_spend_all_time").copy()
    sample.to_excel(PATTERNS_XLSX, index=False, engine="openpyxl")
    size_kb = PATTERNS_XLSX.stat().st_size / 1024
    _log(f"Saved: {PATTERNS_XLSX.relative_to(ROOT)}  ({size_kb:.0f} KB, top 500 by spend)")


# Step 10: Print stats and samples

def print_stats(
    patterns: pd.DataFrame,
    lapsed: pd.DataFrame,
    seg_cadence: pd.DataFrame,
    replenishment: pd.DataFrame,
) -> None:
    _s("Step 10: Distribution summary and samples")

    _log(f"Total customers with patterns: {len(patterns):,}")
    _log("")

    _log("Order cadence distribution:")
    if "order_cadence_tier" in patterns.columns:
        for tier, n in patterns["order_cadence_tier"].value_counts().items():
            pct = n / len(patterns) * 100
            _log(f"  {tier:<15}  {n:>8,} customers  ({pct:.1f}%)")

    _log("")
    _log("Activity status breakdown:")
    if "is_churned" in patterns.columns and "is_declining" in patterns.columns:
        n_churned   = int(patterns["is_churned"].sum())
        n_declining = int(patterns["is_declining"].sum())
        n_stable    = len(patterns) - n_churned - n_declining
        _log(f"  Churned   : {n_churned:,} ({n_churned/len(patterns)*100:.1f}%)")
        _log(f"  Declining : {n_declining:,} ({n_declining/len(patterns)*100:.1f}%)")
        _log(f"  Stable    : {n_stable:,} ({n_stable/len(patterns)*100:.1f}%)")

    _log("")
    _log("Data-sufficiency flags:")
    if "is_cold_start" in patterns.columns:
        n_cold = int(patterns["is_cold_start"].sum())
        _log(f"  Cold-start : {n_cold:,} ({n_cold/len(patterns)*100:.1f}%)")
    if "is_single_order_customer" in patterns.columns:
        n_single = int(patterns["is_single_order_customer"].sum())
        _log(f"  Single-order : {n_single:,} ({n_single/len(patterns)*100:.1f}%)")

    _log("")
    _log("Lapsed products:")
    _log(f"  Total lapsed customer-product pairs: {len(lapsed):,}")
    _log(f"  Unique customers with lapsed products: "
         f"{lapsed['cust_id'].nunique():,}")

    _log("")
    _log("Phase 6: Segment-product cadence:")
    if len(seg_cadence) > 0:
        _log(f"  Total (segment x product) pairs: {len(seg_cadence):,}")
        n_alive = int(seg_cadence["is_alive"].sum())
        n_reliable = int(seg_cadence["cadence_is_reliable"].sum())
        _log(f"  Alive in segment              : {n_alive:,}")
        _log(f"  Reliable cadence              : {n_reliable:,}")

    _log("")
    _log("Phase 6: Replenishment candidates:")
    if len(replenishment) > 0:
        _log(f"  Total candidates             : {len(replenishment):,}")
        _log(f"  Customers with candidates    : {replenishment['cust_id'].nunique():,}")
        _log(f"  Mean candidates per customer : "
             f"{len(replenishment) / max(replenishment['cust_id'].nunique(), 1):.1f}")

        # Top 3 sample customers with most replenishment candidates
        top_repl_custs = (
            replenishment.groupby("cust_id").size().nlargest(3).index.tolist()
        )
        if top_repl_custs:
            _log("")
            _log("  Sample 5: Top 3 customers with most replenishment candidates:")
            for cid in top_repl_custs:
                rows = replenishment[replenishment["cust_id"] == cid]
                cust_pat = patterns[patterns["DIM_CUST_CURR_ID"] == cid]
                seg = "?"
                if len(cust_pat) > 0:
                    seg = str(cust_pat.iloc[0].get("segment", "?"))
                _log(f"    Customer {int(cid)} ({seg}): {len(rows)} candidates")
                # Show top 3 most-overdue items
                top_items = rows.nlargest(3, "overdue_ratio")
                for _, r in top_items.iterrows():
                    _log(f"      Item {int(r['item_id'])}: "
                         f"{int(r['days_since_last'])}d since last, "
                         f"segment cadence {r['median_days_between_segment']:.0f}d, "
                         f"overdue {r['overdue_ratio']:.2f}x")
    else:
        _log("  (no candidates - check Phase 6 thresholds)")


# Main

def main() -> None:
    print()
    print("=" * 64)
    print("  CUSTOMER BUYING PATTERNS ANALYSIS - Phase 6")
    print("  (Medline kept in + Replenishment cadence)")
    print("=" * 64)
    start = time.time()

    OUT_PRECOMP.mkdir(parents=True, exist_ok=True)
    OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        max_date, _min_date = load_customer_transactions(con)
        cadence       = compute_order_cadence(con)
        trends        = compute_activity_trends(con, max_date)
        lapsed        = find_lapsed_products(con, max_date)
        portfolio     = compute_portfolio_metrics(con, lapsed)
        seg_cadence   = compute_segment_product_cadence(con, max_date)
        replenishment = find_replenishment_candidates(con, seg_cadence, max_date)
    finally:
        con.close()

    patterns = build_customer_patterns(cadence, trends, portfolio)
    save_outputs(patterns, lapsed, seg_cadence, replenishment)
    print_stats(patterns, lapsed, seg_cadence, replenishment)

    _s("Complete")
    _log(f"Total time: {time.time() - start:.1f}s")
    _log("")
    _log("Outputs:")
    _log(f"  customer_patterns.parquet                ({len(patterns):,} customers)")
    _log(f"  customer_lapsed_products.parquet         ({len(lapsed):,} pairs)")
    _log(f"  product_segment_cadence.parquet          "
         f"({len(seg_cadence):,} (seg x prod) pairs)")
    _log(f"  customer_replenishment_candidates.parquet "
         f"({len(replenishment):,} candidates)")
    _log("")
    _log("Used by recommendation_factors.py for:")
    _log("  - Order cadence matching (frequent/regular/occasional)")
    _log("  - Lapsed recovery recommendations (>6 months ago)")
    _log("  - Declining/churned customer detection")
    _log("  - Cold-start detection (< 5 unique products)")
    _log("  - REPLENISHMENT signal (Phase 6) - peer-validated reorder candidates")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"\nFATAL ERROR: {exc}", file=sys.stderr)
        raise