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
PRODUCT_FILE = DATA_CLEAN / "product"  / "products_clean.parquet"
FEATURE_FILE = DATA_CLEAN / "features" / "customer_features.parquet"
SEG_FILE     = DATA_CLEAN / "serving"  / "precomputed" / "customer_segments.parquet"

OUT_PRECOMP  = DATA_CLEAN / "serving"  / "precomputed"
OUT_ANALYSIS = DATA_CLEAN / "analysis"


# Configuration

FISCAL_YEARS           = ("FY2425", "FY2526")
MIN_BUYERS_PER_PRODUCT = 50
RECENT_WINDOW_DAYS     = 180   # 6 months for recency signals
TOP_N_SEGMENTS         = 3     # Track top-3 segments per product

EXCLUDED_SUPPLIERS = {"MEDLINE", "MEDLINE INDUSTRIES"}
EXCLUDED_FAMILIES  = {"Fee", "Unknown", "NaN", "nan", ""}


# Logging

def _s(title: str) -> None:
    print(f"\n{'-' * 64}\n  {title}\n{'-' * 64}", flush=True)


def _log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def _pq(p: Path) -> str:
    return "'" + p.as_posix() + "'"


# Step 1: Load transactions with customer context

def load_transactions_with_customer_context() -> tuple[pd.DataFrame, str]:
    # Returns: (transactions dataframe, max fiscal date string)

    _s("Step 1: Loading transactions with customer context")
    t0 = time.time()

    if not MERGED_FILE.exists():
        print(f"\nFATAL: merged_dataset.parquet not found at {MERGED_FILE}",
              file=sys.stderr)
        sys.exit(1)

    if not FEATURE_FILE.exists():
        print(f"\nFATAL: customer_features.parquet not found at {FEATURE_FILE}",
              file=sys.stderr)
        sys.exit(1)

    if not SEG_FILE.exists():
        print(f"\nFATAL: customer_segments.parquet not found at {SEG_FILE}",
              file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect()

    # Detect columns
    desc = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({_pq(MERGED_FILE)}) LIMIT 0"
    ).df()
    available = set(desc["column_name"].tolist())

    family_col = next(
        (c for c in ["PROD_FMLY_LVL1_DSC", "PROD_FMLY_LVL1_CD"]
         if c in available), None
    )
    supplier_col = next(
        (c for c in ["SUPLR_ROLLUP_DSC", "SUPLR_DSC"] if c in available), None
    )
    # Check for separate year/month/day columns (this dataset's convention)
    has_date_parts = all(
        c in available for c in ["order_year", "order_month", "order_day"]
    )
    date_expr = None
    if has_date_parts:
        date_expr = "MAKE_DATE(order_year, order_month, order_day)"

    _log(f"Product family column : {family_col}")
    _log(f"Supplier column       : {supplier_col}")
    _log(f"Date construction     : {date_expr or 'not available'}")

    # Find the most recent date in the dataset
    if date_expr:
        max_date_row = con.execute(
            f"SELECT MAX({date_expr}) AS max_dt "
            f"FROM read_parquet({_pq(MERGED_FILE)})"
        ).df()
        max_date = str(max_date_row["max_dt"].iloc[0])
        _log(f"Max transaction date  : {max_date}")
    else:
        max_date = "UNKNOWN"
        _log(f"Date parts not found - recency signals will be skipped")

    # Filters
    fy_sql = ", ".join(f"'{fy}'" for fy in FISCAL_YEARS)
    family_filter = ""
    if family_col:
        excl_fams = ", ".join(f"'{f}'" for f in sorted(EXCLUDED_FAMILIES))
        family_filter = f"AND COALESCE({family_col}, 'Unknown') NOT IN ({excl_fams})"

    supplier_filter = ""
    if supplier_col:
        excl_sups = ", ".join(f"'{s}'" for s in EXCLUDED_SUPPLIERS)
        supplier_filter = f"AND UPPER(COALESCE({supplier_col}, '')) NOT IN ({excl_sups})"

    # Date expression for recency
    if date_expr:
        date_select = (
            f"MAX({date_expr}) AS last_purchase_dt, "
            f"MIN({date_expr}) AS first_purchase_dt"
        )
    else:
        date_select = (
            "CAST(NULL AS DATE) AS last_purchase_dt, "
            "CAST(NULL AS DATE) AS first_purchase_dt"
        )

    # Aggregate to customer-product level with date info and price variance
    _log("Aggregating transactions via DuckDB...")
    txn = con.execute(f"""
        SELECT
            CAST(DIM_CUST_CURR_ID AS BIGINT)      AS cust_id,
            CAST(DIM_ITEM_E1_CURR_ID AS BIGINT)    AS item_id,
            COUNT(*)                               AS line_count,
            SUM(UNIT_SLS_AMT)                      AS total_spend,
            AVG(UNIT_SLS_AMT)                      AS avg_unit_price,
            STDDEV_POP(UNIT_SLS_AMT)               AS std_unit_price,
            {date_select}
        FROM read_parquet({_pq(MERGED_FILE)})
        WHERE UNIT_SLS_AMT > 0
          AND fiscal_year IN ({fy_sql})
          AND DIM_CUST_CURR_ID IS NOT NULL
          AND DIM_ITEM_E1_CURR_ID IS NOT NULL
          {family_filter}
          {supplier_filter}
        GROUP BY
            CAST(DIM_CUST_CURR_ID AS BIGINT),
            CAST(DIM_ITEM_E1_CURR_ID AS BIGINT)
    """).df()
    con.close()

    _log(f"Loaded {len(txn):,} customer-product pairs in {time.time()-t0:.1f}s")
    _log(f"  Unique customers: {txn['cust_id'].nunique():,}")
    _log(f"  Unique products : {txn['item_id'].nunique():,}")

    # Filter to products with enough buyers
    buyer_count = txn.groupby("item_id")["cust_id"].nunique()
    eligible_items = set(buyer_count[buyer_count >= MIN_BUYERS_PER_PRODUCT].index)

    before = txn["item_id"].nunique()
    txn = txn[txn["item_id"].isin(eligible_items)]
    after = txn["item_id"].nunique()
    _log(f"")
    _log(f"Filtered to products with >= {MIN_BUYERS_PER_PRODUCT} buyers:")
    _log(f"  Before : {before:,} products")
    _log(f"  After  : {after:,} products")

    # Join customer context
    _log(f"")
    _log("Joining customer context...")
    t1 = time.time()

    segs = pd.read_parquet(SEG_FILE)
    feat = pd.read_parquet(FEATURE_FILE, columns=[
        "DIM_CUST_CURR_ID",
        "median_monthly_spend",
        "affordability_ceiling",
        "active_months_last_12",
    ])

    custs = segs.merge(feat, on="DIM_CUST_CURR_ID", how="left")
    custs = custs.rename(columns={"DIM_CUST_CURR_ID": "cust_id"})
    custs["cust_id"] = custs["cust_id"].astype("int64")

    txn = txn.merge(custs, on="cust_id", how="left")

    _log(f"Customer context joined in {time.time()-t1:.1f}s")
    _log(f"  Rows after join: {len(txn):,}")
    _log(f"  Rows missing size_tier: {txn['size_tier'].isna().sum():,}")

    # Convert date columns
    if date_expr:
        txn["last_purchase_dt"]  = pd.to_datetime(txn["last_purchase_dt"],  errors="coerce")
        txn["first_purchase_dt"] = pd.to_datetime(txn["first_purchase_dt"], errors="coerce")

    return txn, max_date


# Step 2: Compute per-product behavior measurements

def compute_product_measurements(
    txn: pd.DataFrame, max_date: str
) -> pd.DataFrame:

    _s("Step 2: Computing per-product measurements")
    t0 = time.time()

    # Basic aggregations
    _log("Computing basic aggregates...")
    per_product = txn.groupby("item_id").agg(
        n_buyers=("cust_id", "nunique"),
        total_transactions=("line_count", "sum"),
        total_spend=("total_spend", "sum"),
        median_line_value=("total_spend", "median"),
        median_unit_price=("avg_unit_price", "median"),
        mean_unit_price=("avg_unit_price", "mean"),
        std_of_means=("avg_unit_price", "std"),
    ).reset_index()

    _log(f"  Basic aggregates computed for {len(per_product):,} products")

    _log("Computing price stability (unit_price_cv)...")
    per_product["unit_price_cv"] = (
        per_product["std_of_means"] / per_product["mean_unit_price"].replace(0, np.nan)
    ).fillna(0.0)
    per_product = per_product.drop(columns=["mean_unit_price", "std_of_means"])

    # Repeat purchase rate
    _log("Computing repeat purchase rate...")
    repeat_stats = txn.groupby("item_id").agg(
        n_buyers_with_repeat=("line_count", lambda x: (x > 1).sum()),
    ).reset_index()
    per_product = per_product.merge(repeat_stats, on="item_id", how="left")
    per_product["repeat_purchase_rate"] = (
        per_product["n_buyers_with_repeat"] / per_product["n_buyers"]
    )
    per_product = per_product.drop(columns=["n_buyers_with_repeat"])

    _log("Computing tenure-adjusted purchase frequency...")
    txn_copy = txn.copy()
    txn_copy["tenure_months"] = txn_copy["active_months_last_12"].fillna(6).clip(lower=3)
    txn_copy["annualized_rate"] = (
        txn_copy["line_count"] / (txn_copy["tenure_months"] / 12.0)
    )

    freq_stats = txn_copy.groupby("item_id").agg(
        avg_purchases_per_buyer_per_year=("annualized_rate", "mean"),
        median_purchases_per_buyer_per_year=("annualized_rate", "median"),
    ).reset_index()

    per_product = per_product.merge(freq_stats, on="item_id", how="left")

    if "last_purchase_dt" in txn.columns and txn["last_purchase_dt"].notna().any():
        _log("Computing recency signals...")

        max_dt = pd.to_datetime(max_date)
        cutoff_dt = max_dt - pd.Timedelta(days=RECENT_WINDOW_DAYS)

        recency_stats = txn.groupby("item_id").agg(
            latest_purchase_dt=("last_purchase_dt", "max"),
            recent_buyer_count_6mo=(
                "last_purchase_dt",
                lambda x: (x >= cutoff_dt).sum()
            ),
        ).reset_index()

        recency_stats["months_since_last_buyer"] = (
            (max_dt - recency_stats["latest_purchase_dt"]).dt.days / 30.0
        ).round(1)

        recency_stats = recency_stats.drop(columns=["latest_purchase_dt"])
        per_product = per_product.merge(recency_stats, on="item_id", how="left")
    else:
        _log("Skipping recency signals (no date column)")
        per_product["recent_buyer_count_6mo"] = 0
        per_product["months_since_last_buyer"] = -1.0

    _log(f"  Behavior signals computed in {time.time()-t0:.1f}s")

    return per_product


# Step 3: Buyer mix (markets, size tiers, and segments)

def compute_buyer_mix(
    txn: pd.DataFrame, per_product: pd.DataFrame
) -> pd.DataFrame:
    # LOOPHOLE 7 FIX: Add segment-level mix (top 3 segments per product)

    _s("Step 3: Computing buyer mix")
    t0 = time.time()

    # One customer counted once per product
    unique_pairs = txn[[
        "item_id", "cust_id", "mkt_cd_clean", "size_tier", "segment"
    ]].drop_duplicates()

    # Market mix
    _log("Computing market distribution...")
    market_mix = unique_pairs.groupby(
        ["item_id", "mkt_cd_clean"]
    ).size().reset_index(name="n")

    market_wide = market_mix.pivot_table(
        index="item_id",
        columns="mkt_cd_clean",
        values="n",
        fill_value=0,
    ).reset_index()

    market_cols = [c for c in market_wide.columns if c != "item_id"]
    total_market = market_wide[market_cols].sum(axis=1).replace(0, 1)
    for col in market_cols:
        market_wide[f"pct_buyers_{col}"] = market_wide[col] / total_market
        market_wide = market_wide.drop(columns=[col])

    pct_mkt_cols = [c for c in market_wide.columns if c.startswith("pct_buyers_")]

    # Market HHI
    market_wide["market_hhi"] = (market_wide[pct_mkt_cols] ** 2).sum(axis=1)

    # Primary market
    market_wide["primary_market"] = market_wide[pct_mkt_cols].idxmax(axis=1).str.replace(
        "pct_buyers_", "", regex=False
    )
    market_wide["primary_market_pct"] = market_wide[pct_mkt_cols].max(axis=1)

    _log(f"  Market distribution computed for {len(market_wide):,} products")

    # Size tier mix
    _log("Computing size tier distribution...")
    size_mix = unique_pairs.groupby(
        ["item_id", "size_tier"]
    ).size().reset_index(name="n")

    size_wide = size_mix.pivot_table(
        index="item_id",
        columns="size_tier",
        values="n",
        fill_value=0,
    ).reset_index()

    size_cols = [c for c in size_wide.columns if c != "item_id"]
    total_size = size_wide[size_cols].sum(axis=1).replace(0, 1)
    for col in size_cols:
        size_wide[f"pct_buyers_{col}"] = size_wide[col] / total_size
        size_wide = size_wide.drop(columns=[col])

    _log(f"  Size distribution computed for {len(size_wide):,} products")

    # LOOPHOLE 7 FIX: Top-3 segment mix
    _log(f"Computing top-{TOP_N_SEGMENTS} segment distribution per product...")
    seg_mix = unique_pairs.groupby(
        ["item_id", "segment"]
    ).size().reset_index(name="n")

    total_per_item = seg_mix.groupby("item_id")["n"].sum().rename("total").reset_index()
    seg_mix = seg_mix.merge(total_per_item, on="item_id")
    seg_mix["pct"] = seg_mix["n"] / seg_mix["total"]

    seg_mix["rank"] = seg_mix.groupby("item_id")["pct"].rank(
        method="first", ascending=False
    )
    seg_mix = seg_mix[seg_mix["rank"] <= TOP_N_SEGMENTS]

    # Pivot to top_segment_1, top_segment_1_pct, etc.
    top_segs_rows = []
    for item_id, grp in seg_mix.groupby("item_id"):
        grp = grp.sort_values("rank")
        row = {"item_id": item_id}
        for _, r in grp.iterrows():
            rk = int(r["rank"])
            row[f"top_segment_{rk}"] = r["segment"]
            row[f"top_segment_{rk}_pct"] = float(r["pct"])
        top_segs_rows.append(row)
    top_segs = pd.DataFrame(top_segs_rows)

    # Ensure all 3 columns exist
    for i in range(1, TOP_N_SEGMENTS + 1):
        if f"top_segment_{i}" not in top_segs.columns:
            top_segs[f"top_segment_{i}"] = ""
        if f"top_segment_{i}_pct" not in top_segs.columns:
            top_segs[f"top_segment_{i}_pct"] = 0.0

    _log(f"  Segment mix computed for {len(top_segs):,} products")

    # Segment HHI
    _log("Computing segment HHI...")
    all_seg_mix = unique_pairs.groupby(["item_id", "segment"]).size().reset_index(name="n")
    total_seg = all_seg_mix.groupby("item_id")["n"].sum().rename("total").reset_index()
    all_seg_mix = all_seg_mix.merge(total_seg, on="item_id")
    all_seg_mix["pct_sq"] = (all_seg_mix["n"] / all_seg_mix["total"]) ** 2
    seg_hhi = all_seg_mix.groupby("item_id")["pct_sq"].sum().rename("segment_hhi").reset_index()

    # Merge everything
    per_product = per_product.merge(market_wide, on="item_id", how="left")
    per_product = per_product.merge(size_wide, on="item_id", how="left")
    per_product = per_product.merge(top_segs, on="item_id", how="left")
    per_product = per_product.merge(seg_hhi, on="item_id", how="left")

    # Fill missing pct columns with 0
    for col in per_product.columns:
        if col.startswith("pct_buyers_"):
            per_product[col] = per_product[col].fillna(0.0)

    _log(f"Buyer mix complete in {time.time()-t0:.1f}s")

    return per_product


# Step 4: Affordability signals

def compute_affordability_signals(
    txn: pd.DataFrame, per_product: pd.DataFrame
) -> pd.DataFrame:
    _s("Step 4: Computing affordability signals")
    t0 = time.time()

    unique_pairs = txn[[
        "item_id", "cust_id", "affordability_ceiling", "median_monthly_spend"
    ]].drop_duplicates(subset=["item_id", "cust_id"])

    _log("Computing affordability percentiles...")
    afford_stats = unique_pairs.groupby("item_id").agg(
        buyer_affordability_p10=("affordability_ceiling", lambda x: x.quantile(0.10)),
        buyer_affordability_p50=("affordability_ceiling", lambda x: x.quantile(0.50)),
        buyer_affordability_p90=("affordability_ceiling", lambda x: x.quantile(0.90)),
        buyer_monthly_spend_p50=("median_monthly_spend", lambda x: x.quantile(0.50)),
    ).reset_index()

    per_product = per_product.merge(afford_stats, on="item_id", how="left")

    _log(f"Affordability signals computed in {time.time()-t0:.1f}s")

    return per_product


# Step 5: Adoption rate and within-family price percentile

def compute_adoption_and_price_tier(
    per_product: pd.DataFrame, total_customers: int
) -> pd.DataFrame:
    # LOOPHOLE 6 FIX: specialty_score combines HHI with adoption

    _s("Step 5: Computing adoption rate and price tier")
    t0 = time.time()

    per_product["adoption_rate"] = per_product["n_buyers"] / total_customers
    _log(f"  Total customers for adoption rate: {total_customers:,}")

    # Price percentile within family
    if not PRODUCT_FILE.exists():
        _log("  Warning: products_clean.parquet not found - skipping price percentile")
        per_product["price_percentile_in_family"] = 0.5
    else:
        _log("  Joining product family for price percentile...")
        products = pd.read_parquet(PRODUCT_FILE, columns=[
            "DIM_ITEM_E1_CURR_ID", "PROD_FMLY_LVL1_DSC"
        ])
        products["DIM_ITEM_E1_CURR_ID"] = products["DIM_ITEM_E1_CURR_ID"].astype("int64")
        products = products.rename(columns={"DIM_ITEM_E1_CURR_ID": "item_id"})

        per_product["item_id"] = per_product["item_id"].astype("int64")
        per_product = per_product.merge(products, on="item_id", how="left")

        per_product["price_percentile_in_family"] = per_product.groupby(
            "PROD_FMLY_LVL1_DSC"
        )["median_unit_price"].rank(pct=True, method="average")

        per_product["price_percentile_in_family"] = per_product[
            "price_percentile_in_family"
        ].fillna(0.5)

    # A product is truly specialty only if it has BOTH high HHI AND meaningful adoption.
    # Formula: market_hhi * sqrt(adoption_rate)
    # Noise product with HHI=1 but 0.001 adoption gets score ~0.03 (low)
    # Real specialty with HHI=0.8 and 0.1 adoption gets score ~0.25 (high)
    _log("Computing specialty_score (HHI adjusted for adoption)...")
    per_product["specialty_score"] = (
        per_product["market_hhi"] * np.sqrt(per_product["adoption_rate"])
    )

    _log(f"Adoption and price tier computed in {time.time()-t0:.1f}s")

    return per_product


# Step 6: Enrich with product metadata

def enrich_with_product_metadata(per_product: pd.DataFrame) -> pd.DataFrame:
    _s("Step 6: Enriching with product metadata")

    if not PRODUCT_FILE.exists():
        _log("products_clean.parquet not found - skipping")
        return per_product

    # PROD_FMLY_LVL1_DSC already merged in step 5
    products = pd.read_parquet(PRODUCT_FILE, columns=[
        "DIM_ITEM_E1_CURR_ID", "ITEM_DSC",
        "PROD_CTGRY_LVL2_DSC", "SUPLR_ROLLUP_DSC",
        "is_private_brand", "is_discontinued",
    ])
    products["DIM_ITEM_E1_CURR_ID"] = products["DIM_ITEM_E1_CURR_ID"].astype("int64")
    products = products.rename(columns={"DIM_ITEM_E1_CURR_ID": "item_id"})

    per_product["item_id"] = per_product["item_id"].astype("int64")
    per_product = per_product.merge(products, on="item_id", how="left")

    _log(f"Metadata merged: {len(per_product):,} products")
    _log(f"  Products with is_private_brand: "
         f"{int(per_product['is_private_brand'].sum()):,}")
    _log(f"  Products with is_discontinued : "
         f"{int(per_product['is_discontinued'].sum()):,}")

    return per_product


# Step 7: Save

def save_outputs(per_product: pd.DataFrame) -> None:
    _s("Step 7: Saving outputs")

    OUT_PRECOMP.mkdir(parents=True, exist_ok=True)
    OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

    # Final column order
    identity_cols = [
        "item_id",
        "ITEM_DSC",
        "PROD_FMLY_LVL1_DSC",
        "PROD_CTGRY_LVL2_DSC",
        "SUPLR_ROLLUP_DSC",
        "is_private_brand",
        "is_discontinued",
    ]

    behavior_cols = [
        "n_buyers",
        "adoption_rate",
        "total_transactions",
        "total_spend",
        "repeat_purchase_rate",
        "avg_purchases_per_buyer_per_year",
        "median_purchases_per_buyer_per_year",
        "median_line_value",
        "recent_buyer_count_6mo",
        "months_since_last_buyer",
    ]

    economic_cols = [
        "median_unit_price",
        "unit_price_cv",
        "price_percentile_in_family",
    ]

    affordability_cols = [
        "buyer_affordability_p10",
        "buyer_affordability_p50",
        "buyer_affordability_p90",
        "buyer_monthly_spend_p50",
    ]

    market_cols = sorted([c for c in per_product.columns
                          if c.startswith("pct_buyers_") and
                          c.replace("pct_buyers_", "") in
                          {"PO", "LTC", "SC", "LC", "HC", "AC", "OTHER"}])

    size_cols = sorted([c for c in per_product.columns
                        if c.startswith("pct_buyers_") and
                        c.replace("pct_buyers_", "") in
                        {"new", "small", "mid", "large", "enterprise"}])

    concentration_cols = [
        "primary_market",
        "primary_market_pct",
        "market_hhi",
        "segment_hhi",
        "specialty_score",
    ]

    top_segment_cols = [
        "top_segment_1", "top_segment_1_pct",
        "top_segment_2", "top_segment_2_pct",
        "top_segment_3", "top_segment_3_pct",
    ]

    final_cols = (
        identity_cols + behavior_cols + economic_cols +
        affordability_cols + market_cols + size_cols +
        concentration_cols + top_segment_cols
    )

    final_cols = [c for c in final_cols if c in per_product.columns]
    per_product = per_product[final_cols]

    per_product = per_product.rename(columns={"item_id": "DIM_ITEM_E1_CURR_ID"})

    # Type enforcement
    per_product["DIM_ITEM_E1_CURR_ID"]   = per_product["DIM_ITEM_E1_CURR_ID"].astype("int64")
    per_product["n_buyers"]               = per_product["n_buyers"].astype("int64")
    per_product["total_transactions"]     = per_product["total_transactions"].astype("int64")
    per_product["recent_buyer_count_6mo"] = per_product["recent_buyer_count_6mo"].astype("int64")

    float32_cols = [
        "adoption_rate", "repeat_purchase_rate",
        "avg_purchases_per_buyer_per_year",
        "median_purchases_per_buyer_per_year",
        "months_since_last_buyer",
        "unit_price_cv", "price_percentile_in_family",
        "primary_market_pct", "market_hhi", "segment_hhi", "specialty_score",
        "top_segment_1_pct", "top_segment_2_pct", "top_segment_3_pct",
    ]
    for col in float32_cols:
        if col in per_product.columns:
            per_product[col] = per_product[col].astype("float32")

    for col in per_product.columns:
        if col.startswith("pct_buyers_"):
            per_product[col] = per_product[col].astype("float32")

    per_product = per_product.sort_values("n_buyers", ascending=False).reset_index(drop=True)

    # Save
    out_path = OUT_PRECOMP / "product_segments.parquet"
    per_product.to_parquet(out_path, index=False)
    size_mb = out_path.stat().st_size / (1024 * 1024)
    _log(f"Saved: {out_path.relative_to(ROOT)}  "
         f"({size_mb:.1f} MB, {len(per_product):,} products, "
         f"{len(per_product.columns)} cols)")

    # Top 1000 for inspection
    top_1000 = per_product.head(1000)
    excel_path = OUT_ANALYSIS / "product_segments_top1000.xlsx"
    top_1000.to_excel(excel_path, index=False, engine="openpyxl")
    size_mb = excel_path.stat().st_size / (1024 * 1024)
    _log(f"Saved: {excel_path.relative_to(ROOT)}  ({size_mb:.1f} MB, top 1000)")


# Step 8: Distribution stats

def print_distribution_stats(per_product: pd.DataFrame) -> None:
    _s("Step 8: Measurement distribution summary")

    _log(f"Total products measured: {len(per_product):,}")
    _log(f"Total columns per product: {len(per_product.columns)}")
    _log("")

    _log("Behavior signals - distribution:")
    for col in ["n_buyers", "repeat_purchase_rate",
                "avg_purchases_per_buyer_per_year",
                "median_purchases_per_buyer_per_year",
                "median_line_value", "recent_buyer_count_6mo"]:
        if col in per_product.columns:
            vals = per_product[col]
            _log(f"  {col:<46}  "
                 f"p10={vals.quantile(0.10):>10,.2f}  "
                 f"median={vals.quantile(0.50):>10,.2f}  "
                 f"p90={vals.quantile(0.90):>10,.2f}")

    _log("")
    _log("Recency signal (months since last buyer):")
    if "months_since_last_buyer" in per_product.columns:
        vals = per_product["months_since_last_buyer"]
        very_active = (vals <= 0.5).sum()
        active = ((vals > 0.5) & (vals <= 3)).sum()
        slow = ((vals > 3) & (vals <= 6)).sum()
        stale = (vals > 6).sum()
        _log(f"  Very active (<= 2 weeks)  : {very_active:>6,} products  ({very_active/len(per_product)*100:.1f}%)")
        _log(f"  Active (2 weeks - 3 mo)   : {active:>6,} products  ({active/len(per_product)*100:.1f}%)")
        _log(f"  Slow (3-6 months)         : {slow:>6,} products  ({slow/len(per_product)*100:.1f}%)")
        _log(f"  Stale (> 6 months)        : {stale:>6,} products  ({stale/len(per_product)*100:.1f}%)")

    _log("")
    _log("Price stability (unit_price_cv):")
    if "unit_price_cv" in per_product.columns:
        vals = per_product["unit_price_cv"]
        stable = (vals < 0.2).sum()
        moderate = ((vals >= 0.2) & (vals < 0.5)).sum()
        variable = (vals >= 0.5).sum()
        _log(f"  Stable (CV < 0.2)       : {stable:>6,} products  "
             f"({stable/len(per_product)*100:.1f}%)")
        _log(f"  Moderate (CV 0.2-0.5)   : {moderate:>6,} products  "
             f"({moderate/len(per_product)*100:.1f}%)")
        _log(f"  Variable (CV >= 0.5)    : {variable:>6,} products  "
             f"({variable/len(per_product)*100:.1f}%)")

    _log("")
    _log("Affordability signals - distribution:")
    for col in ["buyer_affordability_p10", "buyer_affordability_p50",
                "buyer_affordability_p90"]:
        if col in per_product.columns:
            vals = per_product[col]
            _log(f"  {col:<46}  "
                 f"p10={vals.quantile(0.10):>10,.0f}  "
                 f"median={vals.quantile(0.50):>10,.0f}  "
                 f"p90={vals.quantile(0.90):>10,.0f}")

    _log("")
    _log("Market concentration (HHI) distribution:")
    if "market_hhi" in per_product.columns:
        hhi = per_product["market_hhi"]
        universal = (hhi < 0.30).sum()
        broad = ((hhi >= 0.30) & (hhi < 0.55)).sum()
        specialty = (hhi >= 0.55).sum()
        _log(f"  Universal-like (HHI < 0.30)    : {universal:>6,} products "
             f"({universal/len(per_product)*100:.1f}%)")
        _log(f"  Broad-like (HHI 0.30-0.55)     : {broad:>6,} products "
             f"({broad/len(per_product)*100:.1f}%)")
        _log(f"  Specialty-like (HHI >= 0.55)   : {specialty:>6,} products "
             f"({specialty/len(per_product)*100:.1f}%)")

    _log("")
    _log("Specialty score (HHI * sqrt(adoption)) distribution:")
    if "specialty_score" in per_product.columns:
        vals = per_product["specialty_score"]
        _log(f"  p10={vals.quantile(0.10):.4f}  "
             f"median={vals.quantile(0.50):.4f}  "
             f"p90={vals.quantile(0.90):.4f}")
        high_specialty = (vals > 0.20).sum()
        _log(f"  High specialty (score > 0.20): {high_specialty:,} products")

    _log("")
    _log("Primary market distribution:")
    if "primary_market" in per_product.columns:
        for mkt, n in per_product["primary_market"].value_counts().items():
            pct = n / len(per_product) * 100
            _log(f"  {mkt:<10}  {n:>6,} products  ({pct:.1f}%)")

    _log("")
    _log("Sample products (top 5 most-bought):")
    for _, r in per_product.head(5).iterrows():
        _log(f"")
        _log(f"  Product: {str(r.get('ITEM_DSC', '?'))[:55]}")
        _log(f"    Family: {str(r.get('PROD_FMLY_LVL1_DSC', '?'))[:40]}")
        _log(f"    Buyers: {r['n_buyers']:,}  "
             f"Adoption: {r['adoption_rate']*100:.1f}%  "
             f"Repeat: {r['repeat_purchase_rate']*100:.0f}%")
        _log(f"    Freq (mean): {r['avg_purchases_per_buyer_per_year']:.1f}/yr  "
             f"(median): {r['median_purchases_per_buyer_per_year']:.1f}/yr")
        _log(f"    Price: ${r['median_unit_price']:,.2f}  "
             f"(CV: {r['unit_price_cv']:.2f})  "
             f"Recency: {r['months_since_last_buyer']:.1f}mo  "
             f"Recent buyers: {r['recent_buyer_count_6mo']:,}")
        _log(f"    Affordability p10: ${r['buyer_affordability_p10']:,.0f}  "
             f"Primary market: {r['primary_market']} ({r['primary_market_pct']*100:.0f}%)")
        _log(f"    Market HHI: {r['market_hhi']:.2f}  "
             f"Specialty score: {r['specialty_score']:.3f}")
        _log(f"    Top segment: {r['top_segment_1']} ({r['top_segment_1_pct']*100:.0f}%), "
             f"{r['top_segment_2']} ({r['top_segment_2_pct']*100:.0f}%)")


# Main

def main() -> None:
    print()
    print("=" * 64)
    print("  PRODUCT MEASUREMENT SEGMENTATION")
    print("=" * 64)
    start = time.time()

    OUT_PRECOMP.mkdir(parents=True, exist_ok=True)
    OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

    txn, max_date = load_transactions_with_customer_context()
    total_customers = txn["cust_id"].nunique()

    per_product = compute_product_measurements(txn, max_date)
    per_product = compute_buyer_mix(txn, per_product)
    per_product = compute_affordability_signals(txn, per_product)
    per_product = compute_adoption_and_price_tier(per_product, total_customers)
    per_product = enrich_with_product_metadata(per_product)
    save_outputs(per_product)
    print_distribution_stats(per_product)

    _s("Complete")
    _log(f"Total time: {time.time() - start:.1f}s")
    _log("")
    _log(f"Output: data_clean/serving/precomputed/product_segments.parquet")
    _log(f"  {len(per_product):,} products with comprehensive measurements")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"\nFATAL ERROR: {exc}", file=sys.stderr)
        raise