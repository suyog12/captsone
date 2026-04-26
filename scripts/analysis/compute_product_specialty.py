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

OUT_PRECOMP  = DATA_CLEAN / "serving"  / "precomputed"
OUT_ANALYSIS = DATA_CLEAN / "analysis"

SLIM_OUT     = OUT_PRECOMP  / "product_specialty.parquet"
FULL_OUT     = OUT_ANALYSIS / "product_specialty_with_metadata.parquet"
XLSX_OUT     = OUT_ANALYSIS / "product_specialty_analysis.xlsx"


# Configuration

FISCAL_YEARS           = ("FY2425", "FY2526")
MIN_BUYERS_PER_PRODUCT = 50    # Same threshold as other files
TOP_N_SPECIALTIES      = 5     # Store top 5 specialties per product
MIN_SPECIALTY_PCT      = 0.05  # 5% of buyers must be this specialty to count

EXCLUDED_SUPPLIERS = {"MEDLINE", "MEDLINE INDUSTRIES"}
EXCLUDED_FAMILIES  = {"Fee", "Unknown", "NaN", "nan", ""}


# Logging

def _s(title: str) -> None:
    print(f"\n{'-' * 64}\n  {title}\n{'-' * 64}", flush=True)


def _log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def _pq(p: Path) -> str:
    return "'" + p.as_posix() + "'"


# Step 1: Load customer-product-specialty transactions

def load_customer_product_specialty() -> pd.DataFrame:
    _s("Step 1: Loading customer-product-specialty data")
    t0 = time.time()

    if not MERGED_FILE.exists():
        print(f"\nFATAL: merged_dataset.parquet not found", file=sys.stderr)
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
    has_specialty = "SPCLTY_CD" in available

    _log(f"Product family column : {family_col}")
    _log(f"Supplier column       : {supplier_col}")
    _log(f"Specialty column      : {'SPCLTY_CD' if has_specialty else 'NOT FOUND'}")

    if not has_specialty:
        print("\nFATAL: SPCLTY_CD column required", file=sys.stderr)
        sys.exit(1)

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

    _log("Aggregating customer-product-specialty pairs via DuckDB...")
    txn = con.execute(f"""
        SELECT
            CAST(DIM_CUST_CURR_ID AS BIGINT)     AS cust_id,
            CAST(DIM_ITEM_E1_CURR_ID AS BIGINT)   AS item_id,
            COALESCE(SPCLTY_CD, 'UNKNOWN')        AS specialty_cd
        FROM read_parquet({_pq(MERGED_FILE)})
        WHERE UNIT_SLS_AMT > 0
          AND fiscal_year IN ({fy_sql})
          AND DIM_CUST_CURR_ID IS NOT NULL
          AND DIM_ITEM_E1_CURR_ID IS NOT NULL
          {family_filter}
          {supplier_filter}
        GROUP BY
            CAST(DIM_CUST_CURR_ID AS BIGINT),
            CAST(DIM_ITEM_E1_CURR_ID AS BIGINT),
            COALESCE(SPCLTY_CD, 'UNKNOWN')
    """).df()
    con.close()

    _log(f"Loaded {len(txn):,} unique customer-product-specialty triples in {time.time()-t0:.1f}s")
    _log(f"  Unique customers : {txn['cust_id'].nunique():,}")
    _log(f"  Unique products  : {txn['item_id'].nunique():,}")
    _log(f"  Unique specialties: {txn['specialty_cd'].nunique():,}")

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

    return txn


# Step 2: Compute specialty distribution per product

def compute_specialty_distribution(txn: pd.DataFrame) -> pd.DataFrame:
    _s("Step 2: Computing specialty distribution per product")
    t0 = time.time()

    # For each product, count unique customers per specialty
    _log("Counting buyers per product-specialty pair...")
    spec_counts = txn.groupby(["item_id", "specialty_cd"])["cust_id"].nunique().reset_index()
    spec_counts.columns = ["item_id", "specialty_cd", "n_buyers"]

    # Total buyers per product
    total_per_item = spec_counts.groupby("item_id")["n_buyers"].sum().rename("total_buyers")
    spec_counts = spec_counts.merge(total_per_item, on="item_id")

    # Percentage
    spec_counts["pct_buyers"] = spec_counts["n_buyers"] / spec_counts["total_buyers"]

    # Keep only specialties with >= MIN_SPECIALTY_PCT
    spec_counts = spec_counts[spec_counts["pct_buyers"] >= MIN_SPECIALTY_PCT]

    # Rank within each product
    spec_counts["rank"] = spec_counts.groupby("item_id")["pct_buyers"].rank(
        method="first", ascending=False
    )
    spec_counts = spec_counts[spec_counts["rank"] <= TOP_N_SPECIALTIES]

    _log(f"  Filtered to top-{TOP_N_SPECIALTIES} specialties with >= {MIN_SPECIALTY_PCT*100:.0f}% share")
    _log(f"  Total product-specialty rows: {len(spec_counts):,}")
    _log(f"  Products with at least 1 specialty: {spec_counts['item_id'].nunique():,}")

    # Compute specialty HHI (concentration index) per product
    _log("Computing specialty HHI per product...")
    all_specs = txn.groupby(["item_id", "specialty_cd"])["cust_id"].nunique().reset_index(name="n")
    total_all = all_specs.groupby("item_id")["n"].sum().rename("total")
    all_specs = all_specs.merge(total_all, on="item_id")
    all_specs["pct_sq"] = (all_specs["n"] / all_specs["total"]) ** 2
    specialty_hhi = all_specs.groupby("item_id")["pct_sq"].sum().rename("specialty_hhi").reset_index()

    _log(f"Specialty HHI computed for {len(specialty_hhi):,} products")

    # Pivot to wide format: top_specialty_1, top_specialty_1_pct, etc.
    _log("Pivoting to wide format...")
    top_rows = []
    for item_id, grp in spec_counts.groupby("item_id"):
        grp = grp.sort_values("rank")
        row = {"item_id": item_id}
        for _, r in grp.iterrows():
            rk = int(r["rank"])
            row[f"top_specialty_{rk}"]     = r["specialty_cd"]
            row[f"top_specialty_{rk}_pct"] = float(r["pct_buyers"])
        top_rows.append(row)

    top_df = pd.DataFrame(top_rows)

    # Ensure all top_specialty_N columns exist
    for i in range(1, TOP_N_SPECIALTIES + 1):
        if f"top_specialty_{i}" not in top_df.columns:
            top_df[f"top_specialty_{i}"] = ""
        if f"top_specialty_{i}_pct" not in top_df.columns:
            top_df[f"top_specialty_{i}_pct"] = 0.0

    # Merge HHI
    top_df = top_df.merge(specialty_hhi, on="item_id", how="left")

    # Fill NaN in pct columns with 0
    for i in range(1, TOP_N_SPECIALTIES + 1):
        top_df[f"top_specialty_{i}"] = top_df[f"top_specialty_{i}"].fillna("")
        top_df[f"top_specialty_{i}_pct"] = top_df[f"top_specialty_{i}_pct"].fillna(0.0)

    _log(f"Distribution computed in {time.time()-t0:.1f}s")

    return top_df


# Step 3: Enrich with product metadata

def enrich_with_metadata(df: pd.DataFrame) -> pd.DataFrame:
    _s("Step 3: Enriching with product metadata")

    if not PRODUCT_FILE.exists():
        _log("products_clean.parquet not found - skipping")
        return df

    products = pd.read_parquet(PRODUCT_FILE, columns=[
        "DIM_ITEM_E1_CURR_ID", "ITEM_DSC",
        "PROD_FMLY_LVL1_DSC", "PROD_CTGRY_LVL2_DSC",
    ])
    products["DIM_ITEM_E1_CURR_ID"] = products["DIM_ITEM_E1_CURR_ID"].astype("int64")
    products = products.rename(columns={"DIM_ITEM_E1_CURR_ID": "item_id"})

    df["item_id"] = df["item_id"].astype("int64")
    df = df.merge(products, on="item_id", how="left")

    _log(f"Metadata merged: {len(df):,} rows")

    return df


# Step 4: Save outputs

def save_outputs(df: pd.DataFrame) -> None:
    _s("Step 4: Saving outputs")

    OUT_PRECOMP.mkdir(parents=True, exist_ok=True)
    OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

    # Reorder columns for readability
    base_cols = ["item_id", "ITEM_DSC", "PROD_FMLY_LVL1_DSC", "PROD_CTGRY_LVL2_DSC"]
    base_cols = [c for c in base_cols if c in df.columns]

    spec_cols = []
    for i in range(1, TOP_N_SPECIALTIES + 1):
        spec_cols.append(f"top_specialty_{i}")
        spec_cols.append(f"top_specialty_{i}_pct")
    spec_cols = [c for c in spec_cols if c in df.columns]

    final_cols = base_cols + spec_cols + ["specialty_hhi"]
    final_cols = [c for c in final_cols if c in df.columns]
    df = df[final_cols].copy()

    df = df.rename(columns={"item_id": "DIM_ITEM_E1_CURR_ID"})

    # Types
    df["DIM_ITEM_E1_CURR_ID"] = df["DIM_ITEM_E1_CURR_ID"].astype("int64")
    for i in range(1, TOP_N_SPECIALTIES + 1):
        col = f"top_specialty_{i}_pct"
        if col in df.columns:
            df[col] = df[col].astype("float32")
    if "specialty_hhi" in df.columns:
        df["specialty_hhi"] = df["specialty_hhi"].astype("float32")

    # Sort by specialty_hhi descending (most specialty-concentrated first)
    df = df.sort_values("specialty_hhi", ascending=False).reset_index(drop=True)

    # Slim version (just the essentials for recommendation engine)
    slim_cols = ["DIM_ITEM_E1_CURR_ID"] + spec_cols + ["specialty_hhi"]
    slim_cols = [c for c in slim_cols if c in df.columns]
    slim = df[slim_cols].copy()
    slim.to_parquet(SLIM_OUT, index=False)
    size_kb = SLIM_OUT.stat().st_size / 1024
    _log(f"Saved slim version : {SLIM_OUT.relative_to(ROOT)}  ({size_kb:.0f} KB)")

    # Full version
    df.to_parquet(FULL_OUT, index=False)
    size_kb = FULL_OUT.stat().st_size / 1024
    _log(f"Saved full version : {FULL_OUT.relative_to(ROOT)}  ({size_kb:.0f} KB)")

    # XLSX: top 500 products by specialty_hhi for inspection
    top_500 = df.head(500)
    top_500.to_excel(XLSX_OUT, index=False, engine="openpyxl")
    size_kb = XLSX_OUT.stat().st_size / 1024
    _log(f"Saved xlsx          : {XLSX_OUT.relative_to(ROOT)}  ({size_kb:.0f} KB, top 500)")


# Step 5: Print stats and samples

def print_stats(df: pd.DataFrame) -> None:
    _s("Step 5: Distribution and samples")

    _log(f"Total products with specialty data: {len(df):,}")
    _log("")

    if "specialty_hhi" in df.columns:
        hhi = df["specialty_hhi"]
        _log(f"Specialty HHI distribution:")
        _log(f"  p10={hhi.quantile(0.10):.3f}  median={hhi.quantile(0.50):.3f}  p90={hhi.quantile(0.90):.3f}")

        # Categorize
        highly_concentrated = (hhi >= 0.55).sum()
        moderately = ((hhi >= 0.30) & (hhi < 0.55)).sum()
        diverse = (hhi < 0.30).sum()

        _log(f"  Highly specialty-concentrated (HHI >= 0.55): {highly_concentrated:,} products "
             f"({highly_concentrated/len(df)*100:.1f}%)")
        _log(f"  Moderately (HHI 0.30-0.55)                 : {moderately:,} products "
             f"({moderately/len(df)*100:.1f}%)")
        _log(f"  Diverse (HHI < 0.30)                        : {diverse:,} products "
             f"({diverse/len(df)*100:.1f}%)")

    # Top specialty distribution
    _log("")
    _log("Top primary specialty distribution (most common top_specialty_1):")
    if "top_specialty_1" in df.columns:
        primary = df["top_specialty_1"].value_counts().head(15)
        for spec, n in primary.items():
            pct = n / len(df) * 100
            _log(f"  {spec:<12}  {n:>6,} products  ({pct:.1f}%)")

    # Sample products with different specialty profiles
    _log("")
    _log("Sample 1: Most specialty-concentrated products (top specialty_hhi):")
    for _, r in df.head(5).iterrows():
        desc = str(r.get("ITEM_DSC", "?"))[:50]
        fam  = str(r.get("PROD_FMLY_LVL1_DSC", "?"))[:30]
        hhi  = r.get("specialty_hhi", 0)
        s1   = str(r.get("top_specialty_1", ""))
        s1p  = r.get("top_specialty_1_pct", 0)
        s2   = str(r.get("top_specialty_2", ""))
        s2p  = r.get("top_specialty_2_pct", 0)
        _log(f"")
        _log(f"  {desc}")
        _log(f"    Family: {fam}  HHI: {hhi:.2f}")
        _log(f"    Top specialties: {s1}({s1p*100:.0f}%), {s2}({s2p*100:.0f}%)")

    # Sample diverse products
    _log("")
    _log("Sample 2: Most specialty-diverse products (low specialty_hhi):")
    for _, r in df.tail(5).iterrows():
        desc = str(r.get("ITEM_DSC", "?"))[:50]
        fam  = str(r.get("PROD_FMLY_LVL1_DSC", "?"))[:30]
        hhi  = r.get("specialty_hhi", 0)
        s1   = str(r.get("top_specialty_1", ""))
        s1p  = r.get("top_specialty_1_pct", 0)
        s2   = str(r.get("top_specialty_2", ""))
        s2p  = r.get("top_specialty_2_pct", 0)
        _log(f"")
        _log(f"  {desc}")
        _log(f"    Family: {fam}  HHI: {hhi:.2f}")
        _log(f"    Top specialties: {s1}({s1p*100:.0f}%), {s2}({s2p*100:.0f}%)")


# Main

def main() -> None:
    print()
    print("=" * 64)
    print("  PRODUCT SPECIALTY DISTRIBUTION")
    print("=" * 64)
    start = time.time()

    OUT_PRECOMP.mkdir(parents=True, exist_ok=True)
    OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

    txn = load_customer_product_specialty()
    df = compute_specialty_distribution(txn)
    df = enrich_with_metadata(df)
    save_outputs(df)
    print_stats(df)

    _s("Complete")
    _log(f"Total time: {time.time() - start:.1f}s")
    _log("")
    _log(f"Output: {SLIM_OUT.relative_to(ROOT)}")
    _log(f"  Per-product specialty distribution (top {TOP_N_SPECIALTIES} specialties)")
    _log(f"  Used by recommendation_factors.py for specialty match scoring")
    _log("")
    _log("How this is used in recommendation engine:")
    _log("  - For each recommendation candidate, check customer's SPCLTY_CD")
    _log("  - If customer specialty is in product's top_specialty_1..5 -> boost (1.2x)")
    _log("  - If customer specialty is NOT in product's top specialties -> penalty (0.7x)")
    _log("  - Prevents recommending pediatric products to adult specialists and vice versa")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"\nFATAL ERROR: {exc}", file=sys.stderr)
        raise