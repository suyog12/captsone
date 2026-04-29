from __future__ import annotations

import csv
import sys
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import text

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backend.db_connection import get_engine, get_schema  # noqa: E402


OUTPUT_FILE = HERE / "demo_customers.csv"
CUSTOMER_PATTERNS_FILE = (
    PROJECT_ROOT / "data_clean" / "serving" / "precomputed" / "customer_patterns.parquet"
)


# Target distribution: 30 demo customers total.
# Each tuple: (status, segment_substring, count)
# We aim for spread across markets and size tiers.
TARGET_PICKS: list[tuple[str, list[str], int]] = [
    # STABLE WARM (20 total)
    # PO market: 6 customers across size tiers
    ("stable_warm", ["PO_new"],         1),
    ("stable_warm", ["PO_small"],       2),
    ("stable_warm", ["PO_mid"],         2),
    ("stable_warm", ["PO_large"],       1),
    # LTC market: 4 customers
    ("stable_warm", ["LTC_small"],      1),
    ("stable_warm", ["LTC_mid"],        1),
    ("stable_warm", ["LTC_large"],      1),
    ("stable_warm", ["LTC_enterprise"], 1),
    # SC market: 3 customers
    ("stable_warm", ["SC_small"],       1),
    ("stable_warm", ["SC_mid"],         1),
    ("stable_warm", ["SC_large"],       1),
    # HC market: 2 customers
    ("stable_warm", ["HC_small"],       1),
    ("stable_warm", ["HC_mid"],         1),
    # LC market: 2 customers
    ("stable_warm", ["LC_small"],       1),
    ("stable_warm", ["LC_mid"],         1),
    # AC market: 3 customers
    ("stable_warm", ["AC_mid"],         1),
    ("stable_warm", ["AC_large"],       1),
    ("stable_warm", ["AC_enterprise"],  1),

    # DECLINING WARM (5 total)
    ("declining_warm", ["PO_small"],      1),
    ("declining_warm", ["PO_mid"],        1),
    ("declining_warm", ["LTC_mid"],       1),
    ("declining_warm", ["SC_mid"],        1),
    ("declining_warm", ["AC_large"],      1),

    # CHURNED WARM (5 total)
    ("churned_warm", ["PO_small"],     1),
    ("churned_warm", ["PO_mid"],       1),
    ("churned_warm", ["LTC_small"],    1),
    ("churned_warm", ["LTC_large"],    1),
    ("churned_warm", ["SC_mid"],       1),
]


def _section(t: str) -> None:
    print("-" * 70); print(" ", t); print("-" * 70)


def determine_status(row: pd.Series) -> str:
    """Compute status from is_cold_start / is_declining / is_churned flags."""
    if bool(row.get("is_cold_start", False)):
        return "cold_start"
    if bool(row.get("is_churned", False)):
        return "churned_warm"
    if bool(row.get("is_declining", False)):
        return "declining_warm"
    return "stable_warm"


def main() -> None:
    print()
    print("=" * 70)
    print("  PICK DEMO CUSTOMERS")
    print("=" * 70)
    t0 = time.time()

    schema = get_schema()
    engine = get_engine()

    # Load customer patterns parquet to get the status flags and order count
    _section("Step 1: Load customer status flags from parquet")
    if not CUSTOMER_PATTERNS_FILE.exists():
        raise FileNotFoundError(f"Missing: {CUSTOMER_PATTERNS_FILE}")
    pat = pd.read_parquet(CUSTOMER_PATTERNS_FILE)
    pat = pat.rename(columns={"DIM_CUST_CURR_ID": "cust_id"})
    if "cust_id" not in pat.columns:
        raise KeyError("customer_patterns parquet missing cust_id / DIM_CUST_CURR_ID")
    pat["status"] = pat.apply(determine_status, axis=1)
    print(f"  Loaded {len(pat):,} pattern rows")
    print(f"  Status distribution:")
    for s, n in pat["status"].value_counts().items():
        print(f"    {s:<20}: {n:>9,}")

    # Load all customers from Postgres
    _section("Step 2: Load customers from Postgres")
    with engine.connect() as conn:
        cust = pd.read_sql(text(
            f"SELECT cust_id, segment, market_code, specialty_code FROM {schema}.customers"
        ), conn)
    print(f"  Loaded {len(cust):,} customer rows from Postgres")

    # Join customers with patterns to get status + order count
    _section("Step 3: Join customers with status and order count")
    n_orders_col = "n_orders_total" if "n_orders_total" in pat.columns else None
    keep_cols = ["cust_id", "status"] + ([n_orders_col] if n_orders_col else [])
    pat_slim = pat[keep_cols]
    df = cust.merge(pat_slim, on="cust_id", how="inner")
    print(f"  Joined: {len(df):,} customers with both segment and status info")

    # Add size_tier derived from segment ("PO_small" -> "small")
    df["size_tier"] = df["segment"].str.split("_").str[-1]

    # Pick demo customers per the target distribution
    _section("Step 4: Pick 30 demo customers per the target distribution")
    rng_seed = 42
    picked_rows: list[dict] = []
    seen_ids: set[int] = set()

    for status, segment_filters, n_wanted in TARGET_PICKS:
        # Filter pool
        pool = df[df["status"] == status]
        seg_mask = pd.Series([False] * len(pool), index=pool.index)
        for seg in segment_filters:
            seg_mask = seg_mask | pool["segment"].str.startswith(seg)
        pool = pool[seg_mask]
        # Exclude already-picked
        pool = pool[~pool["cust_id"].isin(seen_ids)]

        # Sort by n_orders (richest history first) and pick top N
        if n_orders_col and len(pool) > 0:
            pool = pool.sort_values(n_orders_col, ascending=False)

        if len(pool) == 0:
            print(f"  WARN: no candidates for status={status}, segments={segment_filters}")
            continue

        chosen = pool.head(n_wanted)
        for _, row in chosen.iterrows():
            cid = int(row["cust_id"])
            seen_ids.add(cid)
            picked_rows.append({
                "cust_id":        cid,
                "segment":        row["segment"],
                "market_code":    row["market_code"],
                "size_tier":      row["size_tier"],
                "status":         status,
                "specialty_code": row.get("specialty_code") or "",
                "n_orders":       int(row[n_orders_col]) if n_orders_col else 0,
            })

    print(f"  Picked {len(picked_rows)} demo customers")

    # Write CSV
    _section("Step 5: Write demo_customers.csv")
    if not picked_rows:
        raise RuntimeError("No demo customers picked. Check target distribution against your data.")

    fieldnames = ["cust_id", "segment", "market_code", "size_tier", "status", "specialty_code", "n_orders"]
    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for row in picked_rows:
            w.writerow(row)
    print(f"  Wrote: {OUTPUT_FILE}")

    # Pretty-print the picks
    _section("Selected customers")
    for r in picked_rows:
        print(
            f"  cust_id={r['cust_id']:>10}  "
            f"segment={r['segment']:<18}  "
            f"status={r['status']:<14}  "
            f"specialty={r['specialty_code']:<6}  "
            f"n_orders={r['n_orders']:>5}"
        )

    print()
    print("-" * 70)
    print(f"  Total time: {time.time() - t0:.1f}s")
    print("-" * 70)
    print("  Next step: python scripts/backend/seed_demo_logins.py")


if __name__ == "__main__":
    main()
