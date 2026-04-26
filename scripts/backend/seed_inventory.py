from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backend.db_connection import get_engine, get_schema  # noqa: E402


PRODUCTS_FILE = PROJECT_ROOT / "data_clean" / "serving" / "precomputed" / "product_segments.parquet"

# Reproducibility for the random seed. If we re-run the seed script we
# want the same stock counts so demos are stable.
RNG_SEED = 42

# Stock tier ranges (min inclusive, max inclusive)
TIER_RANGES = {
    "high":  (50_000, 500_000),
    "mid":   (5_000,  50_000),
    "low":   (100,    5_000),
}

CHUNK_SIZE = 5_000


def _section(title: str) -> None:
    print("-" * 70)
    print(" ", title)
    print("-" * 70)


def assign_tiers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tag each product with high / mid / low volume tier based on n_buyers.

    Splitting on terciles of n_buyers across the whole catalogue. We use
    n_buyers (not n_orders or revenue) because it is the most direct
    measure of how broadly the product moves.
    """
    _section("Step 1: Tag products with volume tiers")

    if "n_buyers" not in df.columns:
        raise KeyError(
            "product_segments.parquet has no 'n_buyers' column. "
            "Cannot determine volume tier."
        )

    # Compute terciles. Using rank then cut so we get even-sized groups
    # rather than equal-width bins (n_buyers is heavily right-skewed).
    df = df.copy()
    df["_rank"] = df["n_buyers"].rank(method="first", ascending=True)
    n = len(df)
    third = n / 3.0

    def _to_tier(r: float) -> str:
        if r > 2 * third:
            return "high"
        if r > third:
            return "mid"
        return "low"

    df["_tier"] = df["_rank"].apply(_to_tier)

    print(f"  high volume tier : {(df['_tier'] == 'high').sum():>7,} products")
    print(f"  mid volume tier  : {(df['_tier'] == 'mid').sum():>7,} products")
    print(f"  low volume tier  : {(df['_tier'] == 'low').sum():>7,} products")
    return df


def generate_stock_counts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Draw a random stock count from the tier-appropriate range for each
    product. Uses log-uniform sampling within the range so we get a
    realistic spread (lots of mid-range counts, fewer at the extremes)
    rather than uniform.
    """
    _section("Step 2: Generate random stock counts")
    rng = np.random.default_rng(RNG_SEED)

    out_rows = []
    for tier, (lo, hi) in TIER_RANGES.items():
        mask = df["_tier"] == tier
        sub = df.loc[mask]
        # log-uniform sample
        log_lo = np.log10(lo)
        log_hi = np.log10(hi)
        samples = 10 ** rng.uniform(log_lo, log_hi, size=len(sub))
        stock = samples.astype(int).clip(lo, hi)

        for item_id, units in zip(sub["item_id"].astype("int64"), stock):
            out_rows.append({
                "item_id": int(item_id),
                "units_available": int(units),
            })

    out = pd.DataFrame(out_rows)

    print(f"  Stock rows generated : {len(out):,}")
    print(f"  Min stock            : {out['units_available'].min():,}")
    print(f"  Median stock         : {int(out['units_available'].median()):,}")
    print(f"  Max stock            : {out['units_available'].max():,}")
    print(f"  Total inventory units: {out['units_available'].sum():,}")
    return out


def insert_into_postgres(df: pd.DataFrame) -> None:
    _section("Step 3: Insert / upsert into recdash.inventory")

    schema = get_schema()
    engine = get_engine()

    upsert_sql = text(f"""
        INSERT INTO {schema}.inventory (item_id, units_available, last_updated)
        VALUES (:item_id, :units_available, NOW())
        ON CONFLICT (item_id) DO UPDATE SET
            units_available = EXCLUDED.units_available,
            last_updated    = NOW()
    """)

    total = len(df)
    written = 0
    t_start = time.time()

    with engine.begin() as conn:
        for start in range(0, total, CHUNK_SIZE):
            chunk = df.iloc[start:start + CHUNK_SIZE].to_dict(orient="records")
            conn.execute(upsert_sql, chunk)
            written += len(chunk)
            elapsed = time.time() - t_start
            rate = written / elapsed if elapsed > 0 else 0
            print(f"  Wrote {written:>7,} / {total:,}   "
                  f"({elapsed:5.1f}s, {rate:>6,.0f} rows/sec)")

    print(f"  Done. {written:,} inventory rows written.")


def verify() -> None:
    _section("Step 4: Verify")
    schema = get_schema()
    engine = get_engine()
    with engine.connect() as conn:
        n = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.inventory")).scalar()
        total_units = conn.execute(text(
            f"SELECT SUM(units_available) FROM {schema}.inventory"
        )).scalar()
        sample = conn.execute(text(
            f"SELECT i.item_id, p.description, i.units_available "
            f"FROM {schema}.inventory i "
            f"JOIN {schema}.products p ON p.item_id = i.item_id "
            f"ORDER BY i.units_available DESC LIMIT 5"
        )).fetchall()
        sample_low = conn.execute(text(
            f"SELECT i.item_id, p.description, i.units_available "
            f"FROM {schema}.inventory i "
            f"JOIN {schema}.products p ON p.item_id = i.item_id "
            f"ORDER BY i.units_available ASC LIMIT 5"
        )).fetchall()

    print(f"  Inventory rows       : {n:,}")
    print(f"  Total units in stock : {total_units:,}")
    print(f"  Highest stock samples:")
    for row in sample:
        print(f"    {row}")
    print(f"  Lowest stock samples:")
    for row in sample_low:
        print(f"    {row}")


def main() -> None:
    print()
    print("=" * 70)
    print("  SEED INVENTORY  (random tiered stock counts)")
    print("=" * 70)
    t0 = time.time()

    if not PRODUCTS_FILE.exists():
        raise FileNotFoundError(f"Missing: {PRODUCTS_FILE}")

    df = pd.read_parquet(PRODUCTS_FILE)
    if "DIM_ITEM_E1_CURR_ID" in df.columns:
        df = df.rename(columns={"DIM_ITEM_E1_CURR_ID": "item_id"})
    df["item_id"] = df["item_id"].astype("int64")

    # Filter to only the item_ids that actually exist in recdash.products.
    # The parquet may contain a few items that got rejected during the products
    # import (NULL ids, duplicates absorbed by ON CONFLICT, etc.), and inserting
    # inventory rows for non-existent items would violate the foreign key.
    schema = get_schema()
    engine = get_engine()
    with engine.connect() as conn:
        existing = pd.read_sql(
            text(f"SELECT item_id FROM {schema}.products"),
            conn,
        )
    existing_ids = set(existing["item_id"].astype("int64"))

    before = len(df)
    df = df[df["item_id"].isin(existing_ids)].copy()
    after = len(df)
    if before != after:
        print(f"  Filtered to items that exist in {schema}.products: "
              f"{before:,} -> {after:,} ({before - after:,} dropped)")

    df = assign_tiers(df)
    stock = generate_stock_counts(df)
    insert_into_postgres(stock)
    verify()

    elapsed = time.time() - t0
    print()
    print("-" * 70)
    print(f"  Total time: {elapsed:.1f}s")
    print("-" * 70)


if __name__ == "__main__":
    main()