from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import text

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backend.db_connection import get_engine, get_schema  # noqa: E402


PRODUCTS_FILE = PROJECT_ROOT / "data_clean" / "serving" / "precomputed" / "product_segments.parquet"

CHUNK_SIZE = 5_000


def _section(title: str) -> None:
    print("-" * 70)
    print(" ", title)
    print("-" * 70)


def load_products() -> pd.DataFrame:
    _section("Step 1: Load product_segments.parquet")
    if not PRODUCTS_FILE.exists():
        raise FileNotFoundError(f"Missing: {PRODUCTS_FILE}")

    df = pd.read_parquet(PRODUCTS_FILE)
    print(f"  Rows    : {len(df):>9,}")
    print(f"  Columns : {len(df.columns)}")
    return df


def shape_for_insert(df: pd.DataFrame) -> pd.DataFrame:
    _section("Step 2: Shape for the products table")

    def first_present(candidates: list[str]) -> str | None:
        for c in candidates:
            if c in df.columns:
                return c
        return None

    id_col = first_present(["DIM_ITEM_E1_CURR_ID", "item_id"])
    desc_col = first_present(["ITEM_DSC", "description"])
    fam_col = first_present(["PROD_FMLY_LVL1_DSC", "family"])
    cat_col = first_present(["PROD_CTGRY_LVL2_DSC", "category"])
    pb_col = first_present(["is_private_brand"])
    price_col = first_present(["median_unit_price", "mean_unit_price", "unit_price"])
    sup_col = first_present(["SUPLR_ROLLUP_DSC", "MFR_NAME", "supplier"])

    if id_col is None:
        raise KeyError("No item ID column found in product_segments.parquet")

    print(f"  item_id source       : {id_col}")
    print(f"  description source   : {desc_col or '(not available)'}")
    print(f"  family source        : {fam_col or '(not available)'}")
    print(f"  category source      : {cat_col or '(not available)'}")
    print(f"  is_private_brand src : {pb_col or '(not available, default FALSE)'}")
    print(f"  unit_price source    : {price_col or '(not available, default 0)'}")
    print(f"  supplier source      : {sup_col or '(not available)'}")

    out = pd.DataFrame()
    out["item_id"] = df[id_col].astype("int64")

    out["description"] = df[desc_col].astype("string").str.slice(0, 500) if desc_col else None
    out["family"] = df[fam_col].astype("string").str.slice(0, 200) if fam_col else None
    out["category"] = df[cat_col].astype("string").str.slice(0, 200) if cat_col else None

    if pb_col:
        out["is_private_brand"] = df[pb_col].fillna(False).astype(bool)
    else:
        out["is_private_brand"] = False

    if price_col:
        # Defensive: coerce to numeric then clip extreme outliers
        prices = pd.to_numeric(df[price_col], errors="coerce")
        prices = prices.fillna(0).clip(lower=0, upper=99999.99)
        out["unit_price"] = prices.round(2)
    else:
        out["unit_price"] = 0.0

    out["supplier"] = df[sup_col].astype("string").str.slice(0, 200) if sup_col else None
    out["pack_size"] = None       # not available in product_segments
    out["image_url"] = None       # not available in product_segments

    out = out.where(pd.notna(out), None)
    print(f"  Shaped rows          : {len(out):,}")
    return out


def insert_into_postgres(df: pd.DataFrame) -> None:
    _section("Step 3: Insert into recdash.products")

    schema = get_schema()
    engine = get_engine()

    insert_sql = text(f"""
        INSERT INTO {schema}.products
            (item_id, description, family, category,
             is_private_brand, unit_price, supplier, pack_size, image_url)
        VALUES
            (:item_id, :description, :family, :category,
             :is_private_brand, :unit_price, :supplier, :pack_size, :image_url)
        ON CONFLICT (item_id) DO NOTHING
    """)

    total = len(df)
    inserted = 0
    t_start = time.time()

    with engine.begin() as conn:
        for start in range(0, total, CHUNK_SIZE):
            chunk = df.iloc[start:start + CHUNK_SIZE].to_dict(orient="records")
            conn.execute(insert_sql, chunk)
            inserted += len(chunk)
            elapsed = time.time() - t_start
            rate = inserted / elapsed if elapsed > 0 else 0
            print(f"  Inserted {inserted:>7,} / {total:,}   "
                  f"({elapsed:5.1f}s, {rate:>6,.0f} rows/sec)")

    print(f"  Done. {inserted:,} rows inserted.")


def verify() -> None:
    _section("Step 4: Verify")
    schema = get_schema()
    engine = get_engine()
    with engine.connect() as conn:
        n = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.products")).scalar()
        n_pb = conn.execute(text(
            f"SELECT COUNT(*) FROM {schema}.products WHERE is_private_brand = TRUE"
        )).scalar()
        sample = conn.execute(text(
            f"SELECT item_id, description, family, is_private_brand, unit_price, supplier "
            f"FROM {schema}.products ORDER BY item_id LIMIT 5"
        )).fetchall()

    print(f"  Total products       : {n:,}")
    print(f"  Private-brand items  : {n_pb:,}")
    print(f"  Sample rows:")
    for row in sample:
        print(f"    {row}")


def main() -> None:
    print()
    print("=" * 70)
    print("  IMPORT PRODUCTS  (parquet -> Postgres)")
    print("=" * 70)
    t0 = time.time()

    df = load_products()
    shaped = shape_for_insert(df)
    insert_into_postgres(shaped)
    verify()

    elapsed = time.time() - t0
    print()
    print("-" * 70)
    print(f"  Total time: {elapsed:.1f}s")
    print("-" * 70)


if __name__ == "__main__":
    main()
