from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import text

# Make sibling modules importable when run as `python scripts/backend/import_customers.py`
HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backend.db_connection import get_engine, get_schema  # noqa: E402


# Source files
SEGMENTS_FILE = PROJECT_ROOT / "data_clean" / "serving" / "precomputed" / "customer_segments.parquet"
# customer_features.parquet lives in data_clean/features/, not in precomputed/.
# It is the per-customer feature matrix produced by segment_customer_features.py
# during the segmentation pipeline.
FEATURES_FILE = PROJECT_ROOT / "data_clean" / "features" / "customer_features.parquet"
PATTERNS_FILE = PROJECT_ROOT / "data_clean" / "serving" / "precomputed" / "customer_patterns.parquet"

# Insert chunk size. 5,000 rows per executemany call gives a good balance
# of round-trip count vs. memory.
CHUNK_SIZE = 5_000


def _section(title: str) -> None:
    print("-" * 70)
    print(" ", title)
    print("-" * 70)


def load_customer_data() -> pd.DataFrame:
    """
    Load the three customer-level parquets and join into one wide
    DataFrame ready for insert.
    """
    _section("Step 1: Load source parquet files")

    if not SEGMENTS_FILE.exists():
        raise FileNotFoundError(f"Missing: {SEGMENTS_FILE}")
    if not FEATURES_FILE.exists():
        raise FileNotFoundError(f"Missing: {FEATURES_FILE}")

    segs = pd.read_parquet(SEGMENTS_FILE)
    print(f"  customer_segments  : {len(segs):>9,} rows  ({len(segs.columns)} cols)")

    feats = pd.read_parquet(FEATURES_FILE)
    print(f"  customer_features  : {len(feats):>9,} rows  ({len(feats.columns)} cols)")

    # Patterns is optional; supplier_profile lives there. If missing, we just
    # skip the supplier_profile column.
    patterns = None
    if PATTERNS_FILE.exists():
        patterns = pd.read_parquet(PATTERNS_FILE)
        print(f"  customer_patterns  : {len(patterns):>9,} rows  ({len(patterns.columns)} cols)")
    else:
        print("  customer_patterns  : (not found, supplier_profile will be NULL)")

    # Find the customer ID column (might be DIM_CUST_CURR_ID or cust_id depending on file)
    def _id_col(df: pd.DataFrame) -> str:
        for c in ("DIM_CUST_CURR_ID", "cust_id"):
            if c in df.columns:
                return c
        raise KeyError(f"No customer ID column found in {df.columns.tolist()[:10]}")

    seg_id = _id_col(segs)
    feat_id = _id_col(feats)

    # Standardise on cust_id
    segs = segs.rename(columns={seg_id: "cust_id"})
    feats = feats.rename(columns={feat_id: "cust_id"})

    # Join
    df = segs.merge(feats, on="cust_id", how="left", suffixes=("", "_feat"))
    if patterns is not None:
        pat_id = _id_col(patterns)
        patterns = patterns.rename(columns={pat_id: "cust_id"})
        df = df.merge(patterns, on="cust_id", how="left", suffixes=("", "_pat"))

    print(f"  After join         : {len(df):>9,} rows")
    return df


def shape_for_insert(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map the wide source DataFrame to the exact columns of recdash.customers.
    """
    _section("Step 2: Shape for the customers table")

    # Pick the right column for each target field. Source column names
    # vary across our pipeline outputs, so we try a few and fall back
    # to None if nothing matches.
    def first_present(candidates: list[str]) -> str | None:
        for c in candidates:
            if c in df.columns:
                return c
        return None

    name_col = first_present(["CUST_NAME", "customer_name", "DIM_CUST_NAME"])
    spec_col = first_present(["SPCLTY_CD", "specialty_code"])
    mkt_col = first_present(["mkt_cd_clean", "MKT_CD", "market_code"])
    seg_col = first_present(["segment"])
    sup_col = first_present(["supplier_profile"])

    print(f"  customer_name source : {name_col or '(not available, will be NULL)'}")
    print(f"  specialty source     : {spec_col or '(not available)'}")
    print(f"  market source        : {mkt_col or '(not available)'}")
    print(f"  segment source       : {seg_col or '(not available)'}")
    print(f"  supplier_profile     : {sup_col or '(not available)'}")

    out = pd.DataFrame()
    out["cust_id"] = df["cust_id"].astype("int64")
    out["customer_name"] = df[name_col] if name_col else None
    out["specialty_code"] = df[spec_col].astype("string") if spec_col else None
    out["market_code"] = df[mkt_col].astype("string") if mkt_col else None
    out["segment"] = df[seg_col].astype("string") if seg_col else None
    out["supplier_profile"] = df[sup_col].astype("string") if sup_col else None

    # Truncate to the VARCHAR limits in the schema. Defensive against any
    # unusually long values in the source.
    if "customer_name" in out.columns and out["customer_name"] is not None:
        out["customer_name"] = out["customer_name"].astype("string").str.slice(0, 200)
    out["specialty_code"] = out["specialty_code"].str.slice(0, 20) if out["specialty_code"] is not None else None
    out["market_code"] = out["market_code"].str.slice(0, 20) if out["market_code"] is not None else None
    out["segment"] = out["segment"].str.slice(0, 50) if out["segment"] is not None else None
    out["supplier_profile"] = out["supplier_profile"].str.slice(0, 50) if out["supplier_profile"] is not None else None

    # Replace pandas NA / NaN with python None so psycopg2 sends proper NULLs.
    out = out.where(pd.notna(out), None)

    print(f"  Shaped rows          : {len(out):,}")
    return out


def insert_into_postgres(df: pd.DataFrame) -> None:
    """
    Insert the shaped customers DataFrame into recdash.customers, in chunks.
    Uses ON CONFLICT DO NOTHING so a re-run is safe (idempotent).
    """
    _section("Step 3: Insert into recdash.customers")

    schema = get_schema()
    engine = get_engine()

    insert_sql = text(f"""
        INSERT INTO {schema}.customers
            (cust_id, customer_name, specialty_code, market_code,
             segment, supplier_profile)
        VALUES
            (:cust_id, :customer_name, :specialty_code, :market_code,
             :segment, :supplier_profile)
        ON CONFLICT (cust_id) DO NOTHING
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

    print(f"  Done. {inserted:,} rows inserted (or skipped on conflict).")


def verify(expected_min: int) -> None:
    """Sanity-check the row count after insert."""
    _section("Step 4: Verify")

    schema = get_schema()
    engine = get_engine()
    with engine.connect() as conn:
        n = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.customers")).scalar()
        n_with_segment = conn.execute(text(
            f"SELECT COUNT(*) FROM {schema}.customers WHERE segment IS NOT NULL"
        )).scalar()
        sample = conn.execute(text(
            f"SELECT cust_id, market_code, segment, specialty_code, supplier_profile "
            f"FROM {schema}.customers LIMIT 5"
        )).fetchall()

    print(f"  Total customers in table : {n:,}")
    print(f"  Customers with segment   : {n_with_segment:,}")
    print(f"  Sample rows:")
    for row in sample:
        print(f"    {row}")

    if n < expected_min:
        print(f"  WARNING: expected at least {expected_min:,} rows, got {n:,}")
    else:
        print("  Verification OK.")


def main() -> None:
    print()
    print("=" * 70)
    print("  IMPORT CUSTOMERS  (parquet -> Postgres)")
    print("=" * 70)
    t0 = time.time()

    df = load_customer_data()
    shaped = shape_for_insert(df)
    insert_into_postgres(shaped)
    verify(expected_min=int(len(shaped) * 0.95))  # allow up to 5% loss to conflicts

    elapsed = time.time() - t0
    print()
    print("-" * 70)
    print(f"  Total time: {elapsed:.1f}s")
    print("-" * 70)


if __name__ == "__main__":
    main()