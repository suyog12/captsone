from __future__ import annotations

import csv
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd
from sqlalchemy import text

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backend.db_connection import get_engine, get_schema  # noqa: E402


CSV_FILE = HERE / "demo_customers.csv"
MERGED_FILE = PROJECT_ROOT / "data_clean" / "serving" / "merged_dataset.parquet"

CHUNK_SIZE = 5_000


def _section(t: str) -> None:
    print("-" * 70); print(" ", t); print("-" * 70)


def main() -> None:
    print()
    print("=" * 70)
    print("  COPY PURCHASE HISTORY FOR DEMO CUSTOMERS")
    print("=" * 70)
    t0 = time.time()

    if not CSV_FILE.exists():
        raise FileNotFoundError(f"Missing {CSV_FILE}. Run pick_demo_customers.py first.")
    if not MERGED_FILE.exists():
        raise FileNotFoundError(f"Missing {MERGED_FILE}.")

    schema = get_schema()
    engine = get_engine()

    # Load demo cust_ids
    _section("Step 1: Load demo cust_ids")
    cust_ids: list[int] = []
    with CSV_FILE.open("r", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            cust_ids.append(int(r["cust_id"]))
    print(f"  {len(cust_ids)} demo customers")

    # Get the set of valid item_ids (so we can filter out lines whose items
    # were not loaded into recdash.products, which would FK-violate)
    _section("Step 2: Load valid item_ids from products table")
    with engine.connect() as conn:
        prod_rows = conn.execute(text(
            f"SELECT item_id FROM {schema}.products"
        )).fetchall()
    valid_item_ids = set(int(r[0]) for r in prod_rows)
    print(f"  {len(valid_item_ids):,} valid product item_ids")

    # Use DuckDB to extract their purchase lines from the merged dataset
    _section("Step 3: Extract purchase lines via DuckDB")
    print(f"  Reading from: {MERGED_FILE.name} (7.46GB)")
    print(f"  Filtering to {len(cust_ids)} customers ...")

    cust_ids_sql = ",".join(str(c) for c in cust_ids)

    # The merged dataset has columns we need. Take the most useful ones for
    # purchase history. We need a 'date' column and a quantity and a price.
    # Following the column conventions from the cleaning pipeline:
    #   DIM_CUST_CURR_ID, DIM_ITEM_E1_CURR_ID, ORDR_QTY, UNIT_SLS_AMT, order_date
    # If column names differ in your actual file we'll surface that here.
    con = duckdb.connect()

    # First, peek at columns to confirm what we have
    schema_df = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet(?) LIMIT 1",
        [str(MERGED_FILE)],
    ).fetchdf()
    cols_avail = set(schema_df["column_name"].tolist())
    print(f"  Columns available: {len(cols_avail)} total")

    def first_avail(candidates: list[str]) -> str | None:
        for c in candidates:
            if c in cols_avail:
                return c
        return None

    cust_col = first_avail(["DIM_CUST_CURR_ID", "cust_id"])
    item_col = first_avail(["DIM_ITEM_E1_CURR_ID", "item_id"])
    qty_col = first_avail(["ORDR_QTY", "quantity"])
    price_col = first_avail(["UNIT_SLS_AMT", "unit_price"])
    # DIM_ORDR_DT_ID is the date column in our merged_dataset; it can be a date
    # or an integer like 20250115 (YYYYMMDD). The script handles both downstream.
    date_col = first_avail(["order_date", "DIM_ORDR_DT_ID", "ORDR_DT", "sold_at"])

    missing = [n for n, v in {"cust_id": cust_col, "item_id": item_col,
                              "qty": qty_col, "price": price_col,
                              "date": date_col}.items() if v is None]
    if missing:
        raise RuntimeError(
            f"merged_dataset.parquet is missing expected columns for: {missing}.\n"
            f"Columns I see: {sorted(cols_avail)[:30]}..."
        )

    print(f"  Mapping: cust={cust_col}, item={item_col}, qty={qty_col}, "
          f"price={price_col}, date={date_col}")

    # Pull all matching rows. We do this in DuckDB to a DataFrame because
    # for 30 customers the result is small enough to fit in memory
    # (typically <1M rows total).
    df = con.execute(
        f"""
        SELECT
            {cust_col}  AS cust_id,
            {item_col}  AS item_id,
            {qty_col}   AS quantity,
            {price_col} AS unit_price,
            {date_col}  AS sold_at
        FROM read_parquet(?)
        WHERE {cust_col} IN ({cust_ids_sql})
          AND {qty_col} > 0
          AND {price_col} > 0
        """,
        [str(MERGED_FILE)],
    ).fetchdf()
    con.close()

    print(f"  Extracted {len(df):,} purchase lines from parquet")

    # Filter to lines whose item_id exists in the products table
    before = len(df)
    df = df[df["item_id"].astype("int64").isin(valid_item_ids)]
    after = len(df)
    if before != after:
        print(f"  Filtered out {before - after:,} lines with items not in recdash.products")

    if len(df) == 0:
        print("  WARNING: no purchase lines after filtering. Nothing to insert.")
        return

    # Coerce types
    df["cust_id"] = df["cust_id"].astype("int64")
    df["item_id"] = df["item_id"].astype("int64")
    df["quantity"] = df["quantity"].astype("int64").clip(lower=1)
    df["unit_price"] = df["unit_price"].astype(float).round(2)

    # Date handling: DIM_ORDR_DT_ID can be either a real date/datetime or an
    # integer in YYYYMMDD form. Convert to a real timestamp either way.
    sold_at = df["sold_at"]
    if pd.api.types.is_integer_dtype(sold_at) or pd.api.types.is_float_dtype(sold_at):
        # YYYYMMDD integer form -> convert via string
        df["sold_at"] = pd.to_datetime(
            sold_at.astype("Int64").astype(str),
            format="%Y%m%d",
            errors="coerce",
        )
    else:
        # Already a date / datetime / string -> coerce to datetime
        df["sold_at"] = pd.to_datetime(sold_at, errors="coerce")

    # Drop any rows where the date could not be parsed
    before_dates = len(df)
    df = df[df["sold_at"].notna()]
    dropped = before_dates - len(df)
    if dropped:
        print(f"  Dropped {dropped:,} lines with unparseable dates")

    # Print per-customer line counts
    line_counts = df.groupby("cust_id").size().reset_index(name="lines")
    print(f"  Per-customer line counts: "
          f"min={line_counts['lines'].min()}, "
          f"median={int(line_counts['lines'].median())}, "
          f"max={line_counts['lines'].max()}")

    # Insert into Postgres
    _section("Step 4: INSERT into recdash.purchase_history")
    insert_sql = text(f"""
        INSERT INTO {schema}.purchase_history
            (cust_id, item_id, quantity, unit_price, sold_at)
        VALUES
            (:cust_id, :item_id, :quantity, :unit_price, :sold_at)
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
            print(f"  Inserted {inserted:>8,} / {total:,}   "
                  f"({elapsed:5.1f}s, {rate:>6,.0f} rows/sec)")

    # Verify
    _section("Step 5: Verify")
    with engine.connect() as conn:
        n = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.purchase_history")).scalar()
        n_custs = conn.execute(text(
            f"SELECT COUNT(DISTINCT cust_id) FROM {schema}.purchase_history"
        )).scalar()
    print(f"  purchase_history rows: {n:,}")
    print(f"  Distinct customers   : {n_custs}")

    print()
    print("-" * 70)
    print(f"  Total time: {time.time() - t0:.1f}s")
    print("-" * 70)


if __name__ == "__main__":
    main()