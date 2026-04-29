"""
load_demo_users_and_history_v2.py

V2 fixes:
  1. Excludes customers with zero transactions from picks (was picking ghost
     customers that exist in customer_features.parquet but have no transactions
     in merged_dataset.parquet).
  2. Uses a temporary in-memory DuckDB table for cust_id filtering instead of
     a giant IN clause string (more reliable for 100+ IDs).
  3. Skips re-creating users that already exist (idempotent).

Usage:
    python load_demo_users_and_history_v2.py --dry-run
    python load_demo_users_and_history_v2.py
    python load_demo_users_and_history_v2.py --truncate     # wipe history first
    python load_demo_users_and_history_v2.py --no-users     # skip user creation
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import bcrypt
import duckdb
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# Paths

ROOT       = Path(__file__).resolve().parent
DATA_CLEAN = ROOT / "data_clean"
PRECOMP    = DATA_CLEAN / "serving" / "precomputed"
FEATURES   = DATA_CLEAN / "features"

PATTERNS_FILE  = PRECOMP / "customer_patterns.parquet"
SEGMENTS_FILE  = PRECOMP / "customer_segments.parquet"
FEATURES_FILE  = FEATURES / "customer_features.parquet"
MERGED_FILE    = DATA_CLEAN / "serving" / "merged_dataset.parquet"

DEMO_PASSWORD  = "Demo1234!"


# Postgres config

def load_pg_config() -> dict:
    env_path = ROOT / ".env"
    config = {
        "host":     "localhost",
        "port":     5432,
        "dbname":   "recommendation_dashboard",
        "user":     "postgres",
        "password": "",
        "schema":   "recdash",
    }
    if not env_path.exists():
        print(f"WARNING: .env not found, using defaults", file=sys.stderr)
        return config

    with env_path.open() as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip().lower()
            val = val.strip().strip('"').strip("'")
            mapping = {
                "postgres_host": "host",
                "postgres_port": "port",
                "postgres_db": "dbname",
                "postgres_user": "user",
                "postgres_password": "password",
                "postgres_schema": "schema",
            }
            if key in mapping:
                config[mapping[key]] = int(val) if key == "postgres_port" else val
    return config


# Step 1: Build customer attribute table

def build_customer_table() -> pd.DataFrame:
    """Pull customer attributes from parquet. ONLY return customers that have
    transactions (i.e., exist in customer_patterns.parquet)."""
    print("\n[Step 1] Loading customer attributes from parquet")
    t0 = time.time()

    for f in [PATTERNS_FILE, SEGMENTS_FILE, FEATURES_FILE]:
        if not f.exists():
            print(f"FATAL: {f} not found", file=sys.stderr)
            sys.exit(1)

    con = duckdb.connect()

    # Use customer_patterns as the BASE (since it only has customers with transactions)
    patterns = con.execute(f"""
        SELECT
            CAST(DIM_CUST_CURR_ID AS BIGINT)         AS cust_id,
            COALESCE(is_cold_start, 0)               AS is_cold_start,
            COALESCE(is_churned, 0)                  AS is_churned,
            COALESCE(is_declining, 0)                AS is_declining,
            COALESCE(n_unique_products_total, 0)     AS n_unique_products,
            COALESCE(total_spend_all_time, 0)        AS total_spend
        FROM read_parquet('{PATTERNS_FILE.as_posix()}')
        WHERE COALESCE(n_unique_products_total, 0) > 0
    """).df()
    print(f"  Loaded {len(patterns):,} customers with transactions from patterns")

    segs = con.execute(f"""
        SELECT
            CAST(DIM_CUST_CURR_ID AS BIGINT) AS cust_id,
            segment, size_tier, mkt_cd_clean
        FROM read_parquet('{SEGMENTS_FILE.as_posix()}')
    """).df()

    cols_q = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{FEATURES_FILE.as_posix()}')"
    ).df()
    feat_cols = set(cols_q["column_name"].tolist())

    select_cols = ["DIM_CUST_CURR_ID", "SPCLTY_CD"]
    if "SPCLTY_DSC" in feat_cols: select_cols.append("SPCLTY_DSC")
    if "MKT_CD" in feat_cols:     select_cols.append("MKT_CD")
    if "supplier_profile" in feat_cols: select_cols.append("supplier_profile")

    features = con.execute(f"""
        SELECT {", ".join(select_cols)}
        FROM read_parquet('{FEATURES_FILE.as_posix()}')
    """).df()
    features = features.rename(columns={"DIM_CUST_CURR_ID": "cust_id"})
    features["cust_id"] = features["cust_id"].astype("int64")

    con.close()

    df = patterns.merge(segs, on="cust_id", how="inner")  # inner = both must exist
    df = df.merge(features, on="cust_id", how="left")

    # Compute status
    def _status(r):
        if r["is_cold_start"] == 1: return "cold_start"
        if r["is_churned"] == 1:    return "churned_warm"
        if r["is_declining"] == 1:  return "declining_warm"
        return "stable_warm"
    df["status"] = df.apply(_status, axis=1)

    # Compute archetype
    archetype_rules = [
        ("skilled_nursing",      {"HIA", "SKL"}),
        ("home_care_provider",   {"HMC", "HMH"}),
        ("home_infusion",        {"HMI", "INF"}),
        ("surgery_center",       {"GS", "OPS", "ORT", "URO", "OTO", "OPH", "PLS", "NSG"}),
        ("primary_care",         {"FP", "IM", "GP"}),
        ("pediatric",            {"PED", "NEO"}),
        ("multispecialty_group", {"M01", "M08", "MUL"}),
        ("community_health",     {"COMHC", "FQHC", "RHC"}),
        ("government",           {"VA", "DOD", "FED", "PRS"}),
        ("educational",          {"EDU", "SCH", "COL", "UNV"}),
        ("pharmacy",             {"RX", "PHM", "RXP"}),
        ("lab_pathology",        {"PTH", "LAB", "CYT"}),
        ("veterinary",           {"VET", "ANM"}),
        ("marketplace_reseller", {"FBA", "MKT", "X"}),
    ]

    def _archetype(r):
        spc = str(r.get("SPCLTY_CD", "")).strip().upper() if pd.notna(r.get("SPCLTY_CD")) else ""
        mkt = str(r.get("MKT_CD", "")).strip().upper() if pd.notna(r.get("MKT_CD", "")) else ""
        if not spc:
            return "unknown"
        if mkt == "AC":
            return "hospital_acute"
        for archetype, kws in archetype_rules:
            if spc in kws:
                return archetype
        return "specialty_clinic"

    df["archetype"] = df.apply(_archetype, axis=1)

    if "supplier_profile" not in df.columns:
        df["supplier_profile"] = "mixed"
    else:
        df["supplier_profile"] = df["supplier_profile"].fillna("mixed")

    print(f"  Built attribute table: {len(df):,} customers in {time.time()-t0:.1f}s")
    print(f"  All customers in this table have at least 1 transaction")
    return df


# Step 2: Pick representatives - prefer real-history customers

def pick_representatives(df: pd.DataFrame) -> pd.DataFrame:
    """Select one customer per dimension cell. Prefers customers with rich
    history (high n_unique_products + total_spend)."""
    print("\n[Step 2] Picking representative customers per dimension cell")

    df = df.copy()
    # Score: combine product diversity + spend (capped to avoid outlier dominance)
    df["picked_score"] = (
        df["n_unique_products"].fillna(0) +
        (df["total_spend"].fillna(0) / 1000).clip(upper=200)
    )

    selected = pd.DataFrame()

    # 1. One per (segment, status)
    seg_status = (
        df.sort_values("picked_score", ascending=False)
          .groupby(["segment", "status"], as_index=False)
          .first()
    )
    seg_status["pick_reason"] = "segment_x_status"
    print(f"  Segment x status: {len(seg_status)} customers")
    selected = pd.concat([selected, seg_status])

    # 2. One per (archetype, status) - skip already-picked
    arch_status = (
        df.sort_values("picked_score", ascending=False)
          .groupby(["archetype", "status"], as_index=False)
          .first()
    )
    arch_status["pick_reason"] = "archetype_x_status"
    new_arch = arch_status[~arch_status["cust_id"].isin(selected["cust_id"])]
    print(f"  Archetype x status: {len(arch_status)} customers ({len(new_arch)} new)")
    selected = pd.concat([selected, new_arch])

    # 3. One per (supplier_profile, status)
    sup_status = (
        df.sort_values("picked_score", ascending=False)
          .groupby(["supplier_profile", "status"], as_index=False)
          .first()
    )
    sup_status["pick_reason"] = "supplier_x_status"
    new_sup = sup_status[~sup_status["cust_id"].isin(selected["cust_id"])]
    print(f"  Supplier x status: {len(sup_status)} customers ({len(new_sup)} new)")
    selected = pd.concat([selected, new_sup])

    selected = selected.drop_duplicates(subset=["cust_id"], keep="first")

    print(f"\n  Total unique representatives: {len(selected)}")
    print(f"\n  Status breakdown:")
    for status, n in selected["status"].value_counts().items():
        print(f"    {status:<20} {n}")

    print(f"\n  Pick reason breakdown:")
    for reason, n in selected["pick_reason"].value_counts().items():
        print(f"    {reason:<25} {n}")

    return selected[[
        "cust_id", "segment", "size_tier", "mkt_cd_clean",
        "status", "archetype", "supplier_profile",
        "n_unique_products", "total_spend", "pick_reason",
    ]].reset_index(drop=True)


# Step 3: Ensure customer rows in DB

def ensure_customers_in_db(pg_config: dict, picks: pd.DataFrame, dry_run: bool) -> None:
    """Make sure each picked cust_id has a row in recdash.customers."""
    print("\n[Step 3] Ensuring customer rows exist in recdash.customers")
    schema = pg_config["schema"]

    conn = psycopg2.connect(
        host=pg_config["host"], port=pg_config["port"],
        dbname=pg_config["dbname"], user=pg_config["user"],
        password=pg_config["password"],
    )
    cur = conn.cursor()
    cur.execute(f"SELECT cust_id FROM {schema}.customers")
    existing = set(row[0] for row in cur.fetchall())

    missing = [int(c) for c in picks["cust_id"].tolist() if int(c) not in existing]
    print(f"  Already in DB: {len(picks) - len(missing)}")
    print(f"  Missing:       {len(missing)}")

    if missing and not dry_run:
        rows = []
        for _, r in picks.iterrows():
            if int(r["cust_id"]) in missing:
                rows.append((
                    int(r["cust_id"]),
                    f"Demo Customer {int(r['cust_id'])}",
                    None,
                    r["mkt_cd_clean"],
                    r["segment"],
                    r["supplier_profile"],
                    r["status"],
                    r["archetype"],
                ))
        execute_values(
            cur,
            f"""
            INSERT INTO {schema}.customers
                (cust_id, customer_name, specialty_code, market_code,
                 segment, supplier_profile, status, archetype)
            VALUES %s
            ON CONFLICT (cust_id) DO NOTHING
            """,
            rows,
        )
        conn.commit()
        print(f"  Inserted {len(rows)} missing customer rows")

    cur.close()
    conn.close()


# Step 4: Create users (idempotent)

def create_users(pg_config: dict, picks: pd.DataFrame, dry_run: bool) -> None:
    """Create a customer-role user for each picked cust_id if missing."""
    print("\n[Step 4] Creating user accounts")
    schema = pg_config["schema"]

    conn = psycopg2.connect(
        host=pg_config["host"], port=pg_config["port"],
        dbname=pg_config["dbname"], user=pg_config["user"],
        password=pg_config["password"],
    )
    cur = conn.cursor()

    cur.execute(f"""
        SELECT user_id, cust_id, username
          FROM {schema}.users
         WHERE cust_id IS NOT NULL
    """)
    cust_to_user = {}
    existing_usernames = set()
    for uid, cid, uname in cur.fetchall():
        cust_to_user[cid] = uid
        existing_usernames.add(uname)

    cur.execute(f"SELECT username FROM {schema}.users WHERE cust_id IS NULL")
    for (uname,) in cur.fetchall():
        existing_usernames.add(uname)

    pw_hash = bcrypt.hashpw(DEMO_PASSWORD.encode(), bcrypt.gensalt(12)).decode()

    new_users = []
    skipped = 0
    for _, r in picks.iterrows():
        cust_id = int(r["cust_id"])
        if cust_id in cust_to_user:
            skipped += 1
            continue

        seg = (r["segment"] or "unknown").lower()
        status = (r["status"] or "unknown").replace("_warm", "").replace("_start", "")
        username = f"demo_{seg}_{status}_{cust_id % 10000:04d}"
        i = 1
        original = username
        while username in existing_usernames:
            username = f"{original}_{i}"
            i += 1
        existing_usernames.add(username)

        new_users.append({
            "username": username,
            "password_hash": pw_hash,
            "role": "customer",
            "full_name": f"Demo {r['archetype'].replace('_', ' ').title()}",
            "email": f"{username}@capstone.local",
            "cust_id": cust_id,
        })

    print(f"  Already have user accounts: {skipped}")
    print(f"  New users to create:        {len(new_users)}")

    if new_users and not dry_run:
        rows = [
            (
                u["username"], u["password_hash"], u["role"],
                u["full_name"], u["email"], u["cust_id"],
                datetime.now(), True,
            )
            for u in new_users
        ]
        execute_values(
            cur,
            f"""
            INSERT INTO {schema}.users
                (username, password_hash, role, full_name, email,
                 cust_id, created_at, is_active)
            VALUES %s
            ON CONFLICT (username) DO NOTHING
            """,
            rows,
        )
        conn.commit()
        print(f"  Inserted {len(new_users)} users (password: {DEMO_PASSWORD})")

    cur.close()
    conn.close()


# Step 5: Pull purchase history using DuckDB JOIN (not big IN clause)

def pull_purchase_history(picks: pd.DataFrame) -> pd.DataFrame:
    """Pull ALL transactions for picked customers using a DuckDB join with an
    in-memory table (more reliable than huge IN clause)."""
    print(f"\n[Step 5] Pulling all transactions from merged_dataset.parquet")
    t0 = time.time()

    if not MERGED_FILE.exists():
        print(f"FATAL: {MERGED_FILE} not found", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect()

    # Register the picks DataFrame as a DuckDB table - reliable for any size
    pick_ids_df = picks[["cust_id"]].copy()
    pick_ids_df["cust_id"] = pick_ids_df["cust_id"].astype("int64")
    con.register("pick_ids", pick_ids_df)

    # Use JOIN instead of IN clause (more reliable, faster for many IDs)
    df = con.execute(f"""
        SELECT
            CAST(m.DIM_CUST_CURR_ID AS BIGINT)      AS cust_id,
            CAST(m.DIM_ITEM_E1_CURR_ID AS BIGINT)   AS item_id,
            COALESCE(m.PRMRY_QTY, m.ORDR_QTY, m.SHIP_QTY, 1) AS quantity_raw,
            m.UNIT_SLS_AMT                          AS line_total,
            m.DIM_ORDR_DT_ID                         AS order_date_int
          FROM read_parquet('{MERGED_FILE.as_posix()}') m
          INNER JOIN pick_ids p
            ON CAST(m.DIM_CUST_CURR_ID AS BIGINT) = p.cust_id
         WHERE m.UNIT_SLS_AMT IS NOT NULL
           AND m.UNIT_SLS_AMT > 0
           AND m.DIM_ITEM_E1_CURR_ID IS NOT NULL
    """).df()
    con.close()

    print(f"  Pulled {len(df):,} transaction rows in {time.time()-t0:.1f}s")

    if len(df) == 0:
        print("  WARNING: 0 rows returned - check the diagnostic")
        return df

    # Convert quantity safely
    df["quantity"] = pd.to_numeric(df["quantity_raw"], errors="coerce")
    df["quantity"] = df["quantity"].fillna(1).astype("int64").clip(lower=1)

    # Compute unit price
    df["line_total"] = pd.to_numeric(df["line_total"], errors="coerce")
    df["unit_price"] = (df["line_total"] / df["quantity"]).round(2)
    df["unit_price"] = df["unit_price"].fillna(0)

    # Convert YYYYMMDD int to datetime
    df["order_date_int"] = pd.to_numeric(df["order_date_int"], errors="coerce")
    df = df.dropna(subset=["order_date_int"])
    df["sold_at"] = pd.to_datetime(
        df["order_date_int"].astype("int64").astype(str),
        format="%Y%m%d", errors="coerce"
    )
    df = df.dropna(subset=["sold_at"])

    print(f"  After cleaning: {len(df):,} rows")
    print(f"  Date range: {df['sold_at'].min().date()} to {df['sold_at'].max().date()}")
    print(f"  Customers covered: {df['cust_id'].nunique():,} of {len(picks)} picks")
    print(f"  Avg transactions per customer: {len(df) / df['cust_id'].nunique():.1f}")

    return df[["cust_id", "item_id", "quantity", "unit_price", "sold_at"]]


# Step 6: Filter to products in DB

def filter_to_db_products(pg_config: dict, df: pd.DataFrame) -> pd.DataFrame:
    print(f"\n[Step 6] Filtering to products that exist in DB")
    if len(df) == 0:
        return df
    schema = pg_config["schema"]

    conn = psycopg2.connect(
        host=pg_config["host"], port=pg_config["port"],
        dbname=pg_config["dbname"], user=pg_config["user"],
        password=pg_config["password"],
    )
    cur = conn.cursor()
    cur.execute(f"SELECT item_id FROM {schema}.products")
    db_items = set(row[0] for row in cur.fetchall())
    cur.close()
    conn.close()

    print(f"  {len(db_items):,} products in DB")
    before = len(df)
    df = df[df["item_id"].isin(db_items)].copy()
    after = len(df)
    pct = (before - after) / before * 100 if before else 0
    print(f"  Kept {after:,} rows, dropped {before - after:,} ({pct:.1f}%) for missing products")
    return df


# Step 7: Insert into purchase_history

def insert_purchase_history(pg_config: dict, df: pd.DataFrame, truncate: bool, dry_run: bool):
    print(f"\n[Step 7] Inserting {len(df):,} rows into purchase_history")
    t0 = time.time()
    schema = pg_config["schema"]

    if dry_run:
        if truncate:
            print(f"  DRY RUN - would TRUNCATE {schema}.purchase_history first")
        print(f"  DRY RUN - would insert {len(df):,} rows")
        return

    if len(df) == 0:
        print(f"  Nothing to insert")
        return

    conn = psycopg2.connect(
        host=pg_config["host"], port=pg_config["port"],
        dbname=pg_config["dbname"], user=pg_config["user"],
        password=pg_config["password"],
    )
    cur = conn.cursor()

    if truncate:
        print(f"  Truncating {schema}.purchase_history first")
        cur.execute(f"TRUNCATE {schema}.purchase_history RESTART IDENTITY CASCADE")
        conn.commit()

    # Build rows in chunks to avoid memory issues with large datasets
    chunk_size = 10000
    total_inserted = 0
    n_chunks = (len(df) + chunk_size - 1) // chunk_size

    for chunk_idx in range(n_chunks):
        start = chunk_idx * chunk_size
        end = min(start + chunk_size, len(df))
        chunk = df.iloc[start:end]

        rows = [
            (
                int(r["cust_id"]),
                int(r["item_id"]),
                int(r["quantity"]),
                float(r["unit_price"]),
                r["sold_at"].to_pydatetime(),
            )
            for _, r in chunk.iterrows()
        ]
        execute_values(
            cur,
            f"""
            INSERT INTO {schema}.purchase_history
                (cust_id, item_id, quantity, unit_price, sold_at)
            VALUES %s
            """,
            rows,
            page_size=2000,
        )
        total_inserted += len(rows)
        print(f"  Chunk {chunk_idx+1}/{n_chunks}: inserted {total_inserted:,} / {len(df):,}")

    conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {schema}.purchase_history")
    total = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"  Done in {time.time()-t0:.1f}s")
    print(f"  Total rows in purchase_history now: {total:,}")


# Step 8: Verification

def verify(pg_config: dict, picks: pd.DataFrame):
    print(f"\n[Step 8] Verification")
    schema = pg_config["schema"]

    conn = psycopg2.connect(
        host=pg_config["host"], port=pg_config["port"],
        dbname=pg_config["dbname"], user=pg_config["user"],
        password=pg_config["password"],
    )
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM {schema}.purchase_history")
    n_rows = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(DISTINCT cust_id) FROM {schema}.purchase_history")
    n_custs = cur.fetchone()[0]
    cur.execute(f"SELECT MIN(sold_at), MAX(sold_at) FROM {schema}.purchase_history")
    min_dt, max_dt = cur.fetchone()
    cur.execute(f"""
        SELECT COUNT(*) FROM {schema}.users
         WHERE role = 'customer' AND cust_id IS NOT NULL
    """)
    n_users = cur.fetchone()[0]

    print(f"  Total purchase_history rows: {n_rows:,}")
    print(f"  Distinct customers in history: {n_custs:,}")
    print(f"  Date range: {min_dt} to {max_dt}")
    print(f"  Customer-role users with cust_id: {n_users}")

    cur.execute(f"""
        SELECT c.cust_id, c.segment, c.status, c.archetype, COUNT(ph.purchase_id) AS n_orders
          FROM {schema}.customers c
          LEFT JOIN {schema}.purchase_history ph ON c.cust_id = ph.cust_id
         WHERE c.cust_id IN (
            SELECT cust_id FROM {schema}.users WHERE role='customer' AND cust_id IS NOT NULL
         )
         GROUP BY c.cust_id, c.segment, c.status, c.archetype
         ORDER BY n_orders DESC
         LIMIT 10
    """)
    print(f"\n  Top 10 customers by order count:")
    print(f"  {'cust_id':<14} {'segment':<18} {'status':<16} {'archetype':<22} {'orders':>8}")
    for row in cur.fetchall():
        print(f"  {row[0]:<14} {str(row[1]):<18} {str(row[2]):<16} {str(row[3]):<22} {row[4]:>8,}")

    cur.execute(f"""
        SELECT COUNT(*) FROM {schema}.customers c
         WHERE c.cust_id IN (
             SELECT cust_id FROM {schema}.users WHERE role='customer' AND cust_id IS NOT NULL
         )
         AND NOT EXISTS (
             SELECT 1 FROM {schema}.purchase_history ph
              WHERE ph.cust_id = c.cust_id
         )
    """)
    n_zero = cur.fetchone()[0]
    if n_zero > 0:
        print(f"\n  WARNING: {n_zero} demo customers have NO purchase history rows")

    cur.close()
    conn.close()


# Main

def main():
    parser = argparse.ArgumentParser(
        description="V2: Load representative customers + their full purchase history"
    )
    parser.add_argument("--dry-run",  action="store_true")
    parser.add_argument("--truncate", action="store_true", help="Wipe purchase_history first")
    parser.add_argument("--no-users", action="store_true")
    args = parser.parse_args()

    print("=" * 72)
    print("  DEMO USERS + PURCHASE HISTORY LOADER (V2)")
    print("=" * 72)
    print(f"  Dry run:       {args.dry_run}")
    print(f"  Truncate:      {args.truncate}")
    print(f"  Create users:  {not args.no_users}")

    pg_config = load_pg_config()
    print(f"\n  Postgres: {pg_config['user']}@{pg_config['host']}:{pg_config['port']}/{pg_config['dbname']} (schema={pg_config['schema']})")

    df_all = build_customer_table()
    picks = pick_representatives(df_all)

    picks_path = ROOT / "demo_picks_v2.csv"
    picks.to_csv(picks_path, index=False)
    print(f"\n  Saved pick list: {picks_path}")

    ensure_customers_in_db(pg_config, picks, args.dry_run)

    if not args.no_users:
        create_users(pg_config, picks, args.dry_run)

    history = pull_purchase_history(picks)
    history = filter_to_db_products(pg_config, history)
    insert_purchase_history(pg_config, history, args.truncate, args.dry_run)

    if not args.dry_run:
        verify(pg_config, picks)

    print("\n" + "=" * 72)
    print("  COMPLETE")
    print("=" * 72)
    if not args.dry_run:
        print(f"\n  All new customer users have password: {DEMO_PASSWORD}")
        print(f"  Pick list saved to: {picks_path.name}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"\nFATAL ERROR: {exc}", file=sys.stderr)
        raise