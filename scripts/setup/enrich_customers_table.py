"""
enrich_customers_table.py

Pulls customer status (cold_start / stable_warm / declining_warm / churned_warm)
and archetype (specialty_clinic, primary_care, surgery_center, etc.) from
parquet files, then alters the recdash.customers table to add these columns
and backfills the values for every customer.

Run once after re-running the recommendation pipeline. Idempotent: safe to
re-run if you ever update the underlying parquet data.

Usage:
    cd C:\\Users\\maina\\Desktop\\Capstone
    conda activate CTBA
    python enrich_customers_table.py

Optional flags:
    --dry-run     Only print what would happen, don't modify the database
    --no-archetype  Skip archetype computation (only add status)
    --no-status     Skip status computation (only add archetype)
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# Paths

ROOT       = Path(__file__).resolve().parent
DATA_CLEAN = ROOT / "data_clean"
PRECOMP    = DATA_CLEAN / "serving" / "precomputed"
FEATURES   = DATA_CLEAN / "features"

PATTERNS_FILE = PRECOMP / "customer_patterns.parquet"
SEGMENTS_FILE = PRECOMP / "customer_segments.parquet"
FEATURE_FILE  = FEATURES / "customer_features.parquet"


# Postgres config — reads from .env via your existing backend config

def load_pg_config() -> dict:
    """Read Postgres credentials from .env file."""
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
        print(f"WARNING: .env not found at {env_path}, using defaults", file=sys.stderr)
        return config

    with env_path.open() as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip().lower()
            val = val.strip().strip('"').strip("'")
            if key == "postgres_host":
                config["host"] = val
            elif key == "postgres_port":
                config["port"] = int(val)
            elif key == "postgres_db":
                config["dbname"] = val
            elif key == "postgres_user":
                config["user"] = val
            elif key == "postgres_password":
                config["password"] = val
            elif key == "postgres_schema":
                config["schema"] = val
    return config


# Step 1: Load status from customer_patterns.parquet

def load_status() -> pd.DataFrame:
    """Compute status label for every customer based on the 3 boolean flags."""
    print("\n[Step 1] Loading customer status from customer_patterns.parquet")
    t0 = time.time()

    if not PATTERNS_FILE.exists():
        print(f"FATAL: {PATTERNS_FILE} not found.")
        print("Run analyze_buying_patterns.py first.", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect()
    df = con.execute(f"""
        SELECT
            CAST(DIM_CUST_CURR_ID AS BIGINT) AS cust_id,
            COALESCE(is_cold_start, 0)       AS is_cold_start,
            COALESCE(is_churned, 0)          AS is_churned,
            COALESCE(is_declining, 0)        AS is_declining,
            COALESCE(n_unique_products_total, 0) AS n_unique_products
        FROM read_parquet('{PATTERNS_FILE.as_posix()}')
    """).df()
    con.close()

    df["cust_id"] = df["cust_id"].astype("int64")

    # Compute status using the same logic as recommendation_factors.py
    def get_status(row):
        if row["is_cold_start"] == 1:
            return "cold_start"
        if row["is_churned"] == 1:
            return "churned_warm"
        if row["is_declining"] == 1:
            return "declining_warm"
        return "stable_warm"

    df["status"] = df.apply(get_status, axis=1)

    counts = df["status"].value_counts()
    print(f"  Loaded {len(df):,} customers in {time.time()-t0:.1f}s")
    print(f"  Status distribution:")
    for status, n in counts.items():
        pct = n / len(df) * 100
        print(f"    {status:<20} {n:>10,} ({pct:.1f}%)")

    return df[["cust_id", "status", "is_cold_start", "is_churned", "is_declining"]]


# Step 2: Compute archetype from features parquet (same logic as compute_customer_archetypes.py)

# Mirror the archetype rules used in compute_customer_archetypes.py.
# If you change the rules there, update them here too.

ARCHETYPE_RULES = [
    # (archetype_name, set of SPCLTY_CD or SPCLTY_DSC keywords that match)
    ("hospital_acute",       {"AC"}),  # market-based; specialty list below as backup
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


def load_archetype() -> pd.DataFrame:
    """Compute archetype for every customer based on SPCLTY_CD + market."""
    print("\n[Step 2] Loading customer archetype from customer_features.parquet")
    t0 = time.time()

    if not FEATURE_FILE.exists():
        print(f"FATAL: {FEATURE_FILE} not found.")
        print("Run clean_data.py first.", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect()
    # Pull the columns we need - SPCLTY_CD, SPCLTY_DSC if present, and market for fallback
    cols_query = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{FEATURE_FILE.as_posix()}')"
    ).df()
    avail_cols = set(cols_query["column_name"].tolist())

    select_cols = ["DIM_CUST_CURR_ID", "SPCLTY_CD"]
    if "SPCLTY_DSC" in avail_cols:
        select_cols.append("SPCLTY_DSC")
    if "MKT_CD" in avail_cols:
        select_cols.append("MKT_CD")

    df = con.execute(f"""
        SELECT {", ".join(select_cols)}
        FROM read_parquet('{FEATURE_FILE.as_posix()}')
    """).df()
    con.close()

    df = df.rename(columns={"DIM_CUST_CURR_ID": "cust_id"})
    df["cust_id"] = df["cust_id"].astype("int64")

    # Apply archetype rules in priority order. First match wins.
    def assign_archetype(row):
        spclty = str(row.get("SPCLTY_CD", "")).strip().upper() if pd.notna(row.get("SPCLTY_CD")) else ""
        spclty_dsc = str(row.get("SPCLTY_DSC", "")).strip().upper() if pd.notna(row.get("SPCLTY_DSC", "")) else ""
        mkt = str(row.get("MKT_CD", "")).strip().upper() if pd.notna(row.get("MKT_CD", "")) else ""

        # No specialty info at all
        if not spclty and not spclty_dsc:
            return "unknown"

        # Hospital acute care: prioritize MKT_CD == AC
        if mkt == "AC":
            return "hospital_acute"

        # Run through archetype rules
        for archetype, keywords in ARCHETYPE_RULES:
            if archetype == "hospital_acute":
                continue  # handled above
            if spclty in keywords:
                return archetype
            # Also check description keyword match
            for kw in keywords:
                if kw in spclty_dsc:
                    return archetype

        # Catch-all
        return "specialty_clinic"

    df["archetype"] = df.apply(assign_archetype, axis=1)

    counts = df["archetype"].value_counts()
    print(f"  Computed archetype for {len(df):,} customers in {time.time()-t0:.1f}s")
    print(f"  Top 10 archetypes:")
    for archetype, n in counts.head(10).items():
        pct = n / len(df) * 100
        print(f"    {archetype:<22} {n:>10,} ({pct:.1f}%)")

    return df[["cust_id", "archetype"]]


# Step 3: Alter the customers table

def alter_table(pg_config: dict, add_status: bool, add_archetype: bool, dry_run: bool) -> None:
    """Add status and/or archetype columns if they don't exist."""
    print("\n[Step 3] Altering recdash.customers table")
    schema = pg_config["schema"]

    if dry_run:
        print(f"  DRY RUN - would run:")
        if add_status:
            print(f"    ALTER TABLE {schema}.customers ADD COLUMN IF NOT EXISTS status VARCHAR(20)")
        if add_archetype:
            print(f"    ALTER TABLE {schema}.customers ADD COLUMN IF NOT EXISTS archetype VARCHAR(50)")
        return

    conn = psycopg2.connect(
        host=pg_config["host"],
        port=pg_config["port"],
        dbname=pg_config["dbname"],
        user=pg_config["user"],
        password=pg_config["password"],
    )
    conn.autocommit = True
    cur = conn.cursor()

    if add_status:
        sql = f"ALTER TABLE {schema}.customers ADD COLUMN IF NOT EXISTS status VARCHAR(20)"
        print(f"  Running: {sql}")
        cur.execute(sql)
        # Add index for fast filtering
        sql = f"CREATE INDEX IF NOT EXISTS idx_customers_status ON {schema}.customers (status)"
        print(f"  Running: {sql}")
        cur.execute(sql)

    if add_archetype:
        sql = f"ALTER TABLE {schema}.customers ADD COLUMN IF NOT EXISTS archetype VARCHAR(50)"
        print(f"  Running: {sql}")
        cur.execute(sql)
        sql = f"CREATE INDEX IF NOT EXISTS idx_customers_archetype ON {schema}.customers (archetype)"
        print(f"  Running: {sql}")
        cur.execute(sql)

    cur.close()
    conn.close()
    print("  Schema updated.")


# Step 4: Backfill values

def backfill_values(
    pg_config: dict,
    df: pd.DataFrame,
    update_status: bool,
    update_archetype: bool,
    dry_run: bool,
) -> None:
    """Update existing customer rows with status and archetype values."""
    print(f"\n[Step 4] Backfilling values for {len(df):,} customers")
    t0 = time.time()
    schema = pg_config["schema"]

    if dry_run:
        print(f"  DRY RUN - sample of what would be updated:")
        print(df.head(5).to_string(index=False))
        return

    conn = psycopg2.connect(
        host=pg_config["host"],
        port=pg_config["port"],
        dbname=pg_config["dbname"],
        user=pg_config["user"],
        password=pg_config["password"],
    )
    cur = conn.cursor()

    # Use temp table + UPDATE FROM for fast bulk update
    cur.execute("""
        CREATE TEMPORARY TABLE _enrichment_staging (
            cust_id BIGINT PRIMARY KEY,
            status VARCHAR(20),
            archetype VARCHAR(50)
        )
    """)

    rows = []
    for _, r in df.iterrows():
        rows.append((
            int(r["cust_id"]),
            r.get("status") if update_status and "status" in df.columns else None,
            r.get("archetype") if update_archetype and "archetype" in df.columns else None,
        ))

    print(f"  Inserting {len(rows):,} rows into staging table...")
    execute_values(
        cur,
        "INSERT INTO _enrichment_staging (cust_id, status, archetype) VALUES %s",
        rows,
        page_size=5000,
    )

    # Build UPDATE statement based on what we're updating
    set_clauses = []
    if update_status:
        set_clauses.append("status = s.status")
    if update_archetype:
        set_clauses.append("archetype = s.archetype")

    if set_clauses:
        update_sql = f"""
            UPDATE {schema}.customers c
               SET {", ".join(set_clauses)}
              FROM _enrichment_staging s
             WHERE c.cust_id = s.cust_id
        """
        print(f"  Running bulk UPDATE...")
        cur.execute(update_sql)
        n_updated = cur.rowcount
        print(f"  Updated {n_updated:,} customer rows")

    conn.commit()
    cur.close()
    conn.close()
    print(f"  Backfill complete in {time.time()-t0:.1f}s")


# Step 5: Verification

def verify(pg_config: dict, update_status: bool, update_archetype: bool) -> None:
    """Run sanity queries to confirm the data was written correctly."""
    print(f"\n[Step 5] Verifying enrichment")
    schema = pg_config["schema"]

    conn = psycopg2.connect(
        host=pg_config["host"],
        port=pg_config["port"],
        dbname=pg_config["dbname"],
        user=pg_config["user"],
        password=pg_config["password"],
    )
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM {schema}.customers")
    total = cur.fetchone()[0]
    print(f"  Total customers in DB: {total:,}")

    if update_status:
        cur.execute(f"""
            SELECT status, COUNT(*) AS n
              FROM {schema}.customers
             WHERE status IS NOT NULL
             GROUP BY status
             ORDER BY n DESC
        """)
        print(f"\n  Status distribution:")
        for row in cur.fetchall():
            pct = row[1] / total * 100
            print(f"    {row[0]:<20} {row[1]:>10,} ({pct:.1f}%)")

        cur.execute(f"SELECT COUNT(*) FROM {schema}.customers WHERE status IS NULL")
        n_null = cur.fetchone()[0]
        if n_null > 0:
            print(f"  WARNING: {n_null:,} customers have NULL status (probably new users not in parquet)")

    if update_archetype:
        cur.execute(f"""
            SELECT archetype, COUNT(*) AS n
              FROM {schema}.customers
             WHERE archetype IS NOT NULL
             GROUP BY archetype
             ORDER BY n DESC
             LIMIT 10
        """)
        print(f"\n  Top 10 archetypes:")
        for row in cur.fetchall():
            pct = row[1] / total * 100
            print(f"    {row[0]:<22} {row[1]:>10,} ({pct:.1f}%)")

        cur.execute(f"SELECT COUNT(*) FROM {schema}.customers WHERE archetype IS NULL")
        n_null = cur.fetchone()[0]
        if n_null > 0:
            print(f"  WARNING: {n_null:,} customers have NULL archetype")

    # Sample query: how many of each combination?
    if update_status and update_archetype:
        cur.execute(f"""
            SELECT status, archetype, COUNT(*) AS n
              FROM {schema}.customers
             WHERE status IS NOT NULL AND archetype IS NOT NULL
             GROUP BY status, archetype
             ORDER BY n DESC
             LIMIT 10
        """)
        print(f"\n  Top 10 (status, archetype) combinations:")
        for row in cur.fetchall():
            print(f"    {row[0]:<18} {row[1]:<22} {row[2]:>8,}")

    cur.close()
    conn.close()


# Main

def main():
    parser = argparse.ArgumentParser(
        description="Enrich recdash.customers table with status + archetype columns"
    )
    parser.add_argument("--dry-run",       action="store_true", help="Print what would happen, don't modify DB")
    parser.add_argument("--no-status",    action="store_true", help="Skip status column")
    parser.add_argument("--no-archetype", action="store_true", help="Skip archetype column")
    args = parser.parse_args()

    update_status    = not args.no_status
    update_archetype = not args.no_archetype

    if not update_status and not update_archetype:
        print("ERROR: nothing to do. Don't pass both --no-status and --no-archetype.")
        sys.exit(1)

    print("=" * 70)
    print("  CUSTOMER TABLE ENRICHMENT")
    print("=" * 70)
    print(f"  Update status:    {update_status}")
    print(f"  Update archetype: {update_archetype}")
    print(f"  Dry run:          {args.dry_run}")

    pg_config = load_pg_config()
    print(f"\n  Postgres: {pg_config['user']}@{pg_config['host']}:{pg_config['port']}/{pg_config['dbname']} (schema={pg_config['schema']})")

    # Step 1-2: Load enrichment data
    df_status = load_status() if update_status else None
    df_archetype = load_archetype() if update_archetype else None

    # Merge status + archetype
    if update_status and update_archetype:
        df = df_status[["cust_id", "status"]].merge(
            df_archetype[["cust_id", "archetype"]],
            on="cust_id", how="outer"
        )
    elif update_status:
        df = df_status[["cust_id", "status"]]
    else:
        df = df_archetype[["cust_id", "archetype"]]

    print(f"\n  Total customers to enrich: {len(df):,}")

    # Step 3: Alter the table
    alter_table(pg_config, update_status, update_archetype, args.dry_run)

    # Step 4: Backfill
    backfill_values(pg_config, df, update_status, update_archetype, args.dry_run)

    # Step 5: Verify
    if not args.dry_run:
        verify(pg_config, update_status, update_archetype)

    print("\n" + "=" * 70)
    print("  COMPLETE")
    print("=" * 70)
    if not args.dry_run:
        print("\n  Next steps:")
        print("  1. Update backend SQLAlchemy model (backend/models/customer.py):")
        print("     Add:")
        print("       status:    Mapped[Optional[str]] = mapped_column(String(20))")
        print("       archetype: Mapped[Optional[str]] = mapped_column(String(50))")
        print("  2. Update CustomerResponse schema (backend/schemas/customer.py)")
        print("  3. Restart backend to pick up the new columns")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"\nFATAL ERROR: {exc}", file=sys.stderr)
        raise