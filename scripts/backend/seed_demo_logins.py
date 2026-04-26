from __future__ import annotations

import csv
import sys
import time
from collections import defaultdict
from pathlib import Path

import bcrypt
from sqlalchemy import text

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backend.db_connection import get_engine, get_schema  # noqa: E402


CSV_FILE = HERE / "demo_customers.csv"
PASSWORD = "demo123"

# Pool of fictional clinic names. We pick by status + market so the names
# are thematically appropriate. Real names not used.
CLINIC_NAMES_BY_MARKET = {
    "PO": [
        "Sunrise Family Practice", "Westside Medical Group", "Cornerstone Pediatrics",
        "Maplewood Internal Medicine", "Ridgeview Family Care", "Oakdale Primary Care",
        "Lakeshore Medical Associates", "Brookfield Health Center", "Hillside Care",
    ],
    "LTC": [
        "Greenwood Senior Living", "Bayview Nursing Center", "Cedarcrest Care Home",
        "Riverside Manor", "Pine Hill Rehab", "Stonebridge Senior Care",
        "Magnolia Gardens", "Heritage Assisted Living",
    ],
    "SC": [
        "Lakeside Surgical Center", "Pinewood Ambulatory", "Cornerstone Surgery Center",
        "Northpoint Surgical", "Bayfront Surgery Group", "Pacific Coast Surgical",
    ],
    "HC": [
        "Bayview Home Health", "Hometown Care Services", "Patriot Home Healthcare",
        "Comfort Care at Home",
    ],
    "LC": [
        "Quest Diagnostics Lab North", "Westgate Clinical Lab", "Hillcrest Laboratory",
        "Apex Diagnostic Services",
    ],
    "AC": [
        "Riverside General Hospital", "Mercy Memorial Medical Center",
        "Cornerstone Regional Hospital", "Northshore General Hospital",
        "Pinewood Acute Care",
    ],
}


def _section(t: str) -> None:
    print("-" * 70); print(" ", t); print("-" * 70)


def hash_pw(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def main() -> None:
    print()
    print("=" * 70)
    print("  SEED 30 DEMO CUSTOMER LOGINS")
    print("=" * 70)
    t0 = time.time()

    if not CSV_FILE.exists():
        raise FileNotFoundError(
            f"Missing {CSV_FILE}. Run pick_demo_customers.py first."
        )

    schema = get_schema()
    engine = get_engine()

    # Load picks
    _section("Step 1: Load picks from CSV")
    rows: list[dict] = []
    with CSV_FILE.open("r", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            rows.append(r)
    print(f"  {len(rows)} demo customers to seed")

    # Build username + clinic name + hash for each
    _section("Step 2: Build login records")
    name_pool_idx: dict[str, int] = defaultdict(int)
    seg_counter: dict[str, int] = defaultdict(int)
    records: list[dict] = []
    for r in rows:
        seg = r["segment"]
        seg_counter[seg] += 1
        username = f"demo_{seg.lower()}_{seg_counter[seg]:02d}"

        market = r["market_code"]
        pool = CLINIC_NAMES_BY_MARKET.get(market, ["Demo Clinic"])
        idx = name_pool_idx[market] % len(pool)
        clinic_name = pool[idx] + (f" #{1 + name_pool_idx[market] // len(pool)}"
                                   if name_pool_idx[market] >= len(pool) else "")
        name_pool_idx[market] += 1

        cust_id_int = int(r["cust_id"])
        records.append({
            "username":      username,
            "password_hash": hash_pw(PASSWORD),
            "role":          "customer",
            "full_name":     clinic_name,
            "email":         f"{username}@capstone.local",
            "cust_id":       cust_id_int,
        })

    # Upsert into recdash.users
    _section("Step 3: Upsert into recdash.users")
    upsert_sql = text(f"""
        INSERT INTO {schema}.users (username, password_hash, role, full_name, email, cust_id)
        VALUES (:username, :password_hash, :role, :full_name, :email, :cust_id)
        ON CONFLICT (username) DO UPDATE SET
            password_hash = EXCLUDED.password_hash,
            full_name     = EXCLUDED.full_name,
            email         = EXCLUDED.email,
            cust_id       = EXCLUDED.cust_id
    """)

    with engine.begin() as conn:
        for rec in records:
            conn.execute(upsert_sql, rec)
    print(f"  Upserted {len(records)} demo login rows")

    # Verify
    _section("Step 4: Verify")
    with engine.connect() as conn:
        result = conn.execute(text(
            f"SELECT username, full_name, cust_id FROM {schema}.users "
            f"WHERE username LIKE 'demo_%' ORDER BY username"
        )).fetchall()
    print(f"  Found {len(result)} demo login rows in DB:")
    for row in result:
        print(f"    {row[0]:<28}  -> {row[1]:<35}  cust_id={row[2]}")

    print()
    print("-" * 70)
    print(f"  Total time: {time.time() - t0:.1f}s")
    print("-" * 70)
    print(f"  All 30 demo logins use password: {PASSWORD!r}")
    print("  Next step: python scripts/backend/copy_purchase_history.py")


if __name__ == "__main__":
    main()
