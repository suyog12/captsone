"""
reset_admin_seller_passwords.py

Resets all admin and seller user passwords to Demo1234! so all roles use the
same password. Idempotent.

Usage:
    cd C:\\Users\\maina\\Desktop\\Capstone
    conda activate CTBA
    pip install bcrypt psycopg2-binary
    python reset_admin_seller_passwords.py --dry-run
    python reset_admin_seller_passwords.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import bcrypt
import psycopg2

ROOT = Path(__file__).resolve().parent
NEW_PASSWORD = "Demo1234!"


def load_pg_config() -> dict:
    env = ROOT / ".env"
    cfg = {"host": "localhost", "port": 5432,
           "dbname": "recommendation_dashboard",
           "user": "postgres", "password": "",
           "schema": "recdash"}
    if not env.exists():
        return cfg
    for line in env.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip().lower(), v.strip().strip('"').strip("'")
        mp = {"postgres_host": "host", "postgres_port": "port",
              "postgres_db": "dbname", "postgres_user": "user",
              "postgres_password": "password", "postgres_schema": "schema"}
        if k in mp:
            cfg[mp[k]] = int(v) if k == "postgres_port" else v
    return cfg


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--password", default=NEW_PASSWORD,
                        help=f"New password (default: {NEW_PASSWORD})")
    args = parser.parse_args()

    print("=" * 60)
    print("  RESET ADMIN & SELLER PASSWORDS")
    print("=" * 60)
    print(f"  New password: {args.password}")
    print(f"  Dry run:      {args.dry_run}")

    cfg = load_pg_config()
    conn = psycopg2.connect(host=cfg["host"], port=cfg["port"],
                            dbname=cfg["dbname"], user=cfg["user"],
                            password=cfg["password"])
    cur = conn.cursor()

    # Show what we're about to change
    cur.execute(f"""
        SELECT user_id, username, role, is_active
          FROM {cfg['schema']}.users
         WHERE role IN ('admin', 'seller')
         ORDER BY role, user_id
    """)
    users = cur.fetchall()

    print(f"\n  Found {len(users)} admin/seller users:")
    for uid, uname, role, active in users:
        active_str = "active" if active else "INACTIVE"
        print(f"    [{role:8s}] user_id={uid:<4} username={uname:<25} ({active_str})")

    if not users:
        print("\n  No admin/seller users to update.")
        cur.close()
        conn.close()
        return

    if args.dry_run:
        print(f"\n  DRY RUN - would update {len(users)} password hashes")
        cur.close()
        conn.close()
        return

    # Hash new password once (bcrypt is slow, just hash once)
    print(f"\n  Hashing new password...")
    pw_hash = bcrypt.hashpw(args.password.encode(), bcrypt.gensalt(12)).decode()

    # Update all admin and seller passwords
    cur.execute(f"""
        UPDATE {cfg['schema']}.users
           SET password_hash = %s
         WHERE role IN ('admin', 'seller')
    """, (pw_hash,))
    n_updated = cur.rowcount
    conn.commit()

    print(f"\n  Updated {n_updated} user password hashes")
    print(f"\n  All admins and sellers now use password: {args.password}")
    print(f"  All previous passwords are invalid")

    cur.close()
    conn.close()

    print("\n" + "=" * 60)
    print("  COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nFATAL: {e}", file=sys.stderr)
        raise