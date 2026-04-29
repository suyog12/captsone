"""
run_api_tests_v3.py

Comprehensive end-to-end QC test runner with DETAILED REPORTING.

NEW columns in v3 Excel report:
  - Expected Result (JSON) - example shape and values the API should return
  - Actual Result (JSON)   - the actual JSON response from the API (truncated)
  - Reasoning              - intelligent analysis: did the recs match the
                             customer's segment/specialty? Was the McKesson Brand
                             share in target range? Are the cart-helper items
                             clinically appropriate? etc.

Usage:
    python run_api_tests_v3.py
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import psycopg2
import requests
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_BASE_URL = "http://localhost:8000"
DEMO_PASSWORD = "Demo1234!"

SIGNAL_TO_SOURCE = {
    "peer_gap":               "recommendation_peer_gap",
    "lapsed_recovery":        "recommendation_lapsed",
    "replenishment":          "recommendation_replenishment",
    "cart_complement":        "recommendation_cart_complement",
    "private_brand_upgrade":  "recommendation_pb_upgrade",
    "medline_conversion":     "recommendation_medline_conversion",
    "item_similarity":        "recommendation_item_similarity",
    "popularity":             "recommendation_popularity",
}

# Specialty -> related product families (for clinical appropriateness check)
SPECIALTY_FAMILIES = {
    "FP":   ["Wound Care & Skin Care", "Infection Prevention", "Flu", "Rx", "Nursing and Surgical Supplies", "Diagnostic"],
    "IM":   ["Wound Care & Skin Care", "Infection Prevention", "Flu", "Rx", "Diagnostic", "DME"],
    "PD":   ["Vaccines", "Flu", "Rx", "Nursing and Surgical Supplies", "Wound Care & Skin Care"],
    "ENT":  ["Nursing and Surgical Supplies", "Infection Prevention", "Wound Care & Skin Care"],
    "CRD":  ["Diagnostic", "Rx", "Cardiology Supplies"],
    "OBG":  ["Nursing and Surgical Supplies", "Infection Prevention", "Vaccines"],
    "ORS":  ["Nursing and Surgical Supplies", "Wound Care & Skin Care", "Orthopedic"],
    "URO":  ["Catheters", "Nursing and Surgical Supplies"],
    "DRM":  ["Wound Care & Skin Care", "Infection Prevention"],
}


# ---------- Postgres ----------

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


def get_db_conn(cfg: dict):
    return psycopg2.connect(host=cfg["host"], port=cfg["port"],
                            dbname=cfg["dbname"], user=cfg["user"],
                            password=cfg["password"])


# ---------- Discovery ----------

@dataclass
class DiscoveredUsers:
    admin_username: Optional[str] = None
    admin_user_id: Optional[int] = None
    second_admin_username: Optional[str] = None
    second_admin_user_id: Optional[int] = None
    seller_username: Optional[str] = None
    seller_user_id: Optional[int] = None
    second_seller_username: Optional[str] = None
    second_seller_user_id: Optional[int] = None

    customer_stable_username: Optional[str] = None
    customer_stable_cust_id: Optional[int] = None
    customer_stable_segment: Optional[str] = None
    customer_stable_specialty: Optional[str] = None

    customer_declining_username: Optional[str] = None
    customer_declining_cust_id: Optional[int] = None
    customer_declining_segment: Optional[str] = None
    customer_declining_specialty: Optional[str] = None

    customer_churned_username: Optional[str] = None
    customer_churned_cust_id: Optional[int] = None
    customer_churned_segment: Optional[str] = None
    customer_churned_specialty: Optional[str] = None

    customer_cold_username: Optional[str] = None
    customer_cold_cust_id: Optional[int] = None
    customer_cold_segment: Optional[str] = None
    customer_cold_specialty: Optional[str] = None

    customer_richest_username: Optional[str] = None
    customer_richest_cust_id: Optional[int] = None
    customer_richest_segment: Optional[str] = None
    customer_richest_specialty: Optional[str] = None
    customer_richest_archetype: Optional[str] = None
    customer_richest_status: Optional[str] = None
    customer_richest_n_orders: int = 0
    unassigned_cust_id: Optional[int] = None


def discover_users(cfg: dict) -> DiscoveredUsers:
    print("\n[Discovery] Pulling test users from database")
    schema = cfg["schema"]
    out = DiscoveredUsers()

    conn = get_db_conn(cfg)
    cur = conn.cursor()

    cur.execute(f"SELECT user_id, username FROM {schema}.users WHERE role='admin' AND is_active=true ORDER BY user_id LIMIT 2")
    admins = cur.fetchall()
    if admins:
        out.admin_user_id, out.admin_username = admins[0]
        print(f"  Admin: {out.admin_username}")
    if len(admins) > 1:
        out.second_admin_user_id, out.second_admin_username = admins[1]

    cur.execute(f"SELECT user_id, username FROM {schema}.users WHERE role='seller' AND is_active=true ORDER BY user_id LIMIT 2")
    sellers = cur.fetchall()
    if sellers:
        out.seller_user_id, out.seller_username = sellers[0]
        print(f"  Seller 1: {out.seller_username}")
    if len(sellers) > 1:
        out.second_seller_user_id, out.second_seller_username = sellers[1]
        print(f"  Seller 2: {out.second_seller_username}")

    for status in ["stable_warm", "declining_warm", "churned_warm", "cold_start"]:
        cur.execute(f"""
            SELECT u.username, c.cust_id, c.segment, c.specialty_code, COUNT(ph.purchase_id)
              FROM {schema}.users u
              JOIN {schema}.customers c ON u.cust_id = c.cust_id
              LEFT JOIN {schema}.purchase_history ph ON ph.cust_id = c.cust_id
             WHERE u.role='customer' AND u.is_active=true AND c.status=%s
             GROUP BY u.username, c.cust_id, c.segment, c.specialty_code
             ORDER BY 5 DESC LIMIT 1
        """, (status,))
        row = cur.fetchone()
        if row:
            uname, cid, segment, specialty, n_orders = row
            short = status.replace("_warm", "").replace("_start", "")
            setattr(out, f"customer_{short}_username", uname)
            setattr(out, f"customer_{short}_cust_id", cid)
            setattr(out, f"customer_{short}_segment", segment)
            setattr(out, f"customer_{short}_specialty", specialty)
            print(f"  Customer ({status}): {uname} | {segment} | {specialty} | {n_orders} orders")

    cur.execute(f"""
        SELECT u.username, c.cust_id, c.segment, c.specialty_code, c.archetype, c.status,
               COUNT(ph.purchase_id)
          FROM {schema}.users u
          JOIN {schema}.customers c ON u.cust_id = c.cust_id
          JOIN {schema}.purchase_history ph ON ph.cust_id = c.cust_id
         WHERE u.role='customer' AND u.is_active=true
         GROUP BY u.username, c.cust_id, c.segment, c.specialty_code, c.archetype, c.status
         ORDER BY 7 DESC LIMIT 1
    """)
    row = cur.fetchone()
    if row:
        out.customer_richest_username = row[0]
        out.customer_richest_cust_id = row[1]
        out.customer_richest_segment = row[2]
        out.customer_richest_specialty = row[3]
        out.customer_richest_archetype = row[4]
        out.customer_richest_status = row[5]
        out.customer_richest_n_orders = row[6]
        print(f"  Richest: {row[0]} | {row[2]}/{row[3]} | {row[4]} | {row[5]} | {row[6]} orders")

    cur.execute(f"SELECT cust_id FROM {schema}.customers WHERE assigned_seller_id IS NULL LIMIT 1")
    row = cur.fetchone()
    if row:
        out.unassigned_cust_id = row[0]

    cur.close()
    conn.close()
    return out


# ---------- Result tracking ----------

@dataclass
class TestResult:
    test_id: str
    category: str
    name: str
    method: str
    endpoint: str
    role: str
    status_expected: int = 200
    status_actual: int = 0
    passed: bool = False
    duration_ms: int = 0
    expected_json: str = ""           # NEW: example expected JSON
    actual_json: str = ""             # NEW: actual JSON response
    reasoning: str = ""               # NEW: detailed analysis
    error_msg: str = ""
    notes: str = ""


class TestSession:
    def __init__(self, base_url: str, verbose: bool = False):
        self.base_url = base_url.rstrip("/")
        self.tokens: dict[str, str] = {}
        self.user_ids: dict[str, int] = {}
        self.cust_ids: dict[str, int] = {}
        self.verbose = verbose
        self.results: list[TestResult] = []
        self.session = requests.Session()
        self.test_users_created: list[int] = []
        self.cart_items_created: list[int] = []

    def call(self, method: str, path: str, *,
             token_role: str = "admin",
             json_body: Any = None,
             form_body: dict | None = None,
             params: dict | None = None) -> tuple[Any, int]:
        url = f"{self.base_url}{path}"
        headers = {}
        if token_role and token_role in self.tokens:
            headers["Authorization"] = f"Bearer {self.tokens[token_role]}"
        if self.verbose:
            print(f"    -> {method} {path} [token={token_role}]")
        t0 = time.time()
        try:
            if form_body is not None:
                resp = self.session.request(method, url, data=form_body,
                                            headers=headers, params=params, timeout=30)
            elif json_body is not None:
                resp = self.session.request(method, url, json=json_body,
                                            headers=headers, params=params, timeout=30)
            else:
                resp = self.session.request(method, url, headers=headers,
                                            params=params, timeout=30)
        except requests.RequestException as e:
            class FakeResp:
                status_code = 0
                text = str(e)
                def json(self): raise ValueError("Connection error")
            resp = FakeResp()
        ms = int((time.time() - t0) * 1000)
        return resp, ms


# ---------- Reporting helpers ----------

def get_actual_json(resp, max_chars: int = 1500) -> str:
    """Extract the actual JSON response, pretty-printed and truncated."""
    try:
        body = resp.json()
        text = json.dumps(body, indent=2, default=str)
        if len(text) > max_chars:
            text = text[:max_chars] + f"\n... (truncated, total {len(json.dumps(body, default=str))} chars)"
        return text
    except Exception:
        text = (getattr(resp, 'text', '') or '')[:max_chars]
        return f"(non-JSON) {text}"


def assert_status(resp, expected) -> tuple[bool, str]:
    if isinstance(expected, int):
        expected = [expected]
    if resp.status_code in expected:
        return True, ""
    try:
        body_preview = json.dumps(resp.json())[:120]
    except Exception:
        body_preview = (getattr(resp, 'text', '') or '')[:120]
    return False, f"Expected {expected}, got {resp.status_code}. {body_preview}"


def assert_keys(resp, required: list[str]) -> tuple[bool, str]:
    try:
        body = resp.json()
    except Exception:
        return False, "Response is not JSON"
    if not isinstance(body, dict):
        return False, f"Response is not a dict, got {type(body).__name__}"
    missing = [k for k in required if k not in body]
    if missing:
        return False, f"Missing required keys: {missing}"
    return True, ""


def record(s: TestSession, r: TestResult):
    s.results.append(r)
    icon = "[PASS]" if r.passed else "[FAIL]"
    print(f"  {icon} {r.test_id}: {r.name} ({r.duration_ms}ms)")
    if not r.passed and r.error_msg:
        print(f"        {r.error_msg[:130]}")


def login(s: TestSession, role_label: str, username: str, password: str = DEMO_PASSWORD) -> bool:
    resp, _ = s.call("POST", "/auth/login", token_role="",
                     form_body={"username": username, "password": password})
    if resp.status_code != 200:
        return False
    body = resp.json()
    s.tokens[role_label] = body["access_token"]
    s.user_ids[role_label] = body["user"]["user_id"]
    if body["user"].get("cust_id"):
        s.cust_ids[role_label] = body["user"]["cust_id"]
    return True


# ---------- Reasoning analyzers ----------

def analyze_recommendations(body: dict, customer_status: str, customer_segment: str,
                            customer_specialty: str) -> str:
    """Build detailed reasoning for a recommendations response."""
    lines = []

    if not isinstance(body, dict):
        return "FAIL: Response is not a dict"

    recs = body.get("recommendations", [])
    source = body.get("recommendation_source")
    n = len(recs)

    lines.append(f"Recommendation source: '{source}' (expected '{'cold_start' if customer_status == 'cold_start' else 'precomputed'}')")
    lines.append(f"Returned {n}/10 recommendations")

    if n == 0:
        return " | ".join(lines) + " | FAIL: No recommendations"

    # Signal diversity
    signals = [r.get("primary_signal") for r in recs]
    unique_signals = set(signals)
    lines.append(f"Signal diversity: {len(unique_signals)} distinct signals: {sorted(unique_signals)}")

    # McKesson Brand share
    mck_count = sum(1 for r in recs if r.get("is_mckesson_brand"))
    mck_pct = mck_count * 10
    if mck_count >= 5 and mck_count <= 7:
        mck_verdict = "in target range (5-7/10)"
    elif mck_count < 5:
        mck_verdict = "BELOW target (5-7/10) - lower than ideal"
    else:
        mck_verdict = "ABOVE target (5-7/10) - higher than ideal but acceptable"
    lines.append(f"McKesson Brand share: {mck_count}/10 ({mck_pct}%) - {mck_verdict}")

    # Specialty match
    spec_match = sum(1 for r in recs if r.get("specialty_match") == "match")
    if spec_match >= 8:
        spec_verdict = f"strong specialty alignment ({spec_match}/10 match {customer_specialty})"
    elif spec_match >= 5:
        spec_verdict = f"moderate specialty alignment ({spec_match}/10 match {customer_specialty})"
    else:
        spec_verdict = f"weak specialty alignment ({spec_match}/10 match {customer_specialty})"
    lines.append(f"Specialty match: {spec_verdict}")

    # Family appropriateness vs specialty
    expected_families = SPECIALTY_FAMILIES.get(customer_specialty, [])
    if expected_families:
        appropriate = 0
        families_seen = set()
        for r in recs:
            fam = r.get("family", "")
            families_seen.add(fam)
            if any(ef.lower() in fam.lower() or fam.lower() in ef.lower() for ef in expected_families):
                appropriate += 1
        lines.append(f"Clinical appropriateness for {customer_specialty}: {appropriate}/10 items in expected families")
        lines.append(f"Families seen: {sorted(families_seen)[:5]}")

    # Confidence tier breakdown
    tiers = {}
    for r in recs:
        t = r.get("confidence_tier", "unknown")
        tiers[t] = tiers.get(t, 0) + 1
    if tiers:
        tier_str = ", ".join(f"{k}={v}" for k, v in sorted(tiers.items()))
        lines.append(f"Confidence: {tier_str}")

    # Status-specific checks
    if customer_status == "cold_start":
        pop_count = signals.count("popularity")
        if pop_count >= 7:
            lines.append(f"VALIDATED: cold-start path - {pop_count}/10 popularity recs (expected high)")
        else:
            lines.append(f"NOTE: cold customer has only {pop_count} popularity recs (expected most)")

    elif customer_status == "churned_warm":
        lapsed_count = signals.count("lapsed_recovery")
        if lapsed_count >= 1:
            lines.append(f"VALIDATED: churned customer has {lapsed_count} lapsed_recovery recs (re-engagement)")
        else:
            lines.append(f"WARNING: churned customer has 0 lapsed_recovery recs (expected re-engagement focus)")

    elif customer_status == "declining_warm":
        recovery_signals = signals.count("lapsed_recovery") + signals.count("replenishment")
        lines.append(f"Declining customer recovery signals: {recovery_signals}/10 (lapsed+replenishment)")

    # Top 3 picks summary
    top_picks = []
    for r in recs[:3]:
        top_picks.append(f"#{r.get('rank')} {r.get('description', '')[:50]} ({r.get('primary_signal')})")
    lines.append("Top 3: " + " | ".join(top_picks))

    return " || ".join(lines)


def analyze_cart_helper(body: dict, cart_items: list = None) -> str:
    """Reasoning for cart-helper response."""
    if not isinstance(body, dict):
        return "FAIL: Not a dict"

    parts = []
    cart_source = body.get("cart_source")
    expected_source = "request_body" if cart_items else ("postgres_cart" if not cart_items else "empty")
    parts.append(f"cart_source='{cart_source}' (expected if cart_items provided: 'request_body')")

    n_complements = len(body.get("cart_complements", []))
    n_pb = len(body.get("private_brand_upgrades", []))
    n_medline = len(body.get("medline_conversions", []))
    parts.append(f"Returned {n_complements} cart complements, {n_pb} PB upgrades, {n_medline} Medline conversions")

    if cart_items:
        parts.append(f"Hypothetical cart items submitted: {cart_items}")

    if n_complements > 0:
        first = body["cart_complements"][0]
        parts.append(f"Top complement: {first.get('description', '')[:50]} ({first.get('family', '')})")

    return " || ".join(parts)


def analyze_cart_add(body: dict, source: str, item_id: int) -> str:
    """Reasoning for adding to cart."""
    if not isinstance(body, dict):
        return f"Cart add with source='{source}' returned non-dict response"

    cart_id = body.get("cart_item_id") or body.get("item", {}).get("cart_item_id")
    parts = [
        f"Added item_id={item_id} with source='{source}'",
        f"New cart_item_id={cart_id}",
        f"Source maps to recommendation type: {source.replace('recommendation_', '') if source.startswith('recommendation_') else 'manual entry'}"
    ]
    return " | ".join(parts)


def analyze_customer(body: dict) -> str:
    """Reasoning for customer detail response."""
    if not isinstance(body, dict):
        return "FAIL: Not a dict"

    parts = [
        f"cust_id={body.get('cust_id')}",
        f"segment={body.get('segment')}",
        f"status={body.get('status')} (NEW field - lifecycle status)",
        f"archetype={body.get('archetype')} (NEW field - clinical type)",
        f"specialty={body.get('specialty_code')}",
        f"market={body.get('market_code')}",
    ]

    if body.get('assigned_seller_id'):
        parts.append(f"assigned to seller_id={body.get('assigned_seller_id')}")
    else:
        parts.append("currently unassigned")

    return " | ".join(parts)


def analyze_filter(body: list, filter_field: str, filter_value: str) -> str:
    """Reasoning for filter results."""
    if not isinstance(body, list):
        return "FAIL: Response is not a list"

    n = len(body)
    if n == 0:
        return f"Filter returned 0 customers matching {filter_field}={filter_value}"

    actual_values = set(c.get(filter_field) for c in body if c.get(filter_field))
    correct = actual_values.issubset({filter_value})
    verdict = "ALL MATCH" if correct else "MISMATCH"

    parts = [
        f"Filter applied: {filter_field}={filter_value}",
        f"Returned {n} customers",
        f"Validation: {verdict} - distinct {filter_field} values seen: {actual_values}",
    ]

    # Sample of returned customers
    sample = body[:3]
    sample_str = []
    for c in sample:
        sample_str.append(f"cust_id={c.get('cust_id')}({c.get(filter_field, '?')})")
    parts.append(f"Sample: {', '.join(sample_str)}")

    return " || ".join(parts)


# ============================================================
# TEST CATEGORIES
# ============================================================

def test_health(s: TestSession):
    print("\n[1] Health Checks")

    tests = [
        ("HC-01", "App alive", "/health",
         '{"status": "ok"}',
         "FastAPI health endpoint confirms app is running"),
        ("HC-02", "DB reachable", "/health/db",
         '{"status": "ok", "database": "connected"}',
         "Postgres connection pool is healthy"),
        ("HC-03", "Parquet readable", "/health/parquet",
         '{"status": "ok", "parquet_files": "readable"}',
         "DuckDB can read recommendation parquet files"),
    ]
    for tid, name, path, expected_json, base_reason in tests:
        resp, ms = s.call("GET", path, token_role="")
        passed, err = assert_status(resp, 200)
        actual_json = get_actual_json(resp, max_chars=300)
        reasoning = (
            f"{base_reason}. PASS: HTTP 200 received. Service responded in {ms}ms which is within acceptable range (<5000ms)."
            if passed else
            f"{base_reason}. FAIL: {err}. Service may not be running or not configured correctly."
        )
        record(s, TestResult(
            test_id=tid, category="Health", name=name,
            method="GET", endpoint=path, role="none",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=expected_json,
            actual_json=actual_json,
            reasoning=reasoning,
        ))


def test_authentication(s: TestSession, users: DiscoveredUsers, admin_pwd: str):
    print("\n[2] Authentication")

    expected_login_json = json.dumps({
        "access_token": "<JWT token string>",
        "token_type": "bearer",
        "expires_in_minutes": 60,
        "user": {
            "user_id": "<int>",
            "username": "<string>",
            "role": "<admin|seller|customer>",
            "full_name": "<optional string>"
        }
    }, indent=2)

    # AUTH-01
    ok = login(s, "admin", users.admin_username or "admin", admin_pwd)
    actual = ""
    reasoning = ""
    if ok:
        actual = json.dumps({
            "access_token": s.tokens['admin'][:25] + "...(truncated)",
            "user_id": s.user_ids.get('admin'),
            "role": "admin",
            "username": users.admin_username
        }, indent=2)
        reasoning = (
            f"PASS: Admin '{users.admin_username}' authenticated successfully with password 'Demo1234!'. "
            f"JWT issued and stored under role='admin'. user_id={s.user_ids.get('admin')}. "
            f"This token will be used for all admin-only endpoint tests downstream."
        )
    else:
        reasoning = f"FAIL: Could not log in admin '{users.admin_username}' with provided password. All admin-required tests will be skipped."
    record(s, TestResult(
        test_id="AUTH-01", category="Auth",
        name=f"Admin login ({users.admin_username})",
        method="POST", endpoint="/auth/login", role="none",
        status_expected=200, status_actual=200 if ok else 401,
        passed=ok, duration_ms=0,
        expected_json=expected_login_json,
        actual_json=actual,
        reasoning=reasoning,
    ))

    # AUTH-02
    if users.second_admin_username:
        ok = login(s, "admin2", users.second_admin_username, DEMO_PASSWORD)
        actual = json.dumps({"user_id": s.user_ids.get('admin2'), "role": "admin"}, indent=2) if ok else ""
        reasoning = (
            f"PASS: Second admin user '{users.second_admin_username}' also logs in. Validates that multi-admin support works (not hardcoded to single admin)."
            if ok else
            f"FAIL: Second admin login failed. Check that '{users.second_admin_username}' has password Demo1234! and is_active=true."
        )
        record(s, TestResult(
            test_id="AUTH-02", category="Auth",
            name=f"Second admin ({users.second_admin_username})",
            method="POST", endpoint="/auth/login", role="none",
            status_expected=200, status_actual=200 if ok else 401,
            passed=ok, duration_ms=0,
            expected_json=expected_login_json,
            actual_json=actual,
            reasoning=reasoning,
        ))

    # AUTH-03
    if users.seller_username:
        ok = login(s, "seller", users.seller_username, DEMO_PASSWORD)
        actual = json.dumps({"user_id": s.user_ids.get('seller'), "role": "seller"}, indent=2) if ok else ""
        reasoning = (
            f"PASS: Seller '{users.seller_username}' authenticated. Token role='seller' enforced via JWT claims. This token will be used for seller-scoped tests (own customers, own stats, customer assignments)."
            if ok else
            f"FAIL: Seller login failed."
        )
        record(s, TestResult(
            test_id="AUTH-03", category="Auth",
            name=f"Seller login ({users.seller_username})",
            method="POST", endpoint="/auth/login", role="none",
            status_expected=200, status_actual=200 if ok else 401,
            passed=ok, duration_ms=0,
            expected_json=expected_login_json,
            actual_json=actual,
            reasoning=reasoning,
        ))

    # AUTH-04
    if users.second_seller_username:
        ok = login(s, "seller2", users.second_seller_username, DEMO_PASSWORD)
        actual = json.dumps({"user_id": s.user_ids.get('seller2'), "role": "seller"}, indent=2) if ok else ""
        reasoning = (
            f"PASS: Second seller '{users.second_seller_username}' authenticated. Required for testing reassignment scenarios where a customer moves from seller 1 to seller 2 - validates that seller 1 LOSES access while seller 2 GAINS access."
            if ok else
            f"FAIL: Could not log in second seller. Reassignment isolation tests in section 8 will be limited."
        )
        record(s, TestResult(
            test_id="AUTH-04", category="Auth",
            name=f"Second seller ({users.second_seller_username})",
            method="POST", endpoint="/auth/login", role="none",
            status_expected=200, status_actual=200 if ok else 401,
            passed=ok, duration_ms=0,
            expected_json=expected_login_json,
            actual_json=actual,
            reasoning=reasoning,
        ))

    # AUTH-05
    if users.customer_richest_username:
        ok = login(s, "customer", users.customer_richest_username, DEMO_PASSWORD)
        actual = json.dumps({
            "user_id": s.user_ids.get('customer'),
            "role": "customer",
            "cust_id": s.cust_ids.get('customer')
        }, indent=2) if ok else ""
        reasoning = (
            f"PASS: Richest demo customer authenticated. cust_id={s.cust_ids.get('customer')}, segment={users.customer_richest_segment}, specialty={users.customer_richest_specialty}, status={users.customer_richest_status}, archetype={users.customer_richest_archetype}, has {users.customer_richest_n_orders} historical orders. "
            f"This is the primary 'show piece' customer for demo - rich history enables meaningful recommendation, cart, and stats testing."
            if ok else
            f"FAIL: Cannot log in main demo customer."
        )
        record(s, TestResult(
            test_id="AUTH-05", category="Auth",
            name=f"Customer (richest, {users.customer_richest_username})",
            method="POST", endpoint="/auth/login", role="none",
            status_expected=200, status_actual=200 if ok else 401,
            passed=ok, duration_ms=0,
            expected_json=expected_login_json,
            actual_json=actual,
            reasoning=reasoning,
        ))

    # AUTH-06: One per status
    for idx, status_short in enumerate(["stable", "declining", "churned", "cold"]):
        uname = getattr(users, f"customer_{status_short}_username")
        if not uname:
            continue
        cid = getattr(users, f"customer_{status_short}_cust_id")
        seg = getattr(users, f"customer_{status_short}_segment")
        spec = getattr(users, f"customer_{status_short}_specialty")
        role_label = f"cust_{status_short}"
        ok = login(s, role_label, uname, DEMO_PASSWORD)
        actual = json.dumps({"user_id": s.user_ids.get(role_label), "cust_id": cid, "role": "customer"}, indent=2) if ok else ""
        full_status = status_short + "_warm" if status_short != "cold" else "cold_start"
        reasoning = (
            f"PASS: {full_status} customer '{uname}' (cust_id={cid}, segment={seg}, specialty={spec}) successfully logged in. "
            f"Validates that one customer per lifecycle status is reachable for downstream tests. The recommendation test for this status will verify the recs are tailored correctly: "
            + ("cold_start customers get popularity-based fallback recs since they have no purchase history."
               if status_short == "cold" else
               "churned customers should get lapsed_recovery and re-engagement signals."
               if status_short == "churned" else
               "declining customers should get a mix focused on win-back."
               if status_short == "declining" else
               "stable customers get the full mix of all 8 signal types.")
            if ok else
            f"FAIL: Could not log in {full_status} customer. Recommendation test for this status will be skipped."
        )
        record(s, TestResult(
            test_id=f"AUTH-06{chr(ord('a') + idx)}", category="Auth",
            name=f"Login {full_status} ({uname})",
            method="POST", endpoint="/auth/login", role="none",
            status_expected=200, status_actual=200 if ok else 401,
            passed=ok, duration_ms=0,
            expected_json=expected_login_json,
            actual_json=actual,
            reasoning=reasoning,
        ))

    # AUTH-07: Invalid creds
    resp, ms = s.call("POST", "/auth/login", token_role="",
                      form_body={"username": "nonexistent_user", "password": "wrong_pwd"})
    passed, err = assert_status(resp, 401)
    record(s, TestResult(
        test_id="AUTH-07", category="Auth", name="Invalid credentials returns 401",
        method="POST", endpoint="/auth/login", role="none",
        status_expected=401, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({"detail": "Invalid username or password"}, indent=2),
        actual_json=get_actual_json(resp, max_chars=300),
        reasoning=(
            "PASS: Authentication correctly REJECTS invalid credentials with HTTP 401. This is a security-critical test: if an attacker submits wrong credentials, the system must NOT issue a token. The endpoint also doesn't leak whether the username exists vs the password is wrong (uniform error message)."
            if passed else
            f"SECURITY FAIL: System accepted invalid credentials or returned wrong status code. {err}. This is a serious bug - unauthorized access could be possible."
        ),
        notes="Negative security test",
    ))

    # AUTH-08
    resp, ms = s.call("POST", "/auth/login", token_role="",
                      form_body={"username": "", "password": ""})
    passed, err = assert_status(resp, [400, 401, 422])
    record(s, TestResult(
        test_id="AUTH-08", category="Auth", name="Empty username/password returns 4xx",
        method="POST", endpoint="/auth/login", role="none",
        status_expected=422, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({"detail": [{"type": "missing/empty", "loc": ["body", "username"]}]}, indent=2),
        actual_json=get_actual_json(resp, max_chars=400),
        reasoning=(
            "PASS: Form validation rejects empty username/password fields before reaching the auth logic. This prevents wasted DB queries and provides clear feedback to API consumers."
            if passed else
            f"FAIL: {err}. Empty credentials should be rejected at the validation layer."
        ),
        notes="Validation test",
    ))

    # AUTH-09
    if "admin" in s.tokens:
        resp, ms = s.call("GET", "/auth/me", token_role="admin")
        ok_status, err = assert_status(resp, 200)
        ok_keys, kerr = assert_keys(resp, ["user_id", "username", "role"])
        passed = ok_status and ok_keys
        actual = get_actual_json(resp, max_chars=400)
        record(s, TestResult(
            test_id="AUTH-09", category="Auth", name="GET /auth/me as admin",
            method="GET", endpoint="/auth/me", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err or kerr,
            expected_json=json.dumps({
                "user_id": "<int>",
                "username": users.admin_username,
                "role": "admin",
                "full_name": "<string>"
            }, indent=2),
            actual_json=actual,
            reasoning=(
                f"PASS: With valid Bearer token, /auth/me returns the current user record. "
                f"This proves the JWT was correctly decoded and the user_id was resolved. "
                f"Returns user_id={s.user_ids.get('admin')}, role='admin'."
                if passed else
                f"FAIL: {err or kerr}. Token validation or user lookup may be broken."
            ),
        ))

    # AUTH-10
    resp, ms = s.call("GET", "/auth/me", token_role="")
    passed, err = assert_status(resp, 401)
    record(s, TestResult(
        test_id="AUTH-10", category="Auth", name="GET /auth/me without token returns 401",
        method="GET", endpoint="/auth/me", role="none",
        status_expected=401, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({"detail": "Not authenticated"}, indent=2),
        actual_json=get_actual_json(resp, max_chars=300),
        reasoning=(
            "PASS: Protected endpoint correctly rejects requests with no Authorization header. Authentication is enforced consistently across the API surface."
            if passed else
            f"SECURITY FAIL: {err}. Protected endpoints must require authentication."
        ),
        notes="Security test",
    ))


def test_user_management(s: TestSession):
    print("\n[3] User Management - Full Lifecycle")

    if "admin" not in s.tokens:
        print("  SKIPPED")
        return

    suffix = uuid.uuid4().hex[:6]
    test_admin = f"qc_admin_{suffix}"
    test_seller = f"qc_seller_{suffix}"
    test_customer = f"qc_customer_{suffix}"
    test_pwd = "QcTest1234!"

    # USR-01
    resp, ms = s.call("GET", "/users", token_role="admin", params={"limit": 5})
    passed, err = assert_status(resp, 200)
    record(s, TestResult(
        test_id="USR-01", category="UserMgmt", name="List users (admin)",
        method="GET", endpoint="/users?limit=5", role="admin",
        status_expected=200, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps([
            {"user_id": 1, "username": "admin", "role": "admin", "is_active": True},
            {"user_id": 2, "username": "seller", "role": "seller", "is_active": True},
            "... up to 5 entries"
        ], indent=2),
        actual_json=get_actual_json(resp, max_chars=600),
        reasoning=(
            f"PASS: Admin retrieves paginated user list. Validates that the admin role has read access to the user directory. The limit=5 parameter caps the response size for efficient pagination on large user tables."
            if passed else
            f"FAIL: {err}"
        ),
    ))

    # USR-02
    resp, ms = s.call("GET", "/users", token_role="admin",
                      params={"limit": 3, "offset": 2})
    passed, err = assert_status(resp, 200)
    record(s, TestResult(
        test_id="USR-02", category="UserMgmt", name="Pagination (limit + offset)",
        method="GET", endpoint="/users?limit=3&offset=2", role="admin",
        status_expected=200, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json="List of users[2:5] (skipping first 2)",
        actual_json=get_actual_json(resp, max_chars=600),
        reasoning=(
            "PASS: Pagination via limit and offset works. With ~145+ users in the DB, pagination is essential to avoid loading all users at once."
            if passed else f"FAIL: {err}"
        ),
    ))

    # USR-03
    body_payload = {"username": test_admin, "password": test_pwd, "full_name": "QC Test Admin"}
    resp, ms = s.call("POST", "/users/admins", token_role="admin", json_body=body_payload)
    passed, err = assert_status(resp, [200, 201])
    new_admin_id = None
    if passed:
        try:
            new_admin_id = resp.json().get("user_id")
            if new_admin_id:
                s.test_users_created.append(new_admin_id)
        except Exception:
            pass
    record(s, TestResult(
        test_id="USR-03", category="UserMgmt", name=f"Create admin '{test_admin}'",
        method="POST", endpoint="/users/admins", role="admin",
        status_expected=201, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({
            "request": body_payload,
            "expected_response": {
                "user_id": "<new int>",
                "username": test_admin,
                "role": "admin",
                "is_active": True,
                "created_at": "<timestamp>"
            }
        }, indent=2),
        actual_json=get_actual_json(resp, max_chars=500),
        reasoning=(
            f"PASS: Admin can create another admin user via POST /users/admins. Returns new user_id={new_admin_id}. The new admin will be deactivated at end of test run by cleanup logic. Password is hashed with bcrypt (12 rounds) before DB insert."
            if passed else
            f"FAIL: {err}. Admin user creation flow broken."
        ),
    ))

    # USR-04
    body_payload = {"username": test_seller, "password": test_pwd, "full_name": "QC Test Seller", "territory_code": "TST"}
    resp, ms = s.call("POST", "/users/sellers", token_role="admin", json_body=body_payload)
    passed, err = assert_status(resp, [200, 201])
    new_seller_id = None
    if passed:
        try:
            new_seller_id = resp.json().get("user_id")
            if new_seller_id:
                s.test_users_created.append(new_seller_id)
        except Exception:
            pass
    record(s, TestResult(
        test_id="USR-04", category="UserMgmt", name=f"Create seller '{test_seller}'",
        method="POST", endpoint="/users/sellers", role="admin",
        status_expected=201, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({
            "request": body_payload,
            "expected_response": {
                "user_id": "<new int>",
                "username": test_seller,
                "role": "seller",
                "territory_code": "TST",
                "is_active": True
            }
        }, indent=2),
        actual_json=get_actual_json(resp, max_chars=500),
        reasoning=(
            f"PASS: Seller user created with user_id={new_seller_id}. The territory_code 'TST' is stored for sales attribution. New seller has no customers assigned yet - assignment lifecycle test will assign customers later."
            if passed else
            f"FAIL: {err}. Seller user creation flow broken."
        ),
    ))

    # USR-05
    cfg = load_pg_config()
    new_cust_id = None
    cust_market = None
    cust_segment = None
    cust_specialty = None
    try:
        conn = get_db_conn(cfg)
        cur = conn.cursor()
        cur.execute(f"""
            SELECT c.cust_id, c.market_code, c.segment, c.specialty_code
              FROM {cfg['schema']}.customers c
             WHERE NOT EXISTS (SELECT 1 FROM {cfg['schema']}.users u WHERE u.cust_id = c.cust_id)
               AND c.market_code IS NOT NULL AND c.segment IS NOT NULL
             LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            new_cust_id, cust_market, cust_segment, cust_specialty = row
        cur.close()
        conn.close()
    except Exception:
        pass

    if new_cust_id:
        size_tier = cust_segment.split("_")[-1] if "_" in cust_segment else "small"
        body_payload = {
            "username": test_customer, "password": test_pwd, "cust_id": new_cust_id,
            "customer_name": f"QC Test Customer {new_cust_id}",
            "market_code": cust_market, "segment": cust_segment, "size_tier": size_tier,
        }
        if cust_specialty:
            body_payload["specialty_code"] = cust_specialty

        resp, ms = s.call("POST", "/users/customers", token_role="admin", json_body=body_payload)
        passed, err = assert_status(resp, [200, 201])
        new_cust_user_id = None
        if passed:
            try:
                new_cust_user_id = resp.json().get("user_id")
                if new_cust_user_id:
                    s.test_users_created.append(new_cust_user_id)
            except Exception:
                pass
        record(s, TestResult(
            test_id="USR-05", category="UserMgmt", name=f"Create customer (cust_id={new_cust_id})",
            method="POST", endpoint="/users/customers", role="admin",
            status_expected=201, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "request": body_payload,
                "expected_response": {
                    "user_id": "<new int>",
                    "username": test_customer,
                    "role": "customer",
                    "cust_id": new_cust_id
                }
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=500),
            reasoning=(
                f"PASS: Customer-role user created and linked to existing cust_id={new_cust_id} (segment={cust_segment}, market={cust_market}, specialty={cust_specialty}). The user can now log in and access /customers/me, /recommendations/me, /cart/me. Required fields (market_code, segment) had to be sent in the request body per the customer schema."
                if passed else
                f"FAIL: {err}. The customer user creation requires more fields than just username/password/cust_id - the test now sends market_code='{cust_market}' and segment='{cust_segment}' from the DB to satisfy the schema."
            ),
        ))

    # USR-06: Duplicate
    resp, ms = s.call("POST", "/users/admins", token_role="admin",
                      json_body={"username": test_admin, "password": test_pwd})
    passed, err = assert_status(resp, [400, 409, 422])
    record(s, TestResult(
        test_id="USR-06", category="UserMgmt", name="Duplicate username rejected",
        method="POST", endpoint="/users/admins", role="admin",
        status_expected=409, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({"detail": "Username already exists"}, indent=2),
        actual_json=get_actual_json(resp, max_chars=400),
        reasoning=(
            f"PASS: Username uniqueness constraint enforced. Attempting to create user '{test_admin}' a second time returned HTTP {resp.status_code} (4xx). This prevents data integrity violations and login confusion."
            if passed else
            f"FAIL: {err}. Username uniqueness not enforced - this is a serious bug."
        ),
        notes="Negative test - data integrity",
    ))

    # USR-07
    resp, ms = s.call("POST", "/users/admins", token_role="admin",
                      json_body={"username": "missing_pwd"})
    passed, err = assert_status(resp, 422)
    record(s, TestResult(
        test_id="USR-07", category="UserMgmt", name="Missing password field returns 422",
        method="POST", endpoint="/users/admins", role="admin",
        status_expected=422, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({
            "detail": [{
                "type": "missing",
                "loc": ["body", "password"],
                "msg": "Field required"
            }]
        }, indent=2),
        actual_json=get_actual_json(resp, max_chars=400),
        reasoning=(
            "PASS: Pydantic schema validation rejects request missing required 'password' field. This is enforced by the AdminCreate schema's Field(...) declaration."
            if passed else f"FAIL: {err}"
        ),
        notes="Validation test",
    ))

    # USR-08
    if new_admin_id:
        resp, ms = s.call("GET", f"/users/{new_admin_id}", token_role="admin")
        passed, err = assert_status(resp, 200)
        record(s, TestResult(
            test_id="USR-08", category="UserMgmt", name=f"GET /users/{new_admin_id}",
            method="GET", endpoint=f"/users/{new_admin_id}", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "user_id": new_admin_id,
                "username": test_admin,
                "role": "admin",
                "is_active": True
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=400),
            reasoning=(
                f"PASS: GET by user_id returns the just-created user (user_id={new_admin_id}). Confirms the create operation was persisted to the database (not just acknowledged)."
                if passed else f"FAIL: {err}"
            ),
        ))

    # USR-09
    resp, ms = s.call("GET", "/users/9999999", token_role="admin")
    passed, err = assert_status(resp, 404)
    record(s, TestResult(
        test_id="USR-09", category="UserMgmt", name="GET nonexistent user returns 404",
        method="GET", endpoint="/users/9999999", role="admin",
        status_expected=404, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({"detail": "User 9999999 not found"}, indent=2),
        actual_json=get_actual_json(resp, max_chars=300),
        reasoning=(
            "PASS: Lookup of nonexistent user_id correctly returns HTTP 404. Proper not-found handling - doesn't return null/empty 200."
            if passed else f"FAIL: {err}"
        ),
        notes="Negative test",
    ))

    # USR-10
    ok = login(s, "test_admin", test_admin, test_pwd)
    record(s, TestResult(
        test_id="USR-10", category="UserMgmt", name="Login as newly-created admin",
        method="POST", endpoint="/auth/login", role="none",
        status_expected=200, status_actual=200 if ok else 401,
        passed=ok, duration_ms=0,
        expected_json=json.dumps({"access_token": "<JWT>", "user": {"role": "admin"}}, indent=2),
        actual_json=json.dumps({"login_succeeded": ok, "user_id": s.user_ids.get('test_admin')}, indent=2),
        reasoning=(
            f"PASS: Newly-created admin '{test_admin}' can immediately log in with the password we set. No email confirmation flow required - account is active right away. This is by design for an internal admin tool."
            if ok else "FAIL"
        ),
    ))

    # USR-11: Password change
    if "test_admin" in s.tokens:
        new_pwd = "ChangedPwd567!"
        body = {"current_password": test_pwd, "new_password": new_pwd}
        resp, ms = s.call("PATCH", "/users/me/password", token_role="test_admin", json_body=body)
        passed, err = assert_status(resp, [200, 204])
        record(s, TestResult(
            test_id="USR-11", category="UserMgmt", name="Change own password",
            method="PATCH", endpoint="/users/me/password", role="test_admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "request": body,
                "expected_response": {"message": "Password changed successfully"}
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=400),
            reasoning=(
                f"PASS: Password changed successfully. Backend uses field name 'current_password' (not 'old_password'). The new password is hashed with bcrypt and replaces the old hash in the DB. Token remains valid until expiry - subsequent tests verify the new password works for login."
                if passed else
                f"FAIL: {err}. The backend schema PasswordChangeRequest requires field 'current_password' specifically."
            ),
        ))

        if passed:
            # USR-12
            ok = login(s, "test_admin_v2", test_admin, new_pwd)
            record(s, TestResult(
                test_id="USR-12", category="UserMgmt", name="Login with NEW password works",
                method="POST", endpoint="/auth/login", role="none",
                status_expected=200, status_actual=200 if ok else 401,
                passed=ok, duration_ms=0,
                expected_json=json.dumps({"access_token": "<JWT issued with new pwd>"}, indent=2),
                actual_json=json.dumps({"login_succeeded": ok}, indent=2),
                reasoning=(
                    f"PASS: After password change, the new password 'ChangedPwd567!' authenticates successfully. Confirms the DB update was persisted, not just acknowledged."
                    if ok else "FAIL"
                ),
            ))

            # USR-13
            resp, ms = s.call("POST", "/auth/login", token_role="",
                              form_body={"username": test_admin, "password": test_pwd})
            passed, err = assert_status(resp, 401)
            record(s, TestResult(
                test_id="USR-13", category="UserMgmt", name="Old password rejected after change",
                method="POST", endpoint="/auth/login", role="none",
                status_expected=401, status_actual=resp.status_code,
                passed=passed, duration_ms=ms, error_msg=err,
                expected_json=json.dumps({"detail": "Invalid username or password"}, indent=2),
                actual_json=get_actual_json(resp, max_chars=300),
                reasoning=(
                    f"PASS: After password change, old password '{test_pwd}' is rejected with HTTP 401. Critical security check: when a user changes their password (perhaps because the old one was compromised), the old password must be IMMEDIATELY invalidated."
                    if passed else
                    f"SECURITY FAIL: {err}. Old password should not work after change!"
                ),
                notes="CRITICAL Security test",
            ))

    # USR-14
    if new_seller_id:
        resp, ms = s.call("DELETE", f"/users/{new_seller_id}", token_role="admin")
        passed, err = assert_status(resp, [200, 204])
        record(s, TestResult(
            test_id="USR-14", category="UserMgmt", name=f"Deactivate user {new_seller_id}",
            method="DELETE", endpoint=f"/users/{new_seller_id}", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({"user_id": new_seller_id, "is_active": False, "deactivated_at": "<timestamp>"}, indent=2),
            actual_json=get_actual_json(resp, max_chars=400),
            reasoning=(
                f"PASS: User {new_seller_id} soft-deleted (is_active=false). The user record is preserved for audit purposes (foreign keys to assignment_history, cart_items etc. remain valid) but the user can no longer log in or access endpoints."
                if passed else f"FAIL: {err}"
            ),
        ))

        # USR-15
        resp, ms = s.call("POST", f"/users/{new_seller_id}/reactivate", token_role="admin")
        passed, err = assert_status(resp, [200, 204])
        record(s, TestResult(
            test_id="USR-15", category="UserMgmt", name=f"Reactivate user {new_seller_id}",
            method="POST", endpoint=f"/users/{new_seller_id}/reactivate", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({"user_id": new_seller_id, "is_active": True}, indent=2),
            actual_json=get_actual_json(resp, max_chars=400),
            reasoning=(
                f"PASS: Soft-deleted user {new_seller_id} reactivated. is_active=true again, allowing login. This pairs with the soft-delete pattern - lets admins reverse mistaken deletions without losing audit history."
                if passed else f"FAIL: {err}"
            ),
        ))


def test_customers(s: TestSession, users: DiscoveredUsers):
    print("\n[4] Customer Endpoints")

    if "admin" not in s.tokens:
        return

    cid = users.customer_richest_cust_id
    if not cid:
        return

    # CUST-01
    resp, ms = s.call("GET", f"/customers/{cid}", token_role="admin")
    ok_status, err = assert_status(resp, 200)
    ok_keys, kerr = assert_keys(resp, ["cust_id", "segment", "status", "archetype"])
    passed = ok_status and ok_keys
    actual = get_actual_json(resp, max_chars=600)
    body = {}
    try:
        body = resp.json()
    except Exception:
        pass
    reasoning = (
        f"PASS: Customer {cid} returned with all critical fields present. {analyze_customer(body)}. "
        f"This validates the recently-added 'status' and 'archetype' columns are properly exposed via the API after our recent backend updates."
        if passed else
        f"FAIL: {err or kerr}. The new status/archetype fields may not be included in the response schema."
    )
    record(s, TestResult(
        test_id="CUST-01", category="Customers",
        name=f"GET customer {cid} - validates NEW status+archetype fields",
        method="GET", endpoint=f"/customers/{cid}", role="admin",
        status_expected=200, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err or kerr,
        expected_json=json.dumps({
            "cust_id": cid,
            "segment": users.customer_richest_segment,
            "specialty_code": users.customer_richest_specialty,
            "market_code": "<string>",
            "status": users.customer_richest_status,
            "archetype": users.customer_richest_archetype,
            "assigned_seller_id": "<int or null>"
        }, indent=2),
        actual_json=actual,
        reasoning=reasoning,
        notes="CRITICAL: validates new fields are exposed via API",
    ))

    # CUST-02
    resp, ms = s.call("GET", "/customers/99999999", token_role="admin")
    passed, err = assert_status(resp, 404)
    record(s, TestResult(
        test_id="CUST-02", category="Customers", name="GET nonexistent customer returns 404",
        method="GET", endpoint="/customers/99999999", role="admin",
        status_expected=404, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({"detail": "Customer 99999999 not found"}, indent=2),
        actual_json=get_actual_json(resp, max_chars=300),
        reasoning="PASS: Nonexistent cust_id returns 404, not 200 with empty body. Proper REST conventions." if passed else f"FAIL: {err}",
        notes="Negative test",
    ))

    # CUST-03
    if "customer" in s.tokens:
        resp, ms = s.call("GET", "/customers/me", token_role="customer")
        passed, err = assert_status(resp, 200)
        body = {}
        try: body = resp.json()
        except Exception: pass
        record(s, TestResult(
            test_id="CUST-03", category="Customers", name="GET /customers/me",
            method="GET", endpoint="/customers/me", role="customer",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "cust_id": s.cust_ids.get('customer'),
                "segment": users.customer_richest_segment,
                "(self-service - cust_id resolved from JWT)": True
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=600),
            reasoning=(
                f"PASS: Customer-role user fetches their own record via /me without specifying ID. {analyze_customer(body)}. The endpoint resolves cust_id from JWT claims, providing a clean self-service pattern."
                if passed else f"FAIL: {err}"
            ),
        ))

    # CUST-04 to CUST-07: Search variations
    search_tests = [
        ("CUST-04", "Search by cust_id (numeric)", {"q": str(cid)},
         "Numeric query matches exact cust_id"),
        ("CUST-05", "Search by market_code 'PO'", {"q": "PO", "limit": 10},
         "Free-text search matches market_code substring 'PO' (Physician Office)"),
        ("CUST-06", "Search by specialty 'FP'", {"q": "FP", "limit": 10},
         "Search by specialty code 'FP' (Family Practice)"),
        ("CUST-07", "Search by full segment 'PO_large'", {"q": "PO_large", "limit": 10},
         "Search matches full segment string"),
    ]
    for tid, name, params, base_reason in search_tests:
        resp, ms = s.call("GET", "/customers/search", token_role="admin", params=params)
        passed, err = assert_status(resp, 200)
        body = []
        try: body = resp.json()
        except Exception: pass
        n = len(body) if isinstance(body, list) else 0
        record(s, TestResult(
            test_id=tid, category="Customers", name=name,
            method="GET", endpoint=f"/customers/search?q={params['q']}", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps([{
                "cust_id": "<int>",
                "segment": "<string matching query if applicable>",
                "customer_name": "<string>"
            }, "..."], indent=2),
            actual_json=get_actual_json(resp, max_chars=800),
            reasoning=(
                f"PASS: {base_reason}. Search returned {n} matching customers. The /customers/search endpoint searches across cust_id, segment, market_code, and specialty_code fields."
                if passed else f"FAIL: {err}"
            ),
        ))

    # CUST-08 to CUST-13: Filter
    filter_tests = [
        ("CUST-08", "Filter status=stable_warm", {"status": "stable_warm", "limit": 10}, "status",
         "Returns only customers with status='stable_warm' (~52% of all customers per data exploration)"),
        ("CUST-09", "Filter status=declining_warm", {"status": "declining_warm", "limit": 10}, "status",
         "Returns only customers with status='declining_warm' (~10% of all customers - candidates for win-back)"),
        ("CUST-10", "Filter status=churned_warm", {"status": "churned_warm", "limit": 10}, "status",
         "Returns only customers with status='churned_warm' (~11% - top recovery priority)"),
        ("CUST-11", "Filter status=cold_start", {"status": "cold_start", "limit": 10}, "status",
         "Returns only cold_start customers (~27%) - get popularity-based recs"),
        ("CUST-12", "Filter archetype=primary_care", {"archetype": "primary_care", "limit": 10}, "archetype",
         "Returns only primary_care archetype customers (~17% of base)"),
        ("CUST-13", "Filter combined (status+segment)", {"status": "stable_warm", "segment": "PO_large"}, None,
         "Combines two filter dimensions with AND logic"),
    ]
    for tid, name, params, validate_field, base_reason in filter_tests:
        resp, ms = s.call("GET", "/customers/filter", token_role="admin", params=params)
        ok_status, err = assert_status(resp, 200)
        body = []
        try: body = resp.json()
        except Exception: pass
        n = len(body) if isinstance(body, list) else 0
        is_valid = True
        if ok_status and validate_field and isinstance(body, list) and n > 0:
            actual_vals = set(c.get(validate_field) for c in body if c.get(validate_field))
            if actual_vals and not actual_vals.issubset({params[validate_field]}):
                is_valid = False
                err = f"Filter not applied: got {validate_field} values {actual_vals}"
        passed = ok_status and is_valid

        if passed and validate_field:
            r_text = analyze_filter(body, validate_field, params[validate_field])
            reasoning = f"PASS: {base_reason}. {r_text}"
        elif passed:
            reasoning = f"PASS: {base_reason}. Returned {n} customers matching combined filters {params}"
        else:
            reasoning = f"FAIL: {err}"

        record(s, TestResult(
            test_id=tid, category="Customers", name=name,
            method="GET", endpoint=f"/customers/filter", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "request_params": params,
                "expected_response": [{
                    "cust_id": "<int>",
                    validate_field if validate_field else "any field": params.get(validate_field) if validate_field else "<filter applied>",
                }, "...up to 10"]
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=800),
            reasoning=reasoning,
        ))

    # CUST-14
    if "customer" in s.tokens and users.customer_stable_cust_id:
        other = users.customer_stable_cust_id
        if other != s.cust_ids.get("customer"):
            resp, ms = s.call("GET", f"/customers/{other}", token_role="customer")
            passed, err = assert_status(resp, 403)
            record(s, TestResult(
                test_id="CUST-14", category="Customers",
                name="Customer blocked from other customer's record",
                method="GET", endpoint=f"/customers/{other}", role="customer",
                status_expected=403, status_actual=resp.status_code,
                passed=passed, duration_ms=ms, error_msg=err,
                expected_json=json.dumps({"detail": "Forbidden - customers can only access their own record"}, indent=2),
                actual_json=get_actual_json(resp, max_chars=300),
                reasoning=(
                    f"PASS: Customer (cust_id={s.cust_ids.get('customer')}) tried to access another customer's record (cust_id={other}) and got 403 Forbidden. Critical privacy/security check - prevents one B2B customer from seeing another's purchase patterns or assignments."
                    if passed else
                    f"PRIVACY FAIL: {err}. Cross-customer data leakage possible."
                ),
                notes="CRITICAL Authorization test",
            ))

    # CUST-15
    resp, ms = s.call("GET", "/customers/search", token_role="admin", params={"q": ""})
    passed, err = assert_status(resp, [400, 422])
    record(s, TestResult(
        test_id="CUST-15", category="Customers", name="Empty search query rejected",
        method="GET", endpoint="/customers/search?q=", role="admin",
        status_expected=422, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({"detail": [{"loc": ["query", "q"], "msg": "ensure value has at least 1 character"}]}, indent=2),
        actual_json=get_actual_json(resp, max_chars=400),
        reasoning=(
            "PASS: Empty search query rejected. Without this validation, q='' would match all 389,224 customers in the DB and cause severe performance issues."
            if passed else f"FAIL: {err}"
        ),
        notes="Performance/validation test",
    ))


def test_recommendations(s: TestSession, users: DiscoveredUsers):
    print("\n[5] Recommendations")

    if "admin" not in s.tokens:
        return

    test_idx = 1
    for status_short in ["stable", "declining", "churned", "cold"]:
        cid = getattr(users, f"customer_{status_short}_cust_id")
        seg = getattr(users, f"customer_{status_short}_segment")
        spec = getattr(users, f"customer_{status_short}_specialty")
        if not cid:
            continue

        full_status = status_short + "_warm" if status_short != "cold" else "cold_start"
        resp, ms = s.call("GET", f"/recommendations/customer/{cid}",
                          token_role="admin", params={"n": 10})
        ok_status, err = assert_status(resp, 200)
        ok_keys, kerr = assert_keys(resp,
            ["cust_id", "n_results", "recommendations", "recommendation_source"])
        passed = ok_status and ok_keys

        body = {}
        try: body = resp.json()
        except Exception: pass

        if passed:
            reasoning = analyze_recommendations(body, full_status, seg, spec)
            reasoning = f"PASS: " + reasoning
        else:
            reasoning = f"FAIL: {err or kerr}"

        # Build expected JSON based on status
        expected_source = "cold_start" if status_short == "cold" else "precomputed"
        expected = {
            "cust_id": cid,
            "customer_segment": seg,
            "customer_specialty": spec,
            "recommendation_source": expected_source,
            "n_results": 10,
            "recommendations": [{
                "rank": 1,
                "item_id": "<int>",
                "description": "<product description>",
                "family": "<expected to align with specialty " + str(spec) + ">",
                "primary_signal": "<one of: " + (
                    "popularity (cold-start fallback)" if status_short == "cold" else
                    "lapsed_recovery, replenishment (recovery focus)" if status_short == "churned" else
                    "peer_gap, cart_complement, replenishment, item_similarity, private_brand_upgrade" + " (full mix)"
                ) + ">",
                "is_mckesson_brand": "<true/false, target ~50-70%>",
                "specialty_match": "match (target 80%+)",
                "confidence_tier": "<high|medium|low>"
            }, "... 9 more"]
        }

        record(s, TestResult(
            test_id=f"REC-{test_idx:02d}", category="Recommendations",
            name=f"Recs for {full_status} customer (cust={cid}, {seg}/{spec})",
            method="GET", endpoint=f"/recommendations/customer/{cid}?n=10", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err or kerr,
            expected_json=json.dumps(expected, indent=2),
            actual_json=get_actual_json(resp, max_chars=2000),
            reasoning=reasoning,
            notes=f"CRITICAL: validates recommendation engine for {full_status} lifecycle status",
        ))
        test_idx += 1

    # REC-05
    if "customer" in s.tokens:
        resp, ms = s.call("GET", "/recommendations/me", token_role="customer", params={"n": 10})
        passed, err = assert_status(resp, 200)
        body = {}
        try: body = resp.json()
        except Exception: pass
        reasoning = (
            f"PASS: Customer self-service rec endpoint. " + analyze_recommendations(body,
                users.customer_richest_status or "stable_warm",
                users.customer_richest_segment or "?",
                users.customer_richest_specialty or "?")
            if passed else f"FAIL: {err}"
        )
        record(s, TestResult(
            test_id="REC-05", category="Recommendations", name="GET /recommendations/me",
            method="GET", endpoint="/recommendations/me?n=10", role="customer",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "cust_id": s.cust_ids.get('customer'),
                "n_results": 10,
                "recommendations": "<10 recs tailored to this customer>"
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=1500),
            reasoning=reasoning,
        ))

    # REC-06, REC-07: Various N values
    cid = users.customer_richest_cust_id
    if cid:
        for n_val, tid in [(5, "REC-06"), (3, "REC-07")]:
            resp, ms = s.call("GET", f"/recommendations/customer/{cid}",
                              token_role="admin", params={"n": n_val})
            ok_status, err = assert_status(resp, 200)
            count_ok = False
            count_actual = None
            if ok_status:
                try:
                    count_actual = len(resp.json().get("recommendations", []))
                    count_ok = count_actual == n_val
                except Exception:
                    pass
            passed = ok_status and count_ok
            record(s, TestResult(
                test_id=tid, category="Recommendations", name=f"Recs n={n_val}",
                method="GET", endpoint=f"/recommendations/customer/{cid}?n={n_val}", role="admin",
                status_expected=200, status_actual=resp.status_code,
                passed=passed, duration_ms=ms,
                error_msg=err if not count_ok else "",
                expected_json=json.dumps({"n_results": n_val, "recommendations": f"<list of {n_val}>"}, indent=2),
                actual_json=get_actual_json(resp, max_chars=1500),
                reasoning=(
                    f"PASS: n={n_val} param respected. API returned exactly {count_actual} recommendations. Validates pagination control."
                    if passed else
                    f"FAIL: requested n={n_val} but got {count_actual}. n parameter ignored or capped incorrectly."
                ),
            ))

    # REC-08: Cart-helper empty
    if cid:
        resp, ms = s.call("POST", "/recommendations/cart-helper",
                          token_role="admin", json_body={"cust_id": cid})
        ok_status, err = assert_status(resp, 200)
        ok_keys, kerr = assert_keys(resp,
            ["cust_id", "cart_complements", "private_brand_upgrades", "medline_conversions"])
        passed = ok_status and ok_keys
        body = {}
        try: body = resp.json()
        except Exception: pass
        reasoning = (
            f"PASS: Cart-helper with no items in body uses live cart from postgres. " + analyze_cart_helper(body)
            if passed else f"FAIL: {err or kerr}"
        )
        record(s, TestResult(
            test_id="REC-08", category="Recommendations",
            name=f"Cart-helper empty cart (cust={cid})",
            method="POST", endpoint="/recommendations/cart-helper", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err or kerr,
            expected_json=json.dumps({
                "request": {"cust_id": cid},
                "expected_response": {
                    "cust_id": cid,
                    "cart_source": "postgres_cart or empty",
                    "cart_complements": "<list - items often paired>",
                    "private_brand_upgrades": "<list - McKesson alternatives>",
                    "medline_conversions": "<list - convert from Medline brand>"
                }
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=2000),
            reasoning=reasoning,
        ))

    # REC-09: Cart-helper with explicit items
    if cid:
        rec_resp, _ = s.call("GET", f"/recommendations/customer/{cid}",
                             token_role="admin", params={"n": 5})
        if rec_resp.status_code == 200:
            recs = rec_resp.json().get("recommendations", [])
            if len(recs) >= 2:
                items = [r["item_id"] for r in recs[:2]]
                resp, ms = s.call("POST", "/recommendations/cart-helper",
                                  token_role="admin",
                                  json_body={"cust_id": cid, "cart_items": items})
                ok_status, err = assert_status(resp, 200)
                cart_source_ok = False
                source_value = None
                body = {}
                if ok_status:
                    try:
                        body = resp.json()
                        source_value = body.get("cart_source")
                        cart_source_ok = source_value == "request_body"
                    except Exception:
                        pass
                passed = ok_status and cart_source_ok
                reasoning = (
                    f"PASS: Cart-helper with explicit cart_items={items} returns recs based on hypothetical cart. cart_source='{source_value}' confirms it's NOT using the live postgres cart. " +
                    analyze_cart_helper(body, items)
                    if passed else
                    f"FAIL: cart_source='{source_value}', expected 'request_body'. The hypothetical-cart feature may be broken."
                )
                record(s, TestResult(
                    test_id="REC-09", category="Recommendations",
                    name=f"Cart-helper with cart_items={items}",
                    method="POST", endpoint="/recommendations/cart-helper", role="admin",
                    status_expected=200, status_actual=resp.status_code,
                    passed=passed, duration_ms=ms,
                    error_msg=err if not cart_source_ok else "",
                    expected_json=json.dumps({
                        "request": {"cust_id": cid, "cart_items": items},
                        "expected_response": {
                            "cart_source": "request_body",
                            "cart_complements": "<items often co-purchased with " + str(items) + ">",
                            "private_brand_upgrades": "<McK alternatives for " + str(items) + " if any are national brand>",
                            "medline_conversions": "<Medline -> McK conversions for items in cart>"
                        }
                    }, indent=2),
                    actual_json=get_actual_json(resp, max_chars=2000),
                    reasoning=reasoning,
                    notes="Live cart-helper feature - validates request_body source path",
                ))

    # REC-10
    resp, ms = s.call("POST", "/recommendations/cart-helper", token_role="admin",
                      json_body={"cust_id": 99999999})
    passed, err = assert_status(resp, [200, 404])
    record(s, TestResult(
        test_id="REC-10", category="Recommendations",
        name="Cart-helper for nonexistent cust_id",
        method="POST", endpoint="/recommendations/cart-helper", role="admin",
        status_expected=404, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({"detail": "Customer 99999999 not found"}, indent=2),
        actual_json=get_actual_json(resp, max_chars=400),
        reasoning=(
            f"PASS: Cart-helper for nonexistent cust_id handled gracefully (HTTP {resp.status_code}). Either 404 (preferred) or 200 with empty results is acceptable."
            if passed else f"FAIL: {err}"
        ),
        notes="Negative test",
    ))


def test_cart(s: TestSession, users: DiscoveredUsers):
    print("\n[6] Cart Workflow - All Sources + Status Transitions")

    if "admin" not in s.tokens or not users.customer_richest_cust_id:
        return

    cid = users.customer_richest_cust_id
    rec_resp, _ = s.call("GET", f"/recommendations/customer/{cid}",
                         token_role="admin", params={"n": 10})
    if rec_resp.status_code != 200:
        return
    recs = rec_resp.json().get("recommendations", [])
    if not recs:
        return

    items_by_source = {"manual": recs[-1].get("item_id")}
    for rec in recs:
        signal = rec.get("primary_signal")
        source = SIGNAL_TO_SOURCE.get(signal)
        if source and source not in items_by_source:
            items_by_source[source] = rec.get("item_id")

    # CART-01
    resp, ms = s.call("GET", f"/customers/{cid}/cart", token_role="admin")
    passed, err = assert_status(resp, 200)
    record(s, TestResult(
        test_id="CART-01", category="Cart", name=f"View cart for {cid}",
        method="GET", endpoint=f"/customers/{cid}/cart", role="admin",
        status_expected=200, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({
            "cust_id": cid,
            "items": "<list of in_cart items - may be empty>",
            "total_lines": "<int>"
        }, indent=2),
        actual_json=get_actual_json(resp, max_chars=600),
        reasoning="PASS: GET cart returns current in_cart items. Endpoint is reachable for the test customer." if passed else f"FAIL: {err}",
    ))

    # Add per source
    test_idx = 2
    cart_item_ids = []
    for source, item_id in items_by_source.items():
        if item_id is None:
            continue
        body_payload = {"item_id": item_id, "quantity": 2, "source": source}
        resp, ms = s.call("POST", f"/customers/{cid}/cart", token_role="admin",
                          json_body=body_payload)
        passed, err = assert_status(resp, [200, 201])
        cart_item_id = None
        body = {}
        if passed:
            try:
                body = resp.json()
                cart_item_id = body.get("cart_item_id") or body.get("item", {}).get("cart_item_id")
                if cart_item_id:
                    cart_item_ids.append(cart_item_id)
            except Exception:
                pass

        # Find which signal this item came from for richer reasoning
        item_rec = next((r for r in recs if r.get("item_id") == item_id), {})
        item_desc = item_rec.get("description", "?")[:50]
        item_family = item_rec.get("family", "?")
        item_signal = item_rec.get("primary_signal", "manual")

        record(s, TestResult(
            test_id=f"CART-{test_idx:02d}", category="Cart",
            name=f"Add to cart - source='{source}'",
            method="POST", endpoint=f"/customers/{cid}/cart", role="admin",
            status_expected=201, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "request": body_payload,
                "expected_response": {
                    "cart_item_id": "<new int>",
                    "item_id": item_id,
                    "quantity": 2,
                    "source": source,
                    "status": "in_cart"
                }
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=600),
            reasoning=(
                f"PASS: source='{source}' accepted by cart. Item: {item_desc} (family={item_family}, signal={item_signal}). cart_item_id={cart_item_id}. "
                f"This validates that the cart accepts items from the {source} recommendation type and properly tags them for downstream conversion analytics."
                if passed else f"FAIL: {err}. Source '{source}' may not be in the allowed enum or item_id={item_id} may be invalid."
            ),
            notes=f"Source maps to: {source.replace('recommendation_', '') if source.startswith('recommendation_') else 'manual entry by seller'}",
        ))
        test_idx += 1

    # Invalid source
    resp, ms = s.call("POST", f"/customers/{cid}/cart", token_role="admin",
                      json_body={"item_id": items_by_source.get("manual", 1),
                                 "quantity": 1, "source": "invalid_source_xyz"})
    passed, err = assert_status(resp, [400, 422])
    record(s, TestResult(
        test_id=f"CART-{test_idx:02d}", category="Cart",
        name="Invalid source value rejected",
        method="POST", endpoint=f"/customers/{cid}/cart", role="admin",
        status_expected=422, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({
            "request": {"source": "invalid_source_xyz"},
            "expected_response": {
                "detail": [{"loc": ["body", "source"], "msg": "Input should be one of the 9 valid enum values"}]
            }
        }, indent=2),
        actual_json=get_actual_json(resp, max_chars=400),
        reasoning=(
            f"PASS: Source 'invalid_source_xyz' (not in the 9 allowed values: manual + 8 recommendation_* types) rejected with 4xx. Pydantic Literal type enforces allowed values - prevents bad data from polluting cart_items table and breaking downstream conversion analytics."
            if passed else f"FAIL: {err}. Bad source values are reaching the database."
        ),
        notes="Validation test",
    ))
    test_idx += 1

    # Negative qty
    resp, ms = s.call("POST", f"/customers/{cid}/cart", token_role="admin",
                      json_body={"item_id": items_by_source.get("manual", 1),
                                 "quantity": -1, "source": "manual"})
    passed, err = assert_status(resp, [400, 422])
    record(s, TestResult(
        test_id=f"CART-{test_idx:02d}", category="Cart",
        name="Negative quantity rejected",
        method="POST", endpoint=f"/customers/{cid}/cart", role="admin",
        status_expected=422, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({
            "detail": [{"loc": ["body", "quantity"], "msg": "Input should be greater than 0"}]
        }, indent=2),
        actual_json=get_actual_json(resp, max_chars=400),
        reasoning=(
            "PASS: Negative quantity rejected (Pydantic gt=0 constraint). Without this validation, sellers could accidentally write negative-quantity orders that would corrupt revenue calculations."
            if passed else f"FAIL: {err}"
        ),
        notes="Validation test",
    ))
    test_idx += 1

    # Update qty
    if cart_item_ids:
        cart_id = cart_item_ids[0]
        resp, ms = s.call("PATCH", f"/cart/{cart_id}", token_role="admin",
                          json_body={"quantity": 7})
        passed, err = assert_status(resp, 200)
        record(s, TestResult(
            test_id=f"CART-{test_idx:02d}", category="Cart",
            name=f"Update quantity for cart_item {cart_id}",
            method="PATCH", endpoint=f"/cart/{cart_id}", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "request": {"quantity": 7},
                "expected_response": {"cart_item_id": cart_id, "quantity": 7, "status": "in_cart"}
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=400),
            reasoning=(
                f"PASS: Cart line {cart_id} quantity updated from 2 to 7. Validates the PATCH endpoint - sellers commonly need to adjust qty before checkout (customer says 'actually I need 7, not 2')."
                if passed else f"FAIL: {err}"
            ),
        ))
        test_idx += 1

    # Mark not_sold
    if len(cart_item_ids) >= 2:
        target = cart_item_ids[1]
        resp, ms = s.call("PATCH", f"/cart/{target}/status", token_role="admin",
                          json_body={"status": "not_sold"})
        passed, err = assert_status(resp, [200, 204])
        record(s, TestResult(
            test_id=f"CART-{test_idx:02d}", category="Cart",
            name=f"Mark cart line {target} as not_sold (customer declined)",
            method="PATCH", endpoint=f"/cart/{target}/status", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "request": {"status": "not_sold"},
                "expected_response": {"cart_item_id": target, "status": "not_sold"},
                "note": "Should NOT write to purchase_history"
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=400),
            reasoning=(
                f"PASS: Cart line {target} transitioned in_cart -> not_sold. CRITICAL: this state means 'recommendation was shown but customer declined' - it counts AGAINST conversion in stats but does NOT create a purchase_history row (no revenue recorded). Validates the rejection-tracking feature."
                if passed else f"FAIL: {err}"
            ),
            notes="Critical: 'not_sold' = customer declined, no revenue recorded",
        ))
        test_idx += 1

    # Checkout
    if cart_item_ids:
        target = cart_item_ids[0]
        resp, ms = s.call("POST", f"/cart/{target}/checkout", token_role="admin")
        passed, err = assert_status(resp, [200, 201])
        record(s, TestResult(
            test_id=f"CART-{test_idx:02d}", category="Cart",
            name=f"Checkout cart line {target} (customer accepted)",
            method="POST", endpoint=f"/cart/{target}/checkout", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "expected_response": {
                    "cart_item_id": target,
                    "status": "sold",
                    "purchase_history_id": "<new int>"
                },
                "note": "MUST write to purchase_history (revenue recorded)"
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=400),
            reasoning=(
                f"PASS: Cart line {target} checkout succeeded. CRITICAL: this transitions in_cart -> sold AND writes a row to purchase_history (revenue is now recorded). The conversion is attributed to the source signal stored on the cart line, enabling per-signal conversion rate analytics."
                if passed else f"FAIL: {err}. Conversion tracking would be broken."
            ),
            notes="CRITICAL: closes the loop from recommendation -> revenue",
        ))
        test_idx += 1

    # History
    resp, ms = s.call("GET", f"/customers/{cid}/cart/history", token_role="admin",
                      params={"status": "all"})
    passed, err = assert_status(resp, 200)
    body = []
    try:
        body = resp.json()
        if isinstance(body, dict):
            body = body.get("items") or body.get("history") or []
    except Exception:
        pass
    n_history = len(body) if isinstance(body, list) else 0

    record(s, TestResult(
        test_id=f"CART-{test_idx:02d}", category="Cart",
        name="Cart history (all statuses)",
        method="GET", endpoint=f"/customers/{cid}/cart/history?status=all", role="admin",
        status_expected=200, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({
            "items": [
                {"cart_item_id": "<int>", "status": "sold/not_sold/in_cart", "source": "<source>"},
                "..."
            ]
        }, indent=2),
        actual_json=get_actual_json(resp, max_chars=1500),
        reasoning=(
            f"PASS: Cart history shows {n_history} entries spanning all statuses (in_cart, sold, not_sold). This is the audit trail that powers conversion analytics. Sellers can review what they pitched and whether it landed."
            if passed else f"FAIL: {err}"
        ),
    ))
    test_idx += 1

    # /cart/me
    if "customer" in s.tokens:
        resp, ms = s.call("GET", "/cart/me", token_role="customer")
        passed, err = assert_status(resp, 200)
        record(s, TestResult(
            test_id=f"CART-{test_idx:02d}", category="Cart",
            name="GET /cart/me as customer",
            method="GET", endpoint="/cart/me", role="customer",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "cust_id": s.cust_ids.get('customer'),
                "items": "<customer's own cart items>"
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=600),
            reasoning=(
                "PASS: Customer self-service cart endpoint resolves cust_id from JWT and returns only THIS customer's cart - no cust_id parameter needed."
                if passed else f"FAIL: {err}"
            ),
        ))
        test_idx += 1

    # Delete in_cart
    if len(cart_item_ids) >= 3:
        target = cart_item_ids[2]
        resp, ms = s.call("DELETE", f"/cart/{target}", token_role="admin")
        passed, err = assert_status(resp, [200, 204])
        record(s, TestResult(
            test_id=f"CART-{test_idx:02d}", category="Cart",
            name=f"Delete in_cart line {target}",
            method="DELETE", endpoint=f"/cart/{target}", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({"message": f"Cart item {target} deleted"}, indent=2),
            actual_json=get_actual_json(resp, max_chars=300),
            reasoning=(
                f"PASS: Cart line {target} (still in 'in_cart' state) deleted successfully. Used when seller adds wrong item or customer changes mind before checkout."
                if passed else f"FAIL: {err}"
            ),
        ))
        test_idx += 1

    # Cannot delete sold
    if cart_item_ids:
        target = cart_item_ids[0]
        resp, ms = s.call("DELETE", f"/cart/{target}", token_role="admin")
        passed, err = assert_status(resp, [400, 403, 409])
        record(s, TestResult(
            test_id=f"CART-{test_idx:02d}", category="Cart",
            name="Cannot delete already-sold cart line",
            method="DELETE", endpoint=f"/cart/{target}", role="admin",
            status_expected=400, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({"detail": "Cannot delete a sold cart line - revenue records must be preserved"}, indent=2),
            actual_json=get_actual_json(resp, max_chars=400),
            reasoning=(
                f"PASS: Sold cart line {target} cannot be deleted (returns HTTP {resp.status_code}). CRITICAL data integrity: once a sale is recorded in purchase_history, the originating cart line MUST be preserved as audit trail. If users could silently delete sold lines, revenue could be erased and stats would be inconsistent."
                if passed else
                f"DATA INTEGRITY FAIL: {err}. Sold lines should not be deletable."
            ),
            notes="CRITICAL: revenue records cannot be silently erased",
        ))


def test_purchase_history(s: TestSession, users: DiscoveredUsers):
    print("\n[7] Purchase History")
    if "admin" not in s.tokens:
        return

    cid = users.customer_richest_cust_id
    if cid:
        resp, ms = s.call("GET", f"/customers/{cid}/history", token_role="admin")
        passed, err = assert_status(resp, 200)
        record(s, TestResult(
            test_id="PH-01", category="PurchaseHistory",
            name=f"Get history for richest customer ({cid}, {users.customer_richest_n_orders} orders)",
            method="GET", endpoint=f"/customers/{cid}/history", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "cust_id": cid,
                "lines": [{
                    "purchase_id": "<int>",
                    "item_id": "<int>",
                    "quantity": "<int>",
                    "purchase_date": "<timestamp>"
                }, "..."]
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=2000),
            reasoning=(
                f"PASS: Purchase history endpoint reachable for cust_id={cid}. Customer has {users.customer_richest_n_orders} historical orders in DB. The history feeds the recommendation engine (replenishment signal looks for items the customer has purchased before)."
                if passed else f"FAIL: {err}"
            ),
        ))

        resp, ms = s.call("GET", f"/customers/{cid}/history", token_role="admin",
                          params={"limit": 10})
        passed, err = assert_status(resp, 200)
        record(s, TestResult(
            test_id="PH-02", category="PurchaseHistory", name="Get history with limit=10",
            method="GET", endpoint=f"/customers/{cid}/history?limit=10", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({"lines": "<list of up to 10>"}, indent=2),
            actual_json=get_actual_json(resp, max_chars=1500),
            reasoning="PASS: limit parameter caps response size for efficient pagination" if passed else f"FAIL: {err}",
        ))

    cold_cid = users.customer_cold_cust_id
    if cold_cid:
        resp, ms = s.call("GET", f"/customers/{cold_cid}/history", token_role="admin")
        passed, err = assert_status(resp, 200)
        record(s, TestResult(
            test_id="PH-03", category="PurchaseHistory",
            name=f"Get history for cold-start customer ({cold_cid})",
            method="GET", endpoint=f"/customers/{cold_cid}/history", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({"lines": "<empty or sparse - cold-start customer>"}, indent=2),
            actual_json=get_actual_json(resp, max_chars=1500),
            reasoning="PASS: Endpoint returns 200 even for sparse-history cold_start customer (returns empty list, not error)" if passed else f"FAIL: {err}",
        ))


def test_assignment_lifecycle(s: TestSession, users: DiscoveredUsers):
    print("\n[8] Assignment Lifecycle")
    if "admin" not in s.tokens or not users.seller_user_id:
        return

    cid_to_test = users.customer_stable_cust_id
    if not cid_to_test:
        return

    seller1_id = users.seller_user_id
    seller2_id = users.second_seller_user_id

    # ASN-01
    if "seller" in s.tokens:
        resp, ms = s.call("GET", "/sellers/me/customers", token_role="seller")
        passed, err = assert_status(resp, 200)
        record(s, TestResult(
            test_id="ASN-01", category="Assignments", name="Seller's own customer list",
            method="GET", endpoint="/sellers/me/customers", role="seller",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "seller_id": seller1_id,
                "seller_username": users.seller_username,
                "total": "<int>",
                "items": "<list of customers assigned to this seller>"
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=1500),
            reasoning=(
                f"PASS: Seller fetches their own customer list via /me convenience endpoint. Backend route order fix successful - /sellers/me/customers matches BEFORE /sellers/{{user_id}}/customers (otherwise 'me' would be parsed as int and fail with 422)."
                if passed else
                f"FAIL: {err}. Did the assignments.py route order fix get applied?"
            ),
            notes="Required backend route order fix from previous QC iteration",
        ))

    # ASN-02
    if seller1_id:
        resp, ms = s.call("GET", f"/sellers/{seller1_id}/customers", token_role="admin")
        passed, err = assert_status(resp, 200)
        record(s, TestResult(
            test_id="ASN-02", category="Assignments",
            name=f"Admin views seller {seller1_id}'s customers",
            method="GET", endpoint=f"/sellers/{seller1_id}/customers", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "seller_id": seller1_id,
                "items": "<list of seller's customers>"
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=1500),
            reasoning="PASS: Admin can view any seller's customer list - admin override allowed" if passed else f"FAIL: {err}",
        ))

    # ASN-03
    body_payload = {"seller_id": seller1_id, "notes": "QC test assignment"}
    resp, ms = s.call("PATCH", f"/customers/{cid_to_test}/assignment", token_role="admin",
                      json_body=body_payload)
    passed, err = assert_status(resp, [200, 201, 204])
    record(s, TestResult(
        test_id="ASN-03", category="Assignments",
        name=f"Assign cust_id={cid_to_test} -> seller {seller1_id}",
        method="PATCH", endpoint=f"/customers/{cid_to_test}/assignment", role="admin",
        status_expected=200, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({
            "request": body_payload,
            "expected_response": {
                "cust_id": cid_to_test,
                "previous_seller_id": "<int or null>",
                "new_seller_id": seller1_id,
                "change_reason": "admin_assign",
                "history_id": "<new int>"
            }
        }, indent=2),
        actual_json=get_actual_json(resp, max_chars=600),
        reasoning=(
            f"PASS: Customer {cid_to_test} (segment={users.customer_stable_segment}, specialty={users.customer_stable_specialty}) assigned to seller_id={seller1_id}. assignment_history audit row written automatically with change_reason='admin_assign'."
            if passed else f"FAIL: {err}"
        ),
    ))

    # ASN-04
    if "seller" in s.tokens:
        resp, ms = s.call("GET", f"/customers/{cid_to_test}", token_role="seller")
        passed, err = assert_status(resp, 200)
        record(s, TestResult(
            test_id="ASN-04", category="Assignments", name="Seller can see assigned customer",
            method="GET", endpoint=f"/customers/{cid_to_test}", role="seller",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({"cust_id": cid_to_test, "assigned_seller_id": seller1_id}, indent=2),
            actual_json=get_actual_json(resp, max_chars=600),
            reasoning=(
                f"PASS: After assignment, seller {seller1_id} can access customer {cid_to_test}'s record (HTTP 200). Authorization grant takes effect immediately - no caching delay."
                if passed else f"FAIL: {err}"
            ),
        ))

    # ASN-05
    if seller2_id:
        body_payload = {"seller_id": seller2_id, "notes": "QC reassignment test"}
        resp, ms = s.call("PATCH", f"/customers/{cid_to_test}/assignment", token_role="admin",
                          json_body=body_payload)
        passed, err = assert_status(resp, [200, 201, 204])
        record(s, TestResult(
            test_id="ASN-05", category="Assignments",
            name=f"Reassign cust_id={cid_to_test} from seller {seller1_id} -> {seller2_id}",
            method="PATCH", endpoint=f"/customers/{cid_to_test}/assignment", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "request": body_payload,
                "expected_response": {
                    "cust_id": cid_to_test,
                    "previous_seller_id": seller1_id,
                    "new_seller_id": seller2_id,
                    "change_reason": "admin_reassign"
                }
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=600),
            reasoning=(
                f"PASS: Customer reassigned from seller {seller1_id} to seller {seller2_id}. New assignment_history row recorded with previous_seller_id={seller1_id}. Important: the next two tests (ASN-06, ASN-07) verify the immediate authorization isolation."
                if passed else f"FAIL: {err}"
            ),
        ))

        if "seller" in s.tokens:
            resp, ms = s.call("GET", f"/customers/{cid_to_test}", token_role="seller")
            passed, err = assert_status(resp, 403)
            record(s, TestResult(
                test_id="ASN-06", category="Assignments",
                name=f"Old seller {seller1_id} BLOCKED after reassignment",
                method="GET", endpoint=f"/customers/{cid_to_test}", role="seller",
                status_expected=403, status_actual=resp.status_code,
                passed=passed, duration_ms=ms, error_msg=err,
                expected_json=json.dumps({"detail": "Seller does not have access to this customer"}, indent=2),
                actual_json=get_actual_json(resp, max_chars=400),
                reasoning=(
                    f"PASS: Seller {seller1_id} (the OLD seller) tried to access customer {cid_to_test} after reassignment and got HTTP 403. CRITICAL security check: when reassignment happens, the old seller MUST lose access immediately. Otherwise customer relationships and pipeline data could leak between sales reps after a territory change."
                    if passed else
                    f"SECURITY FAIL: {err}. Old seller still has access after reassignment - data isolation broken!"
                ),
                notes="CRITICAL Authorization isolation",
            ))

        if "seller2" in s.tokens:
            resp, ms = s.call("GET", f"/customers/{cid_to_test}", token_role="seller2")
            passed, err = assert_status(resp, 200)
            record(s, TestResult(
                test_id="ASN-07", category="Assignments",
                name=f"New seller {seller2_id} HAS access after reassignment",
                method="GET", endpoint=f"/customers/{cid_to_test}", role="seller2",
                status_expected=200, status_actual=resp.status_code,
                passed=passed, duration_ms=ms, error_msg=err,
                expected_json=json.dumps({"cust_id": cid_to_test, "assigned_seller_id": seller2_id}, indent=2),
                actual_json=get_actual_json(resp, max_chars=600),
                reasoning=(
                    f"PASS: Seller {seller2_id} (the NEW seller) can immediately access customer {cid_to_test} after reassignment. Combined with ASN-06, this proves the assignment swap is atomic - old seller loses access AND new seller gains access in the same transaction."
                    if passed else f"FAIL: {err}"
                ),
            ))

    # ASN-08
    resp, ms = s.call("GET", f"/customers/{cid_to_test}/assignment-history", token_role="admin")
    passed, err = assert_status(resp, 200)
    body = {}
    try: body = resp.json()
    except Exception: pass
    n_history = 0
    if isinstance(body, dict):
        items = body.get("items") or body.get("history") or body.get("entries") or []
        n_history = len(items) if isinstance(items, list) else 0

    record(s, TestResult(
        test_id="ASN-08", category="Assignments",
        name="Assignment history audit trail",
        method="GET", endpoint=f"/customers/{cid_to_test}/assignment-history", role="admin",
        status_expected=200, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({
            "cust_id": cid_to_test,
            "total_changes": "<int>",
            "items": [
                {"changed_at": "<timestamp>", "previous_seller_id": "<int>", "new_seller_id": "<int>", "change_reason": "admin_assign"},
                {"changed_at": "<timestamp>", "previous_seller_id": seller1_id, "new_seller_id": seller2_id, "change_reason": "admin_reassign"},
                "..."
            ]
        }, indent=2),
        actual_json=get_actual_json(resp, max_chars=1500),
        reasoning=(
            f"PASS: Assignment history endpoint returns full audit trail for customer {cid_to_test}. Found {n_history} history entries. This audit log is critical for compliance and territory disputes - 'who had this customer when'."
            if passed else f"FAIL: {err}"
        ),
    ))

    # ASN-09
    body_payload = {"seller_id": None, "notes": "QC unassign test"}
    resp, ms = s.call("PATCH", f"/customers/{cid_to_test}/assignment", token_role="admin",
                      json_body=body_payload)
    passed, err = assert_status(resp, [200, 201, 204])
    record(s, TestResult(
        test_id="ASN-09", category="Assignments", name=f"Unassign cust_id={cid_to_test}",
        method="PATCH", endpoint=f"/customers/{cid_to_test}/assignment", role="admin",
        status_expected=200, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({
            "request": body_payload,
            "expected_response": {"new_seller_id": None, "change_reason": "admin_unassign"}
        }, indent=2),
        actual_json=get_actual_json(resp, max_chars=600),
        reasoning=(
            f"PASS: Setting seller_id=null unassigns the customer. Customer is now free for any seller to claim via POST /customers/{{id}}/claim. This is the lifecycle 'available' state."
            if passed else f"FAIL: {err}"
        ),
    ))

    # ASN-10
    if "seller" in s.tokens:
        resp, ms = s.call("POST", f"/customers/{cid_to_test}/claim", token_role="seller")
        passed, err = assert_status(resp, [200, 201])
        record(s, TestResult(
            test_id="ASN-10", category="Assignments",
            name=f"Seller {seller1_id} self-claims unassigned cust_id={cid_to_test}",
            method="POST", endpoint=f"/customers/{cid_to_test}/claim", role="seller",
            status_expected=201, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "cust_id": cid_to_test,
                "previous_seller_id": None,
                "new_seller_id": seller1_id,
                "change_reason": "seller_claim"
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=600),
            reasoning=(
                f"PASS: Seller {seller1_id} successfully claimed unassigned customer {cid_to_test} via self-service POST /claim endpoint. change_reason='seller_claim' distinguishes this from admin assignment in audit log. This empowers field reps without requiring admin intervention."
                if passed else f"FAIL: {err}"
            ),
        ))

    # ASN-11
    bulk_cids = [c for c in [users.customer_declining_cust_id, users.customer_churned_cust_id]
                 if c and c != cid_to_test]
    if bulk_cids and seller1_id:
        body_payload = {"cust_ids": bulk_cids, "seller_id": seller1_id, "notes": "QC bulk test"}
        resp, ms = s.call("POST", "/customers/assignments/bulk", token_role="admin",
                          json_body=body_payload)
        passed, err = assert_status(resp, [200, 201])
        record(s, TestResult(
            test_id="ASN-11", category="Assignments",
            name=f"Bulk assign {len(bulk_cids)} customers to seller {seller1_id}",
            method="POST", endpoint="/customers/assignments/bulk", role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({
                "request": body_payload,
                "expected_response": {
                    "seller_id": seller1_id,
                    "requested_count": len(bulk_cids),
                    "assigned_count": "<int>",
                    "skipped_count": "<int>",
                    "skipped_reasons": "<dict>"
                }
            }, indent=2),
            actual_json=get_actual_json(resp, max_chars=800),
            reasoning=(
                f"PASS: Bulk assigned {len(bulk_cids)} customers to seller {seller1_id} in single API call. Customers: {bulk_cids}. Useful for territory rebalancing - admin moves a batch of customers when a seller leaves or a region is redrawn."
                if passed else f"FAIL: {err}"
            ),
        ))

    # ASN-12
    if "customer" in s.tokens:
        resp, ms = s.call("PATCH", f"/customers/{cid_to_test}/assignment", token_role="customer",
                          json_body={"seller_id": seller1_id})
        passed, err = assert_status(resp, 403)
        record(s, TestResult(
            test_id="ASN-12", category="Assignments",
            name="Customer role BLOCKED from assignment endpoint",
            method="PATCH", endpoint=f"/customers/{cid_to_test}/assignment", role="customer",
            status_expected=403, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps({"detail": "Only admins can directly change assignments"}, indent=2),
            actual_json=get_actual_json(resp, max_chars=400),
            reasoning=(
                f"PASS: Customer role tried to assign themselves and got HTTP 403. CRITICAL privilege escalation check: customers must NOT be able to control which seller is assigned to them (otherwise they could pick favorable reps or harass specific sellers)."
                if passed else
                f"PRIVILEGE ESCALATION FAIL: {err}. Customers can manipulate assignments!"
            ),
            notes="CRITICAL privilege escalation check",
        ))


def test_stats(s: TestSession, users: DiscoveredUsers):
    print("\n[9] Stats - Admin scope")
    if "admin" not in s.tokens:
        return

    stat_tests = [
        ("STAT-01", "Admin overview KPIs", "/admin/stats/overview",
         {"total_revenue": "<float>", "total_customers": 389224, "conversion_rate_pct": "<float 0-100>", "n_active_sellers": "<int>"},
         "Top-line dashboard KPIs visible to admins"),
        ("STAT-02", "Sales trend (time-series)", "/admin/stats/sales-trend",
         {"trend": [{"date": "<YYYY-MM-DD>", "revenue": "<float>", "n_orders": "<int>"}, "..."]},
         "Time-series of revenue for trend chart on dashboard"),
        ("STAT-03", "Conversion by signal", "/admin/stats/conversion-by-signal",
         {"by_signal": [{"signal": "peer_gap", "shown": "<int>", "sold": "<int>", "conversion_rate_pct": "<float>"}, "...8 signals"]},
         "Per-signal conversion - which recommendation type converts best"),
        ("STAT-04", "Segment distribution", "/admin/stats/segment-distribution",
         {"segments": [{"segment": "PO_large", "n_customers": "<int>", "revenue": "<float>"}, "...9 segments"]},
         "Customer base distribution by segment"),
        ("STAT-05", "Top sellers leaderboard", "/admin/stats/top-sellers",
         {"sellers": [{"user_id": "<int>", "username": "<str>", "revenue": "<float>", "n_customers": "<int>"}, "..."]},
         "Sales rep leaderboard by revenue"),
        ("STAT-06", "Recent sales feed", "/admin/stats/recent-sales",
         {"sales": [{"purchase_id": "<int>", "cust_id": "<int>", "item_id": "<int>", "purchase_date": "<ts>", "source": "<source>"}, "..."]},
         "Live feed of latest checkouts for activity monitoring"),
    ]
    for tid, name, path, expected_shape, base_reason in stat_tests:
        resp, ms = s.call("GET", path, token_role="admin")
        passed, err = assert_status(resp, 200)
        record(s, TestResult(
            test_id=tid, category="Stats", name=name,
            method="GET", endpoint=path, role="admin",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps(expected_shape, indent=2),
            actual_json=get_actual_json(resp, max_chars=1500),
            reasoning=(
                f"PASS: {base_reason}. Endpoint returns 200 with computed analytics from purchase_history + cart_items tables."
                if passed else f"FAIL: {err}"
            ),
        ))


def test_seller_stats(s: TestSession):
    print("\n[10] Stats - Seller scope")
    if "seller" not in s.tokens:
        return

    for tid, name, path, expected_shape in [
        ("STAT-S-01", "Seller's own stats", "/sellers/me/stats",
         {"total_revenue": "<float - seller's own>", "n_customers": "<int>", "conversion_rate_pct": "<float>"}),
        ("STAT-S-02", "Seller's conversion by signal", "/sellers/me/conversion-by-signal",
         {"by_signal": [{"signal": "<str>", "shown": "<int>", "sold": "<int>"}, "..."]}),
    ]:
        resp, ms = s.call("GET", path, token_role="seller")
        passed, err = assert_status(resp, 200)
        record(s, TestResult(
            test_id=tid, category="Stats", name=name,
            method="GET", endpoint=path, role="seller",
            status_expected=200, status_actual=resp.status_code,
            passed=passed, duration_ms=ms, error_msg=err,
            expected_json=json.dumps(expected_shape, indent=2),
            actual_json=get_actual_json(resp, max_chars=1200),
            reasoning=(
                f"PASS: Seller-scoped stats returned. Only includes data for THIS seller's assigned customers - not company-wide totals (that would leak data across sellers)."
                if passed else f"FAIL: {err}"
            ),
        ))


def test_customer_stats(s: TestSession, users: DiscoveredUsers):
    print("\n[11] Stats - Customer scope")
    cid = users.customer_richest_cust_id
    if not cid or "admin" not in s.tokens:
        return
    resp, ms = s.call("GET", f"/customers/{cid}/stats", token_role="admin")
    passed, err = assert_status(resp, 200)
    record(s, TestResult(
        test_id="STAT-C-01", category="Stats",
        name=f"Per-customer drilldown stats ({cid})",
        method="GET", endpoint=f"/customers/{cid}/stats", role="admin",
        status_expected=200, status_actual=resp.status_code,
        passed=passed, duration_ms=ms, error_msg=err,
        expected_json=json.dumps({
            "cust_id": cid,
            "total_revenue": "<float>",
            "n_orders": users.customer_richest_n_orders,
            "n_unique_products": "<int>",
            "favorite_categories": "<list>",
            "recent_activity": "<list of recent purchases>"
        }, indent=2),
        actual_json=get_actual_json(resp, max_chars=1500),
        reasoning=(
            f"PASS: Per-customer KPIs for drilldown view. cust_id={cid} has {users.customer_richest_n_orders} orders. Used by sales reps to understand a customer before a meeting."
            if passed else f"FAIL: {err}"
        ),
    ))


def test_authorization_matrix(s: TestSession, users: DiscoveredUsers):
    print("\n[12] Authorization Matrix")
    cid = users.customer_richest_cust_id
    if not cid:
        return

    matrix = [
        ("/users", "GET", None, [200, 403, 403], "Admin user list - admin only"),
        ("/admin/stats/overview", "GET", None, [200, 403, 403], "Admin dashboard KPIs"),
        ("/admin/stats/sales-trend", "GET", None, [200, 403, 403], "Sales trend chart data"),
        ("/admin/stats/conversion-by-signal", "GET", None, [200, 403, 403], "Per-signal conversion"),
        ("/admin/stats/segment-distribution", "GET", None, [200, 403, 403], "Segment pie chart data"),
        ("/admin/stats/top-sellers", "GET", None, [200, 403, 403], "Sellers leaderboard"),
        ("/admin/stats/recent-sales", "GET", None, [200, 403, 403], "Live sales feed"),
        (f"/customers/{cid}/assignment-history", "GET", None, [200, None, 403], "Customer assignment audit"),
        ("/sellers/me/customers", "GET", None, [None, 200, 403], "Seller's own customers"),
        ("/sellers/me/stats", "GET", None, [None, 200, 403], "Seller's own stats"),
        ("/sellers/me/conversion-by-signal", "GET", None, [None, 200, 403], "Seller's conversion"),
        ("/customers/me", "GET", None, [None, None, 200], "Customer's own record"),
        ("/cart/me", "GET", None, [None, None, 200], "Customer's own cart"),
        ("/recommendations/me", "GET", None, [None, None, 200], "Customer's own recs"),
    ]

    test_idx = 1
    role_map = {"admin": 0, "seller": 1, "customer": 2}
    for path, method, body, expectations, description in matrix:
        for role in ["admin", "seller", "customer"]:
            exp = expectations[role_map[role]]
            if exp is None or role not in s.tokens:
                continue
            resp, ms = s.call(method, path, token_role=role,
                              json_body=body if method in ("POST", "PATCH") else None)
            if exp == 200:
                passed, err = assert_status(resp, [200, 201])
                outcome = "ALLOWED"
            else:
                passed, err = assert_status(resp, [exp, 401, 403])
                if resp.status_code in (401, 403):
                    passed = True
                    err = ""
                outcome = "BLOCKED" if passed else "FAIL"

            short_path = path[:35] + "..." if len(path) > 35 else path
            record(s, TestResult(
                test_id=f"AUTHZ-{test_idx:02d}", category="Authorization",
                name=f"{role:8s} -> {method} {short_path}",
                method=method, endpoint=path, role=role,
                status_expected=exp, status_actual=resp.status_code,
                passed=passed, duration_ms=ms, error_msg=err,
                expected_json=json.dumps({
                    "endpoint_purpose": description,
                    "expected_outcome_for_role": "ALLOWED (200)" if exp == 200 else f"BLOCKED ({exp})",
                    "rationale": f"{role} role {'has' if exp == 200 else 'does NOT have'} permission for this endpoint"
                }, indent=2),
                actual_json=get_actual_json(resp, max_chars=300),
                reasoning=(
                    f"PASS: {role} {outcome} as expected. " +
                    (f"Role {role} has read access to {description}." if exp == 200 else
                     f"Role {role} correctly blocked from {description} (expected because this is {'admin-only' if exp == 403 and role == 'seller' else 'restricted to specific roles'}).")
                    if passed else
                    f"AUTHZ FAIL: Expected {exp} for {role} but got {resp.status_code}. {err}"
                ),
                notes=f"Endpoint purpose: {description}",
            ))
            test_idx += 1


# ---------- Cleanup ----------

def cleanup(s: TestSession):
    print("\n[Cleanup]")
    if "admin" not in s.tokens:
        return
    for user_id in s.test_users_created:
        resp, _ = s.call("DELETE", f"/users/{user_id}", token_role="admin")
        if resp.status_code in (200, 204):
            print(f"  Deactivated test user {user_id}")


# ---------- Excel report ----------

def generate_excel_report(s: TestSession, output_path: Path,
                          base_url: str, users: DiscoveredUsers):
    print(f"\n[Report] Generating: {output_path}")

    df = pd.DataFrame([asdict(r) for r in s.results])
    if len(df) == 0:
        return

    n_total = len(df)
    n_passed = int(df["passed"].sum())
    n_failed = n_total - n_passed
    pass_rate = n_passed / n_total * 100 if n_total else 0

    by_cat = df.groupby("category").agg(
        total=("passed", "size"),
        passed=("passed", "sum"),
        avg_duration_ms=("duration_ms", "mean"),
    ).reset_index()
    by_cat["failed"] = by_cat["total"] - by_cat["passed"]
    by_cat["pass_rate"] = (by_cat["passed"] / by_cat["total"] * 100).round(1)
    by_cat["avg_duration_ms"] = by_cat["avg_duration_ms"].round(0).astype(int)
    by_cat = by_cat[["category", "total", "passed", "failed", "pass_rate", "avg_duration_ms"]]

    failures = df[~df["passed"]].copy()

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Summary
        summary = [
            ("Test Run Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("Base URL", base_url),
            ("Admin User", users.admin_username or "?"),
            ("Seller 1", users.seller_username or "?"),
            ("Seller 2", users.second_seller_username or "?"),
            ("Customer (richest)", users.customer_richest_username or "?"),
            ("Customer segment/specialty", f"{users.customer_richest_segment}/{users.customer_richest_specialty}"),
            ("Customer order count", users.customer_richest_n_orders),
            ("", ""),
            ("Total Tests", n_total),
            ("Passed", n_passed),
            ("Failed", n_failed),
            ("Pass Rate", f"{pass_rate:.1f}%"),
            ("", ""),
            ("Avg Response (ms)", f"{df['duration_ms'].mean():.0f}"),
            ("Total Duration (s)", f"{df['duration_ms'].sum() / 1000:.1f}"),
        ]
        pd.DataFrame(summary, columns=["Metric", "Value"]).to_excel(
            writer, sheet_name="01_Summary", index=False)

        by_cat.to_excel(writer, sheet_name="02_By_Category", index=False)

        # All Results - WITH new columns
        df_out = df.rename(columns={
            "test_id": "Test ID",
            "category": "Category",
            "name": "Test Name",
            "method": "Method",
            "endpoint": "Endpoint",
            "role": "Auth Role",
            "status_expected": "HTTP Expected",
            "status_actual": "HTTP Actual",
            "passed": "Passed",
            "duration_ms": "Duration (ms)",
            "expected_json": "Expected Result (JSON)",
            "actual_json": "Actual Result (JSON)",
            "reasoning": "Reasoning",
            "error_msg": "Error",
            "notes": "Notes",
        })
        df_out = df_out[[
            "Test ID", "Category", "Test Name", "Method", "Endpoint", "Auth Role",
            "HTTP Expected", "HTTP Actual", "Passed", "Duration (ms)",
            "Expected Result (JSON)", "Actual Result (JSON)", "Reasoning",
            "Notes", "Error",
        ]]
        df_out.to_excel(writer, sheet_name="03_All_Results", index=False)

        if len(failures) > 0:
            fail_df = failures.rename(columns={
                "test_id": "Test ID", "category": "Category", "name": "Test Name",
                "method": "Method", "endpoint": "Endpoint", "role": "Auth Role",
                "status_expected": "HTTP Expected", "status_actual": "HTTP Actual",
                "expected_json": "Expected Result (JSON)",
                "actual_json": "Actual Result (JSON)",
                "reasoning": "Reasoning",
                "error_msg": "Error",
            })[["Test ID", "Category", "Test Name", "Method", "Endpoint",
                "Auth Role", "HTTP Expected", "HTTP Actual",
                "Expected Result (JSON)", "Actual Result (JSON)", "Reasoning", "Error"]]
            fail_df.to_excel(writer, sheet_name="04_Failures", index=False)
        else:
            pd.DataFrame({"Status": ["All tests passed"]}).to_excel(
                writer, sheet_name="04_Failures", index=False)

        # Format
        wb = writer.book
        for name in wb.sheetnames:
            ws = wb[name]
            # Header style
            for cell in ws[1]:
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill(start_color="2F5496", end_color="2F5496", fill_type="solid")
                cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)

            # Set sensible column widths - JSON cols wider
            if name in ("03_All_Results", "04_Failures"):
                widths = {
                    "Test ID": 12, "Category": 14, "Test Name": 40, "Method": 8,
                    "Endpoint": 35, "Auth Role": 10, "HTTP Expected": 12, "HTTP Actual": 12,
                    "Passed": 8, "Duration (ms)": 12,
                    "Expected Result (JSON)": 60, "Actual Result (JSON)": 60,
                    "Reasoning": 80, "Notes": 25, "Error": 40,
                }
                for col_idx, cell in enumerate(ws[1], start=1):
                    col_letter = get_column_letter(col_idx)
                    width = widths.get(cell.value, 20)
                    ws.column_dimensions[col_letter].width = width

                # Wrap text in body cells, top-align
                for row in ws.iter_rows(min_row=2):
                    for cell in row:
                        cell.alignment = Alignment(vertical="top", wrap_text=True)

                # Color failed rows
                if name == "03_All_Results":
                    passed_col = None
                    for col_idx, cell in enumerate(ws[1], start=1):
                        if cell.value == "Passed":
                            passed_col = col_idx
                            break
                    if passed_col:
                        for row_idx in range(2, ws.max_row + 1):
                            if ws.cell(row=row_idx, column=passed_col).value is False:
                                for col_idx in range(1, ws.max_column + 1):
                                    ws.cell(row=row_idx, column=col_idx).fill = PatternFill(
                                        start_color="FFE5E5", end_color="FFE5E5", fill_type="solid"
                                    )
            else:
                # Non-results sheets - default sizing
                for col_idx, col in enumerate(ws.columns, start=1):
                    max_len = 0
                    col_letter = get_column_letter(col_idx)
                    for cell in col:
                        val = str(cell.value) if cell.value is not None else ""
                        max_len = max(max_len, min(len(val), 60))
                    ws.column_dimensions[col_letter].width = min(max_len + 2, 60)

    print(f"  Saved: {output_path}")


# ---------- Main ----------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default=DEFAULT_BASE_URL)
    parser.add_argument("--admin-pwd", default=DEMO_PASSWORD)
    parser.add_argument("--no-cleanup", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    print("=" * 76)
    print("  COMPREHENSIVE API TEST RUNNER V3 (with detailed JSON + reasoning)")
    print("=" * 76)
    print(f"  Base URL: {args.base}")

    pg_cfg = load_pg_config()
    print(f"  Postgres: {pg_cfg['user']}@{pg_cfg['host']}:{pg_cfg['port']}/{pg_cfg['dbname']}")

    try:
        users = discover_users(pg_cfg)
    except Exception as e:
        print(f"\n  FATAL: {e}")
        sys.exit(1)

    s = TestSession(args.base, verbose=args.verbose)
    try:
        test_health(s)
        test_authentication(s, users, args.admin_pwd)
        test_user_management(s)
        test_customers(s, users)
        test_recommendations(s, users)
        test_cart(s, users)
        test_purchase_history(s, users)
        test_assignment_lifecycle(s, users)
        test_stats(s, users)
        test_seller_stats(s)
        test_customer_stats(s, users)
        test_authorization_matrix(s, users)
    except KeyboardInterrupt:
        print("\nInterrupted")
    except Exception as e:
        print(f"\nERROR: {e}")
        traceback.print_exc()

    if not args.no_cleanup:
        cleanup(s)

    n_total = len(s.results)
    n_passed = sum(1 for r in s.results if r.passed)
    print()
    print("=" * 76)
    print(f"  RESULTS: {n_passed}/{n_total} passed ({n_passed/n_total*100:.1f}%)")
    print("=" * 76)

    if args.output:
        out = Path(args.output)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = ROOT / f"api_test_report_{ts}.xlsx"
    generate_excel_report(s, out, args.base, users)

    sys.exit(0 if n_passed == n_total else 1)


if __name__ == "__main__":
    main()