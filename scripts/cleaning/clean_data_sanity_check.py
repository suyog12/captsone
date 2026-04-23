from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd


# Path configuration

ROOT       = Path(__file__).resolve().parent.parent.parent
DATA_RAW   = ROOT / "data_raw"
DATA_CLEAN = ROOT / "data_clean"

OUT_CUSTOMER = DATA_CLEAN / "customer"
OUT_PRODUCT  = DATA_CLEAN / "product"
OUT_SALES    = DATA_CLEAN / "sales"
OUT_FEATURES = DATA_CLEAN / "features"
OUT_SERVING  = DATA_CLEAN / "serving"
OUT_AUDIT    = DATA_CLEAN / "audit"

CUSTOMER_FILE   = OUT_CUSTOMER / "customers_clean.parquet"
PRODUCT_FILE    = OUT_PRODUCT  / "products_clean.parquet"
TXN_FY2425_FILE = OUT_SALES    / "transactions_clean_FY2425.parquet"
TXN_FY2526_FILE = OUT_SALES    / "transactions_clean_FY2526.parquet"
RET_FY2425_FILE = OUT_SALES    / "returns_clean_FY2425.parquet"
RET_FY2526_FILE = OUT_SALES    / "returns_clean_FY2526.parquet"
SPECIALTY_FILE  = OUT_FEATURES / "specialty_tiers.parquet"
RFM_FILE        = OUT_FEATURES / "customer_rfm.parquet"
FEATURE_FILE    = OUT_FEATURES / "customer_features.parquet"
MERGED_FILE     = OUT_SERVING  / "merged_dataset.parquet"

SUMMARY_XLSX = OUT_AUDIT / "10_clean_data_sanity_check.xlsx"
SUMMARY_CSV  = OUT_AUDIT / "10_clean_data_sanity_summary.csv"


# Pipeline configuration constants

EXCLUDED_ORDER_NUMS  = frozenset({61408700, 61737401, 35996955})
MAX_ORDER_LINE_VALUE = 10_000_000.0

EXPECTED_SPECIALTIES = 272
EXPECTED_TIER1_MIN   = 60
MIN_CHURN_RATE       = 5.0
MAX_CHURN_RATE       = 40.0

# Step 6 RFM regression guards
MAX_MEDIAN_RECENCY_DAYS   = 180
MAX_ABSOLUTE_RECENCY_DAYS = 800
MIN_NEW_FY2526_CUSTOMERS  = 1_000

# Feature matrix column count.
# 44 base + 4 behavioural + 1 supplier_profile = 49
EXPECTED_FEATURE_COLS = 49

# Supplier profile validation guards.
# Validated against real data (Apr 2026):
#   medline_only      ~0.1% of customers
#   mckesson_primary  ~16%  of customers
#   mixed             ~84%  of customers
VALID_SUPPLIER_PROFILES  = {"medline_only", "mckesson_primary", "mixed"}
MIN_MEDLINE_ONLY_PCT     = 0.0005
MAX_MEDLINE_ONLY_PCT     = 0.10
MIN_MCKESSON_PRIMARY_PCT = 0.05
MAX_MCKESSON_PRIMARY_PCT = 0.50
MIN_MIXED_PCT            = 0.50


# DuckDB helpers

def _pq(path: Path) -> str:
    return "'" + path.as_posix() + "'"


def _glob(folder: Path) -> str:
    return "'" + folder.as_posix() + "/*.parquet'"


def _con(memory_gb: int = 4, threads: int = 1) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit = '{memory_gb}GB'")
    con.execute(f"SET threads = {threads}")
    con.execute("SET preserve_insertion_order = false")
    return con


def _file_size_mb(path: Path) -> float:
    return round(path.stat().st_size / (1024 * 1024), 2) if path.exists() else 0.0


def _discover_raw_fact_folders() -> dict[str, Path]:
    fact_folders: dict[str, Path] = {}
    if not DATA_RAW.exists():
        return fact_folders
    for folder in sorted(DATA_RAW.iterdir()):
        if not folder.is_dir():
            continue
        n = folder.name.lower()
        if ("fct" in n or "sales" in n) and "dim" not in n:
            fy = "FY2425" if "2425" in n else "FY2526"
            fact_folders[fy] = folder
    return fact_folders


# Result accumulator

def _status(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def _row(rows: list[dict], category: str, check_name: str, status: str,
         metric_value, detail: str) -> None:
    rows.append({
        "category":     category,
        "check_name":   check_name,
        "status":       status,
        "metric_value": metric_value,
        "detail":       detail,
        "checked_at":   dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })


# Check 1: Required files

def check_required_files(rows: list[dict]) -> None:
    required = {
        CUSTOMER_FILE:   "critical",
        PRODUCT_FILE:    "critical",
        TXN_FY2425_FILE: "critical",
        TXN_FY2526_FILE: "critical",
        RET_FY2425_FILE: "warning",
        RET_FY2526_FILE: "warning",
        SPECIALTY_FILE:  "critical",
        RFM_FILE:        "critical",
        FEATURE_FILE:    "critical",
        MERGED_FILE:     "critical",
        OUT_AUDIT / "01_dropped_columns.xlsx":         "warning",
        OUT_AUDIT / "02_null_zip_customers.xlsx":      "warning",
        OUT_AUDIT / "03_excluded_outlier_orders.xlsx": "warning",
        OUT_AUDIT / "04_duplicate_rows.xlsx":          "warning",
        OUT_AUDIT / "05_returns_summary.xlsx":         "warning",
        OUT_AUDIT / "06_tier3_specialty_mapping.xlsx": "warning",
        OUT_AUDIT / "07_state_grouping.xlsx":          "warning",
        OUT_AUDIT / "08_churn_labels.xlsx":            "warning",
        OUT_AUDIT / "08b_supplier_profile.xlsx":       "warning",
        OUT_AUDIT / "09_cleaning_run_log.xlsx":        "warning",
        DATA_CLEAN / "cleaning_summary_report.xlsx":   "warning",
    }
    for path, severity in required.items():
        exists = path.exists()
        status = "PASS" if exists else ("FAIL" if severity == "critical" else "WARN")
        _row(rows, "files", f"exists::{path.name}", status,
             _file_size_mb(path) if exists else None,
             str(path.relative_to(ROOT)))


# Check 2: Row counts (INFO)

def check_row_counts(rows: list[dict]) -> None:
    con = _con(memory_gb=3, threads=1)
    files = {
        "customers_clean":           CUSTOMER_FILE,
        "products_clean":            PRODUCT_FILE,
        "transactions_clean_FY2425": TXN_FY2425_FILE,
        "transactions_clean_FY2526": TXN_FY2526_FILE,
        "returns_clean_FY2425":      RET_FY2425_FILE,
        "returns_clean_FY2526":      RET_FY2526_FILE,
        "specialty_tiers":           SPECIALTY_FILE,
        "customer_rfm":              RFM_FILE,
        "customer_features":         FEATURE_FILE,
        "merged_dataset":            MERGED_FILE,
    }
    for name, path in files.items():
        if not path.exists():
            continue
        cnt = con.execute(
            f"SELECT COUNT(*) FROM read_parquet({_pq(path)})"
        ).fetchone()[0]
        _row(rows, "counts", f"row_count::{name}", "INFO", cnt,
             str(path.relative_to(ROOT)))
    con.close()


# Check 3: Customer dimension

def check_customers(rows: list[dict]) -> None:
    if not CUSTOMER_FILE.exists():
        return
    con = _con(memory_gb=3, threads=1)

    dup = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT DIM_CUST_CURR_ID
            FROM   read_parquet({_pq(CUSTOMER_FILE)})
            GROUP  BY DIM_CUST_CURR_ID
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    _row(rows, "customers", "duplicate_customer_ids", _status(dup == 0),
         dup, "DIM_CUST_CURR_ID must be unique")

    invalid_zip = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(CUSTOMER_FILE)})
        WHERE ZIP IS NOT NULL
          AND NOT regexp_matches(CAST(ZIP AS VARCHAR), '^[0-9]{{5}}([0-9]{{4}})?$')
    """).fetchone()[0]
    _row(rows, "customers", "invalid_zip_format", _status(invalid_zip == 0),
         invalid_zip, "ZIP must be 5 or 9 digits when present")

    invalid_actv = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(CUSTOMER_FILE)})
        WHERE ACTV_FLG IS NOT NULL AND ACTV_FLG NOT IN ('Y', 'N')
    """).fetchone()[0]
    _row(rows, "customers", "invalid_actv_flg", _status(invalid_actv == 0),
         invalid_actv, "ACTV_FLG must be Y / N / NULL")

    bad_type = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(CUSTOMER_FILE)})
        WHERE CUST_TYPE_CD IS NOT NULL AND CUST_TYPE_CD NOT IN ('S', 'X', 'B')
    """).fetchone()[0]
    _row(rows, "customers", "invalid_cust_type_cd", _status(bad_type == 0),
         bad_type, "CUST_TYPE_CD must be S / X / B / NULL")

    bad_state = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(CUSTOMER_FILE)})
        WHERE STATE IS NOT NULL
          AND NOT regexp_matches(CAST(STATE AS VARCHAR), '^[A-Z]{{2}}$')
    """).fetchone()[0]
    _row(rows, "customers", "invalid_state_format", _status(bad_state == 0),
         bad_state, "STATE must be 2 uppercase letters when present")

    con.close()


# Check 4: Product dimension

def check_products(rows: list[dict]) -> None:
    if not PRODUCT_FILE.exists():
        return
    con = _con(memory_gb=3, threads=1)

    dup = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT DIM_ITEM_E1_CURR_ID
            FROM   read_parquet({_pq(PRODUCT_FILE)})
            GROUP  BY DIM_ITEM_E1_CURR_ID
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    _row(rows, "products", "duplicate_product_ids", _status(dup == 0),
         dup, "DIM_ITEM_E1_CURR_ID must be unique")

    for col in ["is_private_brand", "is_discontinued", "is_generic"]:
        bad = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet({_pq(PRODUCT_FILE)})
            WHERE {col} IS NOT NULL AND {col} NOT IN (0, 1)
        """).fetchone()[0]
        _row(rows, "products", f"binary_flag::{col}", _status(bad == 0),
             bad, f"{col} must be 0 / 1 / NULL")

    total = con.execute(
        f"SELECT COUNT(*) FROM read_parquet({_pq(PRODUCT_FILE)})"
    ).fetchone()[0]
    pvt = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(PRODUCT_FILE)})
        WHERE is_private_brand = 1
    """).fetchone()[0]
    pvt_pct = round(pvt / max(total, 1) * 100, 2)
    _row(rows, "products", "private_brand_pct_plausible",
         _status(0.1 <= pvt_pct <= 80.0), f"{pvt_pct}%",
         f"{pvt_pct}% private brand — expected between 0.1% and 80%")

    con.close()


# Check 5 and 6: Sales transactions and returns

def _check_sales_file(rows: list[dict], label: str, path: Path,
                      is_return: bool) -> None:
    if not path.exists():
        return
    con = _con(memory_gb=4, threads=1)

    dup = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT ORDR_NUM, ORDR_LINE_NUM
            FROM   read_parquet({_pq(path)})
            GROUP  BY ORDR_NUM, ORDR_LINE_NUM
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    _row(rows, "sales", f"duplicate_keys::{label}", _status(dup == 0),
         dup, "ORDR_NUM + ORDR_LINE_NUM must be unique after cleaning")

    if is_return:
        bad_qty = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet({_pq(path)}) WHERE ORDR_QTY >= 0
        """).fetchone()[0]
        _row(rows, "sales", f"return_qty_direction::{label}", _status(bad_qty == 0),
             bad_qty, "Return rows must have ORDR_QTY < 0")
    else:
        bad_qty = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet({_pq(path)}) WHERE ORDR_QTY < 0
        """).fetchone()[0]
        _row(rows, "sales", f"transaction_qty_direction::{label}", _status(bad_qty == 0),
             bad_qty, "Transaction rows must have ORDR_QTY >= 0")

    blocked_sql = ", ".join(str(x) for x in sorted(EXCLUDED_ORDER_NUMS))
    blocked_left = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(path)})
        WHERE ORDR_NUM IN ({blocked_sql})
    """).fetchone()[0]
    _row(rows, "sales", f"blocked_orders_removed::{label}", _status(blocked_left == 0),
         blocked_left, "Blocked order numbers must not appear in cleaned files")

    over_cap = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(path)})
        WHERE UNIT_SLS_AMT IS NOT NULL AND UNIT_SLS_AMT > {MAX_ORDER_LINE_VALUE}
    """).fetchone()[0]
    _row(rows, "sales", f"revenue_cap_respected::{label}", _status(over_cap == 0),
         over_cap, f"No order line should exceed ${MAX_ORDER_LINE_VALUE:,.0f} cap")

    bad_date = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(path)})
        WHERE DIM_ORDR_DT_ID IS NULL
           OR order_year  IS NULL
           OR order_month IS NULL
           OR order_day   IS NULL
           OR order_month NOT BETWEEN 1 AND 12
           OR order_day   NOT BETWEEN 1 AND 31
    """).fetchone()[0]
    _row(rows, "sales", f"derived_date_fields::{label}", _status(bad_date == 0),
         bad_date, "order_year/month/day must be populated and valid")

    if not is_return:
        fy = "FY2425" if "FY2425" in path.name else "FY2526"
        wrong_fy = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet({_pq(path)})
            WHERE fiscal_year != '{fy}'
        """).fetchone()[0]
        _row(rows, "sales", f"fiscal_year_tag::{label}", _status(wrong_fy == 0),
             wrong_fy, f"All rows must carry fiscal_year = '{fy}'")

    con.close()


def check_sales(rows: list[dict]) -> None:
    _check_sales_file(rows, "transactions_clean_FY2425", TXN_FY2425_FILE, is_return=False)
    _check_sales_file(rows, "transactions_clean_FY2526", TXN_FY2526_FILE, is_return=False)
    _check_sales_file(rows, "returns_clean_FY2425",      RET_FY2425_FILE, is_return=True)
    _check_sales_file(rows, "returns_clean_FY2526",      RET_FY2526_FILE, is_return=True)


# Check 7: Referential integrity

def check_referential_integrity(rows: list[dict]) -> None:
    if not all(p.exists() for p in [TXN_FY2425_FILE, TXN_FY2526_FILE,
                                     CUSTOMER_FILE, PRODUCT_FILE]):
        return
    con = _con(memory_gb=5, threads=1)
    txn_sql = f"[{_pq(TXN_FY2425_FILE)}, {_pq(TXN_FY2526_FILE)}]"

    missing_custs = con.execute(f"""
        SELECT COUNT(*)
        FROM   read_parquet({txn_sql}) s
        LEFT JOIN read_parquet({_pq(CUSTOMER_FILE)}) c
          ON s.DIM_CUST_CURR_ID = c.DIM_CUST_CURR_ID
        WHERE  c.DIM_CUST_CURR_ID IS NULL
    """).fetchone()[0]
    _row(rows, "integrity", "sales_customer_fk", _status(missing_custs == 0),
         missing_custs, "All sales customer IDs must exist in customers_clean")

    missing_prods = con.execute(f"""
        SELECT COUNT(*)
        FROM   read_parquet({txn_sql}) s
        LEFT JOIN read_parquet({_pq(PRODUCT_FILE)}) p
          ON s.DIM_ITEM_E1_CURR_ID = p.DIM_ITEM_E1_CURR_ID
        WHERE  p.DIM_ITEM_E1_CURR_ID IS NULL
    """).fetchone()[0]
    prod_fk_status = "PASS" if missing_prods == 0 else "WARN"
    _row(rows, "integrity", "sales_product_fk", prod_fk_status,
         missing_prods,
         f"{missing_prods:,} transaction product IDs not in products_clean — "
         "expected for discontinued/seasonal items; serving join uses LEFT JOIN")

    con.close()


# Check 8: Specialty tiers

def check_specialty_tiers(rows: list[dict]) -> None:
    if not SPECIALTY_FILE.exists():
        return
    con = _con(memory_gb=2, threads=1)

    total = con.execute(
        f"SELECT COUNT(*) FROM read_parquet({_pq(SPECIALTY_FILE)})"
    ).fetchone()[0]
    _row(rows, "specialties", "total_specialty_count",
         _status(total == EXPECTED_SPECIALTIES), total,
         f"Expected exactly {EXPECTED_SPECIALTIES} specialties, got {total}")

    bad_tier = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(SPECIALTY_FILE)})
        WHERE specialty_tier NOT IN (1, 2, 3)
    """).fetchone()[0]
    _row(rows, "specialties", "valid_tier_values", _status(bad_tier == 0),
         bad_tier, "specialty_tier must be 1, 2, or 3")

    t1 = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(SPECIALTY_FILE)})
        WHERE specialty_tier = 1
    """).fetchone()[0]
    _row(rows, "specialties", "tier1_minimum_count",
         _status(t1 >= EXPECTED_TIER1_MIN), t1,
         f"Expected >= {EXPECTED_TIER1_MIN} Tier 1 specialties, got {t1}")

    t3_total = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(SPECIALTY_FILE)})
        WHERE specialty_tier = 3
    """).fetchone()[0]
    t3_mapped = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(SPECIALTY_FILE)})
        WHERE specialty_tier = 3
          AND tier3_fallback_spclty_cd IS NOT NULL
    """).fetchone()[0]
    t3_pct = round(t3_mapped / max(t3_total, 1) * 100, 1)
    _row(rows, "specialties", "tier3_fallback_coverage",
         _status(t3_pct >= 50.0), f"{t3_pct}%",
         f"{t3_mapped}/{t3_total} Tier 3 specialties have a Tier 1 fallback")

    null_rev = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(SPECIALTY_FILE)})
        WHERE specialty_tier = 1 AND total_revenue IS NULL
    """).fetchone()[0]
    _row(rows, "specialties", "tier1_no_null_revenue", _status(null_rev == 0),
         null_rev, "All Tier 1 specialties must have non-null total_revenue")

    con.close()


# Check 9: RFM scores and churn labels

def check_rfm(rows: list[dict]) -> None:
    if not RFM_FILE.exists():
        return
    con = _con(memory_gb=3, threads=1)

    for score in ["R_score", "F_score", "M_score"]:
        bad = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet({_pq(RFM_FILE)})
            WHERE {score} IS NOT NULL AND {score} NOT BETWEEN 1 AND 5
        """).fetchone()[0]
        _row(rows, "rfm", f"{score}_range", _status(bad == 0),
             bad, f"{score} must be between 1 and 5")

    bad_churn = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(RFM_FILE)})
        WHERE churn_label NOT IN (-1, 0, 1)
    """).fetchone()[0]
    _row(rows, "rfm", "valid_churn_labels", _status(bad_churn == 0),
         bad_churn, "churn_label must be one of {-1, 0, 1}")

    trainable = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(RFM_FILE)})
        WHERE churn_label IN (0, 1)
    """).fetchone()[0]
    churned = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(RFM_FILE)})
        WHERE churn_label = 1
    """).fetchone()[0]
    if trainable > 0:
        churn_pct = round(churned / trainable * 100, 2)
        ok = MIN_CHURN_RATE <= churn_pct <= MAX_CHURN_RATE
        _row(rows, "rfm", "churn_rate_plausible", _status(ok), f"{churn_pct}%",
             f"Churn rate {churn_pct}% — expected between {MIN_CHURN_RATE}% and {MAX_CHURN_RATE}%")

    neg_rec = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(RFM_FILE)})
        WHERE recency_days < 0
    """).fetchone()[0]
    _row(rows, "rfm", "no_negative_recency", _status(neg_rec == 0),
         neg_rec, "recency_days must be >= 0")

    median_rec = con.execute(f"""
        SELECT MEDIAN(recency_days) FROM read_parquet({_pq(RFM_FILE)})
        WHERE recency_days IS NOT NULL
    """).fetchone()[0]
    if median_rec is not None:
        ok = median_rec <= MAX_MEDIAN_RECENCY_DAYS
        _row(rows, "rfm", "median_recency_plausible",
             _status(ok), round(float(median_rec), 1),
             f"Median recency {median_rec:.1f} days — expected <= {MAX_MEDIAN_RECENCY_DAYS}")

    max_rec = con.execute(f"""
        SELECT MAX(recency_days) FROM read_parquet({_pq(RFM_FILE)})
    """).fetchone()[0]
    if max_rec is not None:
        ok = max_rec <= MAX_ABSOLUTE_RECENCY_DAYS
        _row(rows, "rfm", "max_recency_within_bounds",
             _status(ok), int(max_rec),
             f"Max recency {max_rec} days — expected <= {MAX_ABSOLUTE_RECENCY_DAYS}")

    new_fy26 = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(RFM_FILE)})
        WHERE churn_label = -1
    """).fetchone()[0]
    ok = new_fy26 >= MIN_NEW_FY2526_CUSTOMERS
    _row(rows, "rfm", "fy2526_new_cohort_present",
         _status(ok), new_fy26,
         f"{new_fy26:,} new-in-FY2526 customers — expected >= {MIN_NEW_FY2526_CUSTOMERS:,}")

    con.close()


# Check 10: Feature matrix

def check_features(rows: list[dict]) -> None:
    if not FEATURE_FILE.exists() or not RFM_FILE.exists():
        return
    con = _con(memory_gb=3, threads=1)

    required_cols = [
        "DIM_CUST_CURR_ID", "recency_days", "frequency", "monetary",
        "avg_order_gap_days", "R_score", "F_score", "M_score",
        "RFM_score", "churn_label",
        "avg_revenue_per_order",
        "n_categories_bought",
        "category_hhi",
        "cycle_regularity",
        "supplier_profile",
    ]
    feat_cols = set(
        r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet({_pq(FEATURE_FILE)})"
        ).fetchall()
    )
    missing = [c for c in required_cols if c not in feat_cols]
    _row(rows, "features", "required_feature_columns", _status(len(missing) == 0),
         len(missing), "Missing: " + (", ".join(missing) if missing else "None"))

    bad_churn = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(FEATURE_FILE)})
        WHERE churn_label NOT IN (-1, 0, 1)
    """).fetchone()[0]
    _row(rows, "features", "valid_churn_labels", _status(bad_churn == 0),
         bad_churn, "churn_label must be one of {-1, 0, 1}")

    rfm_rows  = con.execute(f"SELECT COUNT(*) FROM read_parquet({_pq(RFM_FILE)})").fetchone()[0]
    feat_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet({_pq(FEATURE_FILE)})").fetchone()[0]
    _row(rows, "features", "rfm_feature_row_match",
         _status(rfm_rows == feat_rows), f"{rfm_rows} vs {feat_rows}",
         "customer_rfm and customer_features must have the same row count")

    df_sample = con.execute(
        f"SELECT * FROM read_parquet({_pq(FEATURE_FILE)}) LIMIT 10000"
    ).df()
    null_cols = [c for c in df_sample.columns if df_sample[c].isna().all()]
    _row(rows, "features", "no_all_null_columns", _status(len(null_cols) == 0),
         len(null_cols),
         "All-null columns: " + (", ".join(null_cols) if null_cols else "None"))

    tier3_present = "tier3_fallback_spclty_cd" in feat_cols
    _row(rows, "features", "tier3_fallback_excluded",
         _status(not tier3_present),
         "present" if tier3_present else "absent",
         "tier3_fallback_spclty_cd must not be in customer_features — use specialty_tiers.parquet")

    _row(rows, "features", "feature_column_count",
         _status(len(feat_cols) == EXPECTED_FEATURE_COLS), len(feat_cols),
         f"Expected {EXPECTED_FEATURE_COLS} columns, got {len(feat_cols)}")

    spec_cols = [c for c in feat_cols if c.startswith("spec_")]
    _row(rows, "features", "specialty_indicator_columns_present",
         _status(len(spec_cols) >= 20), len(spec_cols),
         f"{len(spec_cols)} spec_ columns found — expected >= 20")

    con.close()


# Check 11: Supplier profile

def check_supplier_profile(rows: list[dict]) -> None:
    if not FEATURE_FILE.exists():
        return
    con = _con(memory_gb=3, threads=1)

    feat_cols = set(
        r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet({_pq(FEATURE_FILE)})"
        ).fetchall()
    )
    if "supplier_profile" not in feat_cols:
        _row(rows, "supplier_profile", "column_exists", "FAIL", 0,
             "supplier_profile column missing from customer_features")
        con.close()
        return

    _row(rows, "supplier_profile", "column_exists", "PASS", 1,
         "supplier_profile column present in customer_features")

    null_count = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(FEATURE_FILE)})
        WHERE supplier_profile IS NULL
    """).fetchone()[0]
    _row(rows, "supplier_profile", "no_null_values",
         _status(null_count == 0), null_count,
         "supplier_profile must have no null values — unmatched customers default to 'mixed'")

    invalid_vals = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(FEATURE_FILE)})
        WHERE supplier_profile NOT IN (
            '{"', '".join(sorted(VALID_SUPPLIER_PROFILES))}'
        )
    """).fetchone()[0]
    _row(rows, "supplier_profile", "valid_values_only",
         _status(invalid_vals == 0), invalid_vals,
         f"supplier_profile must be one of: {sorted(VALID_SUPPLIER_PROFILES)}")

    total = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({_pq(FEATURE_FILE)})
    """).fetchone()[0]

    counts = con.execute(f"""
        SELECT supplier_profile, COUNT(*) AS n
        FROM read_parquet({_pq(FEATURE_FILE)})
        GROUP BY supplier_profile
    """).df()

    medline_n   = int(counts.loc[counts["supplier_profile"] == "medline_only", "n"].sum())
    mckesson_n  = int(counts.loc[counts["supplier_profile"] == "mckesson_primary", "n"].sum())
    mixed_n     = int(counts.loc[counts["supplier_profile"] == "mixed", "n"].sum())

    medline_pct  = medline_n / max(total, 1)
    mckesson_pct = mckesson_n / max(total, 1)
    mixed_pct    = mixed_n / max(total, 1)

    ok_med = MIN_MEDLINE_ONLY_PCT <= medline_pct <= MAX_MEDLINE_ONLY_PCT
    _row(rows, "supplier_profile", "medline_only_distribution",
         _status(ok_med), f"{medline_n:,} ({medline_pct*100:.2f}%)",
         f"medline_only must be between {MIN_MEDLINE_ONLY_PCT*100}% and "
         f"{MAX_MEDLINE_ONLY_PCT*100}% of customers")

    ok_mck = MIN_MCKESSON_PRIMARY_PCT <= mckesson_pct <= MAX_MCKESSON_PRIMARY_PCT
    _row(rows, "supplier_profile", "mckesson_primary_distribution",
         _status(ok_mck), f"{mckesson_n:,} ({mckesson_pct*100:.2f}%)",
         f"mckesson_primary must be between {MIN_MCKESSON_PRIMARY_PCT*100}% and "
         f"{MAX_MCKESSON_PRIMARY_PCT*100}% of customers")

    ok_mix = mixed_pct >= MIN_MIXED_PCT
    _row(rows, "supplier_profile", "mixed_is_majority",
         _status(ok_mix), f"{mixed_n:,} ({mixed_pct*100:.2f}%)",
         f"mixed must be >= {MIN_MIXED_PCT*100}% of customers")

    n_profiles = len(counts["supplier_profile"].unique())
    _row(rows, "supplier_profile", "all_profiles_present",
         _status(n_profiles == 3), n_profiles,
         f"Expected all 3 profiles present, got {n_profiles}")

    con.close()


# Check 12: Serving dataset

def check_serving(rows: list[dict]) -> None:
    if not all(p.exists() for p in [MERGED_FILE, TXN_FY2425_FILE, TXN_FY2526_FILE]):
        return
    con = _con(memory_gb=4, threads=1)

    merged_rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet({_pq(MERGED_FILE)})"
    ).fetchone()[0]
    txn_rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet([{_pq(TXN_FY2425_FILE)}, {_pq(TXN_FY2526_FILE)}])"
    ).fetchone()[0]
    _row(rows, "serving", "merged_row_count_matches_transactions",
         _status(merged_rows == txn_rows), f"{merged_rows} vs {txn_rows}",
         "Merged dataset must have the same row count as cleaned transactions combined")

    # Supplier columns must flow through so step 6b can compute supplier_profile
    desc = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({_pq(MERGED_FILE)}) LIMIT 0"
    ).df()
    merged_cols = set(desc["column_name"].tolist())

    has_suplr = "SUPLR_ROLLUP_DSC" in merged_cols
    has_pvt   = "is_private_brand" in merged_cols

    _row(rows, "serving", "supplier_rollup_in_merged",
         _status(has_suplr), int(has_suplr),
         "SUPLR_ROLLUP_DSC must flow into merged_dataset for supplier_profile computation")

    _row(rows, "serving", "is_private_brand_in_merged",
         _status(has_pvt), int(has_pvt),
         "is_private_brand must flow into merged_dataset for supplier_profile computation")

    con.close()


# Check 13: Dedup correctness (raw vs clean)

def check_raw_vs_clean_distinct_keys(rows: list[dict]) -> None:
    raw_fact_folders = _discover_raw_fact_folders()
    if not raw_fact_folders:
        _row(rows, "dedup_correctness", "raw_folders_found", "FAIL", 0,
             "Could not locate raw fact folders in data_raw/")
        return

    con = _con(memory_gb=6, threads=1)
    blocked_sql = ", ".join(str(x) for x in sorted(EXCLUDED_ORDER_NUMS))

    for fy, raw_folder in sorted(raw_fact_folders.items()):
        cleaned_path = TXN_FY2425_FILE if fy == "FY2425" else TXN_FY2526_FILE
        if not cleaned_path.exists():
            continue

        raw_distinct = con.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT ORDR_NUM, ORDR_LINE_NUM
                FROM read_parquet({_glob(raw_folder)})
                WHERE ORDR_NUM NOT IN ({blocked_sql})
                  AND (UNIT_SLS_AMT IS NULL OR UNIT_SLS_AMT <= {MAX_ORDER_LINE_VALUE})
                  AND ORDR_QTY >= 0
            )
        """).fetchone()[0]

        cleaned_rows = con.execute(
            f"SELECT COUNT(*) FROM read_parquet({_pq(cleaned_path)})"
        ).fetchone()[0]

        ratio = round(cleaned_rows / raw_distinct, 6) if raw_distinct else None
        ok = ratio is not None and ratio >= 0.9999

        _row(rows, "dedup_correctness",
             f"raw_distinct_vs_cleaned::{fy}", _status(ok), ratio,
             f"raw_distinct_filtered_keys={raw_distinct:,}  "
             f"cleaned_rows={cleaned_rows:,}  "
             f"ratio={ratio}  (>=0.9999 is acceptable)")

    con.close()


# Excel output

def write_outputs(results: pd.DataFrame) -> None:
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    OUT_AUDIT.mkdir(parents=True, exist_ok=True)
    results.to_csv(SUMMARY_CSV, index=False)

    status_fill = {
        "PASS": "D5F0DC",
        "FAIL": "FFCCCC",
        "WARN": "FFF2CC",
        "INFO": "DDEEFF",
    }
    thin   = Side(style="thin", color="CCCCCC")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    def _style(ws, df):
        for ci, col in enumerate(df.columns, 1):
            c = ws.cell(1, ci, col)
            c.font      = Font(name="Arial", bold=True, size=10, color="FFFFFF")
            c.fill      = PatternFill("solid", fgColor="1F4E79")
            c.alignment = Alignment(horizontal="center", vertical="center")
            c.border    = border
        for ri, row in enumerate(df.itertuples(index=False), 2):
            status = str(getattr(row, "status", ""))
            bg     = status_fill.get(status, "FFFFFF")
            for ci, val in enumerate(row, 1):
                c = ws.cell(ri, ci, val if pd.notna(val) else "")
                c.font      = Font(name="Arial", size=9)
                c.fill      = PatternFill("solid", fgColor=bg)
                c.alignment = Alignment(horizontal="left", vertical="center")
                c.border    = border
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        for col_cells in ws.columns:
            w = max((len(str(c.value)) if c.value is not None else 0) for c in col_cells)
            ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(max(w + 2, 10), 60)

    summary = (
        results.groupby(["category", "status"])
        .size().reset_index(name="count")
        .sort_values(["category", "status"])
    )
    failed = results[results["status"] == "FAIL"].copy()
    if failed.empty:
        failed = pd.DataFrame([{"message": "No failed checks"}])

    with pd.ExcelWriter(SUMMARY_XLSX, engine="openpyxl") as writer:
        results.to_excel(writer, sheet_name="all_checks",    index=False)
        summary.to_excel(writer, sheet_name="status_counts", index=False)
        failed.to_excel(writer, sheet_name="failed_checks",  index=False)

        wb = writer.book
        wb["all_checks"].sheet_properties.tabColor    = "1F4E79"
        wb["status_counts"].sheet_properties.tabColor = "375623"
        wb["failed_checks"].sheet_properties.tabColor = "C00000"

        _style(writer.sheets["all_checks"],    results)
        _style(writer.sheets["status_counts"], summary)
        _style(writer.sheets["failed_checks"], failed)


# Console summary

def print_console_summary(results: pd.DataFrame) -> int:
    print("\n" + "=" * 72)
    print("  CLEAN DATA SANITY CHECK")
    print("=" * 72)

    passes   = int((results["status"] == "PASS").sum())
    fails    = int((results["status"] == "FAIL").sum())
    warnings = int((results["status"] == "WARN").sum())
    infos    = int((results["status"] == "INFO").sum())
    total    = len(results)

    print(f"  Total : {total}")
    print(f"  PASS  : {passes}")
    print(f"  FAIL  : {fails}   (critical — fix before running models)")
    print(f"  WARN  : {warnings}   (non-critical — investigate but not blocking)")
    print(f"  INFO  : {infos}   (row counts — no pass/fail judgment)")

    # Per-category breakdown
    print()
    print("  Category breakdown:")
    cat_summary = (
        results.groupby(["category", "status"]).size().unstack(fill_value=0)
    )
    for col in ["PASS", "FAIL", "WARN", "INFO"]:
        if col not in cat_summary.columns:
            cat_summary[col] = 0
    cat_summary = cat_summary[["PASS", "FAIL", "WARN", "INFO"]]
    for category, row in cat_summary.iterrows():
        print(f"    {category:<22} "
              f"PASS={row['PASS']:<3} FAIL={row['FAIL']:<3} "
              f"WARN={row['WARN']:<3} INFO={row['INFO']:<3}")

    if fails > 0:
        print("\n  FAILED CHECKS:")
        for _, r in results[results["status"] == "FAIL"].iterrows():
            print(f"    [{r['category']}]  {r['check_name']}")
            print(f"        value  : {r['metric_value']}")
            print(f"        detail : {r['detail']}")

    if warnings > 0:
        print("\n  WARNINGS:")
        for _, r in results[results["status"] == "WARN"].iterrows():
            print(f"    [{r['category']}]  {r['check_name']}")
            print(f"        value  : {r['metric_value']}")
            print(f"        detail : {r['detail']}")

    # Supplier profile distribution summary
    sp_rows = results[
        (results["category"] == "supplier_profile") &
        (results["check_name"].isin([
            "medline_only_distribution",
            "mckesson_primary_distribution",
            "mixed_is_majority",
        ]))
    ]
    if len(sp_rows) > 0:
        print("\n  Supplier profile distribution:")
        for _, r in sp_rows.iterrows():
            label = (r["check_name"]
                     .replace("_distribution", "")
                     .replace("_is_majority", ""))
            print(f"    {label:<24} {r['metric_value']}   [{r['status']}]")

    if fails == 0 and warnings == 0:
        print("\n  All checks passed. Data is clean and ready for model training.")
    elif fails == 0:
        print(f"\n  No critical failures. {warnings} warning(s) to investigate.")
    else:
        print(f"\n  {fails} critical failure(s). Do not proceed to model training.")

    print()
    print(f"  CSV   : {SUMMARY_CSV.relative_to(ROOT)}")
    print(f"  Excel : {SUMMARY_XLSX.relative_to(ROOT)}")
    print("=" * 72)
    print()

    return fails


# Main

def main() -> None:
    rows: list[dict] = []

    check_required_files(rows)
    check_row_counts(rows)
    check_customers(rows)
    check_products(rows)
    check_sales(rows)
    check_referential_integrity(rows)
    check_specialty_tiers(rows)
    check_rfm(rows)
    check_features(rows)
    check_supplier_profile(rows)
    check_serving(rows)
    check_raw_vs_clean_distinct_keys(rows)

    results  = pd.DataFrame(rows)
    write_outputs(results)
    failures = print_console_summary(results)

    sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    main()