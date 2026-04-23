from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

import duckdb
import pandas as pd


# Paths

ROOT         = Path(__file__).resolve().parent.parent.parent
DATA_CLEAN   = ROOT / "data_clean"
PRECOMP_DIR  = DATA_CLEAN / "serving" / "precomputed"
FEAT_DIR     = DATA_CLEAN / "features"
AUDIT_DIR    = DATA_CLEAN / "audit"

RECS_FILE       = PRECOMP_DIR / "customer_recommendations.parquet"
RECS_SCHEMA     = PRECOMP_DIR / "customer_recommendations.schema.json"
SEG_FILE        = PRECOMP_DIR / "customer_segments.parquet"
SEQ_FILE        = PRECOMP_DIR / "segment_sequences.parquet"
ADOPTION_FILE   = PRECOMP_DIR / "product_adoption_rates.parquet"
FACTORS_FILE    = PRECOMP_DIR / "recommendation_factors.parquet"
FEAT_FILE       = FEAT_DIR    / "customer_features.parquet"

SUMMARY_XLSX = AUDIT_DIR / "11_recommendation_sanity_check.xlsx"
SUMMARY_CSV  = AUDIT_DIR / "11_recommendation_sanity_summary.csv"


# Expected values

EXPECTED_TOP_N           = 10
EXPECTED_SIGNALS         = {"peer_gap", "lapsed"}
EXPECTED_SUPPLIER_PROFILES = {"medline_only", "mckesson_primary", "mixed"}

# Tier-specific signal distribution thresholds at rank 1.
# With SEGMENT_WEIGHTS 3.5/1.0 for _high, 2.5/2.5 for _mid, 1.0/3.5 for _low,
# we expect this behavior at rank 1:
#   _high : peer_gap should dominate (>= 60%)
#   _low  : lapsed should dominate   (>= 60%)
#   _mid  : balanced (no assertion - both signals acceptable)
MIN_HIGH_PEER_GAP_PCT = 60.0
MIN_LOW_LAPSED_PCT    = 60.0

# Evidence-gate thresholds.
# When peer_gap wins at rank 1 AND a lapsed alternative was available in top-10,
# peer_gap adoption_rate must be above a minimum to prove the system is not
# picking weak cross-sells over reliable reorders.
#
# Derivation:
#   _mid tier: peer_gap must beat lapsed flat score of 2.5.
#     2*a*2.5 >= 2.5  ->  a >= 0.50  (non-private-brand)
#     With PB boost: 2*a*2.5 + 0.5 vs 2.5 (non-PB lapsed) -> a >= 0.40
#     Gate set at 0.40 accepts PB-peer_gap wins at 40%+ adoption.
#
#   _high tier: peer_gap weight 3.5, lapsed 1.0. Gate stays at 0.15.
#
#   _low tier: peer_gap weight 1.0, lapsed 3.5. Gate stays at 0.15.
#
# Key refinement: the gate only applies when a lapsed alternative existed.
# If a customer has no lapsed products, even a low-adoption peer_gap is the
# best available recommendation and should not count as a violation.
MIN_ADOPTION_WHEN_PEER_GAP_WINS = {
    "_high": 0.15,
    "_mid":  0.40,
    "_low":  0.15,
}

# Max acceptable violation rate (percentage of rank-1 peer_gap recommendations
# that fall below the gate AND have a lapsed alternative available).
# 5% allows for edge cases; above 5% suggests a scoring issue.
MAX_VIOLATION_RATE_PCT = 5.0

# Families that must NOT appear in recommendations
EXCLUDED_FAMILIES = {"Fee", "Unknown", "NaN", "nan", ""}

# Segments to spot-check in the customer-level deep dive section
SEGMENTS_TO_CHECK = [
    "PO_high", "PO_low",
    "LTC_high", "LTC_low",
    "HC_low",
    "SC_high",
]


# Logging

def _hdr(title: str) -> None:
    print(f"\n{'='*88}")
    print(f"  {title}")
    print(f"{'='*88}")

def _sub(title: str) -> None:
    print(f"\n{'-'*88}")
    print(f"  {title}")
    print(f"{'-'*88}")


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
        RECS_FILE:     "critical",
        RECS_SCHEMA:   "critical",
        SEG_FILE:      "critical",
        FEAT_FILE:     "critical",
        ADOPTION_FILE: "warning",
        FACTORS_FILE:  "warning",
        SEQ_FILE:      "warning",
    }
    for path, severity in required.items():
        exists = path.exists()
        status = "PASS" if exists else ("FAIL" if severity == "critical" else "WARN")
        size_mb = round(path.stat().st_size / (1024*1024), 2) if exists else None
        _row(rows, "files", f"exists::{path.name}", status, size_mb,
             str(path.relative_to(ROOT)))


# Check 2: Schema completeness

def check_schema(rows: list[dict]) -> None:
    if not RECS_FILE.exists():
        return

    con = duckdb.connect()
    desc = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{RECS_FILE.as_posix()}') LIMIT 0"
    ).df()
    con.close()
    actual_cols = set(desc["column_name"].tolist())

    expected = {
        "cust_id", "rank", "segment", "supplier_profile",
        "item_id", "item_dsc", "prod_family", "prod_category",
        "is_private_brand", "signal", "score",
        "adoption_rate", "peer_group_size", "buyer_count", "median_peer_spend",
        "pitch_message",
    }
    missing = expected - actual_cols
    extra   = actual_cols - expected

    _row(rows, "schema", "required_columns_present", _status(len(missing) == 0),
         len(missing), "Missing: " + (", ".join(sorted(missing)) if missing else "None"))

    if extra:
        _row(rows, "schema", "extra_columns", "INFO", len(extra),
             "Extras: " + ", ".join(sorted(extra)))

    if RECS_SCHEMA.exists():
        try:
            with open(RECS_SCHEMA) as f:
                schema_json = json.load(f)
            schema_cols = {c["name"] for c in schema_json.get("columns", [])}
            diff = schema_cols.symmetric_difference(actual_cols)
            _row(rows, "schema", "sidecar_json_matches_parquet",
                 _status(len(diff) == 0), len(diff),
                 "Diff: " + (", ".join(sorted(diff)) if diff else "None"))
        except Exception as e:
            _row(rows, "schema", "sidecar_json_readable", "FAIL", 0, str(e))
    else:
        _row(rows, "schema", "sidecar_json_exists", "WARN", 0,
             "customer_recommendations.schema.json not found")


# Check 3: Row counts and top-N completeness

def check_counts(rows: list[dict]) -> None:
    if not RECS_FILE.exists():
        return

    con = duckdb.connect()

    total_rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{RECS_FILE.as_posix()}')"
    ).fetchone()[0]
    _row(rows, "counts", "total_rows", "INFO", total_rows,
         "Total recommendation rows")

    n_custs = con.execute(
        f"SELECT COUNT(DISTINCT cust_id) FROM read_parquet('{RECS_FILE.as_posix()}')"
    ).fetchone()[0]
    _row(rows, "counts", "unique_customers", "INFO", n_custs,
         "Unique customers with recommendations")

    avg_ranks = total_rows / max(n_custs, 1)
    _row(rows, "counts", "avg_recs_per_customer", "INFO", round(avg_ranks, 2),
         f"Expected close to {EXPECTED_TOP_N}")

    low_count = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT cust_id, COUNT(*) AS n
            FROM read_parquet('{RECS_FILE.as_posix()}')
            GROUP BY cust_id
            HAVING n < {EXPECTED_TOP_N}
        )
    """).fetchone()[0]
    low_pct = round(low_count / max(n_custs, 1) * 100, 2)
    _row(rows, "counts", "customers_with_fewer_than_topn",
         "INFO", f"{low_count:,} ({low_pct}%)",
         f"Customers with <{EXPECTED_TOP_N} recs (edge cases)")

    rank1_count = con.execute(
        f"SELECT COUNT(DISTINCT cust_id) FROM read_parquet('{RECS_FILE.as_posix()}') "
        f"WHERE rank = 1"
    ).fetchone()[0]
    _row(rows, "counts", "every_customer_has_rank_1",
         _status(rank1_count == n_custs), f"{rank1_count} / {n_custs}",
         "Every customer must have a rank=1 recommendation")

    max_rank = con.execute(
        f"SELECT MAX(rank) FROM read_parquet('{RECS_FILE.as_posix()}')"
    ).fetchone()[0]
    _row(rows, "counts", "max_rank_within_topn",
         _status(max_rank is not None and max_rank <= EXPECTED_TOP_N),
         max_rank, f"Max rank must be <= {EXPECTED_TOP_N}")

    con.close()


# Check 4: Value validity

def check_values(rows: list[dict]) -> None:
    if not RECS_FILE.exists():
        return

    con = duckdb.connect()

    bad_signals = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE signal NOT IN ('peer_gap', 'lapsed')
    """).fetchone()[0]
    _row(rows, "values", "valid_signals", _status(bad_signals == 0),
         bad_signals, f"All signals must be in {sorted(EXPECTED_SIGNALS)}")

    bad_sp = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE supplier_profile NOT IN ('medline_only', 'mckesson_primary', 'mixed')
    """).fetchone()[0]
    _row(rows, "values", "valid_supplier_profiles", _status(bad_sp == 0),
         bad_sp, f"All supplier_profile values must be in {sorted(EXPECTED_SUPPLIER_PROFILES)}")

    bad_pb = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE is_private_brand NOT IN (0, 1)
    """).fetchone()[0]
    _row(rows, "values", "is_private_brand_binary", _status(bad_pb == 0),
         bad_pb, "is_private_brand must be 0 or 1")

    neg_score = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE score < 0
    """).fetchone()[0]
    _row(rows, "values", "no_negative_scores", _status(neg_score == 0),
         neg_score, "Scores must be >= 0")

    bad_rate = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE adoption_rate < 0 OR adoption_rate > 1
    """).fetchone()[0]
    _row(rows, "values", "adoption_rate_in_range", _status(bad_rate == 0),
         bad_rate, "adoption_rate must be between 0 and 1")

    for col in ["pitch_message", "item_dsc", "segment"]:
        null_count = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{RECS_FILE.as_posix()}')
            WHERE {col} IS NULL OR {col} = ''
        """).fetchone()[0]
        _row(rows, "values", f"no_empty::{col}", _status(null_count == 0),
             null_count, f"{col} must not be NULL or empty")

    con.close()


# Check 5: Exclusions enforced

def check_exclusions(rows: list[dict]) -> None:
    if not RECS_FILE.exists():
        return

    con = duckdb.connect()

    excl_sql = ", ".join(f"'{f}'" for f in sorted(EXCLUDED_FAMILIES))
    fee_count = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE prod_family IN ({excl_sql})
    """).fetchone()[0]
    _row(rows, "exclusions", "fee_family_excluded", _status(fee_count == 0),
         fee_count, f"Excluded families must not appear: {sorted(EXCLUDED_FAMILIES)}")

    con.close()


# Check 6: Segment coverage and signal distribution

def check_segment_behavior(rows: list[dict]) -> None:
    if not RECS_FILE.exists():
        return

    con = duckdb.connect()

    null_seg = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE segment IS NULL OR segment = ''
    """).fetchone()[0]
    _row(rows, "segments", "no_null_segments", _status(null_seg == 0),
         null_seg, "Every row must have a segment assigned")

    # Signal distribution at rank 1
    rank1 = con.execute(f"""
        SELECT segment, signal, COUNT(*) AS n
        FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE rank = 1
        GROUP BY segment, signal
    """).df()

    if len(rank1) == 0:
        _row(rows, "segments", "rank1_data_present", "FAIL", 0,
             "No rank=1 data found")
        con.close()
        return

    pivot = rank1.pivot_table(
        index="segment", columns="signal", values="n",
        fill_value=0, aggfunc="sum"
    ).reset_index()
    if "peer_gap" not in pivot.columns: pivot["peer_gap"] = 0
    if "lapsed"   not in pivot.columns: pivot["lapsed"]   = 0
    pivot["total"] = pivot["peer_gap"] + pivot["lapsed"]
    pivot["peer_gap_pct"] = (pivot["peer_gap"] / pivot["total"].clip(lower=1) * 100).round(1)
    pivot["lapsed_pct"]   = (pivot["lapsed"]   / pivot["total"].clip(lower=1) * 100).round(1)

    high_segs = pivot[pivot["segment"].str.endswith("_high") & (pivot["total"] >= 100)]
    for _, r in high_segs.iterrows():
        ok = r["peer_gap_pct"] >= MIN_HIGH_PEER_GAP_PCT
        _row(rows, "segments", f"high_tier_peer_gap_dominant::{r['segment']}",
             _status(ok), f"{r['peer_gap_pct']}% peer_gap",
             f"_high segment must have >= {MIN_HIGH_PEER_GAP_PCT}% peer_gap at rank 1")

    low_segs = pivot[pivot["segment"].str.endswith("_low") & (pivot["total"] >= 100)]
    for _, r in low_segs.iterrows():
        ok = r["lapsed_pct"] >= MIN_LOW_LAPSED_PCT
        _row(rows, "segments", f"low_tier_lapsed_dominant::{r['segment']}",
             _status(ok), f"{r['lapsed_pct']}% lapsed",
             f"_low segment must have >= {MIN_LOW_LAPSED_PCT}% lapsed at rank 1")

    mid_segs = pivot[pivot["segment"].str.endswith("_mid") & (pivot["total"] >= 100)]
    for _, r in mid_segs.iterrows():
        _row(rows, "segments", f"mid_tier_signal_mix::{r['segment']}",
             "INFO", f"{r['peer_gap_pct']}% peer_gap / {r['lapsed_pct']}% lapsed",
             "_mid segments expected to be balanced")

    # Supplier profile coverage
    sp_counts = con.execute(f"""
        SELECT supplier_profile, COUNT(DISTINCT cust_id) AS n_custs
        FROM read_parquet('{RECS_FILE.as_posix()}')
        GROUP BY supplier_profile
    """).df()
    con.close()

    for sp in ["medline_only", "mckesson_primary", "mixed"]:
        n = int(sp_counts.loc[sp_counts["supplier_profile"] == sp, "n_custs"].sum())
        present = n > 0
        status = "PASS" if present else ("WARN" if sp == "medline_only" else "FAIL")
        _row(rows, "segments", f"supplier_profile_present::{sp}",
             status, n, f"Customers with supplier_profile = {sp}")


# Check 7: Evidence-gate validation
# REFINED: a violation is only counted when peer_gap won at rank 1 with
# below-gate adoption AND a lapsed alternative existed in top-10. If no lapsed
# existed, peer_gap is the best available recommendation regardless of adoption.

def check_evidence_gate(rows: list[dict]) -> None:
    if not RECS_FILE.exists():
        return

    con = duckdb.connect()

    # Per-segment summary of rank-1 peer_gap adoption rates
    adop_stats = con.execute(f"""
        SELECT
            segment,
            COUNT(*)                                AS n_peer_gap_rank1,
            ROUND(MIN(adoption_rate), 3)            AS min_adoption,
            ROUND(AVG(adoption_rate), 3)            AS mean_adoption,
            ROUND(MEDIAN(adoption_rate), 3)         AS median_adoption,
            ROUND(MAX(adoption_rate), 3)            AS max_adoption
        FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE rank = 1 AND signal = 'peer_gap'
          AND segment NOT IN ('unknown', 'OTHER_low')
        GROUP BY segment
        HAVING COUNT(*) >= 5
        ORDER BY segment
    """).df()

    if len(adop_stats) == 0:
        _row(rows, "evidence_gate", "rank1_peer_gap_data_present",
             "INFO", 0, "No peer_gap rank-1 data found")
        con.close()
        return

    for _, r in adop_stats.iterrows():
        seg = r["segment"]
        if   seg.endswith("_high"): gate = MIN_ADOPTION_WHEN_PEER_GAP_WINS["_high"]
        elif seg.endswith("_mid"):  gate = MIN_ADOPTION_WHEN_PEER_GAP_WINS["_mid"]
        elif seg.endswith("_low"):  gate = MIN_ADOPTION_WHEN_PEER_GAP_WINS["_low"]
        else:                        gate = 0.15

        total_peer_gap_rank1 = int(r["n_peer_gap_rank1"])

        # Refined violation query: count peer_gap rank-1 recs with below-gate adoption
        # WHERE a lapsed alternative existed in top-10.
        # If no lapsed was available, peer_gap is the best available rec regardless
        # of adoption strength, so it should not count as a violation.
        violations_with_alternative = con.execute(f"""
            WITH weak_peer_gap AS (
                SELECT cust_id
                FROM read_parquet('{RECS_FILE.as_posix()}')
                WHERE rank = 1
                  AND signal = 'peer_gap'
                  AND segment = '{seg}'
                  AND adoption_rate < {gate}
            ),
            had_lapsed_alt AS (
                SELECT DISTINCT r.cust_id
                FROM read_parquet('{RECS_FILE.as_posix()}') r
                INNER JOIN weak_peer_gap w ON r.cust_id = w.cust_id
                WHERE r.signal = 'lapsed'
            )
            SELECT COUNT(*) FROM had_lapsed_alt
        """).fetchone()[0]

        # Violation RATE as pct of total rank-1 peer_gap in this segment
        viol_rate = (violations_with_alternative / max(total_peer_gap_rank1, 1)) * 100

        ok = viol_rate <= MAX_VIOLATION_RATE_PCT
        _row(rows, "evidence_gate",
             f"rank1_peer_gap_adoption_gate::{seg}",
             _status(ok),
             f"{violations_with_alternative} of {total_peer_gap_rank1} "
             f"({viol_rate:.1f}%, gate={gate:.2f})",
             f"{seg} tier peer_gap at rank 1 must have adoption >= {gate:.2f} "
             f"OR no lapsed alternative existed. Max violation rate: "
             f"{MAX_VIOLATION_RATE_PCT:.1f}%.")

        # Informational: median adoption per segment
        _row(rows, "evidence_gate",
             f"rank1_peer_gap_adoption_stats::{seg}",
             "INFO",
             f"median={r['median_adoption']:.1%}",
             f"{seg}: {total_peer_gap_rank1} rank-1 peer_gap recs, "
             f"adoption range [{r['min_adoption']:.1%}, {r['max_adoption']:.1%}]")

    # Cross-segment summary: overall proof the evidence gate is working
    overall = con.execute(f"""
        SELECT
            ROUND(AVG(adoption_rate), 3)    AS overall_mean_adoption,
            ROUND(MEDIAN(adoption_rate), 3) AS overall_median_adoption,
            COUNT(*)                        AS total_rank1_peer_gap
        FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE rank = 1 AND signal = 'peer_gap'
    """).fetchone()

    if overall is not None:
        mean_a, median_a, total = overall
        _row(rows, "evidence_gate", "overall_rank1_peer_gap_adoption",
             "INFO",
             f"median={median_a:.1%}, mean={mean_a:.1%}",
             f"{total:,} rank-1 peer_gap recommendations overall")

    # How many customers with weak peer_gap at rank 1 had no lapsed alternative?
    no_lapsed_edge = con.execute(f"""
        WITH weak_peer_gap_rank1 AS (
            SELECT cust_id
            FROM read_parquet('{RECS_FILE.as_posix()}')
            WHERE rank = 1 AND signal = 'peer_gap' AND adoption_rate < 0.40
        ),
        no_lapsed AS (
            SELECT w.cust_id
            FROM weak_peer_gap_rank1 w
            LEFT JOIN (
                SELECT DISTINCT cust_id
                FROM read_parquet('{RECS_FILE.as_posix()}')
                WHERE signal = 'lapsed'
            ) L ON w.cust_id = L.cust_id
            WHERE L.cust_id IS NULL
        )
        SELECT COUNT(*) FROM no_lapsed
    """).fetchone()[0]

    _row(rows, "evidence_gate", "weak_peer_gap_with_no_lapsed_alternative",
         "INFO", no_lapsed_edge,
         "Customers with rank-1 peer_gap below 40% adoption and NO lapsed "
         "alternative available (acceptable edge case).")

    con.close()


# Check 8: Medline substitution framing in pitch messages

def check_medline_substitution_framing(rows: list[dict]) -> None:
    if not RECS_FILE.exists():
        return

    con = duckdb.connect()
    medline_recs = con.execute(f"""
        SELECT pitch_message
        FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE supplier_profile = 'medline_only' AND rank = 1
    """).df()
    con.close()

    if len(medline_recs) == 0:
        _row(rows, "pitch_framing", "medline_only_recs_exist",
             "WARN", 0, "No medline_only customers found with rank=1 recs")
        return

    medline_recs["has_subst"] = (
        medline_recs["pitch_message"].str.contains(
            r"instead of Medline|in place of Medline|McKesson alternative|Medline substitution",
            regex=True, na=False
        )
    )

    n_with  = int(medline_recs["has_subst"].sum())
    n_total = len(medline_recs)
    pct     = round(n_with / n_total * 100, 1)

    ok = pct >= 25.0
    _row(rows, "pitch_framing", "medline_only_substitution_framing",
         _status(ok), f"{n_with}/{n_total} ({pct}%)",
         "medline_only rank-1 pitches should carry substitution framing (>= 25%)")


# Check 9: Customer-feature coverage

def check_customer_coverage(rows: list[dict]) -> None:
    if not (RECS_FILE.exists() and FEAT_FILE.exists()):
        return

    con = duckdb.connect()
    unmatched = con.execute(f"""
        SELECT COUNT(DISTINCT r.cust_id) FROM
            read_parquet('{RECS_FILE.as_posix()}') r
        LEFT JOIN read_parquet('{FEAT_FILE.as_posix()}') f
            ON r.cust_id = f.DIM_CUST_CURR_ID
        WHERE f.DIM_CUST_CURR_ID IS NULL
    """).fetchone()[0]
    con.close()

    _row(rows, "coverage", "all_cust_ids_in_features",
         _status(unmatched == 0), unmatched,
         "Every cust_id in recommendations must exist in customer_features")


# Customer-level deep dive (for human review)

def customer_deep_dive() -> None:
    if not (RECS_FILE.exists() and FEAT_FILE.exists()):
        return

    recs = pd.read_parquet(RECS_FILE)
    features = pd.read_parquet(FEAT_FILE, columns=[
        "DIM_CUST_CURR_ID", "MKT_CD", "SPCLTY_CD",
        "monetary", "frequency", "recency_days", "R_score", "F_score",
    ])

    _hdr("SAMPLE CUSTOMER DEEP DIVE  -  one representative per segment")

    for segment in SEGMENTS_TO_CHECK:
        seg_custs = recs.loc[recs["segment"] == segment, "cust_id"].unique()
        if len(seg_custs) == 0:
            print(f"\n  {segment}: no customers found.")
            continue

        seg_feat = features[features["DIM_CUST_CURR_ID"].isin(seg_custs)].copy()
        if len(seg_feat) == 0:
            cust_id = int(seg_custs[0])
        else:
            med = seg_feat["monetary"].median()
            seg_feat["distance"] = (seg_feat["monetary"] - med).abs()
            cust_id = int(seg_feat.sort_values("distance").iloc[0]["DIM_CUST_CURR_ID"])

        _sub(f"Customer {cust_id}  |  Segment: {segment}")

        f = features[features["DIM_CUST_CURR_ID"] == cust_id]
        if len(f) > 0:
            f = f.iloc[0]
            print(f"    MKT_CD      : {f.get('MKT_CD', '?')}")
            print(f"    Specialty   : {f.get('SPCLTY_CD', '?')}")
            print(f"    Monetary    : ${f.get('monetary', 0):,.0f}")
            print(f"    Frequency   : {f.get('frequency', 0):.0f} orders")
            print(f"    Recency     : {f.get('recency_days', 0):.0f} days")
            print(f"    R / F score : {f.get('R_score', 0):.0f} / {f.get('F_score', 0):.0f}")

        cust_recs = recs[recs["cust_id"] == cust_id].sort_values("rank").head(5)
        if len(cust_recs) == 0:
            print(f"    No recommendations found.")
            continue

        print(f"\n    Top 5 recommendations:")
        print(f"    {'#':<3} {'Signal':<10} {'Score':<7} {'Adop':<6} {'Family':<28} {'Product':<40}")
        print(f"    {'-'*3} {'-'*10} {'-'*7} {'-'*6} {'-'*28} {'-'*40}")
        for _, r in cust_recs.iterrows():
            sig   = str(r.get("signal", "?"))[:10]
            score = r.get("score", 0)
            adop  = r.get("adoption_rate", 0)
            fam   = str(r.get("prod_family", "?"))[:26]
            prod  = str(r.get("item_dsc", "?"))[:38]
            rank  = int(r.get("rank", 0))
            adop_str = f"{adop*100:.0f}%" if adop > 0 else "-"
            print(f"    {rank:<3} {sig:<10} {score:<7.3f} {adop_str:<6} {fam:<28} {prod:<40}")

        top = cust_recs.iloc[0]
        pitch = str(top.get("pitch_message", ""))[:220]
        sp    = str(top.get("supplier_profile", "?"))
        print(f"\n    Rank-1 supplier_profile: {sp}")
        print(f"    Rank-1 pitch message:")
        print(f"      {pitch}")


# Excel output

def write_outputs(results: pd.DataFrame) -> None:
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
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
    _hdr("RECOMMENDATION SANITY CHECK  -  summary")

    passes   = int((results["status"] == "PASS").sum())
    fails    = int((results["status"] == "FAIL").sum())
    warnings = int((results["status"] == "WARN").sum())
    infos    = int((results["status"] == "INFO").sum())
    total    = len(results)

    print(f"  Total : {total}")
    print(f"  PASS  : {passes}")
    print(f"  FAIL  : {fails}   (critical)")
    print(f"  WARN  : {warnings}   (non-critical)")
    print(f"  INFO  : {infos}   (no judgment)")

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
        print(f"    {category:<18} "
              f"PASS={row['PASS']:<3} FAIL={row['FAIL']:<3} "
              f"WARN={row['WARN']:<3} INFO={row['INFO']:<3}")

    # Rank-1 signal distribution table
    seg_rows = results[
        (results["category"] == "segments") &
        (results["check_name"].str.contains("peer_gap_dominant|lapsed_dominant|signal_mix"))
    ]
    if len(seg_rows) > 0:
        print("\n  Rank-1 signal distribution by segment (validates SEGMENT_WEIGHTS):")
        print(f"    {'Segment':<15} {'Expected':<18} {'Actual':<30} {'Status'}")
        print(f"    {'-'*15} {'-'*18} {'-'*30} {'-'*6}")
        for _, r in seg_rows.iterrows():
            seg = r["check_name"].split("::")[1] if "::" in r["check_name"] else r["check_name"]
            if seg.endswith("_high"):
                expected = ">= 60% peer_gap"
            elif seg.endswith("_low"):
                expected = ">= 60% lapsed"
            else:
                expected = "balanced"
            print(f"    {seg:<15} {expected:<18} {str(r['metric_value']):<30} [{r['status']}]")

    # Evidence-gate table
    gate_rows = results[
        (results["category"] == "evidence_gate") &
        (results["check_name"].str.contains("rank1_peer_gap_adoption_gate"))
    ]
    if len(gate_rows) > 0:
        print("\n  Evidence-gate: rank-1 peer_gap must have adoption>=gate OR no lapsed alternative")
        print(f"    {'Segment':<15} {'Violation rate':<48} {'Status'}")
        print(f"    {'-'*15} {'-'*48} {'-'*6}")
        for _, r in gate_rows.iterrows():
            seg = r["check_name"].split("::")[1] if "::" in r["check_name"] else r["check_name"]
            print(f"    {seg:<15} {str(r['metric_value']):<48} [{r['status']}]")

    # Adoption stats (informational)
    stat_rows = results[
        (results["category"] == "evidence_gate") &
        (results["check_name"].str.contains("rank1_peer_gap_adoption_stats"))
    ]
    if len(stat_rows) > 0:
        print("\n  Median adoption_rate when peer_gap wins at rank 1 (by segment):")
        print(f"    {'Segment':<15} {'Median adoption':<20} {'Detail'}")
        print(f"    {'-'*15} {'-'*20} {'-'*50}")
        for _, r in stat_rows.iterrows():
            seg = r["check_name"].split("::")[1] if "::" in r["check_name"] else r["check_name"]
            detail = str(r["detail"])[:60]
            print(f"    {seg:<15} {str(r['metric_value']):<20} {detail}")

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
            print(f"        detail : {r['detail']}")

    if fails == 0 and warnings == 0:
        print("\n  All checks passed. Recommendations are ready for the backend.")
    elif fails == 0:
        print(f"\n  No critical failures. {warnings} warning(s) to investigate.")
    else:
        print(f"\n  {fails} critical failure(s). Fix before shipping to backend.")

    print()
    print(f"  CSV   : {SUMMARY_CSV.relative_to(ROOT)}")
    print(f"  Excel : {SUMMARY_XLSX.relative_to(ROOT)}")
    print("="*88)
    print()

    return fails


# Main

def main() -> None:
    _hdr("RECOMMENDATION SANITY CHECK  -  customer_recommendations.parquet")

    rows: list[dict] = []

    check_required_files(rows)
    check_schema(rows)
    check_counts(rows)
    check_values(rows)
    check_exclusions(rows)
    check_segment_behavior(rows)
    check_evidence_gate(rows)
    check_medline_substitution_framing(rows)
    check_customer_coverage(rows)

    results = pd.DataFrame(rows)
    write_outputs(results)
    customer_deep_dive()
    failures = print_console_summary(results)

    sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"\nFATAL ERROR: {exc}", file=sys.stderr)
        raise