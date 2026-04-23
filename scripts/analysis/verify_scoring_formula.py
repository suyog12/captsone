from __future__ import annotations

import sys
from pathlib import Path

import duckdb


# Paths

ROOT        = Path(__file__).resolve().parent.parent.parent
DATA_CLEAN  = ROOT / "data_clean"
PRECOMP_DIR = DATA_CLEAN / "serving" / "precomputed"
RECS_FILE   = PRECOMP_DIR / "customer_recommendations.parquet"


# Segment weights (must match recommendation_factors.py)

SEGMENT_WEIGHTS = {
    "PO_high":  (3.5, 1.0), "LTC_high": (3.5, 1.0), "SC_high":  (3.5, 1.0),
    "HC_high":  (3.5, 1.0), "LC_high":  (3.5, 1.0), "AC_high":  (3.5, 1.0),
    "PO_mid":   (2.5, 2.5), "LTC_mid":  (2.5, 2.5), "SC_mid":   (2.5, 2.5),
    "HC_mid":   (2.5, 2.5), "LC_mid":   (2.5, 2.5), "AC_mid":   (2.5, 2.5),
    "PO_low":   (1.0, 3.5), "LTC_low":  (1.0, 3.5), "SC_low":   (1.0, 3.5),
    "HC_low":   (1.0, 3.5), "LC_low":   (1.0, 3.5), "AC_low":   (1.0, 3.5),
    "OTHER_low": (1.0, 3.5), "unknown":   (3.0, 2.0),
}
PRIVATE_BRAND_BONUS = 0.5


def main() -> None:
    print()
    print("=" * 88)
    print("  SCORING FORMULA VERIFICATION")
    print("=" * 88)
    print("  Goal: determine whether customer_recommendations.parquet was produced")
    print("        by the OLD formula (1 + 2*a) * peer_w or NEW formula 2*a * peer_w")
    print()

    if not RECS_FILE.exists():
        print("  ERROR: customer_recommendations.parquet not found.")
        sys.exit(1)

    con = duckdb.connect()

    # Pull a sample of peer_gap rows across all segments.
    # We want diverse (segment, adoption_rate, is_private_brand) combinations.
    sample = con.execute(f"""
        SELECT
            cust_id,
            rank,
            segment,
            adoption_rate,
            is_private_brand,
            score
        FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE signal = 'peer_gap'
          AND adoption_rate > 0
          AND rank <= 3
          AND segment IN (
              'PO_high', 'PO_mid', 'PO_low',
              'LTC_high', 'LTC_mid',
              'SC_high', 'SC_mid',
              'HC_high', 'HC_mid',
              'LC_high', 'LC_mid',
              'AC_high'
          )
        ORDER BY cust_id, rank
        LIMIT 50
    """).fetchall()

    con.close()

    if not sample:
        print("  No peer_gap rows found. Cannot verify.")
        sys.exit(1)

    print(f"  Sampled {len(sample)} peer_gap recommendations across segments.")
    print()
    print(f"  For each row, computing what score the NEW formula would produce:")
    print(f"      new_score = 2.0 * adoption_rate * peer_w  (+0.5 if private_brand)")
    print()
    print(f"  And what score the OLD formula would produce:")
    print(f"      old_score = (1.0 + 2.0 * adoption_rate) * peer_w  (+0.5 if private_brand)")
    print()
    print("-" * 88)
    print(f"  {'segment':<10} {'adop':>6} {'PB':>3} {'actual':>8} {'NEW pred':>9} {'OLD pred':>9} {'matches':>10}")
    print("-" * 88)

    new_match_count = 0
    old_match_count = 0
    neither_count   = 0
    total = 0

    for cust_id, rank, segment, adoption, pb, actual_score in sample:
        weights = SEGMENT_WEIGHTS.get(segment, (3.0, 2.0))
        peer_w = weights[0]

        # NEW formula
        new_pred = 2.0 * float(adoption) * peer_w
        if pb:
            new_pred += PRIVATE_BRAND_BONUS

        # OLD formula
        old_pred = (1.0 + 2.0 * float(adoption)) * peer_w
        if pb:
            old_pred += PRIVATE_BRAND_BONUS

        # Check which formula matches actual (tolerance for float rounding)
        matches_new = abs(actual_score - new_pred) < 0.01
        matches_old = abs(actual_score - old_pred) < 0.01

        if matches_new and not matches_old:
            tag = "NEW"
            new_match_count += 1
        elif matches_old and not matches_new:
            tag = "OLD"
            old_match_count += 1
        elif matches_new and matches_old:
            # Only possible when the two formulas happen to agree (rare edge case)
            tag = "BOTH"
            new_match_count += 1
        else:
            tag = "NEITHER"
            neither_count += 1

        total += 1
        print(f"  {segment:<10} {float(adoption):>6.3f} {int(pb):>3} "
              f"{float(actual_score):>8.3f} {new_pred:>9.3f} {old_pred:>9.3f} "
              f"{tag:>10}")

    print("-" * 88)
    print()
    print(f"  Results across {total} sampled rows:")
    print(f"    NEW formula match    : {new_match_count:>3} ({new_match_count/total*100:.1f}%)")
    print(f"    OLD formula match    : {old_match_count:>3} ({old_match_count/total*100:.1f}%)")
    print(f"    Neither matches      : {neither_count:>3} ({neither_count/total*100:.1f}%)")
    print()

    print("=" * 88)
    print("  VERDICT")
    print("=" * 88)

    if new_match_count == total:
        print()
        print("  100% of sampled scores match the NEW formula.")
        print("  The parquet on disk was produced by the NEW, correct version of")
        print("  recommendation_factors.py. No re-run is needed.")
        print()
    elif old_match_count == total:
        print()
        print("  100% of sampled scores match the OLD formula.")
        print("  The parquet on disk was produced by the OLD version of")
        print("  recommendation_factors.py. You need to re-run the pipeline.")
        print()
    elif new_match_count > old_match_count:
        print()
        print(f"  {new_match_count}/{total} scores match the NEW formula.")
        print(f"  {old_match_count}/{total} scores match the OLD formula.")
        print("  Likely the NEW file ran, but some rows have rounding or edge-case")
        print("  explanations. Investigate the 'NEITHER' rows above.")
        print()
    else:
        print()
        print(f"  {new_match_count}/{total} scores match the NEW formula.")
        print(f"  {old_match_count}/{total} scores match the OLD formula.")
        print("  Mixed or OLD-formula results. You need to re-run the pipeline.")
        print()

    print("=" * 88)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        raise