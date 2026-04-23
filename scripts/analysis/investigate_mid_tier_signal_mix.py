from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd


# Paths

ROOT        = Path(__file__).resolve().parent.parent.parent
DATA_CLEAN  = ROOT / "data_clean"
PRECOMP_DIR = DATA_CLEAN / "serving" / "precomputed"
FEAT_DIR    = DATA_CLEAN / "features"

RECS_FILE     = PRECOMP_DIR / "customer_recommendations.parquet"
ADOPTION_FILE = PRECOMP_DIR / "product_adoption_rates.parquet"
FEAT_FILE     = FEAT_DIR    / "customer_features.parquet"


# Logging

def _hdr(title: str) -> None:
    print(f"\n{'='*88}\n  {title}\n{'='*88}")

def _sub(title: str) -> None:
    print(f"\n{'-'*88}\n  {title}\n{'-'*88}")

def _row(label: str, value) -> None:
    print(f"  {label:<52} {value}")


# Main investigation

def main() -> None:
    _hdr("MID-TIER SIGNAL MIX INVESTIGATION")
    print("  Hypothesis: mid-tier peer_gap candidates have low adoption rates")
    print("              so lapsed wins rank-1 even with balanced scoring formula.")
    print()
    print("  Verification plan:")
    print("    1. Distribution of peer_gap scores for mid-tier rank-1 recs")
    print("    2. Distribution of lapsed  scores for mid-tier rank-1 recs")
    print("    3. Top adoption rates available to mid-tier customers")
    print("    4. Sample: for a random mid-tier customer, show all scored candidates")

    con = duckdb.connect()

    # Question 1: at rank 1 for mid tiers, what are the score distributions?

    _sub("Q1: Rank-1 score distributions — mid tiers")

    q1 = con.execute(f"""
        SELECT
            segment,
            signal,
            COUNT(*)               AS n_rows,
            ROUND(MIN(score), 3)   AS min_score,
            ROUND(AVG(score), 3)   AS avg_score,
            ROUND(MEDIAN(score), 3) AS median_score,
            ROUND(MAX(score), 3)   AS max_score,
            ROUND(MIN(adoption_rate), 3) AS min_adoption,
            ROUND(AVG(adoption_rate), 3) AS avg_adoption,
            ROUND(MAX(adoption_rate), 3) AS max_adoption
        FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE rank = 1 AND segment LIKE '%_mid'
        GROUP BY segment, signal
        ORDER BY segment, signal
    """).df()
    print(q1.to_string(index=False))

    # Question 2: what does the FULL peer_gap population look like (not just rank 1)?

    _sub("Q2: All peer_gap recommendations for mid-tier customers (not just rank 1)")

    q2 = con.execute(f"""
        SELECT
            segment,
            COUNT(*)                AS n_peer_gap_candidates,
            ROUND(MIN(adoption_rate), 3) AS min_adop,
            ROUND(AVG(adoption_rate), 3) AS avg_adop,
            ROUND(MEDIAN(adoption_rate), 3) AS median_adop,
            ROUND(MAX(adoption_rate), 3) AS max_adop,
            ROUND(AVG(score), 3)     AS avg_score,
            ROUND(MAX(score), 3)     AS max_score
        FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE signal = 'peer_gap' AND segment LIKE '%_mid'
        GROUP BY segment
        ORDER BY segment
    """).df()
    print(q2.to_string(index=False))

    # Question 3: for mid-tier peer_groups, what's the top available adoption?

    _sub("Q3: Top adoption rates available in mid-tier peer_groups")
    print("  (peer_groups with |mid in the label)")

    q3 = con.execute(f"""
        SELECT
            peer_group,
            peer_group_size,
            COUNT(*)                      AS n_products_in_group,
            ROUND(MIN(adoption_rate), 3)  AS min_adop,
            ROUND(AVG(adoption_rate), 3)  AS avg_adop,
            ROUND(MAX(adoption_rate), 3)  AS top_adop
        FROM read_parquet('{ADOPTION_FILE.as_posix()}')
        WHERE peer_group LIKE '%|mid'
          AND peer_group_size >= 5
        GROUP BY peer_group, peer_group_size
        ORDER BY peer_group_size DESC
        LIMIT 15
    """).df()
    print(q3.to_string(index=False))

    # Question 4: for a mid-tier customer, walk through the scoring

    _sub("Q4: Mid-tier customer scoring walkthrough")

    # Pick a representative mid-tier customer with both signals available
    q4a = con.execute(f"""
        SELECT cust_id, segment
        FROM read_parquet('{RECS_FILE.as_posix()}')
        WHERE segment = 'PO_mid' AND rank = 1
        LIMIT 1
    """).fetchone()

    if q4a is None:
        print("  No PO_mid customer found.")
    else:
        cid, seg = q4a
        print(f"  Customer: {cid}  |  Segment: {seg}")

        walk = con.execute(f"""
            SELECT
                rank,
                signal,
                ROUND(score, 3)       AS score,
                ROUND(adoption_rate, 3) AS adoption,
                is_private_brand       AS pb,
                prod_family,
                SUBSTR(item_dsc, 1, 48) AS product
            FROM read_parquet('{RECS_FILE.as_posix()}')
            WHERE cust_id = {cid}
            ORDER BY rank
        """).df()
        print(walk.to_string(index=False))

        # And also show what peer_gap candidates this customer had available
        print()
        print(f"  All peer_gap candidates available to this customer's peer_group")
        print(f"  (top 20 by adoption_rate):")

        # Reverse-engineer peer_group
        feat_row = con.execute(f"""
            SELECT SPCLTY_CD, R_score, F_score, specialty_tier
            FROM read_parquet('{FEAT_FILE.as_posix()}')
            WHERE DIM_CUST_CURR_ID = {cid}
        """).fetchone()

        if feat_row is not None:
            spclty, rs, fs, tier = feat_row
            if rs >= 4 and fs >= 4:
                rfm = "high"
            elif rs <= 2 or fs <= 2:
                rfm = "low"
            else:
                rfm = "mid"
            if tier == 3:
                peer_group = f"TIER3|{rfm}"
            else:
                peer_group = f"{spclty}|{rfm}"

            print(f"  peer_group = {peer_group}")

            peer_cands = con.execute(f"""
                SELECT
                    ROUND(adoption_rate, 3) AS adoption,
                    is_private_brand AS pb,
                    prod_family,
                    SUBSTR(item_dsc, 1, 48) AS product,
                    ROUND(2.0 * adoption_rate * 2.5, 3) AS peer_gap_score_base,
                    ROUND(2.0 * adoption_rate * 2.5 + CASE WHEN is_private_brand=1 THEN 0.5 ELSE 0 END, 3) AS peer_gap_score_final
                FROM read_parquet('{ADOPTION_FILE.as_posix()}')
                WHERE peer_group = '{peer_group}'
                ORDER BY adoption_rate DESC
                LIMIT 20
            """).df()
            print(peer_cands.to_string(index=False))

            print()
            print("  Reference: lapsed_w for _mid = 2.5 flat. Add 0.5 if private brand -> 3.0.")
            print("  A peer_gap candidate needs adoption >= 0.50 to beat a non-PB lapsed.")
            print("  A peer_gap candidate needs adoption >= 0.70 to beat a PB lapsed.")

    # Question 5: distribution of top adoption rates across mid-tier customers

    _sub("Q5: For each mid-tier customer, what's their single best peer_gap adoption rate?")

    q5 = con.execute(f"""
        WITH best_per_cust AS (
            SELECT cust_id, segment, MAX(adoption_rate) AS top_adoption
            FROM read_parquet('{RECS_FILE.as_posix()}')
            WHERE signal = 'peer_gap' AND segment LIKE '%_mid'
            GROUP BY cust_id, segment
        )
        SELECT
            segment,
            COUNT(*) AS n_customers,
            ROUND(AVG(top_adoption), 3)        AS mean_top_adoption,
            ROUND(MEDIAN(top_adoption), 3)     AS median_top_adoption,
            ROUND(QUANTILE_CONT(top_adoption, 0.25), 3) AS p25,
            ROUND(QUANTILE_CONT(top_adoption, 0.75), 3) AS p75,
            SUM(CASE WHEN top_adoption >= 0.50 THEN 1 ELSE 0 END) AS n_with_adoption_ge_50pct
        FROM best_per_cust
        GROUP BY segment
        ORDER BY segment
    """).df()
    print(q5.to_string(index=False))

    # Question 6: overall rank-1 signal vs my predictions

    _sub("Q6: Head-to-head — what won rank-1 for mid-tier, and by how much?")

    q6 = con.execute(f"""
        WITH mid_r1 AS (
            SELECT * FROM read_parquet('{RECS_FILE.as_posix()}')
            WHERE rank = 1 AND segment LIKE '%_mid'
        )
        SELECT
            segment,
            signal,
            COUNT(*) AS n,
            ROUND(AVG(score), 3) AS avg_winning_score
        FROM mid_r1
        GROUP BY segment, signal
        ORDER BY segment, signal
    """).df()
    print(q6.to_string(index=False))

    # Diagnosis

    _hdr("DIAGNOSIS")

    summary = con.execute(f"""
        WITH best_per_cust AS (
            SELECT cust_id, segment, MAX(adoption_rate) AS top_adoption
            FROM read_parquet('{RECS_FILE.as_posix()}')
            WHERE signal = 'peer_gap' AND segment LIKE '%_mid'
            GROUP BY cust_id, segment
        )
        SELECT
            ROUND(AVG(top_adoption), 3) AS overall_mean_top_adoption,
            ROUND(MEDIAN(top_adoption), 3) AS overall_median_top_adoption,
            SUM(CASE WHEN top_adoption >= 0.50 THEN 1 ELSE 0 END) AS n_with_adop_ge_50,
            COUNT(*) AS total_mid_customers
        FROM best_per_cust
    """).fetchone()

    if summary is not None:
        mean, median, n_ge_50, total = summary
        pct_ge_50 = n_ge_50 / total * 100 if total else 0

        print()
        _row("Mid-tier customers analyzed:", f"{total:,}")
        _row("Mean of each customer's best peer_gap adoption:", f"{mean:.1%}")
        _row("Median of each customer's best peer_gap adoption:", f"{median:.1%}")
        _row("Customers whose best peer_gap candidate >= 50% adoption:",
             f"{n_ge_50:,} ({pct_ge_50:.1f}%)")

        print()
        print("  INTERPRETATION:")
        if median < 0.45:
            print("  --> My scoring math assumed adoption ~0.50 at rank 1 for mid tiers.")
            print(f"  --> Reality: median customer's BEST peer_gap candidate is {median:.0%} adoption.")
            print(f"  --> At that adoption level, peer_gap score = 2*{median:.2f}*2.5 = {2*median*2.5:.2f}")
            print("  --> Lapsed score = 2.50 (flat)")
            if 2*median*2.5 < 2.5:
                print("  --> Conclusion: Lapsed correctly beats peer_gap for most mid-tier customers.")
                print("                  The data has fewer strong peer_gap candidates than I predicted.")
                print("                  This is a DATA REALITY, not a scoring bug.")
            else:
                print("  --> Something else is going on. Investigate further.")
        else:
            print(f"  --> Median top adoption {median:.0%} is close to 50% — math should work.")
            print("  --> There's likely a scoring bug elsewhere. Investigate further.")

        print()
        print("  BUSINESS MEANING:")
        print(f"  - Only {pct_ge_50:,} mid-tier customers ({pct_ge_50:.1f}%) have a peer_gap")
        print(f"    candidate strong enough (>=50% adoption) to beat a reliable reorder pitch.")
        print(f"  - For the remaining {total - n_ge_50:,} customers, a lapsed-product pitch")
        print(f"    is genuinely the better recommendation given the data available.")
        print(f"  - This aligns with business intuition: a mid-tier customer with no strong")
        print(f"    peer pattern is better served by 'here's something you used to order' than")
        print(f"    'here's a cold cross-sell with 25% peer evidence'.")

    con.close()

    print()
    print("=" * 88)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        raise