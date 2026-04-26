from __future__ import annotations

import gc
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


# Paths

ROOT         = Path(__file__).resolve().parent.parent.parent
DATA_CLEAN   = ROOT / "data_clean"
PRECOMP      = DATA_CLEAN / "serving" / "precomputed"
FEATURES     = DATA_CLEAN / "features"
ANALYSIS     = DATA_CLEAN / "analysis"

# Input files

CUST_SEG_FILE       = PRECOMP  / "customer_segments.parquet"
CUST_PATT_FILE      = PRECOMP  / "customer_patterns.parquet"
SEG_PROFILE_FILE    = PRECOMP  / "segment_category_profiles.parquet"
PROD_SEG_FILE       = PRECOMP  / "product_segments.parquet"
PROD_SPEC_FILE      = PRECOMP  / "product_specialty.parquet"
ITEM_SIM_FILE       = PRECOMP  / "item_similarity.parquet"
COOCCUR_FILE        = PRECOMP  / "product_cooccurrence.parquet"
LAPSED_FILE         = PRECOMP  / "customer_lapsed_products.parquet"
PB_EQUIV_FILE       = PRECOMP  / "private_brand_equivalents.parquet"
# Phase 6: peer-validated replenishment inputs
REPLENISHMENT_FILE  = PRECOMP  / "customer_replenishment_candidates.parquet"
SEG_CADENCE_FILE    = PRECOMP  / "product_segment_cadence.parquet"
FEATURE_FILE        = FEATURES / "customer_features.parquet"
MERGED_FILE         = DATA_CLEAN / "serving" / "merged_dataset.parquet"

# Output files

RECS_OUT          = PRECOMP  / "recommendations.parquet"
RECS_SAMPLE_XLSX  = ANALYSIS / "recommendations_sample.xlsx"


# Configuration

TOP_N_RECS              = 10
TOP_N_PER_TYPE          = 20
TOP_N_CANDIDATES        = 50
MIN_PEER_ADOPTION       = 0.30
MIN_PRODUCT_BUYERS      = 50
AFFORDABILITY_GRACE     = 1.2
RECENCY_MAX_MONTHS      = 6

# Specialty match multipliers
SPECIALTY_MATCH_BOOST    = 1.2
SPECIALTY_NEUTRAL        = 1.0
SPECIALTY_MISMATCH_PENALTY = 0.7
UNIVERSAL_HHI_THRESHOLD  = 0.30

# Cold-start
COLDSTART_MIN_MARKET_PCT = 0.30
COLDSTART_MIN_SIZE_PCT   = 0.25

# Cart complement
CART_MIN_LIFT            = 2.0
CART_TOP_N_PER_CUST      = 15
CART_MAX_HISTORY_PRODS   = 50

# Item similarity
SIM_MIN_SCORE            = 0.10
SIM_TOP_N_PER_CUST       = 15
SIM_MAX_HISTORY_PRODS    = 30

# Lapsed recovery
LAPSED_MIN_HISTORICAL_QTY = 2
LAPSED_TOP_N_PER_CUST     = 10

# Phase 6: Replenishment signal
# customer_replenishment_candidates.parquet is built upstream by
# analyze_buying_patterns.py with peer-validated logic:
#   - Customer must have bought the product before
#   - Peers in the customer's segment must still be buying it (alive >= 30%)
#   - Customer's days_since_last must be >= 1.5x segment median cadence
# Here we just score, rank, and emit recommendations. We do NOT apply
# apply_already_buys_filter for this signal - the whole point is to surface
# things the customer already buys but is overdue on (same exemption that
# lapsed_recovery uses).
REPLENISHMENT_TOP_N_PER_CUST = 10
# Soft cap on overdue_ratio used in scoring. Customers with overdue_ratio
# above this are still surfaced but capped so a 365x outlier doesn't drown
# out a clean 2x candidate. Mirrors how the dollar_factor in PB upgrades
# is clipped before log1p.
REPLENISHMENT_OVERDUE_CAP    = 5.0

# Type-specific score boosts
BOOST_PEER_GAP           = 0.0
BOOST_CART_COMPLEMENT    = 0.5
BOOST_ITEM_SIMILARITY    = 0.3
BOOST_LAPSED_RECOVERY    = 0.7
BOOST_MEDLINE_CONVERSION = 0.8
BOOST_PB_UPGRADE         = 0.5
BOOST_REPLENISHMENT      = 0.6   # Phase 6

# Phase 5: Specialty match enforcement
# When a customer has >= this many history products, we know what they buy,
# so drop "mismatch" recs (specialty_hhi >= 0.30 and not in their specialty).
# Cold-start customers (history < threshold) are exempt - they need broad recs.
SPECIALTY_FILTER_MIN_HISTORY = 10

# Phase 5: Score normalization
# After all signals are generated, normalize scores within each signal type
# to a 0-1 rank-percentile so signals compete fairly in the final ranker.
# This prevents popularity (median ~60) from dominating peer_gap (median ~10),
# while preserving the within-signal ordering.
NORMALIZED_SCORE_COL = "normalized_score"

# Diversification quotas (Phase 3 + Phase 6)
QUOTAS = {
    "medline_conversion":     2,
    "private_brand_upgrade":  2,
    "lapsed_recovery":        2,
    "replenishment":          3,   # Phase 6: peer-validated reorder candidates
    "cart_complement":        3,
    "item_similarity":        2,
    "peer_gap":               4,
    "popularity":             10,
}

BACKFILL_ORDER = [
    "peer_gap", "cart_complement", "popularity",
    "item_similarity", "lapsed_recovery", "replenishment",
]

# Rec purpose mapping (Phase 4 + Phase 6)
# Each signal_type maps to a business purpose for tracking
SIGNAL_TO_PURPOSE = {
    "peer_gap":              "new_product",
    "cart_complement":       "new_product",
    "popularity":            "new_product",
    "lapsed_recovery":       "win_back",
    "replenishment":         "replenishment",   # Phase 6: distinct purpose
    "item_similarity":       "cross_sell",
    "medline_conversion":    "mckesson_substitute",
    "private_brand_upgrade": "mckesson_substitute",
}


# Logging

def _s(title: str) -> None:
    print(f"\n{'-' * 64}\n  {title}\n{'-' * 64}", flush=True)


def _log(msg: str) -> None:
    print(f"  {msg}", flush=True)


# Step 1: Load inputs

def load_inputs() -> dict:
    _s("Step 1: Loading input files")
    t0 = time.time()

    customers = pd.read_parquet(CUST_SEG_FILE)
    customers["DIM_CUST_CURR_ID"] = customers["DIM_CUST_CURR_ID"].astype("int64")
    _log(f"customer_segments        : {len(customers):,} rows")

    features = pd.read_parquet(FEATURE_FILE, columns=[
        "DIM_CUST_CURR_ID", "median_monthly_spend",
        "affordability_ceiling", "active_months_last_12", "SPCLTY_CD",
    ])
    features["DIM_CUST_CURR_ID"] = features["DIM_CUST_CURR_ID"].astype("int64")
    _log(f"customer_features        : {len(features):,} rows")

    patterns = pd.read_parquet(CUST_PATT_FILE)
    patterns["DIM_CUST_CURR_ID"] = patterns["DIM_CUST_CURR_ID"].astype("int64")
    _log(f"customer_patterns        : {len(patterns):,} rows")

    seg_profiles = pd.read_parquet(SEG_PROFILE_FILE)
    _log(f"segment_category_profiles: {len(seg_profiles):,} rows")

    products = pd.read_parquet(PROD_SEG_FILE)
    products["DIM_ITEM_E1_CURR_ID"] = products["DIM_ITEM_E1_CURR_ID"].astype("int64")

    # Filter products with valid descriptions (Phase 4 fix - no nan ITEM_DSC pollution)
    n_before = len(products)
    products = products[
        products["ITEM_DSC"].notna() &
        (products["ITEM_DSC"].astype(str).str.strip() != "") &
        (products["ITEM_DSC"].astype(str).str.lower() != "nan")
    ].copy()
    n_dropped = n_before - len(products)
    _log(f"product_segments         : {len(products):,} rows (dropped {n_dropped:,} with null ITEM_DSC)")

    spec_df = pd.read_parquet(PROD_SPEC_FILE)
    spec_df["DIM_ITEM_E1_CURR_ID"] = spec_df["DIM_ITEM_E1_CURR_ID"].astype("int64")
    _log(f"product_specialty        : {len(spec_df):,} rows")

    item_sim = pd.read_parquet(ITEM_SIM_FILE)
    item_sim["item_a"] = item_sim["item_a"].astype("int64")
    item_sim["item_b"] = item_sim["item_b"].astype("int64")
    _log(f"item_similarity          : {len(item_sim):,} pairs")

    cooccur = pd.read_parquet(COOCCUR_FILE)
    cooccur["product_a"] = cooccur["product_a"].astype("int64")
    cooccur["product_b"] = cooccur["product_b"].astype("int64")
    _log(f"product_cooccurrence     : {len(cooccur):,} pairs")

    lapsed = pd.read_parquet(LAPSED_FILE)
    lapsed["DIM_CUST_CURR_ID"]    = lapsed["DIM_CUST_CURR_ID"].astype("int64")
    lapsed["DIM_ITEM_E1_CURR_ID"] = lapsed["DIM_ITEM_E1_CURR_ID"].astype("int64")
    _log(f"customer_lapsed_products : {len(lapsed):,} pairs")

    pb_equiv = pd.read_parquet(PB_EQUIV_FILE)
    pb_equiv["original_item_id"]   = pb_equiv["original_item_id"].astype("int64")
    pb_equiv["equivalent_item_id"] = pb_equiv["equivalent_item_id"].astype("int64")
    _log(f"private_brand_equivalents: {len(pb_equiv):,} pairs")

    # Phase 6: replenishment candidates and segment cadence
    if REPLENISHMENT_FILE.exists():
        replenishment = pd.read_parquet(REPLENISHMENT_FILE)
        # The Phase 6 file uses cust_id / item_id internally; rename to match
        # everything else in this engine.
        rename_map = {}
        if "cust_id" in replenishment.columns:
            rename_map["cust_id"] = "DIM_CUST_CURR_ID"
        if "item_id" in replenishment.columns:
            rename_map["item_id"] = "DIM_ITEM_E1_CURR_ID"
        if rename_map:
            replenishment = replenishment.rename(columns=rename_map)
        replenishment["DIM_CUST_CURR_ID"]    = replenishment["DIM_CUST_CURR_ID"].astype("int64")
        replenishment["DIM_ITEM_E1_CURR_ID"] = replenishment["DIM_ITEM_E1_CURR_ID"].astype("int64")
        _log(f"customer_replenishment   : {len(replenishment):,} candidates")
    else:
        replenishment = pd.DataFrame()
        _log(f"customer_replenishment   : MISSING (Phase 6 file not found - replenishment signal will be skipped)")

    _log(f"")
    _log(f"All inputs loaded in {time.time()-t0:.1f}s")

    return {
        "customers":   customers, "features": features, "patterns": patterns,
        "seg_profiles": seg_profiles, "products": products, "specialty": spec_df,
        "item_sim": item_sim, "cooccur": cooccur,
        "lapsed": lapsed, "pb_equiv": pb_equiv,
        "replenishment": replenishment,
    }


# Step 2: Customer profile

def build_customer_profile(inputs: dict) -> pd.DataFrame:
    _s("Step 2: Building unified customer profile")
    t0 = time.time()

    cp = inputs["customers"].merge(inputs["features"], on="DIM_CUST_CURR_ID", how="left")
    cp = cp.merge(
        inputs["patterns"][[
            "DIM_CUST_CURR_ID", "is_cold_start", "is_churned", "is_declining",
            "order_cadence_tier", "n_unique_products_total", "is_single_order_customer",
        ]],
        on="DIM_CUST_CURR_ID", how="left"
    )

    cp["is_cold_start"] = cp["is_cold_start"].fillna(1).astype("int8")
    cp["is_churned"]    = cp["is_churned"].fillna(0).astype("int8")
    cp["is_declining"]  = cp["is_declining"].fillna(0).astype("int8")
    cp["order_cadence_tier"]    = cp["order_cadence_tier"].fillna("no_data")
    cp["affordability_ceiling"] = cp["affordability_ceiling"].fillna(0).astype("float32")
    cp["SPCLTY_CD"] = cp["SPCLTY_CD"].fillna("UNKNOWN")
    # Phase 5: keep n_unique_products_total available downstream for the
    # specialty filter (we exempt customers with < SPECIALTY_FILTER_MIN_HISTORY).
    cp["n_unique_products_total"] = cp["n_unique_products_total"].fillna(0).astype("int32")

    n_cold = int(cp["is_cold_start"].sum())
    n_warm = len(cp) - n_cold
    _log(f"  Cold-start: {n_cold:,} ({n_cold/len(cp)*100:.1f}%)")
    _log(f"  Warm      : {n_warm:,} ({n_warm/len(cp)*100:.1f}%)")
    _log(f"Step 2 done in {time.time()-t0:.1f}s")

    return cp


# Step 3: Purchase history

def load_customer_purchase_history() -> pd.DataFrame:
    _s("Step 3: Loading customer purchase history")
    t0 = time.time()

    import duckdb
    con = duckdb.connect()

    history = con.execute(f"""
        SELECT
            CAST(DIM_CUST_CURR_ID AS BIGINT) AS DIM_CUST_CURR_ID,
            CAST(DIM_ITEM_E1_CURR_ID AS BIGINT) AS DIM_ITEM_E1_CURR_ID,
            SUM(UNIT_SLS_AMT) AS total_spend,
            COUNT(*) AS n_lines
        FROM read_parquet('{MERGED_FILE.as_posix()}')
        WHERE UNIT_SLS_AMT > 0
          AND DIM_CUST_CURR_ID IS NOT NULL
          AND DIM_ITEM_E1_CURR_ID IS NOT NULL
        GROUP BY
            CAST(DIM_CUST_CURR_ID AS BIGINT),
            CAST(DIM_ITEM_E1_CURR_ID AS BIGINT)
    """).df()
    con.close()

    history["DIM_CUST_CURR_ID"]   = history["DIM_CUST_CURR_ID"].astype("int64")
    history["DIM_ITEM_E1_CURR_ID"] = history["DIM_ITEM_E1_CURR_ID"].astype("int64")
    history["already_buys"] = 1

    _log(f"Customer-product 'already buys' pairs: {len(history):,}")
    _log(f"Step 3 done in {time.time()-t0:.1f}s")

    return history


# Step 4: Filter eligible products

def filter_eligible_products(products: pd.DataFrame) -> pd.DataFrame:
    _s("Step 4: Filtering eligible products")

    before = len(products)
    products = products[products["is_discontinued"] == 0]
    _log(f"  After is_discontinued=0    : {len(products):,} (dropped {before - len(products):,})")
    before = len(products)
    products = products[products["n_buyers"] >= MIN_PRODUCT_BUYERS]
    _log(f"  After n_buyers>={MIN_PRODUCT_BUYERS}        : {len(products):,} (dropped {before - len(products):,})")
    before = len(products)
    products = products[
        (products["months_since_last_buyer"] <= RECENCY_MAX_MONTHS) |
        (products["months_since_last_buyer"].isna()) |
        (products["months_since_last_buyer"] < 0)
    ]
    _log(f"  After months_since<={RECENCY_MAX_MONTHS}mo : {len(products):,} (dropped {before - len(products):,})")

    return products


# Helper functions

def apply_specialty_scoring(candidates: pd.DataFrame, spec_slim: pd.DataFrame) -> pd.DataFrame:
    candidates = candidates.merge(spec_slim, on="DIM_ITEM_E1_CURR_ID", how="left")

    spec_cols = [f"top_specialty_{i}" for i in range(1, 6)]
    in_top = pd.Series(False, index=candidates.index)
    for col in spec_cols:
        in_top = in_top | (candidates[col] == candidates["SPCLTY_CD"])

    candidates["specialty_match"] = "neutral"
    candidates.loc[in_top, "specialty_match"] = "match"
    is_concentrated = candidates["specialty_hhi"].fillna(0) >= UNIVERSAL_HHI_THRESHOLD
    has_known_spec = candidates["SPCLTY_CD"] != "UNKNOWN"
    is_mismatch = (~in_top) & is_concentrated & has_known_spec
    candidates.loc[is_mismatch, "specialty_match"] = "mismatch"

    candidates["specialty_multiplier"] = candidates["specialty_match"].map({
        "match": SPECIALTY_MATCH_BOOST,
        "neutral": SPECIALTY_NEUTRAL,
        "mismatch": SPECIALTY_MISMATCH_PENALTY,
    }).astype("float32")

    return candidates


def apply_affordability_filter(candidates: pd.DataFrame) -> pd.DataFrame:
    mask = (
        candidates["buyer_affordability_p10"] <=
        candidates["affordability_ceiling"].clip(lower=100) * AFFORDABILITY_GRACE
    )
    return candidates[mask]


def apply_already_buys_filter(candidates: pd.DataFrame, history: pd.DataFrame) -> pd.DataFrame:
    cust_ids = set(candidates["DIM_CUST_CURR_ID"].tolist())
    sub_history = history[history["DIM_CUST_CURR_ID"].isin(cust_ids)][[
        "DIM_CUST_CURR_ID", "DIM_ITEM_E1_CURR_ID", "already_buys"
    ]]
    candidates = candidates.merge(
        sub_history, on=["DIM_CUST_CURR_ID", "DIM_ITEM_E1_CURR_ID"], how="left"
    )
    candidates = candidates[candidates["already_buys"].isna()]
    return candidates.drop(columns=["already_buys"])


# Step 5: Peer gap (segment-chunked + customer-sub-chunked)

def generate_peer_gap_recs(
    cp: pd.DataFrame, seg_profiles: pd.DataFrame,
    products: pd.DataFrame, specialty: pd.DataFrame, history: pd.DataFrame,
) -> pd.DataFrame:
    _s("Step 5: Generating peer-gap recommendations (segment-chunked)")
    t0 = time.time()

    warm = cp[cp["is_cold_start"] == 0].copy()
    _log(f"Warm customers: {len(warm):,}")

    high_adopt_full = seg_profiles[
        seg_profiles["adoption_rate"] >= MIN_PEER_ADOPTION
    ][["segment", "product_family", "adoption_rate"]].copy()
    high_adopt_full = high_adopt_full.rename(columns={
        "adoption_rate": "peer_adoption_rate",
        "product_family": "PROD_FMLY_LVL1_DSC",
    })
    _log(f"  High-adoption pairs: {len(high_adopt_full):,}")

    products = products.copy()
    products["popularity_score"] = (
        np.log1p(products["n_buyers"].astype("float32")) *
        np.log1p(products["recent_buyer_count_6mo"].astype("float32"))
    )
    products["family_rank"] = products.groupby("PROD_FMLY_LVL1_DSC")[
        "popularity_score"
    ].rank(method="first", ascending=False)
    products_top = products[products["family_rank"] <= 100].copy()

    products_top_slim = products_top[[
        "DIM_ITEM_E1_CURR_ID", "ITEM_DSC", "PROD_FMLY_LVL1_DSC",
        "PROD_CTGRY_LVL2_DSC", "n_buyers", "median_unit_price",
        "median_purchases_per_buyer_per_year", "buyer_affordability_p10",
        "is_private_brand", "popularity_score",
        "primary_market", "primary_market_pct",
    ]].copy()

    spec_slim = specialty[[
        "DIM_ITEM_E1_CURR_ID",
        "top_specialty_1", "top_specialty_2", "top_specialty_3",
        "top_specialty_4", "top_specialty_5", "specialty_hhi",
    ]].copy()

    segments = warm["segment"].dropna().unique().tolist()
    _log(f"  Processing {len(segments)} segments...")

    CUST_CHUNK_SIZE = 10000

    def process_segment_chunk(seg_custs_chunk, seg_categories, seg_products):
        if len(seg_custs_chunk) == 0:
            return None

        cust_cats = seg_custs_chunk.merge(seg_categories, on="segment", how="inner")
        candidates = cust_cats.merge(seg_products, on="PROD_FMLY_LVL1_DSC", how="inner")
        if len(candidates) == 0:
            return None

        candidates = apply_affordability_filter(candidates)
        candidates = apply_already_buys_filter(candidates, history)
        if len(candidates) == 0:
            return None

        candidates = apply_specialty_scoring(candidates, spec_slim)

        candidates["base_score"] = (
            candidates["peer_adoption_rate"] *
            np.log1p(candidates["n_buyers"].astype("float32")) *
            candidates["specialty_multiplier"]
        )
        candidates["pb_boost"] = candidates["is_private_brand"].fillna(0) * 0.5
        candidates["declining_boost"] = candidates["is_declining"] * 0.3
        candidates["numeric_score"] = (
            candidates["base_score"] + candidates["pb_boost"] +
            candidates["declining_boost"] + BOOST_PEER_GAP
        ).astype("float32")

        candidates["primary_signal"] = "peer_gap"
        candidates["confidence_tier"] = np.where(
            (candidates["specialty_match"] == "match") &
            (candidates["peer_adoption_rate"] >= 0.5), "high",
            np.where(candidates["specialty_match"] == "mismatch", "low", "medium")
        )

        candidates["rank"] = candidates.groupby("DIM_CUST_CURR_ID")[
            "numeric_score"
        ].rank(method="first", ascending=False)
        candidates = candidates[candidates["rank"] <= TOP_N_PER_TYPE].copy()
        candidates["rank"] = candidates["rank"].astype("int8")

        return candidates

    all_results = []
    for seg_idx, seg in enumerate(segments, 1):
        seg_custs = warm[warm["segment"] == seg][[
            "DIM_CUST_CURR_ID", "segment", "size_tier", "mkt_cd_clean",
            "affordability_ceiling", "SPCLTY_CD", "order_cadence_tier",
            "is_declining", "is_churned", "n_unique_products_total",
        ]]
        if len(seg_custs) == 0:
            continue

        seg_categories = high_adopt_full[high_adopt_full["segment"] == seg]
        if len(seg_categories) == 0:
            continue

        relevant_families = seg_categories["PROD_FMLY_LVL1_DSC"].unique()
        seg_products = products_top_slim[
            products_top_slim["PROD_FMLY_LVL1_DSC"].isin(relevant_families)
        ]

        n_custs = len(seg_custs)
        if n_custs > CUST_CHUNK_SIZE:
            n_chunks = (n_custs + CUST_CHUNK_SIZE - 1) // CUST_CHUNK_SIZE
            for chunk_i in range(n_chunks):
                start = chunk_i * CUST_CHUNK_SIZE
                end   = min(start + CUST_CHUNK_SIZE, n_custs)
                chunk = seg_custs.iloc[start:end].copy()
                result = process_segment_chunk(chunk, seg_categories, seg_products)
                if result is not None and len(result) > 0:
                    all_results.append(result)
                gc.collect()
        else:
            result = process_segment_chunk(seg_custs.copy(), seg_categories, seg_products)
            if result is not None and len(result) > 0:
                all_results.append(result)
            gc.collect()

        if seg_idx % 5 == 0 or seg_idx == len(segments):
            _log(f"    Processed {seg_idx}/{len(segments)} segments  "
                 f"(elapsed {time.time()-t0:.0f}s)")

    if not all_results:
        return pd.DataFrame()

    final = pd.concat(all_results, ignore_index=True)
    _log(f"  Final peer-gap recs: {len(final):,}")
    _log(f"  Customers covered  : {final['DIM_CUST_CURR_ID'].nunique():,}")
    _log(f"Step 5 done in {time.time()-t0:.1f}s")
    return final


# Step 6: Cold-start

def generate_coldstart_recs(
    cp: pd.DataFrame, products: pd.DataFrame,
    specialty: pd.DataFrame, history: pd.DataFrame,
) -> pd.DataFrame:
    _s("Step 6: Generating cold-start recommendations")
    t0 = time.time()

    cold = cp[cp["is_cold_start"] == 1].copy()
    _log(f"Cold-start customers: {len(cold):,}")

    market_size_pairs = cold[["mkt_cd_clean", "size_tier"]].drop_duplicates()

    all_recs = []
    for _, row in market_size_pairs.iterrows():
        mkt, tier = row["mkt_cd_clean"], row["size_tier"]
        mkt_col, tier_col = f"pct_buyers_{mkt}", f"pct_buyers_{tier}"

        if mkt_col not in products.columns or tier_col not in products.columns:
            continue

        pool = products[
            (products[mkt_col]  >= COLDSTART_MIN_MARKET_PCT) &
            (products[tier_col] >= COLDSTART_MIN_SIZE_PCT)
        ].copy()
        if len(pool) == 0:
            pool = products[products[mkt_col] >= COLDSTART_MIN_MARKET_PCT].copy()
        if len(pool) == 0:
            pool = products.copy()

        pool["mkt_cd_clean"] = mkt
        pool["size_tier"]    = tier
        pool["popularity_score"] = (
            np.log1p(pool["n_buyers"].astype("float32")) *
            np.log1p(pool["recent_buyer_count_6mo"].astype("float32"))
        )
        pool = pool.nlargest(TOP_N_CANDIDATES, "popularity_score")
        all_recs.append(pool)

    pool_all = pd.concat(all_recs, ignore_index=True)

    candidates = cold[[
        "DIM_CUST_CURR_ID", "segment", "size_tier", "mkt_cd_clean",
        "affordability_ceiling", "SPCLTY_CD", "n_unique_products_total",
    ]].merge(
        pool_all[[
            "DIM_ITEM_E1_CURR_ID", "ITEM_DSC", "PROD_FMLY_LVL1_DSC",
            "PROD_CTGRY_LVL2_DSC", "n_buyers", "median_unit_price",
            "median_purchases_per_buyer_per_year", "buyer_affordability_p10",
            "is_private_brand", "popularity_score",
            "primary_market", "primary_market_pct",
            "mkt_cd_clean", "size_tier",
        ]],
        on=["mkt_cd_clean", "size_tier"], how="inner"
    )

    candidates = apply_affordability_filter(candidates)
    candidates = apply_already_buys_filter(candidates, history)

    spec_slim = specialty[[
        "DIM_ITEM_E1_CURR_ID",
        "top_specialty_1", "top_specialty_2", "top_specialty_3",
        "top_specialty_4", "top_specialty_5", "specialty_hhi",
    ]].copy()
    candidates = apply_specialty_scoring(candidates, spec_slim)

    candidates["base_score"] = candidates["popularity_score"].astype("float32")
    candidates["pb_boost"]   = candidates["is_private_brand"].fillna(0) * 0.3
    candidates["spec_boost"] = (candidates["specialty_match"] == "match").astype("float32") * 0.5
    candidates["declining_boost"] = 0.0
    candidates["numeric_score"] = (
        candidates["base_score"] + candidates["pb_boost"] + candidates["spec_boost"]
    ).astype("float32")

    candidates["primary_signal"]      = "popularity"
    candidates["confidence_tier"]     = "medium"
    candidates["peer_adoption_rate"]  = 0.0
    candidates["is_declining"] = 0
    candidates["is_churned"]   = 0
    candidates["order_cadence_tier"] = "no_data"

    candidates["rank"] = candidates.groupby("DIM_CUST_CURR_ID")[
        "numeric_score"
    ].rank(method="first", ascending=False)
    candidates = candidates[candidates["rank"] <= TOP_N_PER_TYPE].copy()
    candidates["rank"] = candidates["rank"].astype("int8")

    _log(f"  Final cold-start recs: {len(candidates):,}")
    _log(f"  Customers covered    : {candidates['DIM_CUST_CURR_ID'].nunique():,}")
    _log(f"Step 6 done in {time.time()-t0:.1f}s")
    return candidates


# Step 7: Cart complement

def generate_cart_complement_recs(
    cp: pd.DataFrame, cooccur: pd.DataFrame,
    products: pd.DataFrame, specialty: pd.DataFrame, history: pd.DataFrame,
) -> pd.DataFrame:
    _s("Step 7: Generating cart complement recommendations")
    t0 = time.time()

    warm = cp[cp["is_cold_start"] == 0].copy()

    cooccur_filt = cooccur[cooccur["lift"] >= CART_MIN_LIFT][[
        "product_a", "product_b", "lift", "support_ab"
    ]].copy()
    cooccur_filt = cooccur_filt.rename(columns={
        "product_a": "DIM_ITEM_E1_CURR_ID",
        "product_b": "rec_item_id",
        "support_ab": "support",
    })
    _log(f"  High-lift pairs (lift>={CART_MIN_LIFT}): {len(cooccur_filt):,}")

    products_slim = products[[
        "DIM_ITEM_E1_CURR_ID", "ITEM_DSC", "PROD_FMLY_LVL1_DSC",
        "PROD_CTGRY_LVL2_DSC", "n_buyers", "median_unit_price",
        "median_purchases_per_buyer_per_year", "buyer_affordability_p10",
        "is_private_brand", "primary_market", "primary_market_pct",
    ]].copy()

    spec_slim = specialty[[
        "DIM_ITEM_E1_CURR_ID",
        "top_specialty_1", "top_specialty_2", "top_specialty_3",
        "top_specialty_4", "top_specialty_5", "specialty_hhi",
    ]].copy()

    cust_profile_slim = warm[[
        "DIM_CUST_CURR_ID", "segment", "size_tier", "mkt_cd_clean",
        "affordability_ceiling", "SPCLTY_CD", "order_cadence_tier",
        "is_declining", "is_churned", "n_unique_products_total",
    ]]

    segments = warm["segment"].dropna().unique().tolist()
    _log(f"  Processing {len(segments)} segments...")

    CUST_CHUNK_SIZE = 10000

    def process_cart_chunk(chunk_cust_ids):
        chunk_history = history[history["DIM_CUST_CURR_ID"].isin(chunk_cust_ids)].copy()
        chunk_history["spend_rank"] = chunk_history.groupby("DIM_CUST_CURR_ID")[
            "total_spend"
        ].rank(method="first", ascending=False)
        chunk_history = chunk_history[chunk_history["spend_rank"] <= CART_MAX_HISTORY_PRODS]

        candidates = chunk_history[[
            "DIM_CUST_CURR_ID", "DIM_ITEM_E1_CURR_ID"
        ]].merge(cooccur_filt, on="DIM_ITEM_E1_CURR_ID", how="inner")
        if len(candidates) == 0:
            return None

        candidates = candidates.groupby(
            ["DIM_CUST_CURR_ID", "rec_item_id"], as_index=False
        ).agg({"lift": "max", "support": "max"})
        candidates = candidates.rename(columns={"rec_item_id": "DIM_ITEM_E1_CURR_ID"})

        candidates = candidates.merge(cust_profile_slim, on="DIM_CUST_CURR_ID", how="inner")
        candidates = candidates.merge(products_slim, on="DIM_ITEM_E1_CURR_ID", how="inner")

        candidates = apply_affordability_filter(candidates)
        candidates = apply_already_buys_filter(candidates, history)
        if len(candidates) == 0:
            return None

        candidates = apply_specialty_scoring(candidates, spec_slim)

        candidates["base_score"] = (
            np.log1p(candidates["lift"].astype("float32")) * 2.0 *
            candidates["specialty_multiplier"]
        )
        candidates["pb_boost"] = candidates["is_private_brand"].fillna(0) * 0.3
        candidates["declining_boost"] = candidates["is_declining"] * 0.2
        candidates["numeric_score"] = (
            candidates["base_score"] + candidates["pb_boost"] +
            candidates["declining_boost"] + BOOST_CART_COMPLEMENT
        ).astype("float32")

        candidates["primary_signal"]    = "cart_complement"
        candidates["peer_adoption_rate"] = 0.0
        candidates["confidence_tier"] = np.where(
            candidates["lift"] >= 5.0, "high",
            np.where(candidates["specialty_match"] == "mismatch", "low", "medium")
        )

        candidates["rank"] = candidates.groupby("DIM_CUST_CURR_ID")[
            "numeric_score"
        ].rank(method="first", ascending=False)
        candidates = candidates[candidates["rank"] <= CART_TOP_N_PER_CUST].copy()
        candidates["rank"] = candidates["rank"].astype("int8")

        return candidates

    all_results = []
    for seg_idx, seg in enumerate(segments, 1):
        seg_cust_list = warm[warm["segment"] == seg]["DIM_CUST_CURR_ID"].tolist()
        if not seg_cust_list:
            continue

        n_custs = len(seg_cust_list)
        if n_custs > CUST_CHUNK_SIZE:
            n_chunks = (n_custs + CUST_CHUNK_SIZE - 1) // CUST_CHUNK_SIZE
            for chunk_i in range(n_chunks):
                start = chunk_i * CUST_CHUNK_SIZE
                end   = min(start + CUST_CHUNK_SIZE, n_custs)
                chunk_ids = set(seg_cust_list[start:end])
                result = process_cart_chunk(chunk_ids)
                if result is not None and len(result) > 0:
                    all_results.append(result)
                gc.collect()
        else:
            result = process_cart_chunk(set(seg_cust_list))
            if result is not None and len(result) > 0:
                all_results.append(result)
            gc.collect()

        if seg_idx % 5 == 0 or seg_idx == len(segments):
            _log(f"    Processed {seg_idx}/{len(segments)} segments  "
                 f"(elapsed {time.time()-t0:.0f}s)")

    if not all_results:
        return pd.DataFrame()

    final = pd.concat(all_results, ignore_index=True)
    _log(f"  Final cart-complement recs: {len(final):,}")
    _log(f"  Customers covered         : {final['DIM_CUST_CURR_ID'].nunique():,}")
    _log(f"Step 7 done in {time.time()-t0:.1f}s")
    return final


# Step 8: Item similarity

def generate_item_similarity_recs(
    cp: pd.DataFrame, item_sim: pd.DataFrame,
    products: pd.DataFrame, specialty: pd.DataFrame, history: pd.DataFrame,
) -> pd.DataFrame:
    _s("Step 8: Generating item similarity recommendations")
    t0 = time.time()

    warm = cp[cp["is_cold_start"] == 0].copy()

    sim_filt = item_sim[item_sim["similarity"] >= SIM_MIN_SCORE][[
        "item_a", "item_b", "similarity"
    ]].copy()
    sim_filt = sim_filt.rename(columns={
        "item_a": "DIM_ITEM_E1_CURR_ID",
        "item_b": "rec_item_id",
    })
    _log(f"  Similarity pairs (sim>={SIM_MIN_SCORE}): {len(sim_filt):,}")

    products_slim = products[[
        "DIM_ITEM_E1_CURR_ID", "ITEM_DSC", "PROD_FMLY_LVL1_DSC",
        "PROD_CTGRY_LVL2_DSC", "n_buyers", "median_unit_price",
        "median_purchases_per_buyer_per_year", "buyer_affordability_p10",
        "is_private_brand", "primary_market", "primary_market_pct",
    ]].copy()

    spec_slim = specialty[[
        "DIM_ITEM_E1_CURR_ID",
        "top_specialty_1", "top_specialty_2", "top_specialty_3",
        "top_specialty_4", "top_specialty_5", "specialty_hhi",
    ]].copy()

    cust_profile_slim = warm[[
        "DIM_CUST_CURR_ID", "segment", "size_tier", "mkt_cd_clean",
        "affordability_ceiling", "SPCLTY_CD", "order_cadence_tier",
        "is_declining", "is_churned", "n_unique_products_total",
    ]]

    segments = warm["segment"].dropna().unique().tolist()
    _log(f"  Processing {len(segments)} segments...")

    CUST_CHUNK_SIZE = 10000

    def process_sim_chunk(chunk_cust_ids):
        chunk_history = history[history["DIM_CUST_CURR_ID"].isin(chunk_cust_ids)].copy()
        chunk_history["spend_rank"] = chunk_history.groupby("DIM_CUST_CURR_ID")[
            "total_spend"
        ].rank(method="first", ascending=False)
        chunk_history = chunk_history[chunk_history["spend_rank"] <= SIM_MAX_HISTORY_PRODS]

        candidates = chunk_history[[
            "DIM_CUST_CURR_ID", "DIM_ITEM_E1_CURR_ID"
        ]].merge(sim_filt, on="DIM_ITEM_E1_CURR_ID", how="inner")
        if len(candidates) == 0:
            return None

        candidates = candidates.groupby(
            ["DIM_CUST_CURR_ID", "rec_item_id"], as_index=False
        ).agg({"similarity": "max"})
        candidates = candidates.rename(columns={"rec_item_id": "DIM_ITEM_E1_CURR_ID"})

        candidates = candidates.merge(cust_profile_slim, on="DIM_CUST_CURR_ID", how="inner")
        candidates = candidates.merge(products_slim, on="DIM_ITEM_E1_CURR_ID", how="inner")

        candidates = apply_affordability_filter(candidates)
        candidates = apply_already_buys_filter(candidates, history)
        if len(candidates) == 0:
            return None

        candidates = apply_specialty_scoring(candidates, spec_slim)

        candidates["base_score"] = (
            candidates["similarity"].astype("float32") * 5.0 *
            candidates["specialty_multiplier"]
        )
        candidates["pb_boost"] = candidates["is_private_brand"].fillna(0) * 0.3
        candidates["declining_boost"] = candidates["is_declining"] * 0.2
        candidates["numeric_score"] = (
            candidates["base_score"] + candidates["pb_boost"] +
            candidates["declining_boost"] + BOOST_ITEM_SIMILARITY
        ).astype("float32")

        candidates["primary_signal"]    = "item_similarity"
        candidates["peer_adoption_rate"] = 0.0
        candidates["confidence_tier"] = np.where(
            candidates["similarity"] >= 0.30, "high",
            np.where(candidates["specialty_match"] == "mismatch", "low", "medium")
        )

        candidates["rank"] = candidates.groupby("DIM_CUST_CURR_ID")[
            "numeric_score"
        ].rank(method="first", ascending=False)
        candidates = candidates[candidates["rank"] <= SIM_TOP_N_PER_CUST].copy()
        candidates["rank"] = candidates["rank"].astype("int8")

        return candidates

    all_results = []
    for seg_idx, seg in enumerate(segments, 1):
        seg_cust_list = warm[warm["segment"] == seg]["DIM_CUST_CURR_ID"].tolist()
        if not seg_cust_list:
            continue

        n_custs = len(seg_cust_list)
        if n_custs > CUST_CHUNK_SIZE:
            n_chunks = (n_custs + CUST_CHUNK_SIZE - 1) // CUST_CHUNK_SIZE
            for chunk_i in range(n_chunks):
                start = chunk_i * CUST_CHUNK_SIZE
                end   = min(start + CUST_CHUNK_SIZE, n_custs)
                chunk_ids = set(seg_cust_list[start:end])
                result = process_sim_chunk(chunk_ids)
                if result is not None and len(result) > 0:
                    all_results.append(result)
                gc.collect()
        else:
            result = process_sim_chunk(set(seg_cust_list))
            if result is not None and len(result) > 0:
                all_results.append(result)
            gc.collect()

        if seg_idx % 5 == 0 or seg_idx == len(segments):
            _log(f"    Processed {seg_idx}/{len(segments)} segments  "
                 f"(elapsed {time.time()-t0:.0f}s)")

    if not all_results:
        return pd.DataFrame()

    final = pd.concat(all_results, ignore_index=True)
    _log(f"  Final item-similarity recs: {len(final):,}")
    _log(f"  Customers covered         : {final['DIM_CUST_CURR_ID'].nunique():,}")
    _log(f"Step 8 done in {time.time()-t0:.1f}s")
    return final


# Step 9: Lapsed recovery

def generate_lapsed_recovery_recs(
    cp: pd.DataFrame, lapsed: pd.DataFrame,
    products: pd.DataFrame, specialty: pd.DataFrame,
) -> pd.DataFrame:
    _s("Step 9: Generating lapsed recovery recommendations")
    t0 = time.time()

    if "total_qty" in lapsed.columns:
        lapsed_filt = lapsed[lapsed["total_qty"] >= LAPSED_MIN_HISTORICAL_QTY].copy()
    else:
        lapsed_filt = lapsed.copy()
    _log(f"  Lapsed pairs (qty>={LAPSED_MIN_HISTORICAL_QTY}): {len(lapsed_filt):,}")

    products_slim = products[[
        "DIM_ITEM_E1_CURR_ID", "ITEM_DSC", "PROD_FMLY_LVL1_DSC",
        "PROD_CTGRY_LVL2_DSC", "n_buyers", "median_unit_price",
        "median_purchases_per_buyer_per_year", "buyer_affordability_p10",
        "is_private_brand", "primary_market", "primary_market_pct",
    ]].copy()

    spec_slim = specialty[[
        "DIM_ITEM_E1_CURR_ID",
        "top_specialty_1", "top_specialty_2", "top_specialty_3",
        "top_specialty_4", "top_specialty_5", "specialty_hhi",
    ]].copy()

    cust_profile_slim = cp[[
        "DIM_CUST_CURR_ID", "segment", "size_tier", "mkt_cd_clean",
        "affordability_ceiling", "SPCLTY_CD", "order_cadence_tier",
        "is_declining", "is_churned", "n_unique_products_total",
    ]]

    candidates = lapsed_filt.merge(cust_profile_slim, on="DIM_CUST_CURR_ID", how="inner")
    candidates = candidates.merge(products_slim, on="DIM_ITEM_E1_CURR_ID", how="inner")
    candidates = apply_affordability_filter(candidates)
    candidates = apply_specialty_scoring(candidates, spec_slim)

    if "total_qty" in candidates.columns:
        qty_score = np.log1p(candidates["total_qty"].astype("float32"))
    else:
        qty_score = pd.Series(1.0, index=candidates.index, dtype="float32")

    candidates["base_score"] = (qty_score * 2.0 * candidates["specialty_multiplier"]).astype("float32")
    candidates["pb_boost"] = candidates["is_private_brand"].fillna(0) * 0.3
    candidates["declining_boost"] = (
        candidates["is_declining"] * 0.5 + candidates["is_churned"] * 0.7
    )
    candidates["numeric_score"] = (
        candidates["base_score"] + candidates["pb_boost"] +
        candidates["declining_boost"] + BOOST_LAPSED_RECOVERY
    ).astype("float32")

    candidates["primary_signal"]    = "lapsed_recovery"
    candidates["peer_adoption_rate"] = 0.0
    candidates["confidence_tier"] = np.where(
        (candidates["is_declining"] == 1) | (candidates["is_churned"] == 1), "high",
        np.where(candidates["specialty_match"] == "mismatch", "low", "medium")
    )

    candidates["rank"] = candidates.groupby("DIM_CUST_CURR_ID")[
        "numeric_score"
    ].rank(method="first", ascending=False)
    candidates = candidates[candidates["rank"] <= LAPSED_TOP_N_PER_CUST].copy()
    candidates["rank"] = candidates["rank"].astype("int8")

    _log(f"  Final lapsed recovery recs: {len(candidates):,}")
    _log(f"  Customers covered         : {candidates['DIM_CUST_CURR_ID'].nunique():,}")
    _log(f"Step 9 done in {time.time()-t0:.1f}s")
    return candidates


# Step 9b (Phase 6): Replenishment - peer-validated reorder candidates
#
# This signal comes from the precomputed customer_replenishment_candidates.parquet
# built in analyze_buying_patterns.py with peer-validation logic:
#   - Customer must have bought the product before
#   - Peers in the same segment must still be buying it (alive >= 30%)
#   - Customer is overdue: days_since_last >= 1.5x segment median cadence
#
# This is the only signal besides lapsed_recovery that does NOT call
# apply_already_buys_filter - the whole point is to surface things the
# customer already buys but is overdue on.
#
# Scoring uses:
#   - log1p(overdue_ratio) clipped to REPLENISHMENT_OVERDUE_CAP - so a 30x
#     overdue (ie. dead products) doesn't drown out a clean 2x candidate.
#   - peer_activity_rate as a confidence multiplier - the more peers still
#     active, the more reliable the signal.
#   - specialty_multiplier from the standard specialty scoring path.
#   - declining/churned customer boost (these customers are exactly the
#     replenishment use case).

def generate_replenishment_recs(
    cp: pd.DataFrame, replenishment: pd.DataFrame,
    products: pd.DataFrame, specialty: pd.DataFrame,
) -> pd.DataFrame:
    _s("Step 9b: Generating replenishment recommendations (Phase 6)")
    t0 = time.time()

    if len(replenishment) == 0:
        _log("  No replenishment candidates available - skipping")
        return pd.DataFrame()

    _log(f"  Input replenishment candidates: {len(replenishment):,}")

    # Keep only the columns we need from the precomputed file
    needed_cols = [
        "DIM_CUST_CURR_ID", "DIM_ITEM_E1_CURR_ID",
        "days_since_last", "median_days_between_segment",
        "overdue_ratio", "peer_activity_rate", "n_buyers_segment",
    ]
    keep_cols = [c for c in needed_cols if c in replenishment.columns]
    repl = replenishment[keep_cols].copy()

    # Make sure required columns exist; if not, bail gracefully
    if "overdue_ratio" not in repl.columns:
        _log("  WARNING: overdue_ratio column missing - skipping replenishment")
        return pd.DataFrame()
    if "peer_activity_rate" not in repl.columns:
        repl["peer_activity_rate"] = 0.5  # neutral fallback
    if "median_days_between_segment" not in repl.columns:
        repl["median_days_between_segment"] = np.nan
    if "days_since_last" not in repl.columns:
        repl["days_since_last"] = -1

    # Slim products and specialty for the merge
    products_slim = products[[
        "DIM_ITEM_E1_CURR_ID", "ITEM_DSC", "PROD_FMLY_LVL1_DSC",
        "PROD_CTGRY_LVL2_DSC", "n_buyers", "median_unit_price",
        "median_purchases_per_buyer_per_year", "buyer_affordability_p10",
        "is_private_brand", "primary_market", "primary_market_pct",
    ]].copy()

    spec_slim = specialty[[
        "DIM_ITEM_E1_CURR_ID",
        "top_specialty_1", "top_specialty_2", "top_specialty_3",
        "top_specialty_4", "top_specialty_5", "specialty_hhi",
    ]].copy()

    cust_profile_slim = cp[[
        "DIM_CUST_CURR_ID", "segment", "size_tier", "mkt_cd_clean",
        "affordability_ceiling", "SPCLTY_CD", "order_cadence_tier",
        "is_declining", "is_churned", "n_unique_products_total",
    ]]

    # Merge in everything we need
    candidates = repl.merge(cust_profile_slim, on="DIM_CUST_CURR_ID", how="inner")
    n_after_cust = len(candidates)
    candidates = candidates.merge(products_slim, on="DIM_ITEM_E1_CURR_ID", how="inner")
    n_after_prod = len(candidates)
    _log(f"  After cust merge: {n_after_cust:,}")
    _log(f"  After product merge (drops products outside eligible pool): {n_after_prod:,}")

    if len(candidates) == 0:
        _log("  No candidates after merges - skipping")
        return pd.DataFrame()

    # Apply standard filters. NOTE: NO apply_already_buys_filter - replenishment
    # is intentionally for products the customer already buys.
    candidates = apply_affordability_filter(candidates)
    if len(candidates) == 0:
        return pd.DataFrame()

    candidates = apply_specialty_scoring(candidates, spec_slim)

    # Scoring
    # Cap overdue_ratio so a 365x outlier doesn't dominate. A 5x clip means
    # anything beyond "5x past expected reorder" gets the same numeric_score
    # contribution from overdueness - so a clean 2x candidate doesn't lose
    # to noise.
    overdue_clipped = candidates["overdue_ratio"].clip(upper=REPLENISHMENT_OVERDUE_CAP).astype("float32")
    overdue_score = np.log1p(overdue_clipped)

    # peer confidence: how many peers still buying it (0-1)
    peer_conf = candidates["peer_activity_rate"].clip(lower=0.0, upper=1.0).astype("float32")

    candidates["base_score"] = (
        overdue_score * (1.0 + peer_conf) * 3.0 *
        candidates["specialty_multiplier"]
    ).astype("float32")
    candidates["pb_boost"] = candidates["is_private_brand"].fillna(0) * 0.3
    # Declining/churned customers are the prime replenishment audience
    candidates["declining_boost"] = (
        candidates["is_declining"] * 0.5 + candidates["is_churned"] * 0.7
    ).astype("float32")
    candidates["numeric_score"] = (
        candidates["base_score"] + candidates["pb_boost"] +
        candidates["declining_boost"] + BOOST_REPLENISHMENT
    ).astype("float32")

    candidates["primary_signal"]    = "replenishment"
    candidates["peer_adoption_rate"] = peer_conf  # reuse this column for tracking
    # Confidence: high when peers are very active AND overdue is in a
    # believable range (1.5x to 3x). Low when specialty mismatches.
    is_clean_overdue = (
        (candidates["overdue_ratio"] >= 1.5) & (candidates["overdue_ratio"] <= 3.0)
    )
    is_strong_peers = candidates["peer_activity_rate"] >= 0.5
    candidates["confidence_tier"] = np.where(
        is_clean_overdue & is_strong_peers, "high",
        np.where(candidates["specialty_match"] == "mismatch", "low", "medium")
    )

    candidates["rank"] = candidates.groupby("DIM_CUST_CURR_ID")[
        "numeric_score"
    ].rank(method="first", ascending=False)
    candidates = candidates[candidates["rank"] <= REPLENISHMENT_TOP_N_PER_CUST].copy()
    candidates["rank"] = candidates["rank"].astype("int8")

    _log(f"  Final replenishment recs: {len(candidates):,}")
    _log(f"  Customers covered       : {candidates['DIM_CUST_CURR_ID'].nunique():,}")
    if len(candidates) > 0:
        _log(f"  Score stats: min={candidates['numeric_score'].min():.2f}  "
             f"median={candidates['numeric_score'].median():.2f}  "
             f"max={candidates['numeric_score'].max():.2f}")
        _log(f"  Overdue stats (in selected): "
             f"p10={candidates['overdue_ratio'].quantile(0.10):.2f}x  "
             f"median={candidates['overdue_ratio'].quantile(0.50):.2f}x  "
             f"p90={candidates['overdue_ratio'].quantile(0.90):.2f}x")
    _log(f"Step 9b done in {time.time()-t0:.1f}s")
    return candidates


# Step 10: Medline conversion

def generate_medline_conversion_recs(
    cp: pd.DataFrame, pb_equiv: pd.DataFrame, history: pd.DataFrame,
    products: pd.DataFrame, specialty: pd.DataFrame,
) -> pd.DataFrame:
    _s("Step 10: Generating Medline conversion recommendations")
    t0 = time.time()

    medline = pb_equiv[pb_equiv["match_type"] == "medline_conversion"].copy()

    # Drop price anomalies (Phase 4 fix - prevents implausible price drops)
    n_before = len(medline)
    if "price_anomaly" in medline.columns:
        medline = medline[medline["price_anomaly"].fillna(0) == 0]
        _log(f"  Medline pairs after dropping price anomalies: {len(medline):,} (dropped {n_before - len(medline):,})")
    else:
        _log(f"  Medline pairs: {len(medline):,}")

    if len(medline) == 0:
        return pd.DataFrame()

    medline_orig_items = set(medline["original_item_id"].unique())
    medline_buyers = history[history["DIM_ITEM_E1_CURR_ID"].isin(medline_orig_items)][[
        "DIM_CUST_CURR_ID", "DIM_ITEM_E1_CURR_ID"
    ]].rename(columns={"DIM_ITEM_E1_CURR_ID": "original_item_id"})
    _log(f"  Customer-Medline pairs: {len(medline_buyers):,}")

    candidates = medline_buyers.merge(medline, on="original_item_id", how="inner")
    candidates = candidates.rename(columns={"equivalent_item_id": "DIM_ITEM_E1_CURR_ID"})

    products_slim = products[[
        "DIM_ITEM_E1_CURR_ID", "ITEM_DSC", "PROD_FMLY_LVL1_DSC",
        "PROD_CTGRY_LVL2_DSC", "n_buyers", "median_unit_price",
        "median_purchases_per_buyer_per_year", "buyer_affordability_p10",
        "is_private_brand", "primary_market", "primary_market_pct",
    ]].copy()

    spec_slim = specialty[[
        "DIM_ITEM_E1_CURR_ID",
        "top_specialty_1", "top_specialty_2", "top_specialty_3",
        "top_specialty_4", "top_specialty_5", "specialty_hhi",
    ]].copy()

    cust_profile_slim = cp[[
        "DIM_CUST_CURR_ID", "segment", "size_tier", "mkt_cd_clean",
        "affordability_ceiling", "SPCLTY_CD", "order_cadence_tier",
        "is_declining", "is_churned", "n_unique_products_total",
    ]]

    candidates = candidates.merge(cust_profile_slim, on="DIM_CUST_CURR_ID", how="inner")
    candidates = candidates.merge(products_slim, on="DIM_ITEM_E1_CURR_ID", how="inner")
    candidates = apply_affordability_filter(candidates)
    candidates = apply_already_buys_filter(candidates, history)
    if len(candidates) == 0:
        return pd.DataFrame()

    candidates = apply_specialty_scoring(candidates, spec_slim)

    candidates["base_score"] = (3.0 * candidates["specialty_multiplier"]).astype("float32")
    candidates["pb_boost"] = candidates["is_private_brand"].fillna(0) * 0.3
    candidates["declining_boost"] = candidates["is_declining"] * 0.2
    candidates["numeric_score"] = (
        candidates["base_score"] + candidates["pb_boost"] +
        candidates["declining_boost"] + BOOST_MEDLINE_CONVERSION
    ).astype("float32")

    candidates["primary_signal"]    = "medline_conversion"
    candidates["peer_adoption_rate"] = 0.0
    candidates["confidence_tier"] = "high"

    candidates["rank"] = candidates.groupby("DIM_CUST_CURR_ID")[
        "numeric_score"
    ].rank(method="first", ascending=False)
    candidates = candidates[candidates["rank"] <= 5].copy()
    candidates["rank"] = candidates["rank"].astype("int8")

    _log(f"  Final Medline recs: {len(candidates):,}")
    _log(f"  Customers covered : {candidates['DIM_CUST_CURR_ID'].nunique():,}")
    _log(f"Step 10 done in {time.time()-t0:.1f}s")
    return candidates


# Step 11: Private brand upgrade
# Phase 5 fix: score now scales with absolute dollar savings, not just % savings.
# Old formula: (savings_pct * 5 + 1) * specialty_multiplier
#   -> 26% savings on $2 tape and 26% savings on $50 catheter scored identically.
# New formula: (savings_pct * 5 + 1) * dollar_factor * specialty_multiplier
#   where dollar_factor = log1p(abs_savings_dollars) clipped to a sensible range.
# This pushes high-dollar-impact PB upgrades up the ranker without breaking
# within-signal ordering for low-savings cases.

def generate_pb_upgrade_recs(
    cp: pd.DataFrame, pb_equiv: pd.DataFrame, history: pd.DataFrame,
    products: pd.DataFrame, specialty: pd.DataFrame,
) -> pd.DataFrame:
    _s("Step 11: Generating private brand upgrade recommendations")
    t0 = time.time()

    pb_upgrade = pb_equiv[pb_equiv["match_type"] == "private_brand_upgrade"].copy()

    # Drop price anomalies
    n_before = len(pb_upgrade)
    if "price_anomaly" in pb_upgrade.columns:
        pb_upgrade = pb_upgrade[pb_upgrade["price_anomaly"].fillna(0) == 0]
        _log(f"  PB upgrade pairs after dropping price anomalies: {len(pb_upgrade):,} (dropped {n_before - len(pb_upgrade):,})")
    else:
        _log(f"  PB upgrade pairs: {len(pb_upgrade):,}")

    if len(pb_upgrade) == 0:
        return pd.DataFrame()

    pb_orig_items = set(pb_upgrade["original_item_id"].unique())
    pb_buyers = history[history["DIM_ITEM_E1_CURR_ID"].isin(pb_orig_items)][[
        "DIM_CUST_CURR_ID", "DIM_ITEM_E1_CURR_ID"
    ]].rename(columns={"DIM_ITEM_E1_CURR_ID": "original_item_id"})
    _log(f"  Customer-NationalBrand pairs: {len(pb_buyers):,}")

    candidates = pb_buyers.merge(pb_upgrade, on="original_item_id", how="inner")
    candidates = candidates.rename(columns={"equivalent_item_id": "DIM_ITEM_E1_CURR_ID"})

    products_slim = products[[
        "DIM_ITEM_E1_CURR_ID", "ITEM_DSC", "PROD_FMLY_LVL1_DSC",
        "PROD_CTGRY_LVL2_DSC", "n_buyers", "median_unit_price",
        "median_purchases_per_buyer_per_year", "buyer_affordability_p10",
        "is_private_brand", "primary_market", "primary_market_pct",
    ]].copy()

    spec_slim = specialty[[
        "DIM_ITEM_E1_CURR_ID",
        "top_specialty_1", "top_specialty_2", "top_specialty_3",
        "top_specialty_4", "top_specialty_5", "specialty_hhi",
    ]].copy()

    cust_profile_slim = cp[[
        "DIM_CUST_CURR_ID", "segment", "size_tier", "mkt_cd_clean",
        "affordability_ceiling", "SPCLTY_CD", "order_cadence_tier",
        "is_declining", "is_churned", "n_unique_products_total",
    ]]

    candidates = candidates.merge(cust_profile_slim, on="DIM_CUST_CURR_ID", how="inner")
    candidates = candidates.merge(products_slim, on="DIM_ITEM_E1_CURR_ID", how="inner")
    candidates = apply_affordability_filter(candidates)
    candidates = apply_already_buys_filter(candidates, history)
    if len(candidates) == 0:
        return pd.DataFrame()

    candidates = apply_specialty_scoring(candidates, spec_slim)

    # Percent savings (existing logic)
    if "price_delta_pct" in candidates.columns:
        savings = candidates["price_delta_pct"].fillna(0).astype("float32") * -1
        candidates["savings_score"] = savings.clip(lower=0)
    else:
        candidates["savings_score"] = pd.Series(0.10, index=candidates.index, dtype="float32")

    # Phase 5: absolute dollar savings factor.
    # Compute from original_unit_price - equivalent_unit_price when available.
    # Fall back to median_unit_price * savings_score for older equivalent files.
    if (
        "original_unit_price" in candidates.columns
        and "equivalent_unit_price" in candidates.columns
    ):
        abs_savings = (
            candidates["original_unit_price"].fillna(0).astype("float32") -
            candidates["equivalent_unit_price"].fillna(0).astype("float32")
        ).clip(lower=0)
    else:
        # Fallback: estimate from the equivalent's median price and the savings %.
        # If equiv price = X and savings pct = s, then original = X / (1 - s),
        # so abs_savings = X * s / (1 - s). Clip s to avoid divide-by-zero.
        s_clipped = candidates["savings_score"].clip(upper=0.95)
        eq_price = candidates["median_unit_price"].fillna(0).astype("float32")
        abs_savings = (eq_price * s_clipped / (1.0 - s_clipped).clip(lower=0.05)).clip(lower=0)

    # log1p smooths high outliers; clip at 50 (~$50 savings) so a $500 catheter
    # doesn't completely dominate the ranker.
    candidates["dollar_factor"] = np.log1p(abs_savings.clip(upper=50.0)).astype("float32")

    candidates["base_score"] = (
        (candidates["savings_score"] * 5.0 + 1.0) *
        (1.0 + candidates["dollar_factor"]) *
        candidates["specialty_multiplier"]
    ).astype("float32")
    candidates["pb_boost"] = 0.5
    candidates["declining_boost"] = candidates["is_declining"] * 0.2
    candidates["numeric_score"] = (
        candidates["base_score"] + candidates["pb_boost"] +
        candidates["declining_boost"] + BOOST_PB_UPGRADE
    ).astype("float32")

    candidates["primary_signal"]    = "private_brand_upgrade"
    candidates["peer_adoption_rate"] = 0.0
    candidates["confidence_tier"] = np.where(
        candidates["savings_score"] >= 0.20, "high",
        np.where(candidates["specialty_match"] == "mismatch", "low", "medium")
    )

    candidates["rank"] = candidates.groupby("DIM_CUST_CURR_ID")[
        "numeric_score"
    ].rank(method="first", ascending=False)
    candidates = candidates[candidates["rank"] <= 5].copy()
    candidates["rank"] = candidates["rank"].astype("int8")

    _log(f"  Final PB upgrade recs: {len(candidates):,}")
    _log(f"  Customers covered    : {candidates['DIM_CUST_CURR_ID'].nunique():,}")
    _log(f"  Score stats: min={candidates['numeric_score'].min():.2f}  "
         f"median={candidates['numeric_score'].median():.2f}  "
         f"max={candidates['numeric_score'].max():.2f}")
    _log(f"Step 11 done in {time.time()-t0:.1f}s")
    return candidates


# Step 12: Combine + diversification (Phase 3 + Phase 5 + Phase 6 fixes)
# Phase 5 changes:
#   1. Score normalization: rank-percentile within each signal type before
#      cross-signal comparison. Stops popularity (median ~60) from drowning
#      peer_gap (median ~10) in the final ranker.
#   2. Specialty mismatch filter: for customers with >= 10 history products,
#      drop "mismatch" recs. Cold-start customers are exempt (they need
#      broad recommendations, not narrow specialty fits).
# Phase 6 changes:
#   3. Replenishment signal added with quota=3, plus appears in BACKFILL_ORDER.

def diversified_combine(rec_dfs: list) -> pd.DataFrame:
    _s("Step 12: Combining with diversification quotas (Phase 3 + Phase 5 + Phase 6)")
    t0 = time.time()

    common_cols = [
        "DIM_CUST_CURR_ID", "DIM_ITEM_E1_CURR_ID", "ITEM_DSC",
        "PROD_FMLY_LVL1_DSC", "PROD_CTGRY_LVL2_DSC",
        "primary_signal", "confidence_tier",
        "numeric_score", "peer_adoption_rate",
        "specialty_match", "n_buyers",
        "median_unit_price", "median_purchases_per_buyer_per_year",
        "is_private_brand", "primary_market", "primary_market_pct",
        "segment", "size_tier", "mkt_cd_clean", "SPCLTY_CD",
        "is_declining", "is_churned", "order_cadence_tier",
        "n_unique_products_total",
    ]

    aligned_dfs = []
    for df in rec_dfs:
        if len(df) == 0:
            continue
        cols_present = [c for c in common_cols if c in df.columns]
        aligned_dfs.append(df[cols_present].copy())

    combined = pd.concat(aligned_dfs, ignore_index=True)
    _log(f"  Combined candidates: {len(combined):,}")

    # ---- Phase 5 fix #2: specialty mismatch filter (before dedup so we don't
    # accidentally promote a mismatch rec by virtue of it appearing in 2 signals) ----
    if "n_unique_products_total" in combined.columns and "specialty_match" in combined.columns:
        n_before = len(combined)
        has_history = combined["n_unique_products_total"].fillna(0) >= SPECIALTY_FILTER_MIN_HISTORY
        is_mismatch = combined["specialty_match"] == "mismatch"
        # Drop only when BOTH conditions hold: customer has real history AND rec is a mismatch.
        combined = combined[~(has_history & is_mismatch)].copy()
        n_dropped = n_before - len(combined)
        _log(f"  Phase 5 specialty filter: dropped {n_dropped:,} mismatch recs "
             f"for customers with >= {SPECIALTY_FILTER_MIN_HISTORY} history products")

    # Dedup: if the same (customer, product) appears across multiple signals,
    # keep the highest-raw-score copy. We do this before normalization so the
    # normalization buckets are clean.
    combined = combined.sort_values("numeric_score", ascending=False)
    combined = combined.drop_duplicates(
        subset=["DIM_CUST_CURR_ID", "DIM_ITEM_E1_CURR_ID"], keep="first"
    )
    _log(f"  After deduplication: {len(combined):,}")

    # ---- Phase 5 fix #1: per-signal score normalization ----
    # Compute a 0-1 percentile rank within each signal type. This becomes the
    # primary cross-signal comparison key. We keep numeric_score as well so
    # downstream consumers can still see the raw value and the within-signal
    # ordering is preserved (rank-percentile is monotonic in numeric_score).
    _log(f"")
    _log(f"  Normalizing scores per signal (rank-percentile within signal):")
    combined[NORMALIZED_SCORE_COL] = (
        combined.groupby("primary_signal")["numeric_score"]
        .rank(method="average", pct=True)
        .astype("float32")
    )
    norm_stats = combined.groupby("primary_signal").agg(
        raw_median=("numeric_score", "median"),
        norm_median=(NORMALIZED_SCORE_COL, "median"),
        n=("numeric_score", "size"),
    ).round(3)
    for sig, row in norm_stats.iterrows():
        _log(f"    {sig:<22}  n={int(row['n']):>10,}  "
             f"raw_med={row['raw_median']:>7.2f}  norm_med={row['norm_median']:.3f}")

    _log(f"")
    _log(f"  Candidate distribution (BEFORE quota selection):")
    for sig, n in combined["primary_signal"].value_counts().items():
        _log(f"    {sig:<22}  {n:>10,}")

    _log(f"")
    _log(f"  Applying diversification quotas (max per type):")
    for sig, quota in QUOTAS.items():
        _log(f"    {sig:<22}  {quota}")

    # Within-signal rank uses RAW score (preserves your tuned within-signal ordering)
    combined["within_signal_rank"] = combined.groupby(
        ["DIM_CUST_CURR_ID", "primary_signal"]
    )["numeric_score"].rank(method="first", ascending=False)

    combined["quota_max"] = combined["primary_signal"].map(QUOTAS).fillna(2).astype("int8")
    after_quota = combined[combined["within_signal_rank"] <= combined["quota_max"]].copy()
    _log(f"  After quota selection: {len(after_quota):,}")

    _log(f"")
    _log(f"  Distribution after quota selection:")
    for sig, n in after_quota["primary_signal"].value_counts().items():
        _log(f"    {sig:<22}  {n:>10,}")

    # Cross-signal final rank uses NORMALIZED score so popularity can't dominate
    after_quota["rank"] = after_quota.groupby("DIM_CUST_CURR_ID")[
        NORMALIZED_SCORE_COL
    ].rank(method="first", ascending=False)
    selected = after_quota[after_quota["rank"] <= TOP_N_RECS].copy()

    counts_per_cust = selected.groupby("DIM_CUST_CURR_ID").size()
    underfilled_custs = counts_per_cust[counts_per_cust < TOP_N_RECS].index.tolist()

    if underfilled_custs:
        _log(f"")
        _log(f"  Underfilled customers (< {TOP_N_RECS} recs): {len(underfilled_custs):,}")
        _log(f"  Backfilling from: {BACKFILL_ORDER}")

        already_selected = selected[["DIM_CUST_CURR_ID", "DIM_ITEM_E1_CURR_ID"]].copy()
        already_selected["selected"] = 1

        backfill_pool = combined[combined["DIM_CUST_CURR_ID"].isin(underfilled_custs)].copy()
        backfill_pool = backfill_pool.merge(
            already_selected,
            on=["DIM_CUST_CURR_ID", "DIM_ITEM_E1_CURR_ID"], how="left"
        )
        backfill_pool = backfill_pool[backfill_pool["selected"].isna()].copy()
        backfill_pool = backfill_pool.drop(columns=["selected"])

        backfill_pool = backfill_pool[backfill_pool["primary_signal"].isin(BACKFILL_ORDER)]

        slots_needed = TOP_N_RECS - counts_per_cust
        slots_needed = slots_needed[slots_needed > 0]

        # Backfill rank also uses normalized score for consistency
        backfill_pool["backfill_rank"] = backfill_pool.groupby("DIM_CUST_CURR_ID")[
            NORMALIZED_SCORE_COL
        ].rank(method="first", ascending=False)

        backfill_pool = backfill_pool.merge(
            slots_needed.rename("slots").reset_index(),
            on="DIM_CUST_CURR_ID", how="left"
        )
        backfill_chosen = backfill_pool[
            backfill_pool["backfill_rank"] <= backfill_pool["slots"]
        ].drop(columns=["backfill_rank", "slots"])

        _log(f"  Backfilled recs added: {len(backfill_chosen):,}")

        selected = pd.concat([selected, backfill_chosen], ignore_index=True)

        # Re-rank using normalized score after merge
        selected["rank"] = selected.groupby("DIM_CUST_CURR_ID")[
            NORMALIZED_SCORE_COL
        ].rank(method="first", ascending=False)
        selected = selected[selected["rank"] <= TOP_N_RECS].copy()

    selected["rank"] = selected["rank"].astype("int8")

    drop_cols = ["within_signal_rank", "quota_max"]
    selected = selected.drop(columns=[c for c in drop_cols if c in selected.columns])

    _log(f"")
    _log(f"  Final top-{TOP_N_RECS} per customer: {len(selected):,}")
    _log(f"  Unique customers: {selected['DIM_CUST_CURR_ID'].nunique():,}")
    _log(f"")
    _log(f"  Final signal distribution:")
    for sig, n in selected["primary_signal"].value_counts().items():
        pct = n / len(selected) * 100
        _log(f"    {sig:<22}  {n:>10,} ({pct:.1f}%)")

    _log(f"Step 12 done in {time.time()-t0:.1f}s")
    return selected


# Step 13: Add rec_purpose tags (Phase 4)

def add_rec_purpose(recs: pd.DataFrame) -> pd.DataFrame:
    _s("Step 13: Tagging rec_purpose for business clarity (Phase 4)")
    t0 = time.time()

    recs["rec_purpose"] = recs["primary_signal"].map(SIGNAL_TO_PURPOSE).fillna("other")
    recs["is_mckesson_brand"] = recs["is_private_brand"].fillna(0).astype("int8")

    _log(f"  Distribution by rec_purpose:")
    for purp, n in recs["rec_purpose"].value_counts().items():
        pct = n / len(recs) * 100
        _log(f"    {purp:<22}  {n:>10,} ({pct:.1f}%)")

    _log(f"")
    _log(f"  McKesson Brand penetration:")
    n_mck = int(recs["is_mckesson_brand"].sum())
    pct_mck = n_mck / len(recs) * 100
    _log(f"    Total McKesson Brand recs: {n_mck:,} ({pct_mck:.1f}%)")
    _log(f"")
    _log(f"  McKesson Brand by signal type:")
    by_sig = recs.groupby("primary_signal").agg(
        total=("DIM_ITEM_E1_CURR_ID", "count"),
        mck_count=("is_mckesson_brand", "sum"),
    )
    by_sig["mck_pct"] = (by_sig["mck_count"] / by_sig["total"] * 100).round(1)
    for sig, row in by_sig.iterrows():
        _log(f"    {sig:<22}  {int(row['mck_count']):>8,}/{int(row['total']):<8,}  ({row['mck_pct']:.1f}%)")

    _log(f"")
    _log(f"Step 13 done in {time.time()-t0:.1f}s")
    return recs


# Step 14: Pitch reasons

def add_pitch_reasons(recs: pd.DataFrame) -> pd.DataFrame:
    _s("Step 14: Generating pitch reasons")
    t0 = time.time()

    family_short = recs["PROD_FMLY_LVL1_DSC"].fillna("").astype(str).str[:30]
    seg_str      = recs["segment"].fillna("").astype(str)
    adoption_pct = (recs["peer_adoption_rate"].fillna(0) * 100).round(0).astype(int)
    n_buyers_str = recs.get("n_buyers", pd.Series(0, index=recs.index)).fillna(0).astype(int).map(lambda x: f"{x:,}")

    is_peer    = recs["primary_signal"] == "peer_gap"
    is_pop     = recs["primary_signal"] == "popularity"
    is_cart    = recs["primary_signal"] == "cart_complement"
    is_sim     = recs["primary_signal"] == "item_similarity"
    is_lapsed  = recs["primary_signal"] == "lapsed_recovery"
    is_repl    = recs["primary_signal"] == "replenishment"   # Phase 6
    is_medline = recs["primary_signal"] == "medline_conversion"
    is_pb      = recs["primary_signal"] == "private_brand_upgrade"

    peer_reason = (
        adoption_pct.astype(str) + "% of " + seg_str + " peers buy " +
        family_short + " products. You don't currently."
    )
    pop_reason = (
        "Popular among " + seg_str + " peers (" + n_buyers_str +
        " buyers). A safe starting point while we learn your patterns."
    )
    cart_reason = (
        "Customers who buy your products also buy this. Common pairing in " + family_short + "."
    )
    sim_reason = (
        "Similar to products you already buy. Customers like you tend to use this " + family_short + " item."
    )
    lapsed_reason = (
        "You bought this before but not in 6+ months. Win-back opportunity in " + family_short + "."
    )
    # Phase 6: replenishment pitch
    # If we have peer_adoption_rate (which we reused for peer_activity_rate
    # in this signal), surface it - sellers love seeing peer evidence.
    peer_pct_str = adoption_pct.astype(str) + "%"
    repl_reason = (
        "You usually order this but haven't recently. " + peer_pct_str +
        " of peers in your segment still order it on a regular cadence. Likely due for reorder."
    )
    medline_reason = (
        "You currently buy a Medline product. McKesson alternative in " + family_short + " offers comparable function."
    )
    pb_reason_str = (
        "McKesson Brand alternative to a national brand you buy. Same form, same category, often cheaper."
    )

    recs["pitch_reason"] = ""
    recs.loc[is_peer,    "pitch_reason"] = peer_reason[is_peer]
    recs.loc[is_pop,     "pitch_reason"] = pop_reason[is_pop]
    recs.loc[is_cart,    "pitch_reason"] = cart_reason[is_cart]
    recs.loc[is_sim,     "pitch_reason"] = sim_reason[is_sim]
    recs.loc[is_lapsed,  "pitch_reason"] = lapsed_reason[is_lapsed]
    recs.loc[is_repl,    "pitch_reason"] = repl_reason[is_repl]   # Phase 6
    recs.loc[is_medline, "pitch_reason"] = medline_reason[is_medline]
    recs.loc[is_pb,      "pitch_reason"] = pb_reason_str

    is_match    = recs.get("specialty_match", pd.Series("", index=recs.index)) == "match"
    is_mismatch = recs.get("specialty_match", pd.Series("", index=recs.index)) == "mismatch"
    spclty      = recs.get("SPCLTY_CD", pd.Series("", index=recs.index)).fillna("").astype(str)

    recs.loc[is_match, "pitch_reason"] = (
        recs.loc[is_match, "pitch_reason"] +
        " Aligns with your specialty (" + spclty[is_match] + ")."
    )
    recs.loc[is_mismatch, "pitch_reason"] = (
        recs.loc[is_mismatch, "pitch_reason"] +
        " (Note: may not fit your specialty)"
    )

    recs["pitch_reason"] = recs["pitch_reason"].replace("", "Recommended product.")

    _log(f"Generated {len(recs):,} pitch reasons in {time.time()-t0:.1f}s")
    return recs


# Step 15: Save

def save_recommendations(recs: pd.DataFrame) -> pd.DataFrame:
    _s("Step 15: Saving recommendations")
    t0 = time.time()

    output_cols = [
        "DIM_CUST_CURR_ID", "DIM_ITEM_E1_CURR_ID", "ITEM_DSC",
        "PROD_FMLY_LVL1_DSC", "PROD_CTGRY_LVL2_DSC",
        "rank", "primary_signal", "rec_purpose", "is_mckesson_brand",
        "pitch_reason", "confidence_tier", "numeric_score",
        NORMALIZED_SCORE_COL,
        "peer_adoption_rate", "specialty_match",
        "median_unit_price", "median_purchases_per_buyer_per_year",
        "is_private_brand",
        "segment", "size_tier", "mkt_cd_clean", "SPCLTY_CD",
        "is_declining", "is_churned", "order_cadence_tier",
    ]
    output_cols = [c for c in output_cols if c in recs.columns]
    recs = recs[output_cols].copy()

    recs["DIM_CUST_CURR_ID"]   = recs["DIM_CUST_CURR_ID"].astype("int64")
    recs["DIM_ITEM_E1_CURR_ID"] = recs["DIM_ITEM_E1_CURR_ID"].astype("int64")
    recs["rank"]               = recs["rank"].astype("int8")
    recs["numeric_score"]      = recs["numeric_score"].astype("float32")
    if NORMALIZED_SCORE_COL in recs.columns:
        recs[NORMALIZED_SCORE_COL] = recs[NORMALIZED_SCORE_COL].astype("float32")
    recs["peer_adoption_rate"] = recs["peer_adoption_rate"].fillna(0).astype("float32")
    recs["median_unit_price"]  = recs["median_unit_price"].astype("float32")
    recs["is_private_brand"]   = recs["is_private_brand"].fillna(0).astype("int8")
    recs["is_mckesson_brand"]  = recs["is_mckesson_brand"].fillna(0).astype("int8")
    recs["is_declining"]       = recs["is_declining"].fillna(0).astype("int8")
    recs["is_churned"]         = recs["is_churned"].fillna(0).astype("int8")

    recs = recs.sort_values(["DIM_CUST_CURR_ID", "rank"]).reset_index(drop=True)

    PRECOMP.mkdir(parents=True, exist_ok=True)
    ANALYSIS.mkdir(parents=True, exist_ok=True)

    recs.to_parquet(RECS_OUT, index=False)
    size_mb = RECS_OUT.stat().st_size / (1024 * 1024)
    _log(f"Saved: {RECS_OUT.relative_to(ROOT)}  ({size_mb:.1f} MB)")
    _log(f"  Total recommendations: {len(recs):,}")
    _log(f"  Unique customers     : {recs['DIM_CUST_CURR_ID'].nunique():,}")

    sample_custs = recs["DIM_CUST_CURR_ID"].unique()[:100]
    sample = recs[recs["DIM_CUST_CURR_ID"].isin(sample_custs)]
    sample.to_excel(RECS_SAMPLE_XLSX, index=False, engine="openpyxl")
    _log(f"Saved sample xlsx ({len(sample):,} recs)")

    _log(f"Step 15 done in {time.time()-t0:.1f}s")
    return recs


# Step 16: Stats

def print_stats(recs: pd.DataFrame) -> None:
    _s("Step 16: Distribution summary")

    _log(f"Total recommendations: {len(recs):,}")
    _log(f"Unique customers: {recs['DIM_CUST_CURR_ID'].nunique():,}")
    _log("")

    _log("Primary signal distribution:")
    for sig, n in recs["primary_signal"].value_counts().items():
        pct = n / len(recs) * 100
        _log(f"  {sig:<22}  {n:>10,} ({pct:.1f}%)")

    _log("")
    _log("Rec purpose distribution (BUSINESS VIEW):")
    for purp, n in recs["rec_purpose"].value_counts().items():
        pct = n / len(recs) * 100
        _log(f"  {purp:<22}  {n:>10,} ({pct:.1f}%)")

    _log("")
    n_mck = int(recs["is_mckesson_brand"].sum())
    pct_mck = n_mck / len(recs) * 100
    _log(f"McKesson Brand penetration: {n_mck:,}/{len(recs):,} ({pct_mck:.1f}%)")

    _log("")
    _log("Confidence tier distribution:")
    for tier, n in recs["confidence_tier"].value_counts().items():
        pct = n / len(recs) * 100
        _log(f"  {tier:<10}  {n:>10,} ({pct:.1f}%)")

    _log("")
    if "specialty_match" in recs.columns:
        _log("Specialty match distribution:")
        for m, n in recs["specialty_match"].value_counts().items():
            pct = n / len(recs) * 100
            _log(f"  {m:<10}  {n:>10,} ({pct:.1f}%)")

    _log("")
    fam_div = recs.groupby("DIM_CUST_CURR_ID")["PROD_FMLY_LVL1_DSC"].nunique()
    sig_div = recs.groupby("DIM_CUST_CURR_ID")["primary_signal"].nunique()
    purp_div = recs.groupby("DIM_CUST_CURR_ID")["rec_purpose"].nunique()
    _log(f"Family diversity per customer:  p10={fam_div.quantile(0.10):.0f}  "
         f"median={fam_div.quantile(0.50):.0f}  p90={fam_div.quantile(0.90):.0f}")
    _log(f"Signal diversity per customer:  p10={sig_div.quantile(0.10):.0f}  "
         f"median={sig_div.quantile(0.50):.0f}  p90={sig_div.quantile(0.90):.0f}")
    _log(f"Purpose diversity per customer: p10={purp_div.quantile(0.10):.0f}  "
         f"median={purp_div.quantile(0.50):.0f}  p90={purp_div.quantile(0.90):.0f}")

    _log("")
    _log("Per-customer McKesson Brand counts:")
    mck_per_cust = recs.groupby("DIM_CUST_CURR_ID")["is_mckesson_brand"].sum()
    _log(f"  Customers with 0 McKesson recs : {(mck_per_cust == 0).sum():,}")
    _log(f"  Customers with 1-3 McKesson    : {((mck_per_cust >= 1) & (mck_per_cust <= 3)).sum():,}")
    _log(f"  Customers with 4-6 McKesson    : {((mck_per_cust >= 4) & (mck_per_cust <= 6)).sum():,}")
    _log(f"  Customers with 7-10 McKesson   : {(mck_per_cust >= 7).sum():,}")


# Main

def main() -> None:
    print()
    print("=" * 64)
    print("  RECOMMENDATION ENGINE - PHASE 6 (Replenishment Signal)")
    print("=" * 64)
    start = time.time()

    inputs   = load_inputs()
    cp       = build_customer_profile(inputs)
    history  = load_customer_purchase_history()
    products = filter_eligible_products(inputs["products"])

    rec_dfs = []

    pg = generate_peer_gap_recs(
        cp, inputs["seg_profiles"], products, inputs["specialty"], history
    )
    rec_dfs.append(pg)
    del pg
    gc.collect()

    cs = generate_coldstart_recs(cp, products, inputs["specialty"], history)
    rec_dfs.append(cs)
    del cs
    gc.collect()

    cart = generate_cart_complement_recs(
        cp, inputs["cooccur"], products, inputs["specialty"], history
    )
    rec_dfs.append(cart)
    del cart
    gc.collect()

    sim = generate_item_similarity_recs(
        cp, inputs["item_sim"], products, inputs["specialty"], history
    )
    rec_dfs.append(sim)
    del sim
    gc.collect()

    lapsed = generate_lapsed_recovery_recs(
        cp, inputs["lapsed"], products, inputs["specialty"]
    )
    rec_dfs.append(lapsed)
    del lapsed
    gc.collect()

    # Phase 6: replenishment
    repl = generate_replenishment_recs(
        cp, inputs["replenishment"], products, inputs["specialty"]
    )
    rec_dfs.append(repl)
    del repl
    gc.collect()

    medline = generate_medline_conversion_recs(
        cp, inputs["pb_equiv"], history, products, inputs["specialty"]
    )
    rec_dfs.append(medline)
    del medline
    gc.collect()

    pb_upg = generate_pb_upgrade_recs(
        cp, inputs["pb_equiv"], history, products, inputs["specialty"]
    )
    rec_dfs.append(pb_upg)
    del pb_upg
    gc.collect()

    final = diversified_combine(rec_dfs)
    del rec_dfs
    gc.collect()

    final = add_rec_purpose(final)
    final = add_pitch_reasons(final)
    final = save_recommendations(final)

    print_stats(final)

    _s("Phase 6 Complete")
    _log(f"Total time: {time.time() - start:.1f}s")
    _log("")
    _log(f"Output: {RECS_OUT.relative_to(ROOT)}")
    _log(f"  Top {TOP_N_RECS} recommendations per customer")
    _log(f"  {final['DIM_CUST_CURR_ID'].nunique():,} customers covered")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"\nFATAL ERROR: {exc}", file=sys.stderr)
        raise