from __future__ import annotations

import sys
import time
import warnings
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


# Paths

ROOT         = Path(__file__).resolve().parent.parent.parent
DATA_CLEAN   = ROOT / "data_clean"
MERGED_FILE  = DATA_CLEAN / "serving"  / "merged_dataset.parquet"
PRODUCT_FILE = DATA_CLEAN / "product"  / "products_clean.parquet"

OUT_PRECOMP  = DATA_CLEAN / "serving"  / "precomputed"
OUT_ANALYSIS = DATA_CLEAN / "analysis"

SLIM_OUT     = OUT_PRECOMP  / "private_brand_equivalents.parquet"
FULL_OUT     = OUT_ANALYSIS / "private_brand_equivalents_with_metadata.parquet"
XLSX_OUT     = OUT_ANALYSIS / "private_brand_equivalents_analysis.xlsx"


# Configuration

FISCAL_YEARS              = ("FY2425", "FY2526")
MIN_BUYERS_PER_PRODUCT     = 50     # Same threshold as other files
PRICE_MATCH_TOLERANCE_PCT  = 0.30   # McKesson Brand must be within +/- 30% for upgrade
MAX_EQUIVALENTS_PER_ITEM   = 3      # Keep top 3 equivalents per original
MIN_EQUIVALENTS_N_BUYERS   = 50     # Equivalent must have at least 50 buyers

MEDLINE_SUPPLIERS = {"MEDLINE", "MEDLINE INDUSTRIES"}
EXCLUDED_FAMILIES = {"Fee", "Unknown", "NaN", "nan", ""}


# Logging

def _s(title: str) -> None:
    print(f"\n{'-' * 64}\n  {title}\n{'-' * 64}", flush=True)


def _log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def _pq(p: Path) -> str:
    return "'" + p.as_posix() + "'"


# Product form codes used in medical supply ITEM_DSC
# These appear right after the first comma. Matching on form prevents
# tablet<->cream substitutions.

FORM_CODES = {
    # Oral/ingested
    "TAB", "TABS", "TABLET", "TABLETS", "CAP", "CAPS", "CAPSULE", "CAPSULES",
    "GCAP", "CPLT", "CAPLET", "CAPLETS", "LOZ", "LOZENGE", "LOZENGES",
    "SYRP", "SYRUP", "ELXR", "ELIXIR", "SUSP", "SUSPENSION",
    "POWD", "PWDR", "POWDER", "GRANULES", "CHEW",
    # Topical
    "CRM", "CR", "CREAM", "CREAMS", "OINT", "OINTMENT", "LOT", "LOTION",
    "GEL", "PASTE", "PDR", "PATCH", "PATCHES",
    "STICK", "BAR", "FT", "SHMP", "SHAMPOO", "FOAM",
    # Injectable / solution
    "INJ", "INJECTION", "INJECTIONS", "SDV", "MDV", "VL", "VIAL", "VIALS",
    "AMP", "AMPULE", "AMPULES", "PFS", "PF",
    "SOL", "SOLN", "SOLUTION", "SOLUTIONS", "IV", "IRRG", "IRR", "IRRIGATION",
    "LIQ", "LIQUID", "LIQUIDS", "SOLU", "CONCENTRATE",
    # Inhaled
    "INH", "INHALER", "NEB", "NEBULIZER", "NEBULIZED",
    "SPRY", "SPRAY", "AER", "AEROSOL", "MDI", "DPI",
    # Suppositories and other
    "SUPP", "SUPPOSITORY", "SUPPOSITORIES", "ENEMA",
    "DROPS", "DROP", "EYE", "OPHT", "OPHTHALMIC",
    "PLEDGET", "PLEDGETS",
    # Physical forms (for medical supplies with relevant distinctions)
    "WIPE", "WIPES", "PAD", "PADS", "SWAB", "SWABS",
    "STRIP", "STRIPS", "SHEET", "SHEETS", "ROLL", "ROLLS",
    "KIT", "KITS", "TRAY", "TRAYS", "SET", "SETS",
}

# Mapping to normalize variants (so CREAM and CRM are treated as same form)
FORM_NORMALIZATION = {
    "CREAM": "CRM", "CREAMS": "CRM", "CR": "CRM",
    "OINTMENT": "OINT",
    "LOTION": "LOT",
    "TABLET": "TAB", "TABLETS": "TAB", "TABS": "TAB",
    "CAPSULE": "CAP", "CAPSULES": "CAP", "CAPS": "CAP", "GCAP": "CAP", "CPLT": "CAP", "CAPLET": "CAP", "CAPLETS": "CAP",
    "LOZENGE": "LOZ", "LOZENGES": "LOZ",
    "SYRUP": "SYRP",
    "ELIXIR": "ELXR",
    "SUSPENSION": "SUSP",
    "POWDER": "POWD", "PWDR": "POWD",
    "VIAL": "VL", "VIALS": "VL",
    "AMPULE": "AMP", "AMPULES": "AMP",
    "SOLUTION": "SOL", "SOLUTIONS": "SOL", "SOLN": "SOL", "SOLU": "SOL",
    "LIQUID": "LIQ", "LIQUIDS": "LIQ",
    "INHALER": "INH",
    "NEBULIZER": "NEB", "NEBULIZED": "NEB",
    "SPRAY": "SPRY",
    "AEROSOL": "AER",
    "SUPPOSITORY": "SUPP", "SUPPOSITORIES": "SUPP",
    "OPHTHALMIC": "OPHT",
    "INJECTION": "INJ", "INJECTIONS": "INJ",
    "IRRIGATION": "IRR", "IRRG": "IRR",
    "WIPES": "WIPE",
    "PADS": "PAD",
    "SWABS": "SWAB",
    "STRIPS": "STRIP",
    "SHEETS": "SHEET",
    "ROLLS": "ROLL",
    "KITS": "KIT",
    "TRAYS": "TRAY",
    "SETS": "SET",
}


def extract_product_type(desc: str) -> str:
    # Product type is typically the first word before the first comma.
    # For compound drug names like "HYDROCORTISONE+ALOE", we keep just the
    # base drug before the first '+' or '-' so it matches plain "HYDROCORTISONE".
    if not isinstance(desc, str) or not desc.strip():
        return ""
    first_part = desc.split(",")[0].strip().upper()
    first_word = first_part.split()[0] if first_part else ""
    # Strip compound indicators so HYDROCORTISONE+ALOE matches HYDROCORTISONE
    for sep in ["+", "/"]:
        if sep in first_word:
            first_word = first_word.split(sep)[0]
    return first_word


def extract_form_code(desc: str) -> str:
    # Look for a known FORM_CODES token in the description and normalize.
    # Returns empty string if no form code found - this is OK for medical supplies
    # like catheters, needles, gauze which don't have pharmaceutical forms.
    if not isinstance(desc, str) or not desc.strip():
        return ""
    words = desc.upper().replace(",", " ").replace("/", " ").split()
    for word in words:
        clean_word = word.strip(".()[]0123456789")
        if clean_word in FORM_CODES:
            # Normalize to canonical form
            return FORM_NORMALIZATION.get(clean_word, clean_word)
    return ""


# Step 1: Get product prices and buyer counts from merged_dataset

def load_product_economics(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    # Returns: item_id, median_unit_price, n_buyers, is_medline, is_excluded_family
    # Unlike other scripts, we DO NOT exclude Medline here because we need them
    # for the Medline conversion mapping.

    _s("Step 1: Loading product economics (prices and buyer counts)")
    t0 = time.time()

    if not MERGED_FILE.exists():
        print(f"\nFATAL: merged_dataset.parquet not found", file=sys.stderr)
        sys.exit(1)

    # Detect columns
    desc = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({_pq(MERGED_FILE)}) LIMIT 0"
    ).df()
    available = set(desc["column_name"].tolist())

    family_col = next(
        (c for c in ["PROD_FMLY_LVL1_DSC", "PROD_FMLY_LVL1_CD"]
         if c in available), None
    )
    supplier_col = next(
        (c for c in ["SUPLR_ROLLUP_DSC", "SUPLR_DSC"] if c in available), None
    )

    _log(f"Product family column : {family_col}")
    _log(f"Supplier column       : {supplier_col}")
    _log(f"Loading prices and buyer counts (including Medline)...")

    fy_sql = ", ".join(f"'{fy}'" for fy in FISCAL_YEARS)

    # Per-product: median unit price and buyer count
    # We DON'T filter out Medline here; they're tagged instead.
    economics = con.execute(f"""
        SELECT
            CAST(DIM_ITEM_E1_CURR_ID AS BIGINT) AS item_id,
            MEDIAN(UNIT_SLS_AMT) AS median_unit_price,
            COUNT(DISTINCT DIM_CUST_CURR_ID) AS n_buyers,
            MAX(CASE
                WHEN UPPER(COALESCE({supplier_col}, '')) IN
                    ('MEDLINE', 'MEDLINE INDUSTRIES') THEN 1
                ELSE 0
            END) AS is_medline,
            MAX(CASE
                WHEN COALESCE({family_col}, 'Unknown') IN
                    ('Fee', 'Unknown', 'NaN', 'nan', '') THEN 1
                ELSE 0
            END) AS is_excluded_family
        FROM read_parquet({_pq(MERGED_FILE)})
        WHERE UNIT_SLS_AMT > 0
          AND fiscal_year IN ({fy_sql})
          AND DIM_ITEM_E1_CURR_ID IS NOT NULL
        GROUP BY CAST(DIM_ITEM_E1_CURR_ID AS BIGINT)
    """).df()

    economics["item_id"]              = economics["item_id"].astype("int64")
    economics["n_buyers"]             = economics["n_buyers"].astype("int64")
    economics["is_medline"]           = economics["is_medline"].astype("int8")
    economics["is_excluded_family"]    = economics["is_excluded_family"].astype("int8")

    _log(f"  Total products loaded: {len(economics):,}")
    _log(f"    Medline products   : {int(economics['is_medline'].sum()):,}")
    _log(f"    Excluded family    : {int(economics['is_excluded_family'].sum()):,}")
    _log(f"    Non-Medline, valid : {int(((economics['is_medline']==0) & (economics['is_excluded_family']==0)).sum()):,}")

    _log(f"Step 1 done in {time.time()-t0:.1f}s")

    return economics


# Step 2: Load product metadata and combine with economics

def load_product_catalog(economics: pd.DataFrame) -> pd.DataFrame:
    _s("Step 2: Loading product catalog and combining")
    t0 = time.time()

    if not PRODUCT_FILE.exists():
        print(f"\nFATAL: products_clean.parquet not found", file=sys.stderr)
        sys.exit(1)

    _log("Loading products_clean.parquet...")
    products = pd.read_parquet(PRODUCT_FILE, columns=[
        "DIM_ITEM_E1_CURR_ID", "ITEM_DSC",
        "PROD_FMLY_LVL1_DSC", "PROD_CTGRY_LVL2_DSC",
        "PROD_GRP_LVL3_DSC",  "PROD_SUB_CTGRY_LVL4_DSC",
        "SUPLR_ROLLUP_DSC", "is_private_brand", "is_discontinued",
    ])
    products = products.rename(columns={"DIM_ITEM_E1_CURR_ID": "item_id"})
    products["item_id"] = products["item_id"].astype("int64")

    # Build matching_category: use deepest level available per product.
    # Fall back order: LVL4 -> LVL3 -> LVL2
    # This alone is not enough (LVL4 often duplicates LVL3 and categories like
    # "OTC, Analgesics" mix tablets and creams). We add product_type and form_code
    # extraction from ITEM_DSC as secondary matching keys below.
    products["matching_category"] = (
        products["PROD_SUB_CTGRY_LVL4_DSC"]
            .fillna(products["PROD_GRP_LVL3_DSC"])
            .fillna(products["PROD_CTGRY_LVL2_DSC"])
            .fillna("UNKNOWN_CATEGORY")
    )

    # Extract product type (first word before comma) and form code (TAB/CRM/INJ/etc.)
    # from ITEM_DSC. These are the secondary matching keys that prevent
    # cross-form substitutions (e.g., Mapap tablet -> Hydrocortisone cream).
    _log(f"Extracting product_type and form_code from descriptions...")
    products["product_type"] = products["ITEM_DSC"].apply(extract_product_type)
    products["form_code"]    = products["ITEM_DSC"].apply(extract_form_code)

    # Log what depth we ended up using
    has_lvl4 = products["PROD_SUB_CTGRY_LVL4_DSC"].notna().sum()
    has_lvl3 = (products["PROD_SUB_CTGRY_LVL4_DSC"].isna() &
                products["PROD_GRP_LVL3_DSC"].notna()).sum()
    has_lvl2 = (products["PROD_SUB_CTGRY_LVL4_DSC"].isna() &
                products["PROD_GRP_LVL3_DSC"].isna() &
                products["PROD_CTGRY_LVL2_DSC"].notna()).sum()
    unknown  = (products["matching_category"] == "UNKNOWN_CATEGORY").sum()

    _log(f"  Products in catalog: {len(products):,}")
    _log(f"  Matching category depth:")
    _log(f"    Using LVL4 (most granular): {has_lvl4:,}")
    _log(f"    Using LVL3 (fallback)     : {has_lvl3:,}")
    _log(f"    Using LVL2 (fallback)     : {has_lvl2:,}")
    _log(f"    No category (skipped)     : {unknown:,}")
    _log(f"  Product type extraction:")
    _log(f"    Products with product_type: "
         f"{int((products['product_type'] != '').sum()):,}")
    _log(f"    Products with form_code   : "
         f"{int((products['form_code'] != '').sum()):,}  "
         f"({(products['form_code'] != '').mean()*100:.1f}%)")

    # Merge economics
    catalog = products.merge(economics, on="item_id", how="left")

    # Only keep products with economic data (bought in FY2425 or FY2526)
    before = len(catalog)
    catalog = catalog[catalog["median_unit_price"].notna()]
    _log(f"  After filtering to products with sales: {len(catalog):,}  "
         f"(dropped {before - len(catalog):,} unsold)")

    # Fill missing is_private_brand / is_discontinued
    catalog["is_private_brand"] = catalog["is_private_brand"].fillna(0).astype("int8")
    catalog["is_discontinued"]  = catalog["is_discontinued"].fillna(0).astype("int8")

    # Exclude excluded-family products from ALL analysis
    before = len(catalog)
    catalog = catalog[catalog["is_excluded_family"] == 0]
    _log(f"  After excluding Fee/Unknown families: {len(catalog):,}  "
         f"(dropped {before - len(catalog):,})")

    # Exclude products without any category info (can't match them)
    before = len(catalog)
    catalog = catalog[catalog["matching_category"] != "UNKNOWN_CATEGORY"]
    _log(f"  After excluding products without category: {len(catalog):,}  "
         f"(dropped {before - len(catalog):,})")

    # Breakdown
    n_medline     = int((catalog["is_medline"] == 1).sum())
    n_pb          = int(((catalog["is_medline"] == 0) & (catalog["is_private_brand"] == 1)).sum())
    n_national    = int(((catalog["is_medline"] == 0) & (catalog["is_private_brand"] == 0)).sum())
    _log(f"")
    _log(f"  Catalog composition:")
    _log(f"    Medline products        : {n_medline:,}")
    _log(f"    McKesson Brand (non-Medl): {n_pb:,}")
    _log(f"    National Brand (non-Medl): {n_national:,}")

    _log(f"Step 2 done in {time.time()-t0:.1f}s")
    return catalog


# Step 3: Build McKesson Brand equivalents lookup

def build_mckesson_brand_pool(catalog: pd.DataFrame) -> pd.DataFrame:
    # Products that can be recommended as equivalents: McKesson private brand,
    # not Medline, not discontinued, at least 50 buyers.
    _s("Step 3: Building McKesson Brand equivalents pool")

    pool = catalog[
        (catalog["is_private_brand"] == 1) &
        (catalog["is_medline"] == 0) &
        (catalog["is_discontinued"] == 0) &
        (catalog["n_buyers"] >= MIN_EQUIVALENTS_N_BUYERS)
    ].copy()

    _log(f"McKesson Brand equivalents pool: {len(pool):,} products")
    _log(f"  Spans {pool['PROD_CTGRY_LVL2_DSC'].nunique():,} granular categories")

    return pool


def build_mckesson_all_pool(catalog: pd.DataFrame) -> pd.DataFrame:
    # For Medline conversions: ANY non-Medline, non-discontinued product with 50+ buyers
    # is a potential McKesson equivalent (private brand preferred but not required).
    _s("Step 3b: Building general McKesson equivalents pool (for Medline conversions)")

    pool = catalog[
        (catalog["is_medline"] == 0) &
        (catalog["is_discontinued"] == 0) &
        (catalog["n_buyers"] >= MIN_EQUIVALENTS_N_BUYERS)
    ].copy()

    _log(f"General McKesson pool (for Medline conversions): {len(pool):,} products")

    return pool


# Step 4: Find private brand upgrades

def find_private_brand_upgrades(
    catalog: pd.DataFrame, pb_pool: pd.DataFrame
) -> pd.DataFrame:
    # For each national-brand product (is_private_brand=0, is_medline=0),
    # find McKesson Brand equivalents in the same PROD_CTGRY_LVL2_DSC.
    # Only keep equivalents that are same-or-cheaper price (price_delta_pct <= 0)
    # AND within +/- 30% price range (don't recommend wildly different products).

    _s("Step 4: Finding national brand -> McKesson Brand upgrades")
    t0 = time.time()

    national = catalog[
        (catalog["is_private_brand"] == 0) &
        (catalog["is_medline"] == 0) &
        (catalog["is_discontinued"] == 0) &
        (catalog["n_buyers"] >= MIN_BUYERS_PER_PRODUCT)
    ].copy()

    _log(f"National brand products to upgrade: {len(national):,}")

    # Match within granular sub-category + same product_type + same form_code
    # This is the critical fix combining LVL4 + description parsing.
    # Example: Mapap (TAB) and Hydrocortisone (CRM) both in "OTC, Analgesics" LVL4
    # but have different form_codes, so they won't match.
    _log(f"Matching on: matching_category + PROD_FMLY_LVL1_DSC + product_type + form_code...")

    pb_slim = pb_pool[[
        "item_id", "ITEM_DSC", "matching_category",
        "PROD_CTGRY_LVL2_DSC", "PROD_FMLY_LVL1_DSC",
        "product_type", "form_code",
        "median_unit_price", "n_buyers", "SUPLR_ROLLUP_DSC"
    ]].rename(columns={
        "item_id": "equivalent_item_id",
        "ITEM_DSC": "equivalent_desc",
        "median_unit_price": "equivalent_unit_price",
        "n_buyers": "equivalent_n_buyers",
        "SUPLR_ROLLUP_DSC": "equivalent_supplier",
    })

    national_slim = national[[
        "item_id", "ITEM_DSC", "matching_category",
        "PROD_CTGRY_LVL2_DSC", "PROD_FMLY_LVL1_DSC",
        "product_type", "form_code",
        "median_unit_price", "n_buyers", "SUPLR_ROLLUP_DSC"
    ]].rename(columns={
        "item_id": "original_item_id",
        "ITEM_DSC": "original_desc",
        "median_unit_price": "original_unit_price",
        "n_buyers": "original_n_buyers",
        "SUPLR_ROLLUP_DSC": "original_supplier",
    })

    # Join on all four matching keys
    pairs = national_slim.merge(
        pb_slim,
        on=["matching_category", "PROD_FMLY_LVL1_DSC",
            "product_type", "form_code"],
        how="inner",
        suffixes=("", "_eq")
    )

    # Drop duplicate LVL2 column from merge (we kept it on both sides)
    if "PROD_CTGRY_LVL2_DSC_eq" in pairs.columns:
        pairs = pairs.drop(columns=["PROD_CTGRY_LVL2_DSC_eq"])

    # Remove self-matches (shouldn't happen but defensive)
    pairs = pairs[pairs["original_item_id"] != pairs["equivalent_item_id"]]

    _log(f"  Raw same-sub-category pairs: {len(pairs):,}")

    # Compute price delta
    pairs["price_delta_pct"] = (
        (pairs["equivalent_unit_price"] - pairs["original_unit_price"]) /
        pairs["original_unit_price"].replace(0, np.nan)
    ).fillna(0.0)

    # Filter to same-or-cheaper, within tolerance
    _log(f"Filtering: must be same-or-cheaper AND within +/- {PRICE_MATCH_TOLERANCE_PCT*100:.0f}% price...")
    before = len(pairs)
    pairs = pairs[
        (pairs["price_delta_pct"] <= 0) &
        (pairs["price_delta_pct"] >= -PRICE_MATCH_TOLERANCE_PCT)
    ]
    _log(f"  After price filters: {len(pairs):,}  (dropped {before - len(pairs):,})")

    # Tag
    pairs["match_type"]           = "private_brand_upgrade"
    pairs["is_medline_conversion"] = 0
    pairs["is_price_improvement"]  = (pairs["price_delta_pct"] < 0).astype("int8")

    # Rank within original (cheapest + most popular first)
    # Score: price savings + popularity boost
    pairs["score"] = (
        (-pairs["price_delta_pct"] * 2.0) +  # bigger savings = better
        (np.log1p(pairs["equivalent_n_buyers"]) / 10.0)  # popularity tie-breaker
    )
    pairs["rank"] = pairs.groupby("original_item_id")["score"].rank(
        method="first", ascending=False
    )
    pairs = pairs[pairs["rank"] <= MAX_EQUIVALENTS_PER_ITEM]

    _log(f"  Final upgrade pairs (top {MAX_EQUIVALENTS_PER_ITEM} per original): {len(pairs):,}")
    _log(f"  Original products with at least 1 upgrade: "
         f"{pairs['original_item_id'].nunique():,}")
    _log(f"Step 4 done in {time.time()-t0:.1f}s")

    return pairs


# Step 5: Find Medline conversions

def find_medline_conversions(
    catalog: pd.DataFrame, general_pool: pd.DataFrame
) -> pd.DataFrame:
    # For each Medline product (is_medline=1), find McKesson equivalents in same category.
    # Prefer private brand, but ANY McKesson product qualifies.
    # No price constraint - Medline conversion is always a win regardless of price.

    _s("Step 5: Finding Medline -> McKesson conversions")
    t0 = time.time()

    medline = catalog[
        (catalog["is_medline"] == 1) &
        (catalog["is_discontinued"] == 0) &
        (catalog["n_buyers"] >= MIN_BUYERS_PER_PRODUCT)
    ].copy()

    _log(f"Medline products to convert: {len(medline):,}")

    mckesson_slim = general_pool[[
        "item_id", "ITEM_DSC", "matching_category",
        "PROD_CTGRY_LVL2_DSC", "PROD_FMLY_LVL1_DSC",
        "product_type", "form_code",
        "median_unit_price", "n_buyers", "SUPLR_ROLLUP_DSC", "is_private_brand"
    ]].rename(columns={
        "item_id": "equivalent_item_id",
        "ITEM_DSC": "equivalent_desc",
        "median_unit_price": "equivalent_unit_price",
        "n_buyers": "equivalent_n_buyers",
        "SUPLR_ROLLUP_DSC": "equivalent_supplier",
        "is_private_brand": "equivalent_is_private_brand",
    })

    medline_slim = medline[[
        "item_id", "ITEM_DSC", "matching_category",
        "PROD_CTGRY_LVL2_DSC", "PROD_FMLY_LVL1_DSC",
        "product_type", "form_code",
        "median_unit_price", "n_buyers", "SUPLR_ROLLUP_DSC"
    ]].rename(columns={
        "item_id": "original_item_id",
        "ITEM_DSC": "original_desc",
        "median_unit_price": "original_unit_price",
        "n_buyers": "original_n_buyers",
        "SUPLR_ROLLUP_DSC": "original_supplier",
    })

    # Join on matching_category + family + product_type + form_code
    pairs = medline_slim.merge(
        mckesson_slim,
        on=["matching_category", "PROD_FMLY_LVL1_DSC",
            "product_type", "form_code"],
        how="inner",
        suffixes=("", "_eq")
    )

    # Drop duplicate LVL2 column from merge
    if "PROD_CTGRY_LVL2_DSC_eq" in pairs.columns:
        pairs = pairs.drop(columns=["PROD_CTGRY_LVL2_DSC_eq"])

    _log(f"  Raw same-sub-category Medline->McKesson pairs: {len(pairs):,}")

    # Compute price delta (just for information, no filter)
    pairs["price_delta_pct"] = (
        (pairs["equivalent_unit_price"] - pairs["original_unit_price"]) /
        pairs["original_unit_price"].replace(0, np.nan)
    ).fillna(0.0)

    # Tag
    pairs["match_type"]           = "medline_conversion"
    pairs["is_medline_conversion"] = 1
    pairs["is_price_improvement"]  = (pairs["price_delta_pct"] < 0).astype("int8")

    # Rank: prefer private brand, then price savings, then popularity
    # Score: private_brand_boost + price_savings + popularity
    pairs["score"] = (
        pairs["equivalent_is_private_brand"] * 2.0 +
        (-pairs["price_delta_pct"].clip(lower=-1, upper=1)) +  # cap at +/- 100%
        (np.log1p(pairs["equivalent_n_buyers"]) / 10.0)
    )
    pairs["rank"] = pairs.groupby("original_item_id")["score"].rank(
        method="first", ascending=False
    )
    pairs = pairs[pairs["rank"] <= MAX_EQUIVALENTS_PER_ITEM]

    # Drop the extra column we don't need in final output
    pairs = pairs.drop(columns=["equivalent_is_private_brand"])

    _log(f"  Final conversion pairs (top {MAX_EQUIVALENTS_PER_ITEM} per Medline): "
         f"{len(pairs):,}")
    _log(f"  Medline products with at least 1 conversion: "
         f"{pairs['original_item_id'].nunique():,}")
    _log(f"Step 5 done in {time.time()-t0:.1f}s")

    return pairs


# Step 6: Combine and save

def save_outputs(upgrades: pd.DataFrame, conversions: pd.DataFrame) -> pd.DataFrame:
    _s("Step 6: Combining and saving outputs")

    # Align columns
    common_cols = [
        "original_item_id", "equivalent_item_id", "rank",
        "match_type", "is_medline_conversion", "is_price_improvement",
        "original_unit_price", "equivalent_unit_price", "price_delta_pct",
        "original_desc", "equivalent_desc",
        "original_supplier", "equivalent_supplier",
        "PROD_CTGRY_LVL2_DSC", "PROD_FMLY_LVL1_DSC",
        "original_n_buyers", "equivalent_n_buyers",
    ]

    upgrades_aligned    = upgrades[common_cols].copy()
    conversions_aligned = conversions[common_cols].copy()

    combined = pd.concat([upgrades_aligned, conversions_aligned], ignore_index=True)
    combined = combined.sort_values(
        ["original_item_id", "rank"], ascending=[True, True]
    ).reset_index(drop=True)

    # Add price anomaly flag: true if price difference is > 100% (either direction)
    # This warns seller of potential package-size mismatch (e.g., per-unit vs per-CS)
    combined["price_anomaly"] = (
        combined["price_delta_pct"].abs() > 1.0
    ).astype("int8")

    # Types
    combined["original_item_id"]       = combined["original_item_id"].astype("int64")
    combined["equivalent_item_id"]     = combined["equivalent_item_id"].astype("int64")
    combined["rank"]                    = combined["rank"].astype("int32")
    combined["is_medline_conversion"]   = combined["is_medline_conversion"].astype("int8")
    combined["is_price_improvement"]    = combined["is_price_improvement"].astype("int8")
    combined["original_unit_price"]     = combined["original_unit_price"].astype("float32")
    combined["equivalent_unit_price"]   = combined["equivalent_unit_price"].astype("float32")
    combined["price_delta_pct"]          = combined["price_delta_pct"].astype("float32")
    combined["original_n_buyers"]        = combined["original_n_buyers"].astype("int32")
    combined["equivalent_n_buyers"]      = combined["equivalent_n_buyers"].astype("int32")

    _log(f"Total equivalent pairs: {len(combined):,}")
    _log(f"  Private brand upgrades: "
         f"{(combined['match_type']=='private_brand_upgrade').sum():,}")
    _log(f"  Medline conversions   : "
         f"{(combined['match_type']=='medline_conversion').sum():,}")
    _log(f"  Price anomalies (>100% diff, likely pack-size mismatch): "
         f"{int(combined['price_anomaly'].sum()):,}")

    OUT_PRECOMP.mkdir(parents=True, exist_ok=True)
    OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

    # Slim version for recommendation engine (just IDs and tags)
    slim_cols = [
        "original_item_id", "equivalent_item_id", "rank",
        "match_type", "is_medline_conversion", "is_price_improvement",
        "price_anomaly",
        "original_unit_price", "equivalent_unit_price", "price_delta_pct",
    ]
    slim = combined[slim_cols].copy()
    slim.to_parquet(SLIM_OUT, index=False)
    size_mb = SLIM_OUT.stat().st_size / (1024 * 1024)
    _log(f"Saved slim version : {SLIM_OUT.relative_to(ROOT)}  ({size_mb:.1f} MB)")

    # Full version with descriptions
    combined.to_parquet(FULL_OUT, index=False)
    size_mb = FULL_OUT.stat().st_size / (1024 * 1024)
    _log(f"Saved full version : {FULL_OUT.relative_to(ROOT)}  ({size_mb:.1f} MB)")

    # XLSX with separate sheets for upgrades and conversions
    with pd.ExcelWriter(XLSX_OUT, engine="openpyxl") as writer:
        # Summary sheet
        summary = pd.DataFrame([
            {"metric": "Total equivalent pairs",
             "value": f"{len(combined):,}"},
            {"metric": "Private brand upgrades",
             "value": f"{(combined['match_type']=='private_brand_upgrade').sum():,}"},
            {"metric": "Medline conversions",
             "value": f"{(combined['match_type']=='medline_conversion').sum():,}"},
            {"metric": "",
             "value": ""},
            {"metric": "Unique original products covered",
             "value": f"{combined['original_item_id'].nunique():,}"},
            {"metric": "Unique equivalent products used",
             "value": f"{combined['equivalent_item_id'].nunique():,}"},
            {"metric": "",
             "value": ""},
            {"metric": "Avg price savings (private brand upgrades)",
             "value": f"{combined[combined['match_type']=='private_brand_upgrade']['price_delta_pct'].mean()*100:.1f}%"},
            {"metric": "Avg price delta (Medline conversions)",
             "value": f"{combined[combined['match_type']=='medline_conversion']['price_delta_pct'].mean()*100:.1f}%"},
        ])
        summary.to_excel(writer, sheet_name="01_summary", index=False)

        # Top 500 private brand upgrades (by savings)
        pb_sheet = combined[combined["match_type"] == "private_brand_upgrade"].copy()
        pb_sheet = pb_sheet.nsmallest(500, "price_delta_pct")  # biggest savings first
        pb_sheet.to_excel(writer, sheet_name="02_private_brand_upgrades", index=False)

        # Top 500 Medline conversions (by original popularity - high-volume Medline products)
        md_sheet = combined[combined["match_type"] == "medline_conversion"].copy()
        md_sheet = md_sheet.nlargest(500, "original_n_buyers")
        md_sheet.to_excel(writer, sheet_name="03_medline_conversions", index=False)

    size_kb = XLSX_OUT.stat().st_size / 1024
    _log(f"Saved xlsx         : {XLSX_OUT.relative_to(ROOT)}  ({size_kb:.0f} KB, 3 sheets)")

    return combined


# Step 7: Print samples and validation

def print_samples(combined: pd.DataFrame) -> None:
    _s("Step 7: Sample equivalents")

    # Sample 1: Best price savings on private brand upgrades
    pb = combined[combined["match_type"] == "private_brand_upgrade"]
    if len(pb) > 0:
        _log("")
        _log("  Sample 1: Top 5 private brand upgrades with biggest savings:")
        top_savings = pb.nsmallest(5, "price_delta_pct")
        for _, r in top_savings.iterrows():
            savings_pct = -r["price_delta_pct"] * 100
            _log(f"")
            _log(f"    Original : {str(r['original_desc'])[:55]}")
            _log(f"    McKesson : {str(r['equivalent_desc'])[:55]}")
            _log(f"    Category : {str(r['PROD_CTGRY_LVL2_DSC'])[:40]}")
            _log(f"    Price    : ${r['original_unit_price']:.2f} -> "
                 f"${r['equivalent_unit_price']:.2f}  "
                 f"(save {savings_pct:.0f}%)")
            _log(f"    Pop      : {r['original_n_buyers']:,} -> "
                 f"{r['equivalent_n_buyers']:,} buyers")

    # Sample 2: Medline conversions - most common Medline products
    md = combined[combined["match_type"] == "medline_conversion"]
    if len(md) > 0:
        _log("")
        _log("  Sample 2: Top 5 Medline conversions (high-volume Medline products):")
        # Show rank 1 equivalents only for high-popularity Medline products
        md_top = md[md["rank"] == 1].nlargest(5, "original_n_buyers")
        for _, r in md_top.iterrows():
            delta_pct = r["price_delta_pct"] * 100
            direction = "save" if delta_pct < 0 else "cost"
            _log(f"")
            _log(f"    Medline  : {str(r['original_desc'])[:55]}")
            _log(f"    McKesson : {str(r['equivalent_desc'])[:55]}")
            _log(f"    Supplier : {str(r['equivalent_supplier'])[:30]}")
            _log(f"    Category : {str(r['PROD_CTGRY_LVL2_DSC'])[:40]}")
            _log(f"    Price    : ${r['original_unit_price']:.2f} -> "
                 f"${r['equivalent_unit_price']:.2f}  "
                 f"({direction} {abs(delta_pct):.0f}%)")
            _log(f"    Medline  : {r['original_n_buyers']:,} buyers  "
                 f"McK alt: {r['equivalent_n_buyers']:,} buyers")


# Main

def main() -> None:
    print()
    print("=" * 64)
    print("  PRIVATE BRAND EQUIVALENTS (McKesson + Medline Conversions)")
    print("=" * 64)
    start = time.time()

    OUT_PRECOMP.mkdir(parents=True, exist_ok=True)
    OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        economics = load_product_economics(con)
    finally:
        con.close()

    catalog       = load_product_catalog(economics)
    pb_pool       = build_mckesson_brand_pool(catalog)
    general_pool  = build_mckesson_all_pool(catalog)

    upgrades      = find_private_brand_upgrades(catalog, pb_pool)
    conversions   = find_medline_conversions(catalog, general_pool)

    combined = save_outputs(upgrades, conversions)
    print_samples(combined)

    _s("Complete")
    _log(f"Total time: {time.time() - start:.1f}s")
    _log("")
    _log(f"Outputs:")
    _log(f"  private_brand_equivalents.parquet              (slim, for recommendation engine)")
    _log(f"  private_brand_equivalents_with_metadata.parquet (full, for inspection)")
    _log(f"  private_brand_equivalents_analysis.xlsx        (3 sheets)")
    _log("")
    _log("How this is used in recommendation engine:")
    _log("  - For each product customer buys, lookup by original_item_id")
    _log("  - If match_type='private_brand_upgrade' -> recommend McKesson Brand (Type 5)")
    _log("  - If match_type='medline_conversion' -> recommend alternative (Type 4)")
    _log("  - Runtime cart: if Medline in cart, ALWAYS surface McKesson equivalent")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"\nFATAL ERROR: {exc}", file=sys.stderr)
        raise