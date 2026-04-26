from __future__ import annotations

import sys
import time
import warnings
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.preprocessing import normalize

warnings.filterwarnings("ignore")


# Paths

ROOT         = Path(__file__).resolve().parent.parent.parent
DATA_CLEAN   = ROOT / "data_clean"
MERGED_FILE  = DATA_CLEAN / "serving"  / "merged_dataset.parquet"
PRODUCT_FILE = DATA_CLEAN / "product"  / "products_clean.parquet"
OUT_PRECOMP  = DATA_CLEAN / "serving"  / "precomputed"
OUT_ANALYSIS = DATA_CLEAN / "analysis"


# Configuration

# Fiscal years to include. Both years give more data for robust similarity.
# Product relationships don't change fast in medical supplies.
FISCAL_YEARS = ("FY2425", "FY2526")

# Minimum customers who bought a product to include it in similarity matrix.
# Products below this have noisy patterns and produce unreliable similarity.
MIN_BUYERS_PER_PRODUCT = 50

# Top N similar products to store per product.
# Sarwar 2001 recommends 20-50. We use 50 for recommendation diversity.
TOP_N_SIMILAR = 50

# Minimum similarity score to keep. Pairs below this are not useful
# recommendations and just inflate file size.
MIN_SIMILARITY = 0.05

# Excluded suppliers and families (same as everywhere else)
EXCLUDED_SUPPLIERS = {"MEDLINE", "MEDLINE INDUSTRIES"}
EXCLUDED_FAMILIES  = {"Fee", "Unknown", "NaN", "nan", ""}

# Batch size for similarity computation. Larger = faster but more memory.
# 500 products per batch uses ~4 GB peak memory for 15K product catalog.
BATCH_SIZE = 500


# Logging

def _s(title: str) -> None:
    print(f"\n{'-' * 64}\n  {title}\n{'-' * 64}", flush=True)


def _log(msg: str) -> None:
    print(f"  {msg}", flush=True)


# DuckDB helpers

def _pq(p: Path) -> str:
    return "'" + p.as_posix() + "'"


# Step 1: Load transactions and filter products

def load_transactions() -> pd.DataFrame:
    _s("Step 1: Loading transactions and filtering products")

    if not MERGED_FILE.exists():
        print(f"\nFATAL: merged_dataset.parquet not found at {MERGED_FILE}",
              file=sys.stderr)
        print("Run clean_data.py first.", file=sys.stderr)
        sys.exit(1)

    t0 = time.time()
    con = duckdb.connect()

    # Detect column names
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
    _log(f"Supplier column       : {supplier_col or 'absent — Medline filter skipped'}")

    # Build filter clauses
    fy_sql = ", ".join(f"'{fy}'" for fy in FISCAL_YEARS)

    family_filter = ""
    if family_col:
        excl_fams = ", ".join(f"'{f}'" for f in sorted(EXCLUDED_FAMILIES))
        family_filter = f"AND COALESCE({family_col}, 'Unknown') NOT IN ({excl_fams})"

    supplier_filter = ""
    if supplier_col:
        excl_sups = ", ".join(f"'{s}'" for s in EXCLUDED_SUPPLIERS)
        supplier_filter = f"AND UPPER(COALESCE({supplier_col}, '')) NOT IN ({excl_sups})"

    # Aggregate to one row per customer-product pair.
    # implicit feedback = purchase count (order lines) as "rating" proxy.
    _log("Aggregating customer-product pairs via DuckDB...")
    txn = con.execute(f"""
        SELECT
            CAST(DIM_CUST_CURR_ID AS BIGINT)   AS cust_id,
            CAST(DIM_ITEM_E1_CURR_ID AS BIGINT) AS item_id,
            COUNT(*)                            AS purchase_count,
            SUM(UNIT_SLS_AMT)                   AS total_spend
        FROM read_parquet({_pq(MERGED_FILE)})
        WHERE UNIT_SLS_AMT > 0
          AND fiscal_year IN ({fy_sql})
          AND DIM_CUST_CURR_ID IS NOT NULL
          AND DIM_ITEM_E1_CURR_ID IS NOT NULL
          {family_filter}
          {supplier_filter}
        GROUP BY
            CAST(DIM_CUST_CURR_ID AS BIGINT),
            CAST(DIM_ITEM_E1_CURR_ID AS BIGINT)
    """).df()
    con.close()

    _log(f"Loaded {len(txn):,} customer-product pairs in {time.time()-t0:.1f}s")
    _log(f"  Unique customers: {txn['cust_id'].nunique():,}")
    _log(f"  Unique products : {txn['item_id'].nunique():,}")

    # Filter products to those with at least MIN_BUYERS_PER_PRODUCT buyers
    buyer_count = txn.groupby("item_id")["cust_id"].nunique()
    eligible_items = set(buyer_count[buyer_count >= MIN_BUYERS_PER_PRODUCT].index)

    before = txn["item_id"].nunique()
    txn = txn[txn["item_id"].isin(eligible_items)]
    after = txn["item_id"].nunique()

    _log(f"")
    _log(f"Filtered to products with >= {MIN_BUYERS_PER_PRODUCT} buyers:")
    _log(f"  Before : {before:,} products")
    _log(f"  After  : {after:,} products")
    _log(f"  Dropped: {before - after:,} products ({(before-after)/before*100:.1f}%)")

    # Also filter out customers with only 1 purchase (they can't contribute to co-purchase signals)
    cust_item_count = txn.groupby("cust_id")["item_id"].nunique()
    active_custs = set(cust_item_count[cust_item_count >= 2].index)
    before_cust = txn["cust_id"].nunique()
    txn = txn[txn["cust_id"].isin(active_custs)]
    after_cust = txn["cust_id"].nunique()

    _log(f"")
    _log(f"Filtered to customers with >= 2 unique products:")
    _log(f"  Before : {before_cust:,} customers")
    _log(f"  After  : {after_cust:,} customers")
    _log(f"  Dropped: {before_cust - after_cust:,} customers")

    _log(f"")
    _log(f"Final dataset:")
    _log(f"  Customers: {txn['cust_id'].nunique():,}")
    _log(f"  Products : {txn['item_id'].nunique():,}")
    _log(f"  Pairs    : {len(txn):,}")

    return txn


# Step 2: Build sparse user-item matrix

def build_sparse_matrix(
    txn: pd.DataFrame,
) -> tuple[csr_matrix, dict[int, int], dict[int, int]]:
    # Returns:
    #   matrix: scipy sparse CSR matrix, rows = customers, cols = products
    #   cust_to_row: dict mapping cust_id to row index
    #   item_to_col: dict mapping item_id to column index

    _s("Step 2: Building sparse customer-product matrix")
    t0 = time.time()

    # Map IDs to matrix indices
    unique_custs = sorted(txn["cust_id"].unique())
    unique_items = sorted(txn["item_id"].unique())

    cust_to_row = {c: i for i, c in enumerate(unique_custs)}
    item_to_col = {p: i for i, p in enumerate(unique_items)}

    _log(f"Matrix dimensions: {len(unique_custs):,} customers x {len(unique_items):,} products")

    # Build sparse matrix using log-transformed purchase count
    # (log dampens the effect of super-high-frequency customers)
    rows = txn["cust_id"].map(cust_to_row).values
    cols = txn["item_id"].map(item_to_col).values
    data = np.log1p(txn["purchase_count"].values.astype(np.float32))

    matrix = csr_matrix(
        (data, (rows, cols)),
        shape=(len(unique_custs), len(unique_items)),
        dtype=np.float32,
    )

    density = matrix.nnz / (matrix.shape[0] * matrix.shape[1]) * 100
    _log(f"Matrix built in {time.time() - t0:.1f}s")
    _log(f"  Non-zero entries: {matrix.nnz:,}")
    _log(f"  Density         : {density:.4f}%  (typical for recommender systems)")

    return matrix, cust_to_row, item_to_col


# Step 3: Compute adjusted cosine similarity

def compute_adjusted_cosine_similarity(
    matrix: csr_matrix,
) -> np.ndarray:
    # Adjusted cosine similarity: subtracts each customer's mean before computing
    # cosine between products. This corrects for customers who buy a lot
    # (high values) vs customers who buy a little (low values).

    _s("Step 3: Computing adjusted cosine similarity")
    t0 = time.time()

    n_custs, n_items = matrix.shape
    _log(f"Computing similarity for {n_items:,} products...")

    # Step 3a: Mean-center each customer's ratings (Sarwar's adjusted cosine)
    # We do this by subtracting customer mean from non-zero entries only.
    _log("Step 3a: Mean-centering customer ratings...")

    # Compute customer means (only over non-zero entries)
    matrix_csr = matrix.tocsr()
    row_sums = np.asarray(matrix_csr.sum(axis=1)).ravel()
    row_counts = np.diff(matrix_csr.indptr)
    row_means = np.divide(
        row_sums, row_counts,
        out=np.zeros_like(row_sums),
        where=row_counts > 0,
    )

    # Subtract customer mean from each non-zero entry
    matrix_centered = matrix_csr.copy().astype(np.float32)
    for i in range(n_custs):
        start, end = matrix_centered.indptr[i], matrix_centered.indptr[i + 1]
        matrix_centered.data[start:end] -= row_means[i]

    _log(f"  Mean-centering done in {time.time() - t0:.1f}s")

    # Step 3b: Normalize columns (products) to unit L2 norm
    # After normalization, dot product = cosine similarity
    _log("Step 3b: Normalizing product vectors...")
    t1 = time.time()

    matrix_csc = matrix_centered.tocsc()
    item_matrix = normalize(matrix_csc.T, norm="l2", axis=1, copy=False)
    # item_matrix: rows = products, cols = customers (normalized)

    _log(f"  Normalization done in {time.time() - t1:.1f}s")

    # Step 3c: Compute item-item similarity matrix in batches
    # We compute sim = item_matrix @ item_matrix.T in chunks to save memory
    _log(f"Step 3c: Computing item-item similarity in batches of {BATCH_SIZE}...")
    t2 = time.time()

    similarity_matrix = np.zeros((n_items, n_items), dtype=np.float32)

    n_batches = (n_items + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_idx in range(n_batches):
        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, n_items)

        batch = item_matrix[start:end]
        batch_sim = (batch @ item_matrix.T).toarray()
        similarity_matrix[start:end] = batch_sim

        if (batch_idx + 1) % 10 == 0 or batch_idx == n_batches - 1:
            elapsed = time.time() - t2
            pct = (batch_idx + 1) / n_batches * 100
            _log(f"  Batch {batch_idx + 1}/{n_batches} ({pct:.1f}%)  elapsed: {elapsed:.1f}s")

    # Zero out self-similarity (item with itself is always 1.0, not useful)
    np.fill_diagonal(similarity_matrix, 0.0)

    _log(f"Similarity matrix built in {time.time() - t2:.1f}s")
    _log(f"Total similarity computation: {time.time() - t0:.1f}s")

    return similarity_matrix


# Step 4: Extract top-N similar items per item

def extract_top_n_similar(
    similarity_matrix: np.ndarray,
    item_to_col: dict[int, int],
) -> pd.DataFrame:
    # For each product, keep only its TOP_N_SIMILAR most similar products.
    # This drastically reduces output file size from O(N^2) to O(N * top_n).

    _s(f"Step 4: Extracting top {TOP_N_SIMILAR} similar items per product")
    t0 = time.time()

    n_items = similarity_matrix.shape[0]
    col_to_item = {v: k for k, v in item_to_col.items()}

    # Pre-allocate arrays
    top_n = min(TOP_N_SIMILAR, n_items - 1)
    all_rows = []

    _log(f"Processing {n_items:,} products...")

    for i in range(n_items):
        row = similarity_matrix[i]

        # Find top N indices by similarity (excluding the item itself which is 0)
        # Use argpartition for speed (doesn't fully sort, just finds top N)
        if top_n < n_items:
            top_idx = np.argpartition(row, -top_n)[-top_n:]
            # Sort these top N by similarity descending
            top_idx = top_idx[np.argsort(row[top_idx])[::-1]]
        else:
            top_idx = np.argsort(row)[::-1]

        # Filter out by minimum similarity
        top_scores = row[top_idx]
        mask = top_scores >= MIN_SIMILARITY
        top_idx = top_idx[mask]
        top_scores = top_scores[mask]

        item_a = col_to_item[i]
        for rank, (j, score) in enumerate(zip(top_idx, top_scores), 1):
            all_rows.append({
                "item_a":     item_a,
                "item_b":     col_to_item[j],
                "rank":       rank,
                "similarity": float(score),
            })

        if (i + 1) % 2000 == 0:
            _log(f"  Processed {i + 1:,}/{n_items:,} products")

    df = pd.DataFrame(all_rows)

    _log(f"")
    _log(f"Extracted {len(df):,} similarity pairs in {time.time() - t0:.1f}s")
    _log(f"  Products with similars: {df['item_a'].nunique():,}")
    _log(f"  Avg similars per product: {len(df) / max(df['item_a'].nunique(), 1):.1f}")
    _log(f"  Similarity range: [{df['similarity'].min():.3f}, {df['similarity'].max():.3f}]")
    _log(f"  Median similarity: {df['similarity'].median():.3f}")

    return df


# Step 5: Enrich with product metadata

def enrich_with_metadata(similarity_df: pd.DataFrame) -> pd.DataFrame:
    # Add product descriptions and family info to make the output
    # human-readable and debuggable.

    _s("Step 5: Enriching with product metadata")

    if not PRODUCT_FILE.exists():
        _log("Warning: products_clean.parquet not found — skipping metadata enrichment")
        return similarity_df

    products = pd.read_parquet(PRODUCT_FILE, columns=[
        "DIM_ITEM_E1_CURR_ID", "ITEM_DSC",
        "PROD_FMLY_LVL1_DSC", "PROD_CTGRY_LVL2_DSC",
        "SUPLR_ROLLUP_DSC", "is_private_brand",
    ])
    products["DIM_ITEM_E1_CURR_ID"] = products["DIM_ITEM_E1_CURR_ID"].astype("int64")

    # Enrich item_a
    a_meta = products.rename(columns={
        "DIM_ITEM_E1_CURR_ID": "item_a",
        "ITEM_DSC":            "item_a_desc",
        "PROD_FMLY_LVL1_DSC":  "item_a_family",
        "PROD_CTGRY_LVL2_DSC": "item_a_category",
        "SUPLR_ROLLUP_DSC":    "item_a_supplier",
        "is_private_brand":    "item_a_private_brand",
    })
    similarity_df["item_a"] = similarity_df["item_a"].astype("int64")
    similarity_df = similarity_df.merge(a_meta, on="item_a", how="left")

    # Enrich item_b
    b_meta = products.rename(columns={
        "DIM_ITEM_E1_CURR_ID": "item_b",
        "ITEM_DSC":            "item_b_desc",
        "PROD_FMLY_LVL1_DSC":  "item_b_family",
        "PROD_CTGRY_LVL2_DSC": "item_b_category",
        "SUPLR_ROLLUP_DSC":    "item_b_supplier",
        "is_private_brand":    "item_b_private_brand",
    })
    similarity_df["item_b"] = similarity_df["item_b"].astype("int64")
    similarity_df = similarity_df.merge(b_meta, on="item_b", how="left")

    # Flag same-family pairs (likely substitutes vs cross-sells)
    similarity_df["same_family"] = (
        similarity_df["item_a_family"] == similarity_df["item_b_family"]
    ).astype(int)

    _log(f"Metadata merge complete: {len(similarity_df):,} rows enriched")
    _log(f"  Same-family pairs: {similarity_df['same_family'].sum():,} "
         f"({similarity_df['same_family'].mean()*100:.1f}%)")
    _log(f"  Cross-family pairs: {(1-similarity_df['same_family']).sum():,} "
         f"({(1-similarity_df['same_family'].mean())*100:.1f}%)")

    return similarity_df


# Step 6: Save outputs

def save_outputs(similarity_df: pd.DataFrame) -> None:
    _s("Step 6: Saving outputs")

    OUT_PRECOMP.mkdir(parents=True, exist_ok=True)
    OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

    # Enforce types for downstream consumption
    similarity_df = similarity_df.copy()
    similarity_df["item_a"] = similarity_df["item_a"].astype("int64")
    similarity_df["item_b"] = similarity_df["item_b"].astype("int64")
    similarity_df["rank"] = similarity_df["rank"].astype("int32")
    similarity_df["similarity"] = similarity_df["similarity"].astype("float32")

    # Sort for consistent output
    similarity_df = similarity_df.sort_values(
        ["item_a", "rank"], ascending=[True, True]
    ).reset_index(drop=True)

    # Slim parquet for recommendation engine (minimal columns, fast lookups)
    slim = similarity_df[["item_a", "item_b", "rank", "similarity", "same_family"]].copy()
    slim_path = OUT_PRECOMP / "item_similarity.parquet"
    slim.to_parquet(slim_path, index=False)

    size_mb = slim_path.stat().st_size / (1024 * 1024)
    _log(f"Saved: {slim_path.relative_to(ROOT)}  ({size_mb:.1f} MB)")

    # Full analysis parquet with product metadata for inspection
    analysis_path = OUT_ANALYSIS / "item_similarity_with_metadata.parquet"
    similarity_df.to_parquet(analysis_path, index=False)
    size_mb = analysis_path.stat().st_size / (1024 * 1024)
    _log(f"Saved: {analysis_path.relative_to(ROOT)}  ({size_mb:.1f} MB)")


# Step 7: Print sample similarities

def print_samples(similarity_df: pd.DataFrame) -> None:
    _s("Step 7: Sample similarity results")

    if "item_a_desc" not in similarity_df.columns:
        _log("Metadata not available — skipping samples")
        return

    # Pick 5 popular products to show their top similar items
    popular = similarity_df["item_a"].value_counts().head(5).index.tolist()

    for item_a in popular:
        top5 = similarity_df[similarity_df["item_a"] == item_a].head(5)
        if len(top5) == 0:
            continue

        desc_a = str(top5.iloc[0].get("item_a_desc", "unknown"))[:55]
        fam_a  = str(top5.iloc[0].get("item_a_family", "unknown"))[:40]
        _log(f"")
        _log(f"  Product: {desc_a}")
        _log(f"  Family : {fam_a}")
        _log(f"  Top 5 similar products:")
        for _, r in top5.iterrows():
            desc_b = str(r.get("item_b_desc", "unknown"))[:55]
            fam_b  = str(r.get("item_b_family", "unknown"))[:25]
            same = " [same family]" if r["same_family"] == 1 else ""
            _log(f"    {r['similarity']:.3f}  {desc_b:<57} ({fam_b}){same}")


# Main

def main() -> None:
    print()
    print("=" * 64)
    print("  ITEM-BASED COLLABORATIVE FILTERING (Sarwar 2001)")
    print("=" * 64)
    start = time.time()

    OUT_PRECOMP.mkdir(parents=True, exist_ok=True)
    OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

    txn = load_transactions()
    matrix, cust_to_row, item_to_col = build_sparse_matrix(txn)
    similarity_matrix = compute_adjusted_cosine_similarity(matrix)
    similarity_df = extract_top_n_similar(similarity_matrix, item_to_col)
    similarity_df = enrich_with_metadata(similarity_df)
    save_outputs(similarity_df)
    print_samples(similarity_df)

    _s("Complete")
    _log(f"Total time: {time.time() - start:.1f}s")
    _log("")
    _log("Methodology: Adjusted cosine similarity (Sarwar et al. 2001)")
    _log(f"  - {len(item_to_col):,} products with >= {MIN_BUYERS_PER_PRODUCT} buyers")
    _log(f"  - Top {TOP_N_SIMILAR} similar products stored per product")
    _log(f"  - Minimum similarity threshold: {MIN_SIMILARITY}")
    _log("")
    _log("Key outputs:")
    _log("  item_similarity.parquet                  slim version for recommendation engine")
    _log("  item_similarity_with_metadata.parquet    full version with product descriptions")
    _log("")
    _log("Used by recommendation_factors.py as a second candidate source alongside")
    _log("segment-based peer_gap signals.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"\nFATAL ERROR: {exc}", file=sys.stderr)
        raise