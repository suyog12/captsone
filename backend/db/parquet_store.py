from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from backend.config import settings


# Paths to the parquet files the API needs

PRECOMPUTED_DIR = settings.precomputed_dir

RECOMMENDATIONS_FILE      = PRECOMPUTED_DIR / "recommendations.parquet"
PRODUCT_COOCCURRENCE_FILE = PRECOMPUTED_DIR / "product_cooccurrence.parquet"
PRIVATE_BRAND_FILE         = PRECOMPUTED_DIR / "private_brand_equivalents.parquet"
ITEM_SIMILARITY_FILE       = PRECOMPUTED_DIR / "item_similarity.parquet"
PRODUCT_SEGMENTS_FILE      = PRECOMPUTED_DIR / "product_segments.parquet"
PRODUCT_SPECIALTY_FILE     = PRECOMPUTED_DIR / "product_specialty.parquet"
CUSTOMER_PATTERNS_FILE     = PRECOMPUTED_DIR / "customer_patterns.parquet"
CUSTOMER_SEGMENTS_FILE     = PRECOMPUTED_DIR / "customer_segments.parquet"
CUSTOMER_LAPSED_FILE       = PRECOMPUTED_DIR / "customer_lapsed_products.parquet"
CUSTOMER_REPLENISH_FILE    = PRECOMPUTED_DIR / "customer_replenishment_candidates.parquet"
SEGMENT_PATTERNS_FILE      = PRECOMPUTED_DIR / "segment_patterns.parquet"
SEGMENT_CATEGORY_FILE      = PRECOMPUTED_DIR / "segment_category_profiles.parquet"
SEGMENT_CADENCE_FILE       = PRECOMPUTED_DIR / "product_segment_cadence.parquet"

MERGED_DATASET_FILE = settings.merged_file


# Query helper


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """
    Open a fresh in-memory DuckDB connection.

    The caller is responsible for closing the connection (or use a `with`
    block via the context manager helper below).
    """
    return duckdb.connect(database=":memory:")


def duckdb_query(sql: str, params: list[Any] | None = None) -> pd.DataFrame:
    """
    Run a SQL query through DuckDB and return the result as a pandas
    DataFrame.

    Parameters are passed positionally as a list so the query string
    can use `?` placeholders for safe parameter substitution.

    Example:
        df = duckdb_query(
            "SELECT * FROM read_parquet(?) WHERE customer_id = ?",
            [str(RECOMMENDATIONS_FILE), 123456],
        )
    """
    con = get_duckdb_connection()
    try:
        if params is None:
            return con.execute(sql).fetchdf()
        return con.execute(sql, params).fetchdf()
    finally:
        con.close()


def parquet_health_check() -> dict[str, Any]:
    """
    Quick smoke test that DuckDB can read the recommendations parquet.

    Returns a dict with row count and a sample row, used by the
    /health/parquet endpoint.
    """
    if not RECOMMENDATIONS_FILE.exists():
        return {
            "ok": False,
            "error": f"File not found: {RECOMMENDATIONS_FILE}",
        }

    n_rows = duckdb_query(
        "SELECT COUNT(*) AS n FROM read_parquet(?)",
        [str(RECOMMENDATIONS_FILE)],
    ).iloc[0]["n"]

    sample = duckdb_query(
        "SELECT * FROM read_parquet(?) LIMIT 1",
        [str(RECOMMENDATIONS_FILE)],
    )

    return {
        "ok": True,
        "file": str(RECOMMENDATIONS_FILE.name),
        "n_rows": int(n_rows),
        "sample_columns": list(sample.columns),
    }
