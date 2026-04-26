from __future__ import annotations

from typing import Optional

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.parquet_store import (
    PRIVATE_BRAND_FILE,
    PRODUCT_COOCCURRENCE_FILE,
    PRODUCT_SEGMENTS_FILE,
    PRODUCT_SPECIALTY_FILE,
    RECOMMENDATIONS_FILE,
    duckdb_query,
)
from backend.models import Customer, Inventory, Product
from backend.schemas.recommendation import (
    CartComplement,
    MedlineConversion,
    PrivateBrandUpgrade,
    RecommendationItem,
)


# Inventory / product hydration helpers
async def _fetch_inventory(
    db: AsyncSession, item_ids: list[int]
) -> dict[int, int]:
    """Return dict: item_id -> units_available, for the given item_ids."""
    if not item_ids:
        return {}
    result = await db.execute(
        select(Inventory.item_id, Inventory.units_available).where(
            Inventory.item_id.in_(item_ids)
        )
    )
    return {int(iid): int(units) for iid, units in result.all()}


async def _fetch_product_descriptions(
    db: AsyncSession, item_ids: list[int]
) -> dict[int, dict]:
    """Return dict: item_id -> {description, family, category, is_pb, unit_price}."""
    if not item_ids:
        return {}
    result = await db.execute(
        select(
            Product.item_id,
            Product.description,
            Product.family,
            Product.category,
            Product.is_private_brand,
            Product.unit_price,
        ).where(Product.item_id.in_(item_ids))
    )
    out = {}
    for item_id, desc, family, category, is_pb, unit_price in result.all():
        out[int(item_id)] = {
            "description": desc,
            "family": family,
            "category": category,
            "is_private_brand": bool(is_pb),
            "unit_price": unit_price,
        }
    return out


# Top-N recommendations: precomputed path
def _query_precomputed(cust_id: int, n: int = 10) -> pd.DataFrame:
    """Read top-N recommendations for cust_id from the parquet."""
    return duckdb_query(
        """
        SELECT
            rank,
            DIM_ITEM_E1_CURR_ID AS item_id,
            ITEM_DSC AS description,
            PROD_FMLY_LVL1_DSC AS family,
            PROD_CTGRY_LVL2_DSC AS category,
            primary_signal,
            rec_purpose,
            pitch_reason,
            confidence_tier,
            is_mckesson_brand,
            is_private_brand,
            median_unit_price,
            peer_adoption_rate,
            specialty_match
        FROM read_parquet(?)
        WHERE DIM_CUST_CURR_ID = ?
        ORDER BY rank
        LIMIT ?
        """,
        [str(RECOMMENDATIONS_FILE), int(cust_id), int(n)],
    )


# Top-N recommendations: cold-start fallback
# Rules:
#  - Sort by recent_buyer_count_6mo DESC (recent popularity beats lifetime
#    popularity)
#  - Filter out is_discontinued = 1 products
#  - If customer has a specialty_code, prefer items where top_specialty_1
#    matches the customer's specialty (specialty match wins ties)
def _query_cold_start(
    segment: Optional[str],
    specialty: Optional[str],
    n: int = 10,
) -> pd.DataFrame:
    """Build cold-start recommendations from segment popularity."""
    if specialty:
        return duckdb_query(
            """
            WITH seg_pop AS (
                SELECT
                    DIM_ITEM_E1_CURR_ID AS item_id,
                    ITEM_DSC AS description,
                    PROD_FMLY_LVL1_DSC AS family,
                    PROD_CTGRY_LVL2_DSC AS category,
                    is_private_brand,
                    median_unit_price,
                    n_buyers,
                    recent_buyer_count_6mo
                FROM read_parquet(?)
                WHERE COALESCE(is_discontinued, 0) = 0
            ),
            spec AS (
                SELECT
                    DIM_ITEM_E1_CURR_ID AS item_id,
                    top_specialty_1,
                    top_specialty_1_pct
                FROM read_parquet(?)
            )
            SELECT
                seg_pop.item_id,
                seg_pop.description,
                seg_pop.family,
                seg_pop.category,
                seg_pop.is_private_brand,
                seg_pop.median_unit_price,
                seg_pop.n_buyers,
                seg_pop.recent_buyer_count_6mo,
                spec.top_specialty_1 AS primary_specialty,
                spec.top_specialty_1_pct AS specialty_score
            FROM seg_pop
            LEFT JOIN spec ON seg_pop.item_id = spec.item_id
            ORDER BY
                CASE WHEN spec.top_specialty_1 = ? THEN 0 ELSE 1 END,
                seg_pop.recent_buyer_count_6mo DESC NULLS LAST,
                seg_pop.n_buyers DESC NULLS LAST
            LIMIT ?
            """,
            [
                str(PRODUCT_SEGMENTS_FILE),
                str(PRODUCT_SPECIALTY_FILE),
                specialty,
                int(n),
            ],
        )

    # No specialty info — global popularity only
    return duckdb_query(
        """
        SELECT
            DIM_ITEM_E1_CURR_ID AS item_id,
            ITEM_DSC AS description,
            PROD_FMLY_LVL1_DSC AS family,
            PROD_CTGRY_LVL2_DSC AS category,
            is_private_brand,
            median_unit_price,
            n_buyers,
            recent_buyer_count_6mo
        FROM read_parquet(?)
        WHERE COALESCE(is_discontinued, 0) = 0
        ORDER BY
            recent_buyer_count_6mo DESC NULLS LAST,
            n_buyers DESC NULLS LAST
        LIMIT ?
        """,
        [str(PRODUCT_SEGMENTS_FILE), int(n)],
    )


# Top-N orchestrator
async def get_recommendations(
    db: AsyncSession,
    cust_id: int,
    n: int = 10,
) -> tuple[str, list[RecommendationItem], Optional[str], Optional[str]]:
    """Return (source, items, segment, specialty) for the given customer."""
    customer = await db.get(Customer, cust_id)
    segment = customer.segment if customer else None
    specialty = customer.specialty_code if customer else None

    # --- Try precomputed first ---
    df = _query_precomputed(cust_id, n)

    if not df.empty:
        item_ids = df["item_id"].astype("int64").tolist()
        stock_map = await _fetch_inventory(db, item_ids)

        items: list[RecommendationItem] = []
        for _, row in df.iterrows():
            iid = int(row["item_id"])
            items.append(RecommendationItem(
                rank=int(row["rank"]),
                item_id=iid,
                description=row.get("description"),
                family=row.get("family"),
                category=row.get("category"),
                primary_signal=str(row["primary_signal"]),
                rec_purpose=row.get("rec_purpose"),
                pitch_reason=row.get("pitch_reason"),
                confidence_tier=row.get("confidence_tier"),
                is_mckesson_brand=bool(row.get("is_mckesson_brand", False)),
                is_private_brand=bool(row.get("is_private_brand", False)),
                median_unit_price=row.get("median_unit_price"),
                peer_adoption_rate=(
                    float(row["peer_adoption_rate"])
                    if pd.notna(row.get("peer_adoption_rate"))
                    else None
                ),
                specialty_match=row.get("specialty_match"),
                units_in_stock=stock_map.get(iid),
            ))
        return "precomputed", items, segment, specialty

    # --- Cold-start fallback ---
    df = _query_cold_start(segment, specialty, n)
    if df.empty:
        return "cold_start", [], segment, specialty

    item_ids = df["item_id"].astype("int64").tolist()
    stock_map = await _fetch_inventory(db, item_ids)

    items = []
    for rank, (_, row) in enumerate(df.iterrows(), start=1):
        iid = int(row["item_id"])
        # is_private_brand from parquet is BIGINT (0 or 1); coerce to bool
        is_pb = bool(row.get("is_private_brand", 0))

        if specialty and row.get("primary_specialty") == specialty:
            pitch = f"Popular among customers in your specialty ({specialty})."
            spec_match = "match"
        elif segment:
            pitch = f"Popular among customers in your segment ({segment})."
            spec_match = "neutral"
        else:
            pitch = "Popular product across our customer base."
            spec_match = "neutral"

        items.append(RecommendationItem(
            rank=rank,
            item_id=iid,
            description=row.get("description"),
            family=row.get("family"),
            category=row.get("category"),
            primary_signal="popularity",
            rec_purpose="cold_start",
            pitch_reason=pitch,
            confidence_tier="medium",
            is_mckesson_brand=is_pb,  # in this dataset PB == McKesson Brand
            is_private_brand=is_pb,
            median_unit_price=row.get("median_unit_price"),
            peer_adoption_rate=None,
            specialty_match=spec_match,
            units_in_stock=stock_map.get(iid),
        ))

    return "cold_start", items, segment, specialty


# Cart helper
def _query_cart_complements(cart_items: list[int], n: int = 5) -> pd.DataFrame:
    """Find products that frequently co-occur with cart items."""
    if not cart_items:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in cart_items)
    cart_csv = ",".join(str(int(i)) for i in cart_items)
    params = (
        [str(PRODUCT_COOCCURRENCE_FILE)]
        + [int(i) for i in cart_items]
        + [str(PRODUCT_COOCCURRENCE_FILE)]
        + [int(i) for i in cart_items]
        + [int(n)]
    )
    return duckdb_query(
        f"""
        WITH pairs AS (
            SELECT
                item_a AS trigger_item_id,
                item_b AS partner_item_id,
                lift, support, confidence
            FROM read_parquet(?)
            WHERE item_a IN ({placeholders})
            UNION ALL
            SELECT
                item_b AS trigger_item_id,
                item_a AS partner_item_id,
                lift, support, confidence
            FROM read_parquet(?)
            WHERE item_b IN ({placeholders})
        ),
        ranked AS (
            SELECT
                partner_item_id,
                trigger_item_id,
                lift, support, confidence,
                ROW_NUMBER() OVER (
                    PARTITION BY partner_item_id ORDER BY lift DESC
                ) AS rnk
            FROM pairs
            WHERE partner_item_id NOT IN ({cart_csv})
        )
        SELECT partner_item_id AS item_id,
               trigger_item_id,
               lift, support, confidence
        FROM ranked
        WHERE rnk = 1
        ORDER BY lift DESC
        LIMIT ?
        """,
        params,
    )


def _query_pb_upgrades(cart_items: list[int], n: int = 3) -> pd.DataFrame:
    """Find McKesson Brand alternatives for cart items."""
    if not cart_items:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in cart_items)
    return duckdb_query(
        f"""
        SELECT
            cart_item_id,
            pb_item_id,
            estimated_savings_pct,
            pair_type
        FROM read_parquet(?)
        WHERE cart_item_id IN ({placeholders})
          AND pair_type = 'pb_upgrade'
        ORDER BY estimated_savings_pct DESC NULLS LAST
        LIMIT ?
        """,
        [str(PRIVATE_BRAND_FILE)] + [int(i) for i in cart_items] + [int(n)],
    )


def _query_medline_conversions(cart_items: list[int], n: int = 3) -> pd.DataFrame:
    """Find McKesson alternatives for Medline cart items."""
    if not cart_items:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in cart_items)
    return duckdb_query(
        f"""
        SELECT
            cart_item_id AS medline_item_id,
            pb_item_id AS mckesson_item_id,
            estimated_savings_pct,
            pair_type
        FROM read_parquet(?)
        WHERE cart_item_id IN ({placeholders})
          AND pair_type = 'medline_conversion'
        ORDER BY estimated_savings_pct DESC NULLS LAST
        LIMIT ?
        """,
        [str(PRIVATE_BRAND_FILE)] + [int(i) for i in cart_items] + [int(n)],
    )


async def get_cart_helper(
    db: AsyncSession,
    cust_id: int,
    cart_items: list[int],
) -> tuple[list[CartComplement], list[PrivateBrandUpgrade], list[MedlineConversion]]:
    """Run the three live signals against the cart and return suggestions."""
    cart_items = [int(i) for i in cart_items if i]

    try:
        comp_df = _query_cart_complements(cart_items, n=5)
    except Exception:
        comp_df = pd.DataFrame()

    try:
        pb_df = _query_pb_upgrades(cart_items, n=3)
    except Exception:
        pb_df = pd.DataFrame()

    try:
        med_df = _query_medline_conversions(cart_items, n=3)
    except Exception:
        med_df = pd.DataFrame()

    needed: set[int] = set()
    if not comp_df.empty:
        needed.update(int(x) for x in comp_df["item_id"].tolist())
        needed.update(int(x) for x in comp_df["trigger_item_id"].tolist())
    if not pb_df.empty:
        needed.update(int(x) for x in pb_df["cart_item_id"].tolist())
        needed.update(int(x) for x in pb_df["pb_item_id"].tolist())
    if not med_df.empty:
        needed.update(int(x) for x in med_df["medline_item_id"].tolist())
        needed.update(int(x) for x in med_df["mckesson_item_id"].tolist())

    needed_list = sorted(needed)
    desc_map = await _fetch_product_descriptions(db, needed_list)
    stock_map = await _fetch_inventory(db, needed_list)

    complements: list[CartComplement] = []
    for _, row in comp_df.iterrows():
        iid = int(row["item_id"])
        trigger_id = int(row["trigger_item_id"])
        d = desc_map.get(iid, {})
        td = desc_map.get(trigger_id, {})
        lift = float(row["lift"])
        complements.append(CartComplement(
            trigger_item_id=trigger_id,
            trigger_description=td.get("description"),
            item_id=iid,
            description=d.get("description"),
            family=d.get("family"),
            category=d.get("category"),
            lift=lift,
            support=(
                float(row["support"]) if "support" in row and pd.notna(row["support"])
                else None
            ),
            confidence=(
                float(row["confidence"]) if "confidence" in row and pd.notna(row["confidence"])
                else None
            ),
            is_mckesson_brand=False,
            median_unit_price=d.get("unit_price"),
            units_in_stock=stock_map.get(iid),
            pitch_reason=(
                f"Customers who buy this item also buy your cart product "
                f"with {lift:.1f}x lift."
            ),
        ))

    pb_upgrades: list[PrivateBrandUpgrade] = []
    for _, row in pb_df.iterrows():
        cart_iid = int(row["cart_item_id"])
        pb_iid = int(row["pb_item_id"])
        cd = desc_map.get(cart_iid, {})
        pd_ = desc_map.get(pb_iid, {})
        savings = (
            float(row["estimated_savings_pct"])
            if pd.notna(row.get("estimated_savings_pct"))
            else None
        )
        pitch = "McKesson Brand alternative for an item in your cart."
        if savings:
            pitch += f" Save approximately {savings:.0f} percent."
        pb_upgrades.append(PrivateBrandUpgrade(
            cart_item_id=cart_iid,
            cart_item_description=cd.get("description"),
            pb_item_id=pb_iid,
            pb_description=pd_.get("description"),
            family=pd_.get("family"),
            category=pd_.get("category"),
            estimated_savings_pct=savings,
            median_unit_price=pd_.get("unit_price"),
            units_in_stock=stock_map.get(pb_iid),
            pitch_reason=pitch,
        ))

    conversions: list[MedlineConversion] = []
    for _, row in med_df.iterrows():
        med_iid = int(row["medline_item_id"])
        mck_iid = int(row["mckesson_item_id"])
        md = desc_map.get(med_iid, {})
        mckd = desc_map.get(mck_iid, {})
        savings = (
            float(row["estimated_savings_pct"])
            if pd.notna(row.get("estimated_savings_pct"))
            else None
        )
        pitch = "McKesson alternative for a Medline product in your cart."
        if savings:
            pitch += f" Save approximately {savings:.0f} percent."
        conversions.append(MedlineConversion(
            medline_item_id=med_iid,
            medline_description=md.get("description"),
            mckesson_item_id=mck_iid,
            mckesson_description=mckd.get("description"),
            family=mckd.get("family"),
            category=mckd.get("category"),
            estimated_savings_pct=savings,
            median_unit_price=mckd.get("unit_price"),
            units_in_stock=stock_map.get(mck_iid),
            pitch_reason=pitch,
        ))

    return complements, pb_upgrades, conversions