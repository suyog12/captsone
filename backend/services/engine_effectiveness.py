from __future__ import annotations

from decimal import Decimal
from typing import Optional

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.display_names import (
    is_recommendation_source,
    source_display,
    SOURCE_DISPLAY,
)
from backend.models import CartItem, RecommendationEvent


# Map cart_items.source values to "signal" codes used in
# recommendation_events.signal. Only recommendation_* sources participate.
# 'manual' is intentionally excluded - it isn't an engine signal.
SOURCE_TO_SIGNAL = {
    "recommendation_peer_gap":          "peer_gap",
    "recommendation_popularity":        "popularity",
    "recommendation_cart_complement":   "cart_complement",
    "recommendation_item_similarity":   "item_similarity",
    "recommendation_replenishment":     "replenishment",
    "recommendation_lapsed":            "lapsed_recovery",
    "recommendation_pb_upgrade":        "private_brand_upgrade",
    "recommendation_medline_conversion":"medline_conversion",
}

# Reverse lookup for joining cart-side and rejection-side counters by signal.
SIGNAL_TO_SOURCE = {v: k for k, v in SOURCE_TO_SIGNAL.items()}

# Friendly display names per signal (kept here so this module is self-
# contained; mirrors what the frontend SIGNALS map shows).
SIGNAL_DISPLAY = {
    "peer_gap":              "Peer Gap",
    "popularity":            "Popular Pick",
    "cart_complement":       "Cart Complement",
    "item_similarity":       "Similar Item",
    "replenishment":         "Replenishment",
    "lapsed_recovery":       "Lapsed Recovery",
    "private_brand_upgrade": "Private Brand Upgrade",
    "medline_conversion":    "Medline Conversion",
}


async def get_engine_effectiveness(db: AsyncSession) -> dict:
    """Return a per-signal funnel: adds -> sold + rejected.
    Numerator/denominator math used by the admin panel:
      adds            : count of cart_items WHERE source = recommendation_*
      sold            : adds with status = 'sold'
      not_sold_cart   : adds with status = 'not_sold' (cart-side decline)
      rejected        : count of recommendation_events WHERE outcome = 'rejected'
      total_engaged   : adds + rejected (the seller did *something* with it)
      conversion_rate : sold / adds (when adds > 0)
      acceptance_rate : adds / total_engaged (when total_engaged > 0)
      rejection_rate  : rejected / total_engaged (when total_engaged > 0)
    """
    # 1) Cart-side counters per source. We filter to recommendation_* only;
    # 'manual' adds aren't engine output and shouldn't count toward
    # acceptance/rejection rates.
    sold_expr = func.sum(
        case((CartItem.status == "sold", 1), else_=0)
    ).label("sold")
    not_sold_expr = func.sum(
        case((CartItem.status == "not_sold", 1), else_=0)
    ).label("not_sold")
    revenue_expr = func.coalesce(
        func.sum(
            case(
                (CartItem.status == "sold",
                 CartItem.quantity * CartItem.unit_price_at_add),
                else_=0,
            )
        ),
        0,
    ).label("revenue")

    cart_stmt = (
        select(
            CartItem.source,
            func.count(CartItem.cart_item_id).label("adds"),
            sold_expr,
            not_sold_expr,
            revenue_expr,
        )
        .group_by(CartItem.source)
    )
    cart_rows = (await db.execute(cart_stmt)).all()

    cart_by_signal: dict[str, dict] = {}
    for r in cart_rows:
        src = r[0]
        if not is_recommendation_source(src):
            continue
        signal = SOURCE_TO_SIGNAL.get(src)
        if signal is None:
            continue  # unknown / future source
        cart_by_signal[signal] = {
            "adds": int(r[1] or 0),
            "sold": int(r[2] or 0),
            "not_sold": int(r[3] or 0),
            "revenue": Decimal(str(r[4] or 0)),
        }

    # 2) Rejection counters per signal from recommendation_events
    rej_stmt = (
        select(
            RecommendationEvent.signal,
            func.count(RecommendationEvent.event_id).label("rejected"),
        )
        .where(RecommendationEvent.outcome == "rejected")
        .group_by(RecommendationEvent.signal)
    )
    rej_rows = (await db.execute(rej_stmt)).all()
    rej_by_signal: dict[str, int] = {}
    for r in rej_rows:
        sig = (r[0] or "").strip()
        if not sig:
            continue
        rej_by_signal[sig] = int(r[1] or 0)

    # 3) Reason breakdown across all rejections (top-level summary; not
    # broken out by signal because volume is usually too low for that)
    reason_stmt = (
        select(
            RecommendationEvent.rejection_reason_code,
            func.count(RecommendationEvent.event_id),
        )
        .where(RecommendationEvent.outcome == "rejected")
        .where(RecommendationEvent.rejection_reason_code.isnot(None))
        .group_by(RecommendationEvent.rejection_reason_code)
    )
    reason_rows = (await db.execute(reason_stmt)).all()
    reasons = [
        {
            "code": r[0],
            "count": int(r[1] or 0),
        }
        for r in reason_rows
    ]
    reasons.sort(key=lambda x: x["count"], reverse=True)

    # 4) Build per-signal output row joining cart + rejection counters
    all_signals = set(cart_by_signal.keys()) | set(rej_by_signal.keys())
    rows = []
    totals = {
        "adds": 0,
        "sold": 0,
        "not_sold": 0,
        "rejected": 0,
        "revenue": Decimal("0"),
    }
    for sig in sorted(all_signals):
        cart = cart_by_signal.get(sig, {
            "adds": 0, "sold": 0, "not_sold": 0, "revenue": Decimal("0")
        })
        rejected = rej_by_signal.get(sig, 0)
        engaged = cart["adds"] + rejected

        conversion_pct = round(cart["sold"] / cart["adds"] * 100.0, 2) if cart["adds"] > 0 else 0.0
        acceptance_pct = round(cart["adds"] / engaged * 100.0, 2) if engaged > 0 else 0.0
        rejection_pct = round(rejected / engaged * 100.0, 2) if engaged > 0 else 0.0

        rows.append({
            "signal": {
                "code": sig,
                "display_name": SIGNAL_DISPLAY.get(sig, sig.replace("_", " ").title()),
            },
            "cart_adds": cart["adds"],
            "sold": cart["sold"],
            "not_sold_cart": cart["not_sold"],
            "rejected": rejected,
            "engaged": engaged,
            "conversion_rate_pct": conversion_pct,
            "acceptance_rate_pct": acceptance_pct,
            "rejection_rate_pct": rejection_pct,
            "revenue": cart["revenue"],
        })
        totals["adds"] += cart["adds"]
        totals["sold"] += cart["sold"]
        totals["not_sold"] += cart["not_sold"]
        totals["rejected"] += rejected
        totals["revenue"] += cart["revenue"]

    total_engaged = totals["adds"] + totals["rejected"]
    return {
        "totals": {
            "cart_adds": totals["adds"],
            "sold": totals["sold"],
            "not_sold_cart": totals["not_sold"],
            "rejected": totals["rejected"],
            "engaged": total_engaged,
            "conversion_rate_pct": (
                round(totals["sold"] / totals["adds"] * 100.0, 2)
                if totals["adds"] > 0 else 0.0
            ),
            "acceptance_rate_pct": (
                round(totals["adds"] / total_engaged * 100.0, 2)
                if total_engaged > 0 else 0.0
            ),
            "rejection_rate_pct": (
                round(totals["rejected"] / total_engaged * 100.0, 2)
                if total_engaged > 0 else 0.0
            ),
            "revenue": totals["revenue"],
        },
        "by_signal": rows,
        "by_reason": reasons,
    }
