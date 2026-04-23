from __future__ import annotations

import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.chart import BarChart, Reference

warnings.filterwarnings("ignore")


# Paths

ROOT         = Path(__file__).resolve().parent.parent.parent
DATA_CLEAN   = ROOT / "data_clean"
FEATURE_FILE = DATA_CLEAN / "features"  / "customer_features.parquet"
PRECOMP_DIR  = DATA_CLEAN / "serving"   / "precomputed"
OUT_ANALYSIS = DATA_CLEAN / "analysis"


# Configuration

# Market type display labels
MKT_CD_LABELS = {
    "PO":  "Physician Office",
    "LTC": "Long Term Care",
    "SC":  "Surgery Center / Specialty",
    "LC":  "Lab / Clinical",
    "HC":  "Home Care",
    "AC":  "Acute Care",
}
MKT_CD_OTHER = "OTHER"

# RFM tier thresholds
R_HIGH = 4
F_HIGH = 4
R_LOW  = 2
F_LOW  = 2

# Product family preferences per market type.
# Used for the segmentation_report.xlsx strategy sheet only — informational,
# not for scoring. Scoring logic lives exclusively in recommendation_factors.py.
PRIMARY_FAMILIES = {
    "PO":  ["Nursing and Surgical Supplies", "Infection Prevention",
            "Wound Care & Skin Care", "Rx", "Lab-Waived Lab",
            "Office and Facility Supplies"],
    "LTC": ["Incontinence", "Nursing and Surgical Supplies",
            "Nutrition and Feeding Supplies", "Wound Care & Skin Care",
            "Patient Therapy/Personal Care", "Infection Prevention"],
    "SC":  ["Wound Care & Skin Care", "Infection Prevention",
            "Nursing and Surgical Supplies", "Equipment & Equip Disposables",
            "Office and Facility Supplies", "Rx"],
    "LC":  ["Lab-Waived Lab", "Lab-Non-Waived Lab",
            "Lab-Ancillary Lab Products", "Nursing and Surgical Supplies",
            "Infection Prevention"],
    "HC":  ["Patient Therapy/Personal Care", "Nursing and Surgical Supplies",
            "Incontinence", "Wound Care & Skin Care",
            "Nutrition and Feeding Supplies"],
    "AC":  ["Nursing and Surgical Supplies", "Infection Prevention",
            "Wound Care & Skin Care", "Rx", "Equipment & Equip Disposables",
            "Lab-Waived Lab", "Respiratory Products"],
}

# Scoring weights reference table for the segmentation_report.xlsx strategy sheet.
# These MATCH the values in recommendation_factors.py (SEGMENT_WEIGHTS dict).
# If you change the values in recommendation_factors.py, update this table too.
# Healthcare distribution scoring: high-tier = cross-sell, low-tier = reactivate.
SCORING_REFERENCE = {
    "_high": {"peer_gap": 3.5, "lapsed": 1.0,
              "rationale": "Active customers — cross-sell new categories, minimize reorder nag"},
    "_mid":  {"peer_gap": 2.5, "lapsed": 2.5,
              "rationale": "Balanced — both signals carry equal weight"},
    "_low":  {"peer_gap": 1.0, "lapsed": 3.5,
              "rationale": "At-risk customers — reactivation via lapsed reorders is priority"},
}
PRIVATE_BRAND_BOOST = 0.5


# Logging

def _s(title):
    print(f"\n{'-'*64}\n  {title}\n{'-'*64}", flush=True)

def _log(msg):
    print(f"  {msg}", flush=True)


# Excel helpers

def _style(ws, df, hc="1F4E79"):
    thin = Side(style="thin", color="CCCCCC")
    bdr  = Border(left=thin, right=thin, top=thin, bottom=thin)
    for ci, col in enumerate(df.columns, 1):
        c = ws.cell(1, ci, str(col))
        c.font = Font(name="Arial", bold=True, size=10, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor=hc)
        c.alignment = Alignment(horizontal="center", vertical="center")
        c.border = bdr
    for ri, row in enumerate(df.itertuples(index=False), 2):
        bg = "F2F7FF" if ri % 2 == 0 else "FFFFFF"
        for ci, val in enumerate(row, 1):
            c = ws.cell(ri, ci, val if pd.notna(val) else "")
            c.font = Font(name="Arial", size=9)
            c.fill = PatternFill("solid", fgColor=bg)
            c.alignment = Alignment(horizontal="left", vertical="center")
            c.border = bdr
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    for col_cells in ws.columns:
        w = max((len(str(c.value)) if c.value else 0) for c in col_cells)
        ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(max(w+2,12),55)


def _chart(ws, n, lc, vc, anchor, title, xtitle, color="1F4E79"):
    ch = BarChart()
    ch.type = "bar"; ch.grouping = "clustered"; ch.title = title
    ch.x_axis.title = xtitle; ch.legend = None
    ch.width = 26; ch.height = 16; ch.style = 2
    ch.add_data(Reference(ws, min_col=vc, min_row=1, max_row=n+1), titles_from_data=True)
    ch.set_categories(Reference(ws, min_col=lc, min_row=2, max_row=n+1))
    ch.series[0].graphicalProperties.solidFill = color
    ch.series[0].graphicalProperties.line.solidFill = color
    ws.add_chart(ch, anchor)


# Step 1: Load features

def load_features():
    _s("Step 1: Loading customer features")
    if not FEATURE_FILE.exists():
        print(f"\nFATAL: customer_features.parquet not found", file=sys.stderr)
        sys.exit(1)
    df = pd.read_parquet(FEATURE_FILE)
    _log(f"Loaded : {len(df):,} customers  |  {df.shape[1]} columns")
    required = ["DIM_CUST_CURR_ID", "MKT_CD", "R_score", "F_score",
                "monetary", "recency_days", "frequency",
                "avg_revenue_per_order", "n_categories_bought",
                "category_hhi", "cycle_regularity"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"\nFATAL: Missing columns: {missing}", file=sys.stderr)
        sys.exit(1)
    return df


# Step 2: Assign segments

def assign_segments(df):
    # Build segment label as MKT_CD_tier (e.g. PO_high, LTC_low).
    # MKT_CD values not in the known list fall back to OTHER.
    # R_score/F_score nulls default to 3 (middle), placing that customer in 'mid'.

    _s("Step 2: Assigning segments — MKT_CD x RFM tier")
    df = df.copy()
    df["mkt_cd_clean"] = df["MKT_CD"].fillna(MKT_CD_OTHER).str.strip().str.upper()
    df["mkt_cd_clean"] = df["mkt_cd_clean"].apply(
        lambda x: x if x in MKT_CD_LABELS else MKT_CD_OTHER
    )

    r = df["R_score"].fillna(3).astype(float)
    f = df["F_score"].fillna(3).astype(float)
    conditions = [(r >= R_HIGH) & (f >= F_HIGH), (r <= R_LOW) | (f <= F_LOW)]
    choices    = ["high", "low"]
    df["rfm_tier"] = np.select(conditions, choices, default="mid")
    df["segment"]  = df["mkt_cd_clean"] + "_" + df["rfm_tier"]

    desc_map = {
        "high": "Active frequent buyer — cross-sell opportunity",
        "mid":  "Moderate engagement — balanced cross-sell and reactivation",
        "low":  "Low engagement — reactivate with lapsed products first",
    }
    df["segment_description"] = df["rfm_tier"].map(desc_map)

    seg_counts = df.groupby(["mkt_cd_clean", "rfm_tier"]).size().reset_index(name="n")
    seg_counts["segment"] = seg_counts["mkt_cd_clean"] + "_" + seg_counts["rfm_tier"]
    seg_counts = seg_counts.sort_values("n", ascending=False)

    _log(f"\n  {'Segment':<30} {'Customers':>10} {'% Portfolio':>12}")
    _log(f"  {'-'*30} {'-'*10} {'-'*12}")
    for _, row in seg_counts.iterrows():
        pct = row["n"] / len(df) * 100
        _log(f"  {row['segment']:<30} {row['n']:>10,} {pct:>11.1f}%")

    if "churn_label" in df.columns:
        churn_by_seg = (
            df[df["churn_label"].isin([0,1])]
            .groupby("segment")["churn_label"]
            .mean().mul(100).round(1)
        )
        _log(f"\n  Churn rate by segment (top 10 highest risk):")
        for seg, rate in churn_by_seg.sort_values(ascending=False).head(10).items():
            _log(f"    {seg:<30} {rate:.1f}%")
    return df


# Step 3: Build segment profiles

def build_segment_profiles(df):
    # Aggregate descriptive statistics per segment for the team report.
    # This is read-only analysis — no scoring happens here.

    _s("Step 3: Building segment profiles")
    agg = (
        df.groupby("segment")
        .agg(
            mkt_cd           = ("mkt_cd_clean",         "first"),
            rfm_tier         = ("rfm_tier",              "first"),
            n_customers      = ("DIM_CUST_CURR_ID",      "count"),
            median_monetary  = ("monetary",              "median"),
            median_recency   = ("recency_days",          "median"),
            median_frequency = ("frequency",             "median"),
            median_avg_order = ("avg_revenue_per_order", "median"),
            median_n_cats    = ("n_categories_bought",   "median"),
            median_hhi       = ("category_hhi",          "median"),
            median_cycle     = ("cycle_regularity",      "median"),
        )
        .reset_index()
    )
    if "churn_label" in df.columns:
        churn = (
            df[df["churn_label"].isin([0,1])]
            .groupby("segment")["churn_label"]
            .mean().mul(100).round(1).reset_index()
            .rename(columns={"churn_label": "churn_rate_pct"})
        )
        agg = agg.merge(churn, on="segment", how="left")

    # Attach the scoring weights from the reference table (tier-based)
    def _tier_weights(tier, side):
        key = "_" + tier
        return SCORING_REFERENCE.get(key, SCORING_REFERENCE["_mid"])[side]

    agg["peer_gap_weight"] = agg["rfm_tier"].apply(lambda t: _tier_weights(t, "peer_gap"))
    agg["lapsed_weight"]   = agg["rfm_tier"].apply(lambda t: _tier_weights(t, "lapsed"))
    agg["primary_signal"]  = agg.apply(
        lambda r: "peer_gap" if r["peer_gap_weight"] >= r["lapsed_weight"] else "lapsed",
        axis=1
    )
    agg["mkt_label"] = agg["mkt_cd"].map(lambda x: MKT_CD_LABELS.get(x, "Other"))
    agg = agg.sort_values(["mkt_cd", "rfm_tier"]).reset_index(drop=True)

    _log(f"\n  {'Segment':<30} {'N':>8} {'Churn%':>7} {'Median$':>10} {'$/order':>9} {'Freq':>6}")
    _log(f"  {'-'*30} {'-'*8} {'-'*7} {'-'*10} {'-'*9} {'-'*6}")
    for _, r in agg.iterrows():
        churn = f"{r.get('churn_rate_pct', 0):.1f}%" if pd.notna(r.get("churn_rate_pct")) else "n/a"
        _log(f"  {r['segment']:<30} {r['n_customers']:>8,} {churn:>7} "
             f"${r['median_monetary']:>9,.0f} ${r['median_avg_order']:>8,.0f} "
             f"{r['median_frequency']:>6.0f}")
    return agg


# Step 4: Save outputs

def save_outputs(df, profiles):
    # Write the segment parquet (consumed by recommendation_factors.py) and
    # the team-facing Excel report. No scoring happens here.

    _s("Step 4: Saving outputs")
    PRECOMP_DIR.mkdir(parents=True, exist_ok=True)
    OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

    # customer_segments.parquet — one row per customer
    seg_out = df[[
        "DIM_CUST_CURR_ID", "segment", "mkt_cd_clean", "rfm_tier",
        "segment_description"
    ]].copy()

    # Type safety for downstream joins
    seg_out["DIM_CUST_CURR_ID"] = seg_out["DIM_CUST_CURR_ID"].astype("int64")
    for c in ["segment", "mkt_cd_clean", "rfm_tier", "segment_description"]:
        seg_out[c] = seg_out[c].fillna("").astype(str)

    seg_path = PRECOMP_DIR / "customer_segments.parquet"
    seg_out.to_parquet(seg_path, index=False)
    _log(f"Saved: {seg_path.relative_to(ROOT)}")

    # Strategy-by-segment reference table (tier x market, informational)
    strat_rows = []
    for mkt_cd, label in MKT_CD_LABELS.items():
        fams = PRIMARY_FAMILIES.get(mkt_cd, [])
        for tier in ["high", "mid", "low"]:
            key = "_" + tier
            w = SCORING_REFERENCE[key]
            strat_rows.append({
                "segment":           f"{mkt_cd}_{tier}",
                "customer_type":     label,
                "rfm_tier":          tier,
                "peer_gap_weight":   w["peer_gap"],
                "lapsed_weight":     w["lapsed"],
                "primary_signal":    "peer_gap" if w["peer_gap"] >= w["lapsed"] else "lapsed",
                "scoring_rationale": w["rationale"],
                "primary_families":  ", ".join(fams[:4]),
                "medline_rule":      "Never recommend Medline — substitution framing for medline_only customers",
            })
    strat_df = pd.DataFrame(strat_rows)

    xlsx_path = OUT_ANALYSIS / "segmentation_report.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        profiles.to_excel(writer, sheet_name="01_segment_profiles", index=False)
        strat_df.to_excel(writer, sheet_name="02_strategy_by_segment", index=False)
        wb = writer.book
        for name, color, df_out in [
            ("01_segment_profiles",   "1F4E79", profiles),
            ("02_strategy_by_segment", "833C00", strat_df),
        ]:
            if name in wb.sheetnames:
                wb[name].sheet_properties.tabColor = color
                _style(writer.sheets[name], df_out, hc=color)
    _log(f"Saved: {xlsx_path.relative_to(ROOT)}")


def save_svg(profiles):
    # Legacy SVG dashboard. Kept as-is for now.
    # TODO: replace with PNG charts (segment_sizes, churn_by_segment, spend_distribution)
    # when the frontend doesn't need it anymore.

    _s("Step 5: Saving SVG dashboard")
    SEG_COLORS = {
        "PO":("#1F4E79","#D6E4F0"), "LTC":("#375623","#D4EDE9"),
        "SC":("#7030A0","#F0E6F8"), "LC": ("#833C00","#FEF3DC"),
        "HC":("#C00000","#FDEAEA"), "AC": ("#185FA5","#E6F1FB"),
        MKT_CD_OTHER:("#666666","#F5F5F5"),
    }
    mkt_order = ["PO","LTC","SC","LC","HC","AC",MKT_CD_OTHER]
    n_mkts = sum(1 for m in mkt_order if m in profiles["mkt_cd"].values)
    W=680; PAD=32; TITLE_H=50; CARD_H=96; GAP=8
    TOTAL_H = TITLE_H + PAD + n_mkts*(CARD_H+GAP) + PAD
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 {W} {TOTAL_H}" role="img">',
        f'<rect width="{W}" height="{TOTAL_H}" fill="#F8F9FA"/>',
        f'<rect x="0" y="0" width="{W}" height="{TITLE_H}" fill="#1F4E79"/>',
        f'<text x="{W//2}" y="{TITLE_H//2+6}" text-anchor="middle" fill="white" ',
        f'font-family="Arial" font-size="15" font-weight="bold">',
        f'Customer Segments — Market Type x RFM Tier</text>',
    ]
    y0 = TITLE_H + PAD
    for mkt in mkt_order:
        grp = profiles[profiles["mkt_cd"] == mkt]
        if len(grp) == 0:
            continue
        stroke, fill = SEG_COLORS.get(mkt, SEG_COLORS[MKT_CD_OTHER])
        label   = MKT_CD_LABELS.get(mkt, "Other")
        n_tot   = int(grp["n_customers"].sum())
        med_sp  = grp["median_monetary"].median()
        med_or  = grp["median_avg_order"].median()

        tier_txt = "  ".join([
            f"{r['rfm_tier'].upper()}: {r['n_customers']:,}"
            for _, r in grp.sort_values("rfm_tier").iterrows()
        ])

        lines += [
            f'<rect x="{PAD}" y="{y0}" width="{W-PAD*2}" height="{CARD_H}" rx="8" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>',
            f'<text x="{PAD+14}" y="{y0+22}" font-family="Arial" font-size="13" '
            f'font-weight="bold" fill="{stroke}">{mkt} — {label}</text>',
            f'<text x="{W-PAD-14}" y="{y0+22}" text-anchor="end" font-family="Arial" '
            f'font-size="12" fill="{stroke}">{n_tot:,} customers</text>',
            f'<line x1="{PAD+8}" y1="{y0+32}" x2="{W-PAD-8}" y2="{y0+32}" '
            f'stroke="{stroke}" stroke-width="0.5" opacity="0.4"/>',
            f'<text x="{PAD+14}" y="{y0+50}" font-family="Arial" font-size="10" '
            f'fill="{stroke}" opacity="0.85">Median annual spend: ${med_sp:,.0f}  '
            f'-  Avg order: ${med_or:,.0f}</text>',
            f'<text x="{PAD+14}" y="{y0+68}" font-family="Arial" font-size="10" '
            f'font-weight="bold" fill="{stroke}">RFM tiers: {tier_txt}</text>',
            f'<text x="{PAD+14}" y="{y0+86}" font-family="Arial" font-size="9" '
            f'fill="{stroke}" opacity="0.75">Scoring varies by tier: _high = cross-sell '
            f'(3.5x/1.0x), _mid = balanced (2.5x/2.5x), _low = reactivate (1.0x/3.5x)</text>',
        ]
        y0 += CARD_H + GAP
    lines.append("</svg>")
    svg_path = OUT_ANALYSIS / "segmentation_dashboard.svg"
    svg_path.write_text("\n".join(lines), encoding="utf-8")
    _log(f"Saved: {svg_path.relative_to(ROOT)}")


# Main

def main():
    print(); print("="*64)
    print("  CUSTOMER SEGMENTATION — MKT_CD x RFM TIER")
    print("="*64)
    start = time.time()
    PRECOMP_DIR.mkdir(parents=True, exist_ok=True)
    OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)

    features = load_features()
    df       = assign_segments(features)
    profiles = build_segment_profiles(df)
    save_outputs(df, profiles)
    save_svg(profiles)

    _s("Complete")
    _log(f"Total time: {round(time.time()-start,1)}s")
    _log("")
    _log("Segment logic: MKT_CD (PO/LTC/SC/LC/HC/AC) x RFM tier (high/mid/low)")
    _log("Scoring weights: defined in recommendation_factors.py SEGMENT_WEIGHTS")
    _log("")
    _log("Key outputs:")
    _log("  customer_segments.parquet   — segment per customer (read by recommendation_factors.py)")
    _log("  segmentation_report.xlsx    — profiles and strategy per segment")
    _log("  segmentation_dashboard.svg  — visual summary of market + RFM distribution")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr); sys.exit(1)
    except Exception as exc:
        print(f"\nFATAL ERROR: {exc}", file=sys.stderr); raise