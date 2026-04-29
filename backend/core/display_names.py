from __future__ import annotations

from typing import Optional



# Market codes -> display names


MARKET_DISPLAY: dict[str, str] = {
    "PO":  "Physician Office",
    "LTC": "Long-Term Care",
    "SC":  "Surgery Center",
    "HC":  "Home Care",
    "LC":  "Lab / Diagnostics",
    "AC":  "Acute Care",
}


def market_display(code: Optional[str]) -> Optional[str]:
    """Return the display name for a market code, or the code itself if unknown."""
    if code is None:
        return None
    return MARKET_DISPLAY.get(code, code)



# Size codes -> display names


SIZE_DISPLAY: dict[str, str] = {
    "new":        "New Customer",
    "small":      "Small",
    "mid":        "Mid-Market",
    "large":      "Large",
    "enterprise": "Enterprise",
}


def size_display(code: Optional[str]) -> Optional[str]:
    """Return the display name for a size code, or the code itself if unknown."""
    if code is None:
        return None
    return SIZE_DISPLAY.get(code, code)



# Combined segment codes -> display names


def segment_display(segment_code: Optional[str]) -> Optional[str]:
    """Translate '<MARKET>_<SIZE>' into 'Display Market, Display Size'.

    Examples:
        'PO_large'     -> 'Physician Office, Large'
        'LTC_mid'      -> 'Long-Term Care, Mid-Market'
        'AC_enterprise' -> 'Acute Care, Enterprise'
    """
    if not segment_code:
        return None
    parts = segment_code.split("_", 1)
    if len(parts) != 2:
        return segment_code
    market, size = parts
    return f"{market_display(market)}, {size_display(size)}"



# Specialty codes -> display names


SPECIALTY_DISPLAY: dict[str, str] = {
    "FP":   "Family Practice",
    "IM":   "Internal Medicine",
    "PED":  "Pediatrics",
    "OBG":  "OB/GYN",
    "RL":   "Radiology",
    "HIA":  "Home Infusion",
    "OPH":  "Ophthalmology",
    "ORT":  "Orthopaedics",
    "DERM": "Dermatology",
    "DEN":  "Dentistry",
    "URO":  "Urology",
    "PSYCH": "Psychiatry",
    "CARD": "Cardiology",
    "ENDO": "Endocrinology",
    "GI":   "Gastroenterology",
    "NEU":  "Neurology",
    "ONC":  "Oncology",
    "VAS":  "Vascular",
    "PUL":  "Pulmonology",
    "GS":   "General Surgery",
}


def specialty_display(code: Optional[str]) -> Optional[str]:
    """Return the display name for a specialty code, or the code itself if unknown."""
    if code is None or code == "":
        return None
    return SPECIALTY_DISPLAY.get(code.upper(), code)



# Recommendation source codes -> display names


SOURCE_DISPLAY: dict[str, str] = {
    "manual":                              "Manual",
    "recommendation_peer_gap":             "Peer Gap",
    "recommendation_lapsed":               "Lapsed Buyer",
    "recommendation_replenishment":        "Replenishment",
    "recommendation_cart_complement":      "Cart Complement",
    "recommendation_pb_upgrade":           "Private Brand Upgrade",
    "recommendation_medline_conversion":   "Medline Conversion",
    "recommendation_item_similarity":      "Item Similarity",
    "recommendation_popularity":           "Popularity (Cold Start)",
}


def source_display(code: Optional[str]) -> Optional[str]:
    """Return the display name for a recommendation source code."""
    if code is None:
        return None
    return SOURCE_DISPLAY.get(code, code)


def is_recommendation_source(code: Optional[str]) -> bool:
    """True if the source code represents a recommendation-driven add."""
    if code is None or code == "manual":
        return False
    return code.startswith("recommendation_")
