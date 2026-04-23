# Product Recommendation Dashboard

A B2B product recommendation pipeline that turns raw transactional data into ranked, per-customer recommendations with supporting analytics.

The pipeline takes transaction records, customer metadata, and product metadata as input, and produces a set of Parquet files that a downstream API or dashboard can read directly. Recommendations are segment-aware, evidence-gated, and include seller-facing pitch messages.

---

## Repository layout

```
.
├── README.md
├── requirements.txt
├── .gitignore
└── scripts/
    ├── cleaning/
    │   ├── __init__.py
    │   ├── clean_data.py
    │   └── clean_data_sanity_check.py
    ├── analysis/
    │   ├── segment_customers.py
    │   ├── segment_patterns.py
    │   ├── recommendation_factors.py
    │   ├── sanity_check_recommendations.py
    │   ├── verify_scoring_formula.py
    │   ├── investigate_mid_tier_signal_mix.py
    │   ├── feature_importance.py
    │   ├── pitch_analysis.py
    │   └── inspect_schema.py
    └── profiling/
        └── build_data_summary.py
```

Data files, generated outputs, logs, and images are intentionally not tracked by git. See `.gitignore` for the full exclusion list.

---

## Setup

Requires Python 3.10 or newer.

```bash
# Create a virtual environment
python -m venv venv
source venv/bin/activate    # macOS/Linux
venv\Scripts\activate       # Windows

# Install dependencies
pip install -r requirements.txt
```

All scripts expect to be run from the repository root, for example:

```bash
python scripts/cleaning/clean_data.py
```

---

## Expected input data

The pipeline expects the following Parquet files to exist under `data_raw/` and `data_clean/` before any script runs. These are not committed to the repository; they must be placed locally by the user.

| Path | Contents |
|---|---|
| `data_raw/customers.parquet` | Customer dimension |
| `data_raw/products.parquet` | Product dimension |
| `data_raw/transactions_FY2425.parquet` | Transactions for fiscal year 2024–2025 |
| `data_raw/transactions_FY2526.parquet` | Transactions for fiscal year 2025–2026 |

Directory layout after `clean_data.py` runs:

```
data_clean/
├── customer/customers_clean.parquet
├── product/products_clean.parquet
├── features/customer_features.parquet
├── features/customer_rfm.parquet
├── serving/merged_dataset.parquet
└── serving/precomputed/
    ├── customer_segments.parquet
    ├── segment_patterns.parquet
    ├── segment_sequences.parquet
    ├── segment_category_profiles.parquet
    ├── product_adoption_rates.parquet
    ├── recommendation_factors.parquet
    ├── customer_recommendations.parquet
    └── customer_recommendations.schema.json
```

---

## Pipeline — run order

The main pipeline is four steps. Each step writes Parquet outputs that the next step reads.

### Step 1 — Cleaning and feature engineering

**Scripts:**
- `scripts/cleaning/clean_data.py`
- `scripts/cleaning/clean_data_sanity_check.py`

`clean_data.py` reads the raw customer, product, and transaction files, deduplicates, normalises, computes RFM scores and ~49 customer-level features, and writes the cleaned outputs to `data_clean/`.

`clean_data_sanity_check.py` runs after and validates the outputs against a defined set of quality checks (schema, ranges, null rates, distribution bounds). It writes an Excel audit file to `data_clean/audit/`.

**Usage:**
```bash
python scripts/cleaning/clean_data.py
python scripts/cleaning/clean_data_sanity_check.py
```

### Step 2 — Customer segmentation

**Script:** `scripts/analysis/segment_customers.py`

Assigns each customer to one of 19 segments based on market code and RFM tier (high / mid / low). Writes `customer_segments.parquet`.

**Usage:**
```bash
python scripts/analysis/segment_customers.py
```

### Step 3 — Segment pattern mining

**Script:** `scripts/analysis/segment_patterns.py`

Mines association rules and product-family transition sequences from transaction history. Produces three outputs that describe what each segment buys and in what order:

- `segment_patterns.parquet` — association rules
- `segment_sequences.parquet` — family-to-family transitions with probabilities
- `segment_category_profiles.parquet` — per-segment category adoption rates

**Usage:**
```bash
python scripts/analysis/segment_patterns.py
```

### Step 4 — Build recommendations

**Scripts:**
- `scripts/analysis/recommendation_factors.py`
- `scripts/analysis/sanity_check_recommendations.py`
- `scripts/analysis/verify_scoring_formula.py`

`recommendation_factors.py` is the core scoring engine. It computes peer-adoption rates per product within each peer group, identifies lapsed products per customer, applies a tier-aware scoring formula, and writes the top-10 recommendations per customer to `customer_recommendations.parquet`. It also trains a purchase propensity classifier and writes feature-importance outputs for analysis.

**Scoring formula:**

```
peer_gap_score  = 2.0 * adoption_rate * peer_weight   (+0.5 if private brand)
lapsed_score    = lapsed_weight                       (+0.5 if private brand)
```

Weights per tier:

| Tier | peer_weight | lapsed_weight |
|---|---|---|
| high | 3.5 | 1.0 |
| mid  | 2.5 | 2.5 |
| low  | 1.0 | 3.5 |

`sanity_check_recommendations.py` validates the generated recommendations against a layered contract: schema correctness, value ranges, tier signal distribution, and an evidence gate that proves peer-gap recommendations at rank 1 are always backed by sufficient peer adoption (or have no competing lapsed alternative).

`verify_scoring_formula.py` is an auditor: it reads the generated Parquet and independently verifies that every sampled score matches the published scoring formula arithmetically. Used to confirm the correct formula version produced the artifacts on disk.

**Usage:**
```bash
python scripts/analysis/recommendation_factors.py
python scripts/analysis/sanity_check_recommendations.py
python scripts/analysis/verify_scoring_formula.py
```

---

## Supporting scripts

### `scripts/profiling/build_data_summary.py`
Walks a directory of Parquet files and emits a multi-sheet Excel summary — column inventory, per-file schema, column profiles (null and empty counts), and schema-mismatch detection across files. Useful for a first-pass understanding of a new dataset.

### `scripts/analysis/inspect_schema.py`
Small debugging utility that prints the schema of each cleaned Parquet file in `data_clean/`. Handy when editing downstream code and wanting a quick look at what columns exist without opening an Excel file.

### `scripts/analysis/investigate_mid_tier_signal_mix.py`
Diagnostic script. For mid-tier segments, it answers the question: "when the system recommends a peer-gap product over a lapsed reorder, how strong is the peer evidence?" Computes the distribution of top peer-adoption per customer, which turns out to be the key insight for understanding mid-tier behavior.

### `scripts/analysis/feature_importance.py`
An earlier exploration of customer churn drivers using a gradient-boosted model with SHAP explanations. Not part of the main recommendation pipeline but informed the feature-engineering choices in `clean_data.py`.

### `scripts/analysis/pitch_analysis.py`
An earlier exploration of seller pitch opportunities by comparing each customer's category spend to their peer group's median. Produced the category-gap framing that later evolved into the peer-adoption scoring used by `recommendation_factors.py`.

---

## Outputs consumed by downstream systems

A downstream API or dashboard reads these four Parquet files directly:

| File | Purpose |
|---|---|
| `customer_recommendations.parquet` | Top-10 ranked recommendations per customer |
| `customer_recommendations.schema.json` | Schema sidecar describing each column |
| `customer_segments.parquet` | Customer → segment mapping |
| `product_adoption_rates.parquet` | Peer-adoption rates per (peer group, product) |

Everything else is internal to the analytics pipeline.

---

## Validation summary

The `sanity_check_recommendations.py` contract is the primary gate for shipping recommendations to production:

- **Schema checks** — 2 checks: every required column present, schema sidecar matches Parquet.
- **Value validity** — 8 checks: signals in valid set, supplier profile valid, binary flags binary, scores non-negative, adoption rate in [0, 1], pitches non-empty.
- **Exclusions** — 1 check: excluded product families do not appear.
- **Segment signal distribution** — 12 checks (6 high-tier, 6 low-tier): high tiers must be peer-gap dominant, low tiers must be lapsed dominant.
- **Evidence gate** — 18 checks (one per segment): when peer-gap wins at rank 1, adoption must exceed the tier threshold — unless no lapsed alternative existed for that customer.
- **Supplier profile coverage** — 3 checks: each supplier profile category present.
- **Row counts and coverage** — 3 checks: every customer has a rank-1 recommendation, customer IDs match customer features, max rank within top-N.
- **Pitch framing** — 1 check: substitution framing present for the relevant supplier profile.

Target: zero FAIL, zero WARN. INFO rows are informational statistics (rank-1 adoption medians, signal breakdowns) for review.

---

## Requirements

See `requirements.txt`. Core dependencies:

- `duckdb` — out-of-memory SQL over Parquet
- `pandas`, `numpy` — data manipulation
- `scikit-learn` — propensity model
- `matplotlib` — chart generation
- `openpyxl` — Excel audit outputs
- `shap` — feature attribution (feature_importance.py only)
- `joblib` — model persistence (feature_importance.py only)

---

## Design notes

**Segmentation over clustering.** Segments are deterministic (market code × RFM tier) rather than k-means clusters, so every customer's segment assignment is reproducible and explainable to a seller.

**Tier-aware scoring.** Weights are not universal — a high-tier customer's cross-sell pitch is weighted 3.5× peer-adoption while their lapsed reorder is weighted 1×, so active customers correctly get peer-driven pitches while at-risk customers correctly get reactivation pitches.

**Evidence-gated peer-gap.** A peer-gap recommendation wins at rank 1 only if either (a) its peer adoption exceeds a tier threshold, or (b) the customer has no lapsed alternative. This prevents the system from shipping weak cross-sell pitches when a reliable reorder pitch is available.

**Private-brand priority, supplier exclusion.** Private-brand products receive a scoring bonus. Excluded suppliers are filtered at the aggregation stage so they never enter the recommendation candidate pool.

**Memory-safe pipeline.** Aggregation over 110M transaction rows is done via DuckDB with a disk-backed spill directory and a 4 GB memory ceiling, so the pipeline runs on an 8–16 GB laptop without swapping.