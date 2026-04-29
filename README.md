# Product Recommendation Dashboard

A B2B product recommendation system for medical and surgical supply distribution. The project takes raw transaction history, customer metadata, and product metadata, runs an offline analytics pipeline to compute behavioral signals, and serves personalized recommendations through a REST API. Each recommendation comes with a plain-English pitch reason explaining why that product was chosen for that customer, so a sales rep using the dashboard can have a real conversation about it instead of just throwing items at a wall.

The recommendations are not generic "people who bought X also bought Y" suggestions. They are evidence-gated, segment-aware, and built on top of explicit behavioral signals (peer adoption gaps, replenishment cycles, lapsed recovery, brand alternatives, item co-occurrence, item similarity, popularity backfill). Brand new customers with no purchase history get a separate cold-start path that returns specialty-aligned popularity recommendations instead of failing.

## What the project does

At a high level, the system answers four questions for any given customer:

1. What products should this customer be buying that they currently are not? (peer gap)
2. What products are they running low on or overdue to reorder? (replenishment)
3. What products did they used to buy that they have stopped? (lapsed recovery)
4. What products would pair well with what they already buy? (cart complement and item similarity)

It also flags two brand-strategy opportunities:

5. Where could a private-brand alternative replace a national-brand item already in their cart? (private brand upgrade)
6. Where could a competitor's brand be converted to ours?

And for new customers with no transaction history, a fallback ensures they still get useful recommendations:

7. Cold-start popularity recommendations, biased toward the customer's specialty.

Each customer gets a top-10 list combining all signal types, ranked by confidence. A separate cart-helper endpoint runs in real time when a sales rep is actively building a cart, surfacing complementary items and brand swaps based on what is already in the cart that moment.

## Why segmentation matters

A single global popularity ranking does not work for B2B medical supply distribution because customer needs vary by an order of magnitude based on what kind of medical practice they are. A long-term care facility buys enteral feeding pumps and incontinence products. A family practice clinic buys vaccines, alcohol prep pads, and exam gloves. A surgical center buys wound closure adhesives and sterile gloves. Recommending alcohol prep pads to an LTC facility is not wrong, but it misses what they actually need.

So the project segments customers along multiple axes before running any recommendation logic.

### Market segmentation (what kind of practice)

Customers are bucketed into six market types based on their primary purchase patterns:

- **PO** (Physician Office): family practice, internal medicine, pediatrics, OB/GYN
- **LTC** (Long-Term Care): nursing homes, assisted living, hospice
- **SC** (Surgery Center): ambulatory surgery, outpatient procedures
- **HC** (Home Care): home health agencies, in-home patient services
- **LC** (Lab/Diagnostics): clinical labs, pathology
- **AC** (Acute Care): hospitals, hospital systems

Market is computed from the actual purchase mix, not from a customer-supplied "what kind of business are you" field, because customers misclassify themselves all the time. The classifier looks at which product families the customer buys and how that distribution compares to known archetypes.

### Size segmentation (how big they are)

Within each market, customers are bucketed into five size tiers based on annual purchase volume:

- **new**: less than 12 months of purchase history
- **small**: bottom 40 percent of established customers in their market
- **mid**: 40th to 80th percentile
- **large**: 80th to 95th percentile
- **enterprise**: top 5 percent

Combining the two gives 30 final segments (PO_small, LTC_mid, AC_enterprise, etc.). This is the unit of comparison the recommendation engine uses when computing peer adoption: when we say "92 percent of PO_large peers buy infection prevention products," we mean 92 percent of the customers in this exact market-and-size bucket.

### Lifecycle status (where the customer is in their journey)

Each customer is also tagged with a lifecycle status that captures their current engagement level:

- **stable_warm** (~52% of base): Active customers ordering regularly within their normal cadence
- **declining_warm** (~10%): Active but slowing down, candidates for win-back
- **churned_warm** (~11%): Were active recently but have stopped, top recovery priority
- **cold_start** (~27%): New customers with sparse or no transaction history

Status drives which signals dominate the recommendations. Churned customers get heavier lapsed-recovery weighting. Cold-start customers route to the popularity fallback path entirely. Stable customers get the full mix.

### Archetype (clinical type)

Each customer is also classified into one of ten clinical archetypes based on their product mix:

- specialty_clinic, primary_care, skilled_nursing, surgery_center, educational, multispecialty_group, hospital_acute, government, pharmacy, veterinary

This is exposed via the API and used for filtering and analytics.

### Specialty as a secondary signal

Beyond market, size, status, and archetype, each customer has a specialty code (FP for family practice, IM for internal medicine, OBG for OB/GYN, RL for radiology, HIA for home infusion, etc.) computed from their actual product mix. Specialty is used as a tiebreaker when ranking recommendations: if two products have similar peer-gap evidence, the one that aligns with the customer's specialty wins.

## How the recommendation engine works

The engine runs in two stages. The heavy lifting happens offline in a batch job that produces Parquet files. The API then reads those Parquet files at request time and stitches in live state from a Postgres database.

### Offline stage: signal computation

Multiple analyses run on the cleaned transaction data:

**Peer adoption matrix.** For every product in the catalog and every segment, compute what percentage of customers in that segment buy it. This is the foundation of the peer-gap signal. If 92 percent of PO_large peers buy a given product family and the target customer is in PO_large but does not buy any of that family, that is a peer gap worth surfacing.

**Replenishment cadence.** For every customer-product pair where the customer has bought the product before, compute the typical days-between-purchases. If the gap since their last purchase exceeds the typical cadence by a meaningful margin, the product becomes a replenishment candidate.

**Lapsed recovery.** Identifies products the customer used to buy regularly but has stopped ordering. Critical for declining and churned customers where re-engagement is the goal.

**Item co-occurrence.** Across the entire transaction history, compute lift, support, and confidence for every product pair that gets bought together more often than chance would predict. This produces a sparse matrix of about 42,000 product pairs out of the roughly 27,000-product catalog. These are the cart complements.

**Private brand equivalents.** For every national-brand item, find the closest private-brand alternative based on product family, category, pack size, and price band. Roughly 9,500 such pairs exist in the catalog. Anchor the swap with an estimated savings percentage so the API can show "save approximately 18 percent" in the pitch.

**Medline conversions.** Specifically maps Medline-brand products to McKesson-brand equivalents to support the brand-conversion play.

**Item similarity.** Use TF-IDF on item descriptions plus structured features (family, category, supplier, price band) to compute a similarity score between every pair of items. This drives the cross-sell signal: customers like you tend to also use this similar item.

**Popularity baseline.** Falls back to specialty-weighted popularity when no other signal applies (cold-start customers and edge cases).

### Combining signals into a top-10

Each candidate item for each customer gets a confidence score derived from how strong the underlying signal is (a 92 percent peer adoption rate is more confident than a 60 percent one), how relevant it is to the customer's history, and a brand strategy weight that nudges private-brand items higher when otherwise equivalent. A quota-based diversification system ensures the top 10 contains a mix of signal types rather than being dominated by one (median diversity: 4 distinct signals per customer, with all 8 signal types visible across the customer base).

The pitch reason is generated at this stage, not at API request time. It is a templated string that pulls in the actual peer percentage, segment name, and specialty so it reads as a real explanation rather than boilerplate. Two examples of what gets stored:

> "92 percent of PO_large peers buy Infection Prevention products. You don't currently. Aligns with your specialty (FP)."

> "You usually order this but haven't recently. 78 percent of peers in your segment still order it on a regular cadence. Likely due for reorder."

Each recommendation is also tagged with a **business purpose** (`new_product`, `win_back`, `cross_sell`, `mckesson_substitute`) and an `is_mckesson_brand` flag, so the dashboard can group recommendations by intent and the analytics layer can track McKesson Brand penetration.

### Online stage: API serves recommendations

When a request comes in for a customer's top 10 recommendations, the API looks up the precomputed rows in the Parquet file using DuckDB, fetches the live inventory level for each item from Postgres, and returns the combined response. The Parquet read takes a few milliseconds. The Postgres join adds maybe one or two more.

Two recommendation paths exist:

1. **Precomputed path**: customer has rows in `recommendations.parquet`. Return them.
2. **Cold-start path**: customer is brand new (created via the API after the analytics batch ran, so they have no precomputed recommendations). Query product popularity tables directly, sort by recent buyer count, apply a specialty boost, and return 10 popularity recommendations.

The customer never sees which path was used. The response shape is identical, with a `recommendation_source` flag (`precomputed` or `cold_start`) so the frontend can render slightly different UI hints if it wants.

### The cart helper is a separate flow

Top 10 recommendations are static between batch runs. The cart helper runs live every time a cart changes. The caller can either pass a list of cart item IDs in the request body, or omit it and let the endpoint read the customer's active cart from the cart_items table in Postgres. Either way, the endpoint runs three queries against the Parquet files:

- **Cart complements**: items frequently co-bought with the cart items, ranked by lift
- **Private brand upgrades**: private-brand alternatives for any national-brand items in the cart
- **Medline conversions**: McKesson-brand alternatives for any Medline products in the cart

Each list can have zero to a handful of items. Empty lists are valid responses. They mean no signal fired, not that the engine is broken.

## Validation results

The recommendation logic was validated against held-out transaction data before any of it was wired into an API.

| Metric                          | Result        | Baseline       |
|---------------------------------|---------------|----------------|
| Family hit rate (90-day window) | **91.9%**     | 70.2%          |
| Median cart complement lift     | **49.94x**    | 1.0x (random)  |
| McKesson Brand share            | **~57%**      | Target: 56.6%  |
| Customer coverage               | **97.6%**     | 379,729 / 389,224 |
| Strict SKU hit rate             | 1.8%          | (by design - engine excludes already-bought items) |
| Specialty filter breaches       | **0**         | -              |

Both metrics held when the engine was moved from the offline batch into the API.

## Architecture

The system is split into two stores, each doing what it is best at.

| Store    | What lives there                                                                                | Why                                          |
|----------|-------------------------------------------------------------------------------------------------|----------------------------------------------|
| Postgres | Users, customers, products, inventory, cart, purchase history, customer-seller assignments and history | Live transactional state, ACID writes        |
| Parquet  | Precomputed recommendations, segment metadata, co-occurrence pairs, private-brand equivalents, item similarity | Columnar reads via DuckDB, no joins needed   |

The API stitches both together at request time. DuckDB reads the Parquet for the recommendation list, then SQLAlchemy fetches the inventory level for each item from Postgres before returning the response. This keeps the analytics pipeline decoupled from the live API. The analytics batch can be rerun nightly without touching the running API process.

## Repository layout

```
.
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
│
├── backend/                      FastAPI application
│   ├── main.py                   App entry point, registers routers
│   ├── config.py                 Pydantic-settings configuration
│   ├── core/
│   │   ├── dependencies.py       Auth dependencies (require_admin, etc.)
│   │   └── security.py           JWT and bcrypt helpers
│   ├── db/
│   │   ├── database.py           Async SQLAlchemy engine and session
│   │   └── parquet_store.py      DuckDB query helpers for Parquet files
│   ├── models/                   SQLAlchemy ORM models
│   ├── schemas/                  Pydantic request and response models
│   ├── services/                 Business logic
│   └── routers/                  FastAPI route definitions
│
├── scripts/
│   ├── analysis/                 Offline analytics (segmentation, signal computation)
│   ├── backend/                  Database seeding and import scripts
│   ├── cleaning/                 Raw data cleaning
│   ├── profiling/                Data quality profiling
│   ├── setup/                    Customer enrichment, demo data loaders, password resets
│   ├── audit/                    Pipeline audits
│   └── qc/                       Comprehensive API test runner
│
├── sql/
│   └── init.sql                  One-shot database setup script
│
├── tests/
│   └── test_scripts/             Schema inspection and data sanity scripts
│
├── docs/                         Business and proposal documents
│
├── data_raw/                     Raw input data (gitignored)
├── data_clean/                   Cleaned and computed Parquet files (gitignored)
├── analysis_outputs/             Generated reports and Excel summaries (gitignored)
└── archive/                      Old test reports, debug scripts, legacy zips (gitignored)
```

Data files, generated outputs, logs, and images are intentionally not tracked by git. See `.gitignore` for the full exclusion list.

## What's in the API

The API has 42 endpoints across nine routers.

**Health checks** verify the app is alive, Postgres is reachable, and DuckDB can read the Parquet files.

**Authentication** uses JWT bearer tokens. Login returns a token that the client passes on every subsequent request. The standard OAuth2 password flow is used so the interactive Swagger UI can authenticate via the Authorize button.

**User management** lets admins create admins, sellers, and customers, list users, view individual users, deactivate or reactivate accounts, and lets users change their own password. When a seller is deactivated, all their assigned customers are auto-unassigned with a count returned in the response.

**Customer search and filtering** lets sellers and admins search by customer ID, market code, specialty, or segment, and lets admins filter customers by lifecycle status, archetype, or any combination.

**Recommendations** returns the top 10 for the logged-in customer, the top 10 for any specific customer (admin or assigned seller), or live cart suggestions.

**Customer-seller assignment** is the layer that controls who sees what. An admin can assign, reassign, or unassign customers individually or in bulk. A seller can self-claim any unassigned customer. Every change is recorded in an audit table with a reason code and a free-text note. An admin can pull the full assignment history for any customer at any time. The system rejects redundant no-op assignments to keep the audit log clean.

**Cart management** lets sellers, admins, and customers add items to a customer's cart, view the active cart with line totals and inventory, update quantities, mark items as sold or not_sold, remove items, and check out. Checkout is atomic: it flips the cart line to sold and writes a corresponding row to purchase history in a single transaction. Each cart add is tagged with which recommendation signal drove it (one of nine valid sources), so the business can later measure conversion rates per signal.

**Stats and dashboards** provide admin-scope KPIs (total revenue, conversion rate, customer counts, sales trends, segment distribution, top sellers, recent sales feed), seller-scope drill-downs (own customers, own conversion by signal), and per-customer drill-downs.

## Authorization model

Three roles with different scopes, enforced server-side on every endpoint.

- **Admin**: full access. Sees and manages all users, all customers, all assignments.
- **Seller**: scoped to assigned customers. Cannot view customers assigned to other sellers. Can claim unassigned customers.
- **Customer**: scoped to self. Can view own profile, own history, own recommendations, and run the cart helper for own cart only.

Tests verify that a seller calling another seller's customer list gets 403, that a customer calling cart-helper for another customer gets 403, and that an unauthenticated request to any non-public endpoint gets 401.

## Testing and QC

A comprehensive automated QC test runner lives at `scripts/qc/run_api_tests.py`. It auto-discovers users from the database (no hardcoded credentials), runs **128 distinct test cases** across 12 categories, and generates a multi-sheet Excel report with detailed expected vs actual JSON, pass/fail reasoning, and authorization matrix coverage.

Categories tested:
- Health (3 tests)
- Authentication (13 tests)
- User management lifecycle (15 tests)
- Customer endpoints including new filter (15 tests)
- Recommendations across all 4 lifecycle statuses (10 tests)
- Cart workflow with all 9 source types (16 tests)
- Purchase history (3 tests)
- Assignment lifecycle (12 tests)
- Stats - admin/seller/customer scopes (9 tests)
- Authorization matrix - 14 endpoints x 3 roles (32 tests)

**Latest run: 127/128 passed (99.2%).** The single non-pass is the system correctly rejecting a redundant assignment operation, which is the assignment-history integrity feature working as designed.

To run:

```bash
# From project root
python scripts/qc/run_api_tests.py
# Output: api_test_report_<timestamp>.xlsx
```

Schema inspection and data sanity checks live in `tests/test_scripts/` and `scripts/analysis/sanity_check_recommendations.py`.

## Setup

### Prerequisites

- Python 3.10 or newer (this project uses conda env `CTBA`)
- PostgreSQL 14 or newer
- DBeaver or another SQL client for running the setup script

### 1. Clone and set up environment

```bash
git clone <your-repo-url>
cd <project-folder>

# Using conda (recommended)
conda create -n CTBA python=3.10
conda activate CTBA

# Or using venv
python -m venv venv
source venv/bin/activate         # macOS/Linux
venv\Scripts\activate            # Windows PowerShell

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure environment

Copy the template and fill in your local Postgres connection details:

```bash
cp .env.example .env
```

Edit `.env`:

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=recommendation_dashboard
POSTGRES_USER=postgres
POSTGRES_PASSWORD=<your-password>
POSTGRES_SCHEMA=recdash

PARQUET_PRECOMPUTED_DIR=data_clean/serving/precomputed
PARQUET_MERGED_FILE=data_clean/serving/merged_dataset.parquet
```

`.env` is gitignored. Never commit it.

### 3. Create the Postgres database

```sql
CREATE DATABASE recommendation_dashboard;
```

### 4. Run the database setup script

Open `sql/init.sql` in DBeaver against the new database and execute it. This creates the `recdash` schema, all tables, indexes, and foreign keys.

### 5. Run the data pipeline

```bash
# Clean raw transactions
python scripts/cleaning/clean_data.py

# Segment customers and products
python scripts/analysis/segment_customers.py
python scripts/analysis/segment_products.py
python scripts/analysis/segment_patterns.py

# Compute the signal sources
python scripts/analysis/compute_product_specialty.py
python scripts/analysis/compute_product_cooccurrence.py
python scripts/analysis/compute_private_brand_equivalents.py
python scripts/analysis/compute_item_similarity.py

# Build the recommendation factor scores
python scripts/analysis/recommendation_factors.py
```

### 6. Import data into Postgres

```bash
python scripts/backend/import_customers.py
python scripts/backend/import_products.py
python scripts/backend/seed_inventory.py
python scripts/backend/copy_purchase_history.py
```

### 7. Enrich customers and seed demo users

```bash
# Add status and archetype columns to the customers table
python scripts/setup/enrich_customers_table.py

# Create demo customer users (one per status x segment combination)
python scripts/setup/load_demo_users_and_history.py

# Reset admin and seller passwords to a known value (Demo1234!)
python scripts/setup/reset_password.py
```

After this, all admin, seller, and customer accounts use the password `Demo1234!`. Change these before any non-development use.

### 8. Start the API

```bash
uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000/docs for the interactive Swagger UI.

### 9. Run the QC test suite (optional but recommended)

```bash
python scripts/qc/run_api_tests.py
```

This will produce `api_test_report_<timestamp>.xlsx` with full pass/fail evidence across 128 test cases.

## Tech stack

| Layer              | Technology                                  |
|--------------------|---------------------------------------------|
| Language           | Python 3.10+                                |
| Web framework      | FastAPI                                     |
| ASGI server        | Uvicorn                                     |
| ORM                | SQLAlchemy 2.0 (async, asyncpg)             |
| Validation         | Pydantic 2.5+                               |
| Database           | PostgreSQL 14+                              |
| Analytics engine   | DuckDB                                      |
| Data processing    | pandas, numpy, pyarrow                      |
| ML and similarity  | scikit-learn, scipy, implicit, lightgbm     |
| Auth               | python-jose (JWT), passlib (bcrypt)         |
| Output             | openpyxl (Excel reports), matplotlib        |

## License

This project is part of an academic capstone at William & Mary. Not licensed for commercial use without permission.