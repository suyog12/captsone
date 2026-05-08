# McKesson Medical-Surgical Recommendation Engine

A B2B product recommendation system for medical and surgical supply distribution. The project takes raw transaction history, customer metadata, and product metadata, runs an offline analytics pipeline to compute behavioral signals, and serves personalized recommendations through a REST API and an interactive React dashboard. Each recommendation comes with a plain-English pitch reason explaining why that product was chosen for that customer, so a sales rep using the dashboard can have a real conversation about it instead of throwing items at a wall.

The recommendations are not generic "people who bought X also bought Y" suggestions. They are evidence-gated, segment-aware, and built on top of explicit behavioral signals (peer adoption gaps, replenishment cycles, lapsed recovery, brand alternatives, item co-occurrence, item similarity, popularity backfill). Brand new customers with no purchase history get a separate cold-start path that returns specialty-aligned popularity recommendations instead of failing.

## Vision

The engine exists to solve one specific business problem: McKesson sales reps process the orders customers ask for, but customers do not ask for products they are not aware of. They do not ask for the McKesson Brand alternative to a competitor's product. They do not ask for the gauze they used to buy regularly and stopped reordering. They do not ask for the rest of the chemistry reagent panel when calling about a single item.

Across 389,000 customer locations and $8.23 billion in annual revenue, even a small lift in cross-sell, replenishment recovery, or brand conversion translates into meaningful business impact. The engine surfaces these missed opportunities with the supporting evidence attached, so the rep can confidently raise the recommendation in the next conversation with the customer.

The long-term vision is for the engine output to become a data product that integrates with whatever workflow McKesson chooses, not just the reference dashboard built here. The dashboard demonstrates capability. The engine output is the long-term asset.

## What the project does

At a high level, the system answers four questions for any customer:

1. What products should this customer be buying that they currently are not? (peer gap)
2. What products are they overdue to reorder? (replenishment)
3. What products did they used to buy that they have stopped? (lapsed recovery)
4. What products would pair well with what they already buy? (cart complement and item similarity)

It also flags two brand-strategy opportunities:

5. Where could a McKesson Brand alternative replace a national-brand item already in the cart? (private brand upgrade)
6. Where could a competitor's brand be converted to ours? (competitor conversion)

For new customers with no transaction history, a fallback ensures they still get useful recommendations:

7. Cold-start popularity recommendations, biased toward the customer's specialty.

Each customer gets a top-10 list combining all signal types, ranked by confidence. A separate cart-helper endpoint runs in real time when a sales rep is actively building a cart, surfacing complementary items and brand swaps based on what is already in the cart that moment.

## Why segmentation matters

A single global popularity ranking does not work for B2B medical supply because customer needs vary by an order of magnitude based on what kind of medical practice they are. A long-term care facility buys enteral feeding pumps and incontinence products. A family practice clinic buys vaccines, alcohol prep pads, and exam gloves. A surgical center buys wound closure adhesives and sterile gloves.

So the project segments customers along multiple axes before running any recommendation logic.

### Market segmentation

Customers are bucketed into seven market types based on their primary purchase patterns: PO (Physician Office), LTC (Long-Term Care), SC (Surgery Center), HC (Home Care), LC (Lab/Diagnostics), AC (Acute Care), and OTHER. Market is computed from the actual purchase mix, not from a customer-supplied "what kind of business are you" field, because customers misclassify themselves all the time.

### Size segmentation

Within each market, customers are bucketed into five size tiers: new (fewer than 2 active months), small (under $500/mo spend), mid ($500-$2,499/mo), large ($2,500-$14,999/mo), enterprise ($15,000+/mo). Each customer also has a volume tier (median monthly units). The final size tier is the lower of spend and volume, but never more than one tier below the spend tier (the `max_one` rule). This protects high-cost low-volume specialty Rx buyers from being placed in the wrong peer group.

Market crossed with size gives 32 final segments. This is the unit of comparison the recommendation engine uses when computing peer adoption.

### Lifecycle status

Each customer is also tagged with a lifecycle status. These percentages are verified against the cleaned customer-patterns Parquet:

- **stable_warm** (52.0%): Active customers ordering regularly
- **cold_start** (26.8%): New customers with sparse or no transaction history
- **churned_warm** (11.0%): Were active in FY24-25 but have stopped in FY25-26
- **declining_warm** (10.2%): Active but slowing down

The combined declining and churned population is approximately 81,400 customers (21.2%), the at-risk segment where sales reps should focus engagement effort.

### Archetype and specialty

Each customer is also classified into one of ten clinical archetypes (specialty_clinic, primary_care, skilled_nursing, surgery_center, educational, multispecialty_group, hospital_acute, government, pharmacy, veterinary). Beyond that, each customer has a specialty code (FP, IM, OBG, RL, HIA, etc.) used as a tiebreaker when ranking recommendations.

## How the recommendation engine works

The engine runs in two stages. The heavy lifting happens offline in a batch job that produces Parquet files. The API then reads those Parquet files at request time and stitches in live state from a Postgres database.

### Offline stage: signal computation

**Peer adoption matrix.** For every product and every segment, compute what percentage of customers in that segment buy it. This is the foundation of the peer-gap signal.

**Replenishment cadence.** For every customer-product pair, compute the typical days-between-purchases. If the gap exceeds the typical cadence by a meaningful margin, the product becomes a replenishment candidate.

**Lapsed recovery.** Identifies products the customer used to buy regularly but has stopped ordering.

**Item co-occurrence.** Across the entire transaction history, compute lift, support, and confidence for every product pair. Produces about 42,800 high-lift pairs out of the roughly 27,773-product catalog.

**Private brand equivalents.** For every national-brand item, find the closest McKesson Brand alternative based on family, category, pack size, and price band. Roughly 9,500 such pairs.

**Competitor conversions.** Maps Medline and other competitor-brand products to in-house equivalents.

**Item similarity.** Sarwar 2001 adjusted cosine on the customer-product matrix, sparse-matrix arithmetic over 389,000 customers and 27,000 products, producing 895,257 scored product pairs.

**Popularity baseline.** Falls back to specialty-weighted popularity when no other signal applies.

### Combining signals into a top-10

Each candidate gets a confidence score derived from the strength of the underlying signal. Signals are normalized to percentile ranks so peer_gap (raw scores 5 to 15) and item_similarity (raw scores 0.8 to 2) compete on equal footing.

A quota-based diversification system ensures the top 10 contains a mix of signal types. Quotas: peer_gap=4, cart_complement=3, replenishment=3, item_similarity=2, lapsed_recovery=2, private_brand_upgrade=2, medline_conversion=2, popularity=10 (fallback). Without quotas, median signal diversity per customer was 1. With quotas, it jumped to 4.

The pitch reason is a templated string that pulls in actual peer percentage, segment name, and specialty:

> "92 percent of PO_large peers buy Infection Prevention products. You don't currently. Aligns with your specialty (FP)."

Each recommendation is also tagged with a business purpose (`new_product`, `win_back`, `cross_sell`, `mckesson_substitute`) and a McKesson Brand flag.

### Online stage: API serves recommendations

When a request comes in for a customer's top 10 recommendations, the API looks up the precomputed rows in the Parquet file using DuckDB, fetches the live inventory level for each item from Postgres, and returns the combined response.

Two recommendation paths exist: a precomputed path (customer has rows in `recommendations.parquet`) and a cold-start path (customer is brand new, query product popularity tables directly). The response shape is identical, with a `recommendation_source` flag.

### The cart helper is a separate flow

The cart helper runs live every time a cart changes. It runs three queries against the Parquet files: cart complements (items frequently co-bought with cart items), private brand upgrades (McKesson Brand alternatives for national-brand items in the cart), and competitor conversions (McKesson alternatives for competitor-brand products in the cart).

### Closing the loop: rejection feedback

When a sales rep sees a recommendation that is not useful, they can reject it directly from the recommendation card with one of nine reason codes plus an optional free-text note. The rejection writes to `recommendation_events` with `outcome='rejected'` and the item disappears from the seller's view for the rest of their session.

These signals roll up into the admin dashboard's Engine Effectiveness panel showing per-signal funnel metrics: cart adds, sold conversions, rejections, conversion rate, acceptance rate, rejection rate, and revenue.

## Validation results

The recommendation logic was validated using three independent methods:

| Metric                          | Result        | Baseline / Notes |
|---------------------------------|---------------|------------------|
| Family hit rate (60-day holdout)| **91.9%**     | 70.2% popularity baseline (+21.8 pp lift) |
| Median cart complement lift     | **49.94x**    | 1.0x random (across 100 stratified carts) |
| Cart helper trigger rate        | **59%**       | At least one suggestion on 59 of 100 real carts |
| McKesson Brand share overall    | **56.7%**     | Per-signal: 26% (popularity) to 100% (PB upgrade) |
| Customer coverage               | **97.6%**     | 380,813 / 389,224 customers |
| Specialty filter breaches       | **0**         | Across 60 customers in 30 segments |
| Strict SKU hit rate             | 1.8%          | By design - 89% of holdout activity was replenishment |

The strict SKU number is intentionally low. The engine focuses on the 11% of customer activity that is genuinely new product purchases, not the 89% that is replenishment.

## Architecture

| Layer    | What lives there                                                                                | Why                                          |
|----------|-------------------------------------------------------------------------------------------------|----------------------------------------------|
| Postgres | Users, customers (with status and archetype), products, inventory, cart, purchase history, customer-seller assignments and history, recommendation events with rejection tracking | Live transactional state, ACID writes  |
| Parquet  | Precomputed recommendations, segment metadata, co-occurrence pairs, private-brand equivalents, item similarity | Columnar reads via DuckDB, no joins needed   |
| FastAPI  | All HTTP endpoints, JWT auth, role-based authorization, Pydantic validation                     | Bridges the two stores, runs the cold-start fallback |
| React    | Three role-gated workspaces (admin, seller, customer) with TanStack Query, Tailwind, Vite       | Real dashboard for selling, browsing, and operating the system |

The API stitches both stores together at request time. DuckDB reads the Parquet for the recommendation list, then SQLAlchemy fetches the inventory level for each item from Postgres before returning the response. The analytics batch can be rerun nightly without touching the running API process.

## Repository layout

```
.
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
│
├── backend/                      FastAPI application
│   ├── main.py
│   ├── config.py
│   ├── core/                     Auth, security, display name helpers
│   ├── db/                       SQLAlchemy engine and DuckDB Parquet helpers
│   ├── models/                   SQLAlchemy ORM models
│   ├── schemas/                  Pydantic request and response models
│   ├── services/                 Business logic
│   └── routers/                  FastAPI route definitions
│
├── frontend/                     React + Vite + Tailwind + TanStack Query
│   ├── package.json
│   ├── vite.config.js
│   ├── tailwind.config.js
│   └── src/
│       ├── main.jsx, App.jsx, router.jsx, auth.jsx, api.js
│       ├── lib/                  Display helpers
│       ├── components/
│       │   ├── shell/            AppShell, TopBar, Sidebar (collapsible)
│       │   ├── ui/               Card, Badge, StatCard, Spinner
│       │   ├── charts/           HorizontalBarChart, LineChart
│       │   ├── recs/             Recommendation cards, reject modal, pitch reason
│       │   ├── cart/             CartView, CartHelperPanel, CartItemRow
│       │   ├── catalog/          Catalog browse, variant picker
│       │   ├── customer/         Heatmap, donut, carousel, gauge
│       │   ├── admin/            Engine effectiveness panel, top customers panel
│       │   └── perf/             Performance panel
│       └── pages/
│           ├── LoginPage, ChangePasswordPage
│           ├── admin/            Overview, Users, Customers, Sellers, Products
│           ├── seller/           CustomerList, CustomerProfile, Catalog, Performance
│           └── customer/         Overview, Recs, Cart, Catalog, Orders
│
├── scripts/
│   ├── analysis/                 Offline analytics
│   ├── backend/                  DB seeding and import
│   ├── cleaning/                 Raw data cleaning
│   ├── profiling/                Data quality profiling
│   ├── setup/                    Customer enrichment, demo loaders, password resets
│   ├── audit/                    Pipeline audits
│   └── qc/                       API test runner
│
├── sql/
│   ├── init.sql                  One-shot database setup
│   └── migrations/               Schema migrations
│
├── tests/                        Schema and data sanity scripts
├── docs/                         Business documents
│
├── data_raw/                     Raw input (gitignored)
├── data_clean/                   Cleaned and computed Parquet (gitignored)
├── analysis_outputs/             Generated reports (gitignored)
└── archive/                      Legacy files (gitignored)
```

Data files, generated outputs, logs, and images are intentionally not tracked by git.

## What's in the API

The API has 44 endpoints across ten routers covering health checks, JWT authentication, user management, customer search and creation, recommendations (top-10 and cart helper), customer-seller assignment with full audit trail, cart management, purchase history, and admin/seller/customer-scoped stats and dashboards.

Each cart add is tagged with which recommendation signal drove it, so the business can later measure conversion rates per signal. The engine effectiveness endpoint joins cart conversions and recommendation rejections into a per-signal report.

## What's in the frontend

The dashboard is a single-page React app, role-gated so the route a user lands on is decided by their role.

**Admin workspace.** Platform-wide overview with KPIs, sales trend chart, engine effectiveness funnel, top customers leaderboard, top sellers leaderboard, recent sales feed, customer browser, seller browser, full user management, product catalog browser.

**Seller workspace.** Two tabs on the customer list: "My customers" (assigned) and "All customers" (read-only browse, with claim action on unassigned rows). Inside a customer profile: overview tab with KPI tiles, recommendations tab with top 10 cards, cart tab with the cart helper running live, history tab, catalog tab.

**Customer workspace.** Customers see their own overview (spend KPIs, recent orders, suggested carousel, category mix, replenishment heatmap), their own top 10 recommendations, their own cart with the cart helper, their order history, and a catalog browse view.

**Shared.** The sidebar can be collapsed to icon-only mode and the state persists across page refreshes. The top-bar avatar menu has a Change Password link available to all roles.

## Authorization model

- **Admin**: full access. Sees and manages all users, all customers, all assignments. Sees the engine effectiveness panel.
- **Seller**: scoped to assigned customers by default. Can browse all customers read-only. Can claim unassigned customers. Can create new customer records auto-assigned to themselves. Can reject recommendations.
- **Customer**: scoped to self. Can view own profile, own history, own recommendations, and run the cart helper for own cart only.

All three roles can change their own password.

## Testing and QC

A comprehensive automated QC test runner lives at `scripts/qc/run_api_tests.py`. It auto-discovers users from the database, runs **128 distinct test cases** across 12 categories, and generates a multi-sheet Excel report.

Categories: Health (3), Authentication (13), User management (15), Customer endpoints (15), Recommendations across 4 lifecycle statuses (10), Cart workflow with all 9 source types (16), Purchase history (3), Assignment lifecycle (12), Stats - admin/seller/customer (9), Authorization matrix - 14 endpoints x 3 roles (32).

**Latest run: 127/128 passed (99.2%).** The single non-pass is the system correctly rejecting a redundant assignment operation, which is the assignment-history integrity feature working as designed.

```bash
python scripts/qc/run_api_tests.py
# Output: api_test_report_<timestamp>.xlsx
```

## Setup

### Prerequisites

- Python 3.10 or newer (this project uses conda env `CTBA`)
- Node.js 18 or newer (for the frontend)
- PostgreSQL 14 or newer
- DBeaver or another SQL client for running the setup script

### 1. Clone and set up the Python environment

```bash
git clone <your-repo-url>
cd <project-folder>

conda create -n CTBA python=3.10
conda activate CTBA

pip install -r requirements.txt
```

### 2. Configure environment

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

Then run any migrations in `sql/migrations/` to add later schema additions like rejection tracking. Migrations are idempotent (use `IF NOT EXISTS`) so they are safe to re-run.

### 5. Run the data pipeline

```bash
python scripts/cleaning/clean_data.py

python scripts/analysis/segment_customers.py
python scripts/analysis/segment_products.py
python scripts/analysis/segment_patterns.py

python scripts/analysis/compute_product_specialty.py
python scripts/analysis/compute_product_cooccurrence.py
python scripts/analysis/compute_private_brand_equivalents.py
python scripts/analysis/compute_item_similarity.py

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
python scripts/setup/enrich_customers_table.py
python scripts/setup/load_demo_users_and_history.py
python scripts/setup/reset_password.py
```

After this, all admin, seller, and customer accounts use the password `Demo1234!`. Change these before any non-development use.

### 8. Start the API

```bash
uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000/docs for the interactive Swagger UI.

### 9. Start the frontend

In a separate terminal:

```bash
cd frontend
npm install
npm run dev
```

Open http://localhost:5173 (or whichever port Vite reports). Log in with any of the seeded accounts using password `Demo1234!`.

### 10. Run the QC test suite (optional but recommended)

```bash
python scripts/qc/run_api_tests.py
```

## Tech stack

| Layer              | Technology                                  |
|--------------------|---------------------------------------------|
| Language (backend) | Python 3.10+                                |
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
| Frontend           | React 19, Vite, Tailwind CSS, TanStack Query, axios, lucide-react, recharts |

## Future enhancements

The engine validates well on offline data and runs end-to-end through a working dashboard. Several enhancements are natural next steps for a production rollout.

### Near-term (v2 roadmap)

**Nightly automated pipeline.** Today the pipeline runs as a one-time job on a developer laptop. Production would need a scheduled nightly run on McKesson infrastructure with monitoring, alerting, and proper error recovery.

**Multi-location chain awareness.** Hospital systems with multiple locations currently appear as separate customer records with no parent-child relationship. A 10-location hospital system gets 10 separate recommendation sets rather than chain-aware ones. Adding a chain-relationship layer would let the engine recognize purchasing patterns at the system level.

**Learned ranker on top of the eight signals.** The current engine combines signals through normalized scores and quotas. Once acceptance and rejection feedback accumulates from real sellers using the dashboard, a learned ranker (e.g., LightGBM trained on accepted vs rejected recommendations) could replace the manual quota system and continuously improve.

**CRM integration for account status.** The engine currently infers customer health from purchasing patterns because no account-status flag exists in the source data. Joining real CRM data would let the engine distinguish "paused" from "churned" from "closed" and route recommendations accordingly.

**Catalog gap reporting.** About 3.3 percent of customers (12,353 accounts) receive zero McKesson recommendations because they buy in niches where the catalog has no alternatives (Rx-only, lab reagents only). A regular report flagging these accounts could feed back into McKesson's catalog expansion priorities.

**Real-time inventory awareness in the cart helper.** The cart helper currently does not penalize out-of-stock items in its suggestions. Real-time inventory checks at suggestion time would prevent surfacing items the customer cannot actually order today.

### Medium-term

**Refresh cadences per signal.** Replenishment and lapsed_recovery would benefit from more frequent recomputation than peer_gap or item_similarity. A tiered refresh schedule (cart-helper data hourly, replenishment daily, peer-gap weekly, item-similarity monthly) would balance freshness against compute cost.

**Specialty model improvements.** Customers can be multi-specialty (a clinic with both family practice and OB/GYN), but the current engine assigns one specialty per customer. A multi-label specialty model would handle this more accurately.

**Per-rep configuration.** Different sales reps may have different priorities. An admin-controlled config layer letting reps weight signals (favor brand conversion, favor replenishment, etc.) without changing the underlying engine would make the dashboard more flexible.

**Dashboard analytics deep-dive.** The current admin overview shows engine effectiveness at a high level. A dedicated analytics workspace with cohort views, signal drill-downs, time-series, and exportable reports would help business analysts evaluate engine impact rigorously.

### Long-term

**Production deployment with HIPAA controls.** The current setup is local-only for compliance reasons. A production deployment would need formal HIPAA review, BAAs in place, and infrastructure that satisfies McKesson's enterprise security standards.

**Multi-tenant capability.** If McKesson wanted to extend the engine to other distributor brands or partners, the architecture supports this with minor changes (segment definitions, brand maps, and pitch-reason templates would parameterize per tenant).

**Self-service rep dashboards.** Today reps see what the dashboard shows them. A future enhancement could let reps build their own slice-and-dice views, save customer search queries, set up alerts for at-risk customers.

**API-first integrations.** The recommendation Parquet output is the long-term asset. The dashboard demonstrates capability, but the same Parquet output could feed McKesson's existing CRM, sales enablement tools, or quote generation systems through a well-defined API contract.

## License

This project is part of an academic capstone at William and Mary. Not licensed for commercial use without permission.