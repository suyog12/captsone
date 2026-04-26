-- STEP 1: Create the recdash schema

CREATE SCHEMA IF NOT EXISTS recdash;

SET search_path TO recdash, public;


-- STEP 2: Core tables (8 tables + assignment history)

-- 1. Users (admin, seller, customer login accounts)
CREATE TABLE IF NOT EXISTS recdash.users (
    user_id         SERIAL PRIMARY KEY,
    username        VARCHAR(100) UNIQUE NOT NULL,
    password_hash   VARCHAR(255) NOT NULL,
    role            VARCHAR(20) NOT NULL CHECK (role IN ('admin', 'seller', 'customer')),
    full_name       VARCHAR(200),
    email           VARCHAR(200),
    cust_id         BIGINT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    last_login_at   TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_is_active ON recdash.users(is_active);


-- 2. Customers (the 389K imported business customers)
CREATE TABLE IF NOT EXISTS recdash.customers (
    cust_id              BIGINT PRIMARY KEY,
    customer_name        VARCHAR(200),
    specialty_code       VARCHAR(20),
    market_code          VARCHAR(20),
    segment              VARCHAR(50),
    supplier_profile     VARCHAR(50),
    assigned_seller_id   INT REFERENCES recdash.users(user_id),
    assigned_at          TIMESTAMP,
    created_at           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_customers_assigned_seller ON recdash.customers(assigned_seller_id);
CREATE INDEX IF NOT EXISTS idx_customers_market_code     ON recdash.customers(market_code);


-- 3. Products (the 27,773 catalog items)
CREATE TABLE IF NOT EXISTS recdash.products (
    item_id           BIGINT PRIMARY KEY,
    description       VARCHAR(500),
    family            VARCHAR(200),
    category          VARCHAR(200),
    is_private_brand  BOOLEAN DEFAULT FALSE,
    unit_price        NUMERIC(10,2),
    supplier          VARCHAR(200),
    pack_size         VARCHAR(100),
    image_url         VARCHAR(500),
    created_at        TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_products_family   ON recdash.products(family);
CREATE INDEX IF NOT EXISTS idx_products_category ON recdash.products(category);


-- 4. Inventory (live stock tracked in Postgres, one row per product)
CREATE TABLE IF NOT EXISTS recdash.inventory (
    item_id           BIGINT PRIMARY KEY REFERENCES recdash.products(item_id) ON DELETE CASCADE,
    units_available   INT NOT NULL DEFAULT 0 CHECK (units_available >= 0),
    last_updated      TIMESTAMP DEFAULT NOW(),
    last_updated_by   INT REFERENCES recdash.users(user_id)
);


-- 5. Cart items (the working state of a sales conversation)
CREATE TABLE IF NOT EXISTS recdash.cart_items (
    cart_item_id        SERIAL PRIMARY KEY,
    cust_id             BIGINT NOT NULL REFERENCES recdash.customers(cust_id),
    item_id             BIGINT NOT NULL REFERENCES recdash.products(item_id),
    quantity            INT NOT NULL CHECK (quantity > 0),
    unit_price_at_add   NUMERIC(10,2),
    added_by_user_id    INT NOT NULL REFERENCES recdash.users(user_id),
    added_by_role       VARCHAR(20) NOT NULL CHECK (added_by_role IN ('seller', 'customer')),
    source              VARCHAR(40) DEFAULT 'manual'
                        CHECK (source IN ('manual', 'recommendation_peer_gap', 'recommendation_lapsed',
                                          'recommendation_replenishment', 'recommendation_cart_complement',
                                          'recommendation_pb_upgrade', 'recommendation_medline_conversion',
                                          'recommendation_item_similarity', 'recommendation_popularity')),
    status              VARCHAR(20) DEFAULT 'in_cart'
                        CHECK (status IN ('in_cart', 'sold', 'not_sold')),
    added_at            TIMESTAMP DEFAULT NOW(),
    resolved_at         TIMESTAMP,
    resolved_by_user_id INT REFERENCES recdash.users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_cart_items_cust_id     ON recdash.cart_items(cust_id);
CREATE INDEX IF NOT EXISTS idx_cart_items_status      ON recdash.cart_items(status);
CREATE INDEX IF NOT EXISTS idx_cart_items_cust_status ON recdash.cart_items(cust_id, status);


-- 6. Purchase history (committed transactions)
CREATE TABLE IF NOT EXISTS recdash.purchase_history (
    purchase_id         SERIAL PRIMARY KEY,
    cust_id             BIGINT NOT NULL REFERENCES recdash.customers(cust_id),
    item_id             BIGINT NOT NULL REFERENCES recdash.products(item_id),
    quantity            INT NOT NULL,
    unit_price          NUMERIC(10,2),
    sold_by_seller_id   INT REFERENCES recdash.users(user_id),
    sold_at             TIMESTAMP DEFAULT NOW(),
    cart_item_id        INT REFERENCES recdash.cart_items(cart_item_id)
);

CREATE INDEX IF NOT EXISTS idx_purchase_history_cust_id  ON recdash.purchase_history(cust_id);
CREATE INDEX IF NOT EXISTS idx_purchase_history_sold_at  ON recdash.purchase_history(sold_at);
CREATE INDEX IF NOT EXISTS idx_purchase_history_seller   ON recdash.purchase_history(sold_by_seller_id);


-- 7. Recommendation events (which recs were shown to whom, what happened)
CREATE TABLE IF NOT EXISTS recdash.recommendation_events (
    event_id          SERIAL PRIMARY KEY,
    cust_id           BIGINT NOT NULL REFERENCES recdash.customers(cust_id),
    item_id           BIGINT NOT NULL REFERENCES recdash.products(item_id),
    signal            VARCHAR(40),
    rank              INT,
    shown_to_user_id  INT REFERENCES recdash.users(user_id),
    shown_at          TIMESTAMP DEFAULT NOW(),
    outcome           VARCHAR(20) DEFAULT 'pending'
                      CHECK (outcome IN ('pending', 'purchased', 'rejected')),
    resolved_at       TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_rec_events_cust_id   ON recdash.recommendation_events(cust_id);
CREATE INDEX IF NOT EXISTS idx_rec_events_outcome   ON recdash.recommendation_events(outcome);
CREATE INDEX IF NOT EXISTS idx_rec_events_shown_at  ON recdash.recommendation_events(shown_at);


-- 8. Activity log (audit trail for all user actions)
CREATE TABLE IF NOT EXISTS recdash.activity_log (
    log_id        SERIAL PRIMARY KEY,
    user_id       INT REFERENCES recdash.users(user_id),
    action        VARCHAR(50) NOT NULL,
    entity_type   VARCHAR(30),
    entity_id     BIGINT,
    details       JSONB,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_activity_log_user_id     ON recdash.activity_log(user_id);
CREATE INDEX IF NOT EXISTS idx_activity_log_created_at  ON recdash.activity_log(created_at);
CREATE INDEX IF NOT EXISTS idx_activity_log_action      ON recdash.activity_log(action);


-- 9. Customer assignment history (full audit trail of seller-customer assignments)
CREATE TABLE IF NOT EXISTS recdash.customer_assignment_history (
    history_id          BIGSERIAL PRIMARY KEY,
    cust_id             BIGINT NOT NULL REFERENCES recdash.customers(cust_id),
    previous_seller_id  INT REFERENCES recdash.users(user_id),
    new_seller_id       INT REFERENCES recdash.users(user_id),
    changed_by_user_id  INT NOT NULL REFERENCES recdash.users(user_id),
    change_reason       VARCHAR(50) NOT NULL CHECK (change_reason IN (
                            'admin_assign',
                            'admin_reassign',
                            'admin_unassign',
                            'seller_claim',
                            'seller_deactivated',
                            'auto_assign_on_create'
                        )),
    notes               VARCHAR(500),
    changed_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cust_assign_history_cust_id
    ON recdash.customer_assignment_history(cust_id);
CREATE INDEX IF NOT EXISTS idx_cust_assign_history_new_seller
    ON recdash.customer_assignment_history(new_seller_id);
CREATE INDEX IF NOT EXISTS idx_cust_assign_history_changed_at
    ON recdash.customer_assignment_history(changed_at);


-- STEP 3: Seed initial users for development and demo
-- These three users let you log into the API immediately after setup.
-- All three have well-known capstone credentials. Change before any
-- non-development use.
--
-- Login credentials (capstone-only):
--   admin    / admin123
--   seller   / seller123
--   customer / customer123
--
-- The seed customer record (cust_id=16016658) must exist in the customers
-- table for the customer login to be useful, so the import scripts must
-- run before this user becomes fully functional.

INSERT INTO recdash.users (username, password_hash, role, full_name, email)
VALUES
    ('admin',
     '$2b$12$Xy0hDxEV9ZkmHZIVS3S6oOxAqXlApmcQc0Xx3sYsg8u4S73GBqFaW',
     'admin',
     'Admin User',
     'admin@capstone.local'),
    ('seller',
     '$2b$12$MsO7L8o54t2AsqouUjQtdeB5c9a81SVU5pQgIX92LEI6L8VJ682RG',
     'seller',
     'Test Seller',
     'seller@capstone.local')
ON CONFLICT (username) DO NOTHING;

INSERT INTO recdash.users (username, password_hash, role, full_name, email, cust_id)
VALUES
    ('customer',
     '$2b$12$T4In925yyfqLwpFU0M5.9Oey2cGOIZ48Ib4qTa56POjcm3BWiNqfG',
     'customer',
     'Test Customer',
     'customer@capstone.local',
     16016658)
ON CONFLICT (username) DO NOTHING;


-- STEP 4: Verification queries
-- Run these manually in DBeaver after the setup script completes to verify
-- that everything was created correctly.

-- 4a. All FKs should point at recdash.* tables (not public.*)
SELECT
    tc.table_schema || '.' || tc.table_name AS source_table,
    kcu.column_name AS source_column,
    ccu.table_schema || '.' || ccu.table_name AS target_table
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'recdash'
ORDER BY tc.table_name, kcu.column_name;

-- 4b. Confirm seed users were created
SELECT user_id, username, role, full_name, email, cust_id, is_active
FROM recdash.users
ORDER BY user_id;

-- 4c. Row counts for the data tables (will be 0 until import scripts run)
SELECT
    (SELECT COUNT(*) FROM recdash.customers)                     AS customers,
    (SELECT COUNT(*) FROM recdash.products)                      AS products,
    (SELECT COUNT(*) FROM recdash.inventory)                     AS inventory,
    (SELECT COUNT(*) FROM recdash.purchase_history)              AS purchase_history,
    (SELECT COUNT(*) FROM recdash.customer_assignment_history)   AS assignment_history;