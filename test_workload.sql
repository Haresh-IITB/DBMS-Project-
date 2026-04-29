--------------------------------------------------------------------
-- auto_index test workload
--
-- Run with:
--     psql -d postgres -f test_workload.sql
--
-- Then watch the Postgres log (tail -F on the cluster's logfile).
-- Every interesting decision is prefixed with "auto_index:".
--------------------------------------------------------------------

\timing on
SET client_min_messages = LOG;

--------------------------------------------------------------------
-- 0. Reset
--------------------------------------------------------------------
DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id     INT,
    customer_id  INT,
    status       TEXT,
    amount       INT,
    created_at   DATE
);

INSERT INTO orders
SELECT
    i,
    (i % 1000),
    CASE
        WHEN i % 20 = 0 THEN 'FAILED'
        WHEN i %  5 = 0 THEN 'PENDING'
        ELSE 'COMPLETED'
    END,
    (random() * 1000)::INT,
    DATE '2024-01-01' + ((i % 365) || ' days')::INTERVAL
FROM generate_series(1, 100000) AS i;

ANALYZE orders;

--------------------------------------------------------------------
-- 1. Equality predicate (Phase 1: classify '=' as OP_EQ)
--    Run several times so the per-shape observation_count grows
--    enough that benefit can plausibly cover index creation cost.
--------------------------------------------------------------------
\echo '====[ 1. EQUALITY workload ]===='

SELECT count(*) FROM orders WHERE status = 'FAILED';
SELECT count(*) FROM orders WHERE status = 'FAILED';
SELECT count(*) FROM orders WHERE status = 'FAILED';
SELECT count(*) FROM orders WHERE status = 'FAILED';
SELECT count(*) FROM orders WHERE status = 'FAILED';

--------------------------------------------------------------------
-- 2. Range predicate (Phase 1: <, >; Phase 1: collapse_range_pairs)
--------------------------------------------------------------------
\echo '====[ 2. RANGE workload ]===='

SELECT count(*) FROM orders WHERE amount > 800;
SELECT count(*) FROM orders WHERE amount > 800;
SELECT count(*) FROM orders WHERE amount BETWEEN 100 AND 200;
SELECT count(*) FROM orders WHERE amount BETWEEN 100 AND 200;
SELECT count(*) FROM orders WHERE amount BETWEEN 100 AND 200;

--------------------------------------------------------------------
-- 3. IN-list (Phase 1: ScalarArrayOpExpr)
--------------------------------------------------------------------
\echo '====[ 3. IN-list workload ]===='

SELECT count(*) FROM orders WHERE customer_id IN (1, 2, 3, 4, 5);
SELECT count(*) FROM orders WHERE customer_id IN (1, 2, 3, 4, 5);
SELECT count(*) FROM orders WHERE customer_id IN (1, 2, 3, 4, 5);

--------------------------------------------------------------------
-- 4. Multi-column conjunction (Phase 3: composite vs singleton)
--    The multi-col candidate (status, customer_id) should win once
--    this shape recurs enough.
--------------------------------------------------------------------
\echo '====[ 4. MULTI-COLUMN workload ]===='

SELECT count(*) FROM orders WHERE status = 'PENDING' AND customer_id = 42;
SELECT count(*) FROM orders WHERE status = 'PENDING' AND customer_id = 42;
SELECT count(*) FROM orders WHERE status = 'PENDING' AND customer_id = 42;
SELECT count(*) FROM orders WHERE status = 'PENDING' AND customer_id = 99;
SELECT count(*) FROM orders WHERE status = 'PENDING' AND customer_id = 99;

--------------------------------------------------------------------
-- 5. Mixed equality + range (Phase 3: equality column should be
--    placed FIRST in the composite index)
--------------------------------------------------------------------
\echo '====[ 5. EQ + RANGE workload ]===='

SELECT count(*) FROM orders WHERE status = 'FAILED' AND amount > 500;
SELECT count(*) FROM orders WHERE status = 'FAILED' AND amount > 500;
SELECT count(*) FROM orders WHERE status = 'FAILED' AND amount > 500;

--------------------------------------------------------------------
-- 6. Wait for the bgworker to actually run CREATE INDEX
--    (poll loop wakes every 5s; sleep a bit longer to be safe)
--------------------------------------------------------------------
\echo '====[ 6. Sleep to let bgworker create indexes ]===='
SELECT pg_sleep(8);

--------------------------------------------------------------------
-- 7. Inspect what got created
--------------------------------------------------------------------
\echo '====[ 7. Indexes auto_index actually created ]===='
SELECT indexname, indexdef
  FROM pg_indexes
 WHERE tablename = 'orders'
   AND indexname LIKE 'auto_idx%'
 ORDER BY indexname;

--------------------------------------------------------------------
-- 8. Show that the planner now picks them
--------------------------------------------------------------------
\echo '====[ 8. EXPLAIN: should now use the auto-created index ]===='
EXPLAIN SELECT * FROM orders WHERE status = 'FAILED';
EXPLAIN SELECT * FROM orders WHERE status = 'PENDING' AND customer_id = 42;

--------------------------------------------------------------------
-- 9. Heavy write workload to demonstrate maintenance-cost weighting
--    (write_count goes up; subsequent re-evaluations should treat
--     new candidates as more expensive)
--------------------------------------------------------------------
\echo '====[ 9. WRITE-heavy workload to bump maintenance cost ]===='
INSERT INTO orders
SELECT
    100000 + i, (i % 1000), 'COMPLETED', (random() * 1000)::INT,
    DATE '2024-06-01'
FROM generate_series(1, 50) AS i;

-- A new column predicate after many writes -- should show much higher
-- maint_cost in the LOG line.
SELECT count(*) FROM orders WHERE created_at = DATE '2024-06-01';
SELECT count(*) FROM orders WHERE created_at = DATE '2024-06-01';
SELECT count(*) FROM orders WHERE created_at = DATE '2024-06-01';
SELECT count(*) FROM orders WHERE created_at = DATE '2024-06-01';

\echo '====[ DONE - check the logs and the indexes table above ]===='
