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


\echo '====[ 3. IN-list workload ]===='

SELECT count(*) FROM orders WHERE customer_id IN (1, 2, 3, 4, 5);
SELECT count(*) FROM orders WHERE customer_id IN (1, 2, 3, 4, 5);
SELECT count(*) FROM orders WHERE customer_id IN (1, 2, 3, 4, 5);

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
