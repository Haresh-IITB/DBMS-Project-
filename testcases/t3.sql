--------------------------------------------------------------------
-- TEST 3 — Composite index from multi-column workload
--
-- DEMONSTRATES:
--   - When two columns appear together in the same SeqScan qual list,
--     a composite candidate is generated alongside the singletons.
--   - Composite columns are ordered for B-tree access: equality
--     columns first, more-selective leftmost.  Here customer_id
--     (sel ~0.001) leads status (sel ~0.05-0.80).
--   - The composite normally beats the singletons because eff_sel
--     (product) is much smaller than either column alone.
--
-- HOW IT WORKS:
--   - 100k rows, customer_id has 1000 distinct, status has 3 values
--     (MCV-skewed: COMPLETED 80%, FAILED 5%, PENDING 15%).
--   - Shape {customer_id, status} repeated 5 times.
--   - benefit_per_scan(composite) ≈ 1900, creation ≈ 1457.
--   - First scan: net ≈ 1900 - 1457 = small positive (or negative
--     depending on widths/per_write); typically triggers by scan 2.
--
-- EXPECTED LOG SEQUENCE:
--   auto_index: predicate on public.t3_composite.customer_id ...
--   auto_index: predicate on public.t3_composite.status ...
--   auto_index:   new shape on public.t3_composite with 2 column(s)
--   auto_index:   cand[2,3] cols=2 covers=1 benefit=...
--   auto_index:   cand[2]   cols=1 ...
--   auto_index:   cand[3]   cols=1 ...
--   auto_index: PROPOSING ... ON public.t3_composite
--                 ("customer_id","status")    <-- customer_id LEFT
--
-- EXPECTED FINAL STATE:
--   ONE auto_idx_*_X_Y where the column order is (customer_id, status).
--------------------------------------------------------------------
\timing on
SET client_min_messages = LOG;

DROP TABLE IF EXISTS t3_composite;

CREATE TABLE t3_composite (
    id           SERIAL PRIMARY KEY,
    customer_id  INT,
    status       TEXT,
    payload      TEXT
);

INSERT INTO t3_composite (customer_id, status, payload)
SELECT
    (i % 1000) + 1,
    CASE
        WHEN i % 20 = 0 THEN 'PENDING'        -- 5%
        WHEN i %  5 = 0 THEN 'FAILED'         -- 15%
        ELSE                  'COMPLETED'     -- 80%
    END,
    'p'
FROM generate_series(1, 100000) AS i;

ANALYZE t3_composite;

\echo '====[ Repeated multi-column EQ: customer_id = ? AND status = ? ]===='

SELECT count(*) FROM t3_composite WHERE customer_id = 42 AND status = 'PENDING';
SELECT count(*) FROM t3_composite WHERE customer_id = 99 AND status = 'PENDING';
SELECT count(*) FROM t3_composite WHERE customer_id = 7  AND status = 'PENDING';
SELECT count(*) FROM t3_composite WHERE customer_id = 333 AND status = 'PENDING';
SELECT count(*) FROM t3_composite WHERE customer_id = 555 AND status = 'PENDING';

\echo '====[ Wait for bgworker ]===='
SELECT pg_sleep(8);

\echo '====[ Indexes on t3_composite (expect ONE composite auto_idx_*) ]===='
SELECT indexname, indexdef
  FROM pg_indexes
 WHERE tablename = 't3_composite'
 ORDER BY indexname;

\echo '====[ Plan: should use the composite index, customer_id leading ]===='
EXPLAIN SELECT * FROM t3_composite
 WHERE customer_id = 42 AND status = 'PENDING';