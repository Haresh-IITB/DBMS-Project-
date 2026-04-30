--------------------------------------------------------------------
-- TEST 1 — Singleton index from repeated equality access
--
-- DEMONSTRATES:
--   A column hit by an equality predicate enough times generates a
--   singleton index candidate, accumulates positive net benefit,
--   and gets created by the bgworker.
--
-- HOW IT WORKS:
--   - Table has 100k rows, customer_id with 1000 distinct values
--     (selectivity ~ 0.001).  Index scan returns ~100 rows, much
--     cheaper than a 720-page seq scan.
--   - benefit_per_scan ≈ seq_cost − idx_cost ≈ 1565
--   - creation_cost ≈ 1457, write_count = 0 (no writes here)
--   - net_benefit > 0 after one or two recurrences -> PROPOSE.
--
-- EXPECTED LOG SEQUENCE:
--   auto_index: predicate on public.t1_singleton.customer_id op=EQ ...
--   auto_index:   new shape on public.t1_singleton with 1 column(s)
--   auto_index: evaluating 1 candidate(s) ... propose=yes
--   auto_index:   cand[N] cols=1 covers=1 benefit=... net=POSITIVE
--   auto_index: PROPOSING CREATE INDEX IF NOT EXISTS auto_idx_..._N
--                 ON public.t1_singleton ("customer_id")
--   auto_index: bgworker created index (key=N)
--
-- EXPECTED FINAL STATE:
--   pg_indexes shows one auto_idx_*_N row on t1_singleton.
--------------------------------------------------------------------
\timing on
SET client_min_messages = LOG;

DROP TABLE IF EXISTS t1_singleton;

CREATE TABLE t1_singleton (
    id           SERIAL PRIMARY KEY,
    customer_id  INT,
    payload      TEXT
);

INSERT INTO t1_singleton (customer_id, payload)
SELECT (i % 1000) + 1, 'p'
FROM generate_series(1, 100000) AS i;

ANALYZE t1_singleton;

\echo '====[ Repeated EQ access: customer_id = ? ]===='

SELECT count(*) FROM t1_singleton WHERE customer_id = 42;
SELECT count(*) FROM t1_singleton WHERE customer_id = 99;
SELECT count(*) FROM t1_singleton WHERE customer_id = 7;
SELECT count(*) FROM t1_singleton WHERE customer_id = 333;
SELECT count(*) FROM t1_singleton WHERE customer_id = 555;

\echo '====[ Wait for bgworker ]===='
SELECT pg_sleep(8);

\echo '====[ Indexes on t1_singleton (expect ONE auto_idx_*) ]===='
SELECT indexname, indexdef
  FROM pg_indexes
 WHERE tablename = 't1_singleton'
 ORDER BY indexname;

\echo '====[ Plan after index creation (expect index/bitmap scan) ]===='
EXPLAIN SELECT * FROM t1_singleton WHERE customer_id = 42;