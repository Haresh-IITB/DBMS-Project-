--------------------------------------------------------------------
-- TEST 5 — Existing (a,b) blocks a redundant (a,b,c) proposal
--
-- DEMONSTRATES:
--   - enumerate_existing_indexes() reads pg_index and feeds them to
--     score_candidate as best_existing_cost competitors.
--   - prefix_length_for_shape() lets the existing index (a,b) serve
--     a query on {a,b,c}: prefix=2, c becomes a residual filter.
--   - score_candidate counts benefit only when a candidate strictly
--     beats the best existing option.  Here (a,b,c)'s marginal
--     improvement over (a,b) is small because c has very low
--     selectivity (~0.99), so its net_benefit stays NEGATIVE.
--   - candidate_already_exists also short-circuits cand[a,b] with
--     "skipped (already present)" -- it matches the manually-created
--     index by key_str.
--
-- WHAT TO LOOK FOR IN THE LOG:
--   - "existing_idx[1,2]" (or whichever attnos a,b are) on the
--     evaluating-line.
--   - "cand[1,2] skipped (already present)"
--   - "cand[1,2,3] cols=3 covers=N benefit=SMALL net=NEGATIVE"
--   - "no candidate with positive net benefit" each time.
--
-- EXPECTED FINAL STATE:
--   pg_indexes shows only the manual (a,b) index plus pkey.
--   No auto_idx_* gets created.
--------------------------------------------------------------------
\timing on
SET client_min_messages = LOG;

DROP TABLE IF EXISTS t5_dedup;

CREATE TABLE t5_dedup (
    id   SERIAL PRIMARY KEY,
    a    INT,        -- high cardinality, sel ~ 0.001
    b    INT,        -- medium,           sel ~ 0.05
    c    BOOL,       -- VERY low sel:     ~99% TRUE  -> sel(c=TRUE) ≈ 0.99
    pad  TEXT
);

INSERT INTO t5_dedup (a, b, c, pad)
SELECT
    (i % 1000) + 1,                      -- a: 1..1000
    (i %   20) + 1,                      -- b: 1..20
    (i % 100) > 0,                       -- c: TRUE 99% of the time
    'p'
FROM generate_series(1, 100000) AS i;

ANALYZE t5_dedup;

\echo '====[ Manually create an index on (a, b) ]===='
CREATE INDEX manual_t5_a_b ON t5_dedup (a, b);

\echo '====[ Workload: query on {a, b, c} repeatedly ]===='
\echo '  We force a SeqScan via SET to make sure our hook fires '
\echo '  even though the manual (a,b) index would normally be used.'

SET enable_indexscan       = off;
SET enable_bitmapscan      = off;
SET enable_indexonlyscan   = off;

SELECT count(*) FROM t5_dedup WHERE a = 42 AND b = 5  AND c = TRUE;
SELECT count(*) FROM t5_dedup WHERE a = 99 AND b = 7  AND c = TRUE;
SELECT count(*) FROM t5_dedup WHERE a = 7  AND b = 11 AND c = TRUE;
SELECT count(*) FROM t5_dedup WHERE a = 333 AND b = 3 AND c = TRUE;
SELECT count(*) FROM t5_dedup WHERE a = 555 AND b = 8 AND c = TRUE;

RESET enable_indexscan;
RESET enable_bitmapscan;
RESET enable_indexonlyscan;

\echo '====[ Wait for any (would-be) bgworker activity ]===='
SELECT pg_sleep(8);

\echo '====[ Indexes on t5_dedup: expect pkey + manual_t5_a_b only ]===='
SELECT indexname, indexdef
  FROM pg_indexes
 WHERE tablename = 't5_dedup'
 ORDER BY indexname;

\echo '====[ Plan: planner uses the manual (a,b) index for {a,b,c} ]===='
EXPLAIN SELECT * FROM t5_dedup WHERE a = 42 AND b = 5 AND c = TRUE;