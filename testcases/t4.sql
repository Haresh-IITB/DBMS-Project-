--------------------------------------------------------------------
-- TEST 4 — Write-heavy table: index NOT created
--
-- DEMONSTRATES:
--   The additive cost model:
--       total_cost = creation_cost + per_write_cost × write_count
--   Pre-loading the relation with millions of writes pushes total_cost
--   above what a small number of SELECTs can cover with their benefit.
--   The candidate is scored every time, but its net_benefit stays
--   negative across multiple SELECTs.  No index is created.
--
-- WHAT TO LOOK FOR IN THE LOG:
--   - "writes=2000000" (or thereabouts) on the evaluating-line.
--   - per_write=~0.011 on each cand[...] line.
--   - total_cost dominated by the writes term:
--       creation ≈ 26000   per_write × writes ≈ 22000   total ≈ 48000
--   - net=NEGATIVE on every SELECT.
--   - "no candidate with positive net benefit" for each scan.
--
-- CONTRAST WITH TEST 1:
--   Test 1 has the same table shape and selectivity but write_count=0.
--   It triggers an index in one or two SELECTs.  Here, with write_count
--   huge, three SELECTs are not enough to overcome the write cost.
--
-- EXPECTED FINAL STATE:
--   pg_indexes shows ONLY the primary key.  No auto_idx_*.
--------------------------------------------------------------------
\timing on
SET client_min_messages = LOG;

DROP TABLE IF EXISTS t4_writeheavy;

CREATE TABLE t4_writeheavy (
    id           SERIAL PRIMARY KEY,
    customer_id  INT,
    payload      TEXT
);

INSERT INTO t4_writeheavy (customer_id, payload)
SELECT (i % 1000) + 1, 'p'
FROM generate_series(1, 100000) AS i;

ANALYZE t4_writeheavy;

\echo '====[ Priming the tracker with 1 read ]===='
\echo '  This registers the table in shared memory so future writes are counted.'
SELECT count(*) FROM t4_writeheavy WHERE customer_id > 100;
-- SELECT count(*) FROM t4_writeheavy WHERE customer_id > 999;

\echo '====[ Bulk-inserting 2M rows -> bumps write_count via es_processed ]===='
\echo '  (this is the "heavy write workload" being simulated)'

INSERT INTO t4_writeheavy (customer_id, payload)
SELECT (i % 1000) + 1, 'p'
FROM generate_series(1, 2000000) AS i;

\echo '====[ Now 3 SELECTs that WOULD trigger an index in a read-only world ]===='
\echo '  Watch the log: writes=~2000000, total_cost > 3*benefit_per_scan,'
\echo '  net_benefit STAYS NEGATIVE.'

SELECT count(*) FROM t4_writeheavy WHERE customer_id = 42;
SELECT count(*) FROM t4_writeheavy WHERE customer_id = 99;
SELECT count(*) FROM t4_writeheavy WHERE customer_id = 7;

\echo '====[ Wait for any (would-be) bgworker activity ]===='
SELECT pg_sleep(8);

\echo '====[ Indexes on t4_writeheavy: expect ONLY the primary key ]===='
SELECT indexname, indexdef
  FROM pg_indexes
 WHERE tablename = 't4_writeheavy'
 ORDER BY indexname;

\echo '====[ Plan: still a SeqScan (no auto_idx was created) ]===='
EXPLAIN SELECT * FROM t4_writeheavy WHERE customer_id = 42;