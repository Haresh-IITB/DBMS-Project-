\timing on
SET client_min_messages = LOG;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (id SERIAL PRIMARY KEY, col_a INT, col_b INT, col_c INT);

INSERT INTO test_table (col_a, col_b, col_c) 
SELECT (random() * 100)::INT, (random() * 100)::INT, (random() * 100)::INT FROM generate_series(1, 100000);
ANALYZE test_table;

\echo '====[ TEST 6: Prefix Coverage ]===='
\echo '1. Querying (col_a, col_b) just once to register the shape.'
SELECT count(*) FROM test_table WHERE col_a = 42 AND col_b = 42;

\echo 'Waiting for BgWorker...'
SELECT pg_sleep(6);

\echo '2. Querying (col_a, col_b, col_c) multiple times.'
\echo 'Because cand(col_a, col_b) serves BOTH shapes, its total_benefit will outpace cand(col_a, col_b, col_c)!'

SELECT count(*) FROM test_table WHERE col_a = 42 AND col_b = 42 AND col_c = 42;
SELECT count(*) FROM test_table WHERE col_a = 42 AND col_b = 42 AND col_c = 42;
SELECT count(*) FROM test_table WHERE col_a = 42 AND col_b = 42 AND col_c = 42;
SELECT count(*) FROM test_table WHERE col_a = 42 AND col_b = 42 AND col_c = 42;
SELECT count(*) FROM test_table WHERE col_a = 42 AND col_b = 42 AND col_c = 42;

\echo 'Waiting for BgWorker...'
SELECT pg_sleep(6);

\echo '====[ RESULTS ]===='
\echo 'It should have created an index on JUST (col_a, col_b), because it is smaller and covers both shapes.'
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'test_table' AND indexname LIKE 'auto_idx%';