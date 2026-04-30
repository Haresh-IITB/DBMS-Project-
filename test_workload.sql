--------------------------------------------------------------------
-- auto_index v3 Complete Feature Test
--
-- Run with:
--     psql -U postgres -d postgres -f test_v3.sql
--
-- Tail your logs in another terminal to watch the v3 math!
--------------------------------------------------------------------

\timing on
SET client_min_messages = LOG;

--------------------------------------------------------------------
-- 1. Setup & Data Generation
--------------------------------------------------------------------
\echo '====[ 1. Generating Skewed Test Data ]===='
DROP TABLE IF EXISTS user_logs;

CREATE TABLE user_logs (
    log_id      SERIAL PRIMARY KEY,
    user_id     INT,
    action      TEXT,
    created_at  TIMESTAMP DEFAULT now()
);

-- Insert 100,000 rows. 
-- user_id uses power(random(), 3) so low IDs are common, high IDs are rare.
-- action is mostly 'LOGIN', but rarely 'FAILED'.
INSERT INTO user_logs (user_id, action, created_at)
SELECT 
    (power(random(), 3) * 10000)::INT, 
    CASE 
        WHEN random() < 0.05 THEN 'FAILED'
        WHEN random() < 0.20 THEN 'LOGOUT'
        ELSE 'LOGIN'
    END,
    now() - (random() * 100 || ' days')::interval
FROM generate_series(1, 100000);

ANALYZE user_logs;


--------------------------------------------------------------------
-- 2. Single Attribute Index Test
--------------------------------------------------------------------
\echo '====[ 2. Single Attribute Test ]===='
\echo 'Querying ONLY user_id. The extension should evaluate a single-column'
\echo 'candidate and propose a singleton index.'

SELECT count(*) FROM user_logs WHERE 9999 = user_id;
SELECT count(*) FROM user_logs WHERE 9999 = user_id;
SELECT count(*) FROM user_logs WHERE 9999 = user_id;
SELECT count(*) FROM user_logs WHERE 9999 = user_id;
SELECT count(*) FROM user_logs WHERE 9999 = user_id;

\echo 'Waiting 6 seconds for BgWorker to build the user_id index...'
SELECT pg_sleep(6);


--------------------------------------------------------------------
-- 3. Composite Index Test
--------------------------------------------------------------------
\echo '====[ 3. Composite Attribute Test ]===='
\echo 'Querying action AND created_at (neither has an index yet).'
\echo 'It should evaluate cand(action), cand(created_at), and cand(action, created_at).'

SELECT count(*) FROM user_logs WHERE action = 'FAILED' AND created_at > now() - INTERVAL '10 days';
SELECT count(*) FROM user_logs WHERE action = 'FAILED' AND created_at > now() - INTERVAL '10 days';
SELECT count(*) FROM user_logs WHERE action = 'FAILED' AND created_at > now() - INTERVAL '10 days';
SELECT count(*) FROM user_logs WHERE action = 'FAILED' AND created_at > now() - INTERVAL '10 days';
SELECT count(*) FROM user_logs WHERE action = 'FAILED' AND created_at > now() - INTERVAL '10 days';

\echo 'Waiting 6 seconds for BgWorker to build the composite index...'
SELECT pg_sleep(6);


--------------------------------------------------------------------
-- 4. Write Overhead Test
--------------------------------------------------------------------
\echo '====[ 4. Write Overhead Test ]===='
\echo 'Writing 100,000 new rows to drive up r->write_count.'
\echo 'This drastically increases the write_overhead surcharge for future indexes.'

INSERT INTO user_logs (user_id, action)
SELECT (random() * 10000)::INT, 'LOGIN'
FROM generate_series(1, 100000);


--------------------------------------------------------------------
-- 5. Test Reads with High Write Penalty
--------------------------------------------------------------------
\echo '====[ 5. Read Test (High Write Penalty) ]===='
\echo 'Querying the "action" column alone.'
\echo 'Watch your logs for "wr_overhead=" and "total_cost=". The net_benefit'
\echo 'might be negative due to the massive write penalty we just triggered!'

SELECT count(*) FROM user_logs WHERE action = 'LOGOUT';
SELECT count(*) FROM user_logs WHERE action = 'LOGOUT';
SELECT count(*) FROM user_logs WHERE action = 'LOGOUT';
SELECT count(*) FROM user_logs WHERE action = 'LOGOUT';
SELECT count(*) FROM user_logs WHERE action = 'LOGOUT';


--------------------------------------------------------------------
-- 6. Existing Index Detection Test
--------------------------------------------------------------------
\echo '====[ 6. Existing Index Detection Test ]===='
\echo 'We already have a singleton on user_id (from step 2).'
\echo 'Let us run a multi-column query using user_id and action.'
\echo 'Because the existing user_id index is so selective, best_existing_cost'
\echo 'will beat the cost of creating a brand new composite index.'

SELECT count(*) FROM user_logs WHERE user_id = 9999 AND action = 'FAILED';
SELECT count(*) FROM user_logs WHERE user_id = 9999 AND action = 'FAILED';
SELECT count(*) FROM user_logs WHERE user_id = 9999 AND action = 'FAILED';


--------------------------------------------------------------------
-- 7. Print All Indexes
--------------------------------------------------------------------
\echo ' '
\echo '====[ FINAL: Printing all indexes on the database ]===='
\echo ' '

SELECT 
    schemaname, 
    tablename, 
    indexname, 
    indexdef
FROM pg_indexes
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY tablename, indexname;

\echo '====[ DONE ]===='