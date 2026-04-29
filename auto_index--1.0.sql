-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION auto_index" to load this file. \quit

-- This extension works purely via shared_preload_libraries.
-- The SQL file exists only because PGXS requires it.
