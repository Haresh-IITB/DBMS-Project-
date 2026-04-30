#ifndef AUTO_INDEX_H
#define AUTO_INDEX_H

#include "postgres.h"
#include "access/attnum.h"
#include "storage/block.h"
#include "storage/lwlock.h"

/* ============================================================
 * Tunables (compile-time caps for the shared-memory budget).
 * ============================================================ */
#define MAX_TRACKED_ENTRIES             128
#define MAX_PREDICATES_PER_REL          16
#define MAX_SHAPES_PER_REL              16
#define MAX_CANDIDATES_PER_REL          8
#define MAX_EXISTING_INDEXES_PER_REL    16
#define MAX_SHAPE_COLS                  4
#define MAX_INDEX_KEY_STR               160
#define MAX_CREATE_SQL_LEN              1024

/* ============================================================
 * Operator strategy.  See classify_btree_operator() in the .c
 * file for how an operator OID is mapped to one of these.
 * ============================================================ */
typedef enum OpStrategy
{
    OP_NONE = 0,
    OP_EQ,
    OP_LT,
    OP_LE,
    OP_GT,
    OP_GE,
    OP_NE,
    OP_RANGE
} OpStrategy;

/* ============================================================
 * One per (column, operator) we have observed on a relation.
 * ============================================================ */
typedef struct PredicateInfo
{
    AttrNumber  attno;
    OpStrategy  op_strategy;
    int64       observation_count;
    double      avg_selectivity;
    int         attwidth;               /* avg byte-width from pg_statistic */
    char        attname[NAMEDATALEN];
} PredicateInfo;

/* ============================================================
 * The set of (column, op) pairs that appeared together in one
 * SeqScan's qual list.  attnos[] is stored sorted ascending so
 * {a,b} and {b,a} collapse to the same shape.
 * ============================================================ */
typedef struct ScanShape
{
    int         num_cols;
    AttrNumber  attnos[MAX_SHAPE_COLS];
    OpStrategy  strategies[MAX_SHAPE_COLS];
    double      selectivities[MAX_SHAPE_COLS];
    int64       observation_count;
} ScanShape;

/* ============================================================
 * One existing real B-tree index on the relation, enumerated
 * from pg_index.  Used during candidate scoring so we don't
 * propose indexes that duplicate or barely improve on what is
 * already there.
 * ============================================================ */
typedef struct ExistingIndexInfo
{
    int         num_cols;
    AttrNumber  attnos[MAX_SHAPE_COLS];
    char        key_str[MAX_INDEX_KEY_STR];
} ExistingIndexInfo;

/* ============================================================
 * Per-relation state.
 * ============================================================ */
typedef struct RelStat
{
    Oid         relid;
    char        relname[NAMEDATALEN];
    char        nspname[NAMEDATALEN];

    /* cost-model inputs, refreshed on every observation                  */
    BlockNumber cached_relpages;
    double      cached_reltuples;
    int         cached_avg_width;       /* column-mean width (legacy)     */
    int         cached_row_size;        /* full pganalyze row size:
                                         *  23 (hdr) + 4 (item ptr)
                                         *  + Sum of avg_width(all cols)  */

    /* DML counter -- LOGGING ONLY now; not used by the cost model        */
    int64       write_count;

    int             num_predicates;
    PredicateInfo   predicates[MAX_PREDICATES_PER_REL];

    int             num_shapes;
    ScanShape       shapes[MAX_SHAPES_PER_REL];

    /* indexes auto_index has already created (so we don't re-propose) */
    int             num_created;
    char            created_keys[MAX_CANDIDATES_PER_REL][MAX_INDEX_KEY_STR];

    /* existing real indexes on the relation, refreshed each observation */
    int                 num_existing_indexes;
    ExistingIndexInfo   existing_indexes[MAX_EXISTING_INDEXES_PER_REL];

    /* one pending CREATE INDEX handed off to the bgworker */
    bool        has_pending_request;
    char        pending_sql[MAX_CREATE_SQL_LEN];
    char        pending_key[MAX_INDEX_KEY_STR];
} RelStat;

typedef struct AutoIndexSharedState
{
    LWLockPadded    lock;
    int             num_entries;
    RelStat         entries[MAX_TRACKED_ENTRIES];
} AutoIndexSharedState;

extern AutoIndexSharedState *auto_index_state;

#endif /* AUTO_INDEX_H */
