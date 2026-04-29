#ifndef AUTO_INDEX_H
#define AUTO_INDEX_H

#include "postgres.h"
#include "access/attnum.h"
#include "storage/block.h"
#include "storage/lwlock.h"

/* ============================================================
 * Tunables (compile-time caps for the shared-memory budget).
 *
 * Everything lives in a fixed-size shared segment so we don't
 * need a dshash / DSA.  Bump these if you want larger workloads.
 * ============================================================ */
#define MAX_TRACKED_ENTRIES         128   /* distinct relations */
#define MAX_PREDICATES_PER_REL      16    /* per-(col,op) slots          */
#define MAX_SHAPES_PER_REL          16    /* distinct co-occurring sets  */
#define MAX_CANDIDATES_PER_REL      8     /* indexes auto_index created  */
#define MAX_SHAPE_COLS              4     /* max columns in one index    */
#define MAX_INDEX_KEY_STR           160   /* "12,17,42" canonical key    */
#define MAX_CREATE_SQL_LEN          1024

/* ============================================================
 * Operator strategy.
 *
 * We map any indexable operator we see in a qual to one of these
 * by looking up pg_amop with the B-tree access method.  OP_RANGE
 * is synthesized when we observe both a >=/> and a <=/< on the
 * same column in the same scan.
 * ============================================================ */
typedef enum OpStrategy
{
    OP_NONE = 0,
    OP_EQ,         /* =  (also IN-list / ScalarArrayOpExpr with useOr) */
    OP_LT,         /* <  */
    OP_LE,         /* <= */
    OP_GT,         /* >  */
    OP_GE,         /* >= */
    OP_NE,         /* <> */
    OP_RANGE       /* synthesized: BETWEEN-like (both sides bounded)   */
} OpStrategy;

/* ============================================================
 * Per-(column, operator) observation.
 *
 * One slot per distinct (attno, op_strategy) pair we've ever seen
 * on this relation.  observation_count is the total across queries.
 * ============================================================ */
typedef struct PredicateInfo
{
    AttrNumber  attno;
    OpStrategy  op_strategy;
    int64       observation_count;
    double      avg_selectivity;        /* running mean across observations */
    char        attname[NAMEDATALEN];
} PredicateInfo;

/* ============================================================
 * A "scan shape" is the set of (column, op) pairs that appeared
 * together in ONE seq-scan's qual list.  We record one shape per
 * SeqScan node per query, and bump observation_count when the same
 * shape recurs.
 *
 * attnos[] are stored sorted ascending so that {a,b} and {b,a}
 * collapse to the same shape.
 * ============================================================ */
typedef struct ScanShape
{
    int         num_cols;
    AttrNumber  attnos[MAX_SHAPE_COLS];
    OpStrategy  strategies[MAX_SHAPE_COLS];   /* aligned with attnos[] */
    double      selectivities[MAX_SHAPE_COLS];/* aligned with attnos[] */
    int64       observation_count;
} ScanShape;

/* ============================================================
 * Per-relation state.
 *
 * Holds:
 *   - cached planner-cost inputs (relpages / reltuples / avg_width)
 *   - DML write counter (drives index-maintenance cost)
 *   - per-(col,op) predicate observations
 *   - distinct scan shapes
 *   - keys of indexes auto_index has already created
 *   - one pending CREATE INDEX request the bgworker should execute
 * ============================================================ */
typedef struct RelStat
{
    Oid         relid;
    char        relname[NAMEDATALEN];
    char        nspname[NAMEDATALEN];

    /* cost-model inputs, refreshed every observation                  */
    BlockNumber cached_relpages;
    double      cached_reltuples;
    int         cached_avg_width;

    /* maintenance side of the cost/benefit                            */
    int64       write_count;

    int             num_predicates;
    PredicateInfo   predicates[MAX_PREDICATES_PER_REL];

    int             num_shapes;
    ScanShape       shapes[MAX_SHAPES_PER_REL];

    /* indexes we have already created (so we don't re-propose)        */
    int             num_created;
    char            created_keys[MAX_CANDIDATES_PER_REL][MAX_INDEX_KEY_STR];

    /* one pending CREATE INDEX handed off to the bgworker             */
    bool            has_pending_request;
    char            pending_sql[MAX_CREATE_SQL_LEN];
    char            pending_key[MAX_INDEX_KEY_STR];
} RelStat;

/* ============================================================
 * Top-level shared state.
 * ============================================================ */
typedef struct AutoIndexSharedState
{
    LWLockPadded    lock;
    int             num_entries;
    RelStat         entries[MAX_TRACKED_ENTRIES];
} AutoIndexSharedState;

extern AutoIndexSharedState *auto_index_state;

#endif /* AUTO_INDEX_H */
