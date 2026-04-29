/* ================================================================
 * auto_index.c
 *
 * Automatic index creation extension for PostgreSQL.
 *
 * Sections / phases (matches the agreed plan):
 *
 *   Phase 0  Per-relation tracking with predicate / shape sub-arrays
 *   Phase 1  Generalized operator detection (=, <, <=, >, >=, IN,
 *            range pairs) via the pg_amop catalog
 *   Phase 2  Cost model derived from PostgreSQL's own planner GUCs
 *            (seq_page_cost / random_page_cost / cpu_*_cost) plus
 *            selectivity from pg_statistic
 *   Phase 3  Multi-column candidate generation with B-tree column
 *            ordering (equality first, then by selectivity)
 *   Phase 4  Cross-shape benefit accounting + dedup against indexes
 *            we have already created
 *
 * Logging:
 *   - elog(LOG, ...) is used for every meaningful decision so the
 *     extension's behaviour can be followed by tailing the postmaster
 *     log.  All such lines start with "auto_index:" so they are easy
 *     to grep for.
 *   - elog(DEBUG1, ...) is used for finer trace; enable with
 *     SET client_min_messages = DEBUG1 (or log_min_messages).
 * ================================================================ */

#include "postgres.h"

#include <math.h>

#include "auto_index.h"

#include "access/attnum.h"
#include "access/htup_details.h"
#include "access/stratnum.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_statistic.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "optimizer/cost.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "optimizer/optimizer.h"

PG_MODULE_MAGIC;

/* ================================================================
 *  Globals & forward declarations
 * ================================================================ */

AutoIndexSharedState *auto_index_state = NULL;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorRun_hook_type   prev_ExecutorRun        = NULL;

static volatile sig_atomic_t got_sigterm = false;

/* shared-memory plumbing */
static void  auto_index_shmem_request_hook(void);
static void  auto_index_shmem_startup_hook(void);
static void  auto_index_shmem_request(void);
static void  auto_index_shmem_startup(void);
static Size  auto_index_shmem_size(void);

/* executor hook + plan walker */
static void  auto_index_ExecutorRun(QueryDesc *queryDesc,
                                    ScanDirection direction,
                                    uint64 count);
static void  walk_plan_tree(PlanState *planstate);
static void  process_seqscan_node(SeqScanState *seqstate);
static void  bump_writes_for_modify(QueryDesc *queryDesc);

/* phase 1: operator classification */
static OpStrategy classify_btree_operator(Oid opno);
static const char *opstrategy_name(OpStrategy s);

/* phase 2: selectivity + cost */
static double   estimate_eq_selectivity(Oid relid, AttrNumber attno);
static double   estimate_ineq_selectivity(OpStrategy s);
static double   estimate_index_height(double reltuples, int avg_width);
static double   estimate_seqscan_cost(BlockNumber relpages, double reltuples);
static double   cost_index_scan(double n_matching, double height);
static double   cost_index_creation(BlockNumber relpages);
static double   cost_per_write_maint(double height);

/* phase 0: shared-state mutation helpers */
static RelStat       *find_or_create_relstat(Oid relid,
                                             const char *relname,
                                             const char *nspname,
                                             BlockNumber relpages,
                                             double reltuples,
                                             int avg_width);
static PredicateInfo *find_or_create_predicate(RelStat *r,
                                               AttrNumber attno,
                                               OpStrategy s,
                                               const char *attname);
static void           upsert_scan_shape(RelStat *r,
                                        int n,
                                        AttrNumber *attnos,
                                        OpStrategy *strats,
                                        double *sels);

/* phase 3 + 4: candidate generation, scoring, selection */
typedef struct IndexCandidate
{
    int         num_cols;
    AttrNumber  attnos[MAX_SHAPE_COLS];
    OpStrategy  strategies[MAX_SHAPE_COLS];
    double      selectivities[MAX_SHAPE_COLS];
    char        attnames[MAX_SHAPE_COLS][NAMEDATALEN];
    char        key_str[MAX_INDEX_KEY_STR];

    /* scoring outputs */
    double      total_benefit;
    double      maint_cost;
    double      creation_cost;
    double      net_benefit;
    int         shapes_covered;
} IndexCandidate;

#define MAX_LOCAL_CANDIDATES 64

static int    build_candidates(RelStat *r,
                               IndexCandidate *out, int max_out);
static void   sort_candidate_columns(IndexCandidate *c);
static void   serialize_candidate_key(IndexCandidate *c);
static bool   candidate_already_created(RelStat *r, IndexCandidate *c);
static bool   candidate_can_serve_shape(IndexCandidate *c, ScanShape *s);
static void   score_candidate(IndexCandidate *c, RelStat *r);
static void   evaluate_and_propose(RelStat *r);

/* bgworker */
void          _PG_init(void);
void          _PG_fini(void);
PGDLLEXPORT void auto_index_worker_main(Datum main_arg);
static void   auto_index_sigterm(SIGNAL_ARGS);
static void   maybe_create_pending_index(RelStat *entry);


/* ================================================================
 *  Shared-memory plumbing
 * ================================================================ */

static Size
auto_index_shmem_size(void)
{
    return sizeof(AutoIndexSharedState);
}

static void
auto_index_shmem_request(void)
{
    RequestAddinShmemSpace(auto_index_shmem_size());
    RequestNamedLWLockTranche("auto_index", 1);
}

static void
auto_index_shmem_startup(void)
{
    bool found;

    auto_index_state = ShmemInitStruct("auto_index_state",
                                       auto_index_shmem_size(),
                                       &found);
    if (!found)
    {
        MemSet(auto_index_state, 0, auto_index_shmem_size());
        auto_index_state->lock = *GetNamedLWLockTranche("auto_index");
        auto_index_state->num_entries = 0;
        elog(LOG, "auto_index: shared state initialized "
                  "(MAX_TRACKED_ENTRIES=%d, MAX_PREDICATES_PER_REL=%d, "
                  "MAX_SHAPES_PER_REL=%d, total_size=%zu bytes)",
             MAX_TRACKED_ENTRIES, MAX_PREDICATES_PER_REL,
             MAX_SHAPES_PER_REL, auto_index_shmem_size());
    }
}

static void
auto_index_shmem_request_hook(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();
    auto_index_shmem_request();
}

static void
auto_index_shmem_startup_hook(void)
{
    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();
    auto_index_shmem_startup();
}


/* ================================================================
 *  Extension init / fini
 * ================================================================ */

void
_PG_init(void)
{
    BackgroundWorker worker;

    if (!process_shared_preload_libraries_in_progress)
        ereport(ERROR,
                (errmsg("auto_index must be loaded via "
                        "shared_preload_libraries")));

    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook      = auto_index_shmem_request_hook;

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook      = auto_index_shmem_startup_hook;

    prev_ExecutorRun = ExecutorRun_hook;
    ExecutorRun_hook = auto_index_ExecutorRun;

    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags        = BGWORKER_SHMEM_ACCESS |
                              BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time   = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 5;
    snprintf(worker.bgw_name,         BGW_MAXLEN, "auto_index worker");
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "auto_index");
    snprintf(worker.bgw_function_name,BGW_MAXLEN, "auto_index_worker_main");
    worker.bgw_main_arg   = (Datum) 0;
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);

    elog(LOG, "auto_index: extension loaded");
}

void
_PG_fini(void)
{
    ExecutorRun_hook = prev_ExecutorRun;
}


/* ================================================================
 *  Phase 1 - operator classification
 *
 *  We look the operator up in pg_amop and ask: does any B-tree
 *  operator class contain it, and if so, with which strategy
 *  number?  This is the same lookup the planner uses, so we accept
 *  exactly the operators that B-tree can index.
 * ================================================================ */

static OpStrategy
classify_btree_operator(Oid opno)
{
    CatCList   *list;
    OpStrategy  result = OP_NONE;
    int         i;

    list = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));
    for (i = 0; i < list->n_members; i++)
    {
        HeapTuple       tuple = &list->members[i]->tuple;
        Form_pg_amop    amop  = (Form_pg_amop) GETSTRUCT(tuple);

        if (amop->amopmethod != BTREE_AM_OID)
            continue;

        switch (amop->amopstrategy)
        {
            case BTLessStrategyNumber:         result = OP_LT; break;
            case BTLessEqualStrategyNumber:    result = OP_LE; break;
            case BTEqualStrategyNumber:        result = OP_EQ; break;
            case BTGreaterEqualStrategyNumber: result = OP_GE; break;
            case BTGreaterStrategyNumber:      result = OP_GT; break;
            default:                           result = OP_NONE; break;
        }
        if (result != OP_NONE)
            break;
    }
    ReleaseSysCacheList(list);

    /* PostgreSQL has no btree strategy for <>; detect by oprname. */
    if (result == OP_NONE)
    {
        HeapTuple tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
        if (HeapTupleIsValid(tp))
        {
            Form_pg_operator op = (Form_pg_operator) GETSTRUCT(tp);
            if (strcmp(NameStr(op->oprname), "<>") == 0)
                result = OP_NE;
            ReleaseSysCache(tp);
        }
    }

    return result;
}

static const char *
opstrategy_name(OpStrategy s)
{
    switch (s)
    {
        case OP_EQ:    return "EQ";
        case OP_LT:    return "LT";
        case OP_LE:    return "LE";
        case OP_GT:    return "GT";
        case OP_GE:    return "GE";
        case OP_NE:    return "NE";
        case OP_RANGE: return "RANGE";
        default:       return "NONE";
    }
}


/* ================================================================
 *  Phase 2 - selectivity + cost model
 *
 *  Selectivity comes from pg_statistic (n_distinct for equality,
 *  PostgreSQL planner defaults for inequalities and ranges).  All
 *  costs are expressed in the planner's native units (multiples of
 *  seq_page_cost) so they remain meaningful regardless of how the
 *  DBA tuned the GUCs for the underlying storage.
 * ================================================================ */

static double
estimate_eq_selectivity(Oid relid, AttrNumber attno)
{
    HeapTuple   tp;
    double      sel = DEFAULT_EQ_SEL;     /* 0.005, planner default */

    tp = SearchSysCache3(STATRELATTINH,
                         ObjectIdGetDatum(relid),
                         Int16GetDatum(attno),
                         BoolGetDatum(false));
    if (HeapTupleIsValid(tp))
    {
        Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(tp);
        double            n_distinct = stats->stadistinct;

        if (n_distinct > 0)
            sel = 1.0 / n_distinct;
        else if (n_distinct < 0)
            sel = -n_distinct;            /* fraction of distinct rows */
        /* if n_distinct == 0 we leave the planner default in place    */

        ReleaseSysCache(tp);
    }
    if (sel <= 0.0) sel = DEFAULT_EQ_SEL;
    if (sel >  1.0) sel = 1.0;
    return sel;
}

static double
estimate_ineq_selectivity(OpStrategy s)
{
    /*
     * We don't try to read histograms here -- for the cost model the
     * planner's own defaults give us a sensible mid-range estimate.
     */
    switch (s)
    {
        case OP_LT:
        case OP_LE:
        case OP_GT:
        case OP_GE:    return DEFAULT_INEQ_SEL;          /* 1/3   */
        case OP_RANGE: return DEFAULT_RANGE_INEQ_SEL;    /* 0.005 */
        case OP_NE:    return 1.0 - DEFAULT_EQ_SEL;
        default:       return 0.1;
    }
}

/*
 * Estimate B-tree height as ceil(log_F(N_leaf)) with fanout F = 100.
 * N_leaf is approximated from reltuples and average key width.
 * Returns a value >= 1.
 */
static double
estimate_index_height(double reltuples, int avg_width)
{
    double  per_tuple_bytes;
    double  leaf_pages;
    double  height;
    double  fanout = 100.0;

    if (reltuples <= 0) reltuples = 1.0;
    if (avg_width <= 0) avg_width = 8;

    /* index tuple header (~6) + key + ItemIdData (~4) */
    per_tuple_bytes = (double) avg_width + 16.0;
    leaf_pages = ceil((reltuples * per_tuple_bytes) / (double) BLCKSZ);
    if (leaf_pages < 1.0) leaf_pages = 1.0;

    height = ceil(log(leaf_pages) / log(fanout));
    if (height < 1.0) height = 1.0;
    return height;
}

/*
 * Sequential scan cost (slide A1, in PG units):
 *     seq_page_cost * relpages + (cpu_tuple_cost + cpu_operator_cost)
 *                                                          * reltuples
 */
static double
estimate_seqscan_cost(BlockNumber relpages, double reltuples)
{
    double pages = (relpages > 0) ? (double) relpages : 1.0;
    double rows  = (reltuples > 0) ? reltuples         : 1.0;

    return pages * seq_page_cost
         + rows  * (cpu_tuple_cost + cpu_operator_cost);
}

/*
 * Secondary index scan cost (slide A4 with PG units).
 *   - h_i page reads to descend the tree (random pages)
 *   - n_matching * random_page_cost to fetch each heap tuple
 *     (worst case: every match on a different page)
 *   - per-tuple CPU work
 */
static double
cost_index_scan(double n_matching, double height)
{
    double rows = (n_matching < 1.0) ? 1.0 : n_matching;

    return  height * random_page_cost                            /* descent */
          + rows   * random_page_cost                            /* heap   */
          + rows   * (cpu_index_tuple_cost + cpu_tuple_cost
                      + cpu_operator_cost);
}

/*
 * Index creation cost.  We use a simple (sort + bulk-load) approximation:
 *   - one sequential scan of the table
 *   - O(N log N) sort step priced via cpu_operator_cost
 *   - one sequential write of the index pages
 */
static double
cost_index_creation(BlockNumber relpages)
{
    double pages = (relpages > 0) ? (double) relpages : 1.0;
    double sort_log;

    sort_log = log(pages) / log(2.0);
    if (sort_log < 1.0) sort_log = 1.0;

    return  pages * seq_page_cost           /* read base table */
          + pages * sort_log * cpu_operator_cost
          + pages * seq_page_cost;          /* write index     */
}

/*
 * Per-write maintenance cost: one tree descent + one leaf insert.
 *   (h_i + 1) random pages + cpu_index_tuple_cost
 */
static double
cost_per_write_maint(double height)
{
    return (height + 1.0) * random_page_cost + cpu_index_tuple_cost;
}


/* ================================================================
 *  Phase 0 helpers - shared state mutation
 * ================================================================ */

static RelStat *
find_or_create_relstat(Oid relid, const char *relname, const char *nspname,
                       BlockNumber relpages, double reltuples, int avg_width)
{
    AutoIndexSharedState *state = auto_index_state;
    int i;

    for (i = 0; i < state->num_entries; i++)
    {
        if (state->entries[i].relid == relid)
        {
            RelStat *r = &state->entries[i];
            r->cached_relpages   = relpages;
            r->cached_reltuples  = reltuples;
            r->cached_avg_width  = avg_width;
            return r;
        }
    }

    if (state->num_entries >= MAX_TRACKED_ENTRIES)
        return NULL;

    {
        RelStat *r = &state->entries[state->num_entries++];

        MemSet(r, 0, sizeof(*r));
        r->relid              = relid;
        r->cached_relpages    = relpages;
        r->cached_reltuples   = reltuples;
        r->cached_avg_width   = avg_width;
        strlcpy(r->relname, relname, NAMEDATALEN);
        strlcpy(r->nspname, nspname, NAMEDATALEN);

        elog(LOG, "auto_index: now tracking %s.%s "
                  "(relpages=%u reltuples=%.0f avg_width=%d)",
             nspname, relname, relpages, reltuples, avg_width);
        return r;
    }
}

static PredicateInfo *
find_or_create_predicate(RelStat *r, AttrNumber attno, OpStrategy s,
                         const char *attname)
{
    int i;

    for (i = 0; i < r->num_predicates; i++)
    {
        if (r->predicates[i].attno == attno &&
            r->predicates[i].op_strategy == s)
            return &r->predicates[i];
    }

    if (r->num_predicates >= MAX_PREDICATES_PER_REL)
        return NULL;

    {
        PredicateInfo *p = &r->predicates[r->num_predicates++];

        p->attno              = attno;
        p->op_strategy        = s;
        p->observation_count  = 0;
        p->avg_selectivity    = 0.0;
        strlcpy(p->attname, attname, NAMEDATALEN);
        return p;
    }
}

static void
upsert_scan_shape(RelStat *r, int n,
                  AttrNumber *attnos, OpStrategy *strats, double *sels)
{
    int i, j;

    /* search for an existing matching shape (same column set) */
    for (i = 0; i < r->num_shapes; i++)
    {
        ScanShape *s = &r->shapes[i];
        bool same;

        if (s->num_cols != n)
            continue;
        same = true;
        for (j = 0; j < n; j++)
        {
            if (s->attnos[j] != attnos[j])
            {
                same = false;
                break;
            }
        }
        if (!same)
            continue;

        /* recurrence: bump count, refresh strategies+sels (latest wins) */
        s->observation_count++;
        for (j = 0; j < n; j++)
        {
            s->strategies[j]    = strats[j];
            s->selectivities[j] = sels[j];
        }
        elog(LOG, "auto_index:   shape recurred on %s.%s (count=%ld)",
             r->nspname, r->relname, (long) s->observation_count);
        return;
    }

    if (r->num_shapes >= MAX_SHAPES_PER_REL)
        return;

    {
        ScanShape *s = &r->shapes[r->num_shapes++];

        s->num_cols           = n;
        s->observation_count  = 1;
        for (j = 0; j < n; j++)
        {
            s->attnos[j]        = attnos[j];
            s->strategies[j]    = strats[j];
            s->selectivities[j] = sels[j];
        }
        elog(LOG, "auto_index:   new shape on %s.%s with %d column(s)",
             r->nspname, r->relname, n);
    }
}


/* ================================================================
 *  Predicate harvesting from a SeqScan node
 *
 *  We deliberately do all syscache lookups (op classification,
 *  selectivity, attname) BEFORE acquiring the LWLock, so the lock
 *  is only held during the in-memory state mutation + scoring.
 * ================================================================ */

/*
 * A predicate found in the qual list, after classification.
 * Used as a small stack-allocated buffer per SeqScan.
 */
typedef struct LocalPred
{
    AttrNumber  attno;
    OpStrategy  strategy;
    double      selectivity;
    char        attname[NAMEDATALEN];
} LocalPred;

#define MAX_LOCAL_PREDS 16

/*
 * Merge two predicates on the same column when one is >=/> and the
 * other is <=/<.  Mutates preds[]/n in place.
 */
static void
collapse_range_pairs(LocalPred *preds, int *n_inout)
{
    int i, j, n = *n_inout;

    for (i = 0; i < n; i++)
    {
        bool i_lower = (preds[i].strategy == OP_GT ||
                        preds[i].strategy == OP_GE);
        bool i_upper = (preds[i].strategy == OP_LT ||
                        preds[i].strategy == OP_LE);

        if (!i_lower && !i_upper)
            continue;

        for (j = i + 1; j < n; j++)
        {
            bool j_lower = (preds[j].strategy == OP_GT ||
                            preds[j].strategy == OP_GE);
            bool j_upper = (preds[j].strategy == OP_LT ||
                            preds[j].strategy == OP_LE);

            if (preds[j].attno != preds[i].attno)
                continue;
            if (!((i_lower && j_upper) || (i_upper && j_lower)))
                continue;

            preds[i].strategy    = OP_RANGE;
            preds[i].selectivity = estimate_ineq_selectivity(OP_RANGE);

            /* drop preds[j] by shifting tail down */
            {
                int k;
                for (k = j; k < n - 1; k++)
                    preds[k] = preds[k + 1];
                n--;
            }
            break;
        }
    }
    *n_inout = n;
}

/*
 * Sort predicates by attno ascending so {a,b} and {b,a} collapse to
 * the same shape later.
 */
static void
sort_preds_by_attno(LocalPred *preds, int n)
{
    int i, j;

    for (i = 1; i < n; i++)
    {
        LocalPred tmp = preds[i];
        j = i;
        while (j > 0 && preds[j - 1].attno > tmp.attno)
        {
            preds[j] = preds[j - 1];
            j--;
        }
        preds[j] = tmp;
    }
}

/*
 * Attempt to extract a single (attno, opno) predicate from one node.
 * Returns true and fills *out_attno / *out_opno on success.
 */
static bool
extract_simple_predicate(Node *clause, AttrNumber *out_attno, Oid *out_opno)
{
    if (clause == NULL)
        return false;

    if (IsA(clause, OpExpr))
    {
        OpExpr *op = (OpExpr *) clause;
        Node   *l, *r;
        Var    *var = NULL;

        if (list_length(op->args) != 2)
            return false;

        l = (Node *) linitial(op->args);
        r = (Node *) lsecond(op->args);

        if (IsA(l, Var) && (IsA(r, Const) || IsA(r, Param)))
            var = (Var *) l;
        else if (IsA(r, Var) && (IsA(l, Const) || IsA(l, Param)))
            var = (Var *) r;
        if (var == NULL)
            return false;

        *out_attno = var->varattno;
        *out_opno  = op->opno;
        return true;
    }

    if (IsA(clause, ScalarArrayOpExpr))
    {
        ScalarArrayOpExpr *sa = (ScalarArrayOpExpr *) clause;
        Node              *l;
        Var               *var = NULL;

        /*
         * Treat "col = ANY (array)" as an equality predicate.  We do
         * not try to multiply selectivity by array length; equality
         * selectivity already gives the right cost-model order.
         */
        if (!sa->useOr)
            return false;
        if (list_length(sa->args) < 1)
            return false;

        l = (Node *) linitial(sa->args);
        if (IsA(l, Var))
            var = (Var *) l;
        if (var == NULL)
            return false;

        *out_attno = var->varattno;
        *out_opno  = sa->opno;
        return true;
    }

    return false;
}

/*
 * Process one SeqScan node: harvest predicates, classify, estimate,
 * then update shared state under the lock and re-evaluate candidates.
 */
static void
process_seqscan_node(SeqScanState *seqstate)
{
    Relation     rel;
    Oid          relid;
    char        *relname;
    char        *nspname;
    BlockNumber  relpages;
    double       reltuples;
    int          avg_width;
    SeqScan     *plan;
    List        *quals;
    ListCell    *lc;

    LocalPred    preds[MAX_LOCAL_PREDS];
    int          n_preds = 0;

    rel = seqstate->ss.ss_currentRelation;
    if (rel == NULL)
        return;
    if (RelationGetNamespace(rel) == PG_CATALOG_NAMESPACE)
        return;

    relid     = RelationGetRelid(rel);
    relname   = RelationGetRelationName(rel);
    nspname   = get_namespace_name(RelationGetNamespace(rel));
    relpages  = RelationGetNumberOfBlocks(rel);
    reltuples = rel->rd_rel->reltuples;
    avg_width = (int) rel->rd_rel->relnatts * 8;  /* coarse default */

    if (reltuples <= 0) reltuples = (double) relpages * 100.0;
    if (relpages  == 0) relpages  = 1;

    plan  = (SeqScan *) seqstate->ss.ps.plan;
    quals = plan->scan.plan.qual;
    if (quals == NIL)
        return;

    /* ---- Phase 1: classify each operator we recognize ---- */
    foreach(lc, quals)
    {
        Node       *clause = (Node *) lfirst(lc);
        AttrNumber  attno  = InvalidAttrNumber;
        Oid         opno   = InvalidOid;
        OpStrategy  strat;
        char       *att;

        if (n_preds >= MAX_LOCAL_PREDS)
            break;

        if (!extract_simple_predicate(clause, &attno, &opno))
            continue;

        strat = classify_btree_operator(opno);
        if (strat == OP_NONE || strat == OP_NE)
        {
            elog(DEBUG1, "auto_index: skipping non-indexable opno=%u "
                         "on attno=%d", opno, attno);
            continue;
        }

        att = get_attname(relid, attno, true);
        if (att == NULL)
            continue;

        preds[n_preds].attno       = attno;
        preds[n_preds].strategy    = strat;
        preds[n_preds].selectivity = (strat == OP_EQ)
            ? estimate_eq_selectivity(relid, attno)
            : estimate_ineq_selectivity(strat);
        strlcpy(preds[n_preds].attname, att, NAMEDATALEN);
        n_preds++;

        elog(LOG, "auto_index: predicate on %s.%s.%s op=%s sel=%.4f",
             nspname, relname, att, opstrategy_name(strat),
             preds[n_preds - 1].selectivity);
    }

    if (n_preds == 0)
        return;

    /* Merge >=/> with <=/< on the same column into a single RANGE. */
    collapse_range_pairs(preds, &n_preds);

    /* Canonicalize: shape is identified by attnos sorted ascending. */
    sort_preds_by_attno(preds, n_preds);

    /* ---- Update shared state under the lock + re-evaluate ---- */
    LWLockAcquire(&auto_index_state->lock.lock, LW_EXCLUSIVE);
    {
        RelStat *r = find_or_create_relstat(relid, relname, nspname,
                                            relpages, reltuples, avg_width);
        if (r != NULL)
        {
            int         i;
            AttrNumber  shape_attnos[MAX_SHAPE_COLS];
            OpStrategy  shape_strats[MAX_SHAPE_COLS];
            double      shape_sels[MAX_SHAPE_COLS];
            int         n_shape = 0;

            for (i = 0; i < n_preds; i++)
            {
                PredicateInfo *p = find_or_create_predicate(r,
                                        preds[i].attno,
                                        preds[i].strategy,
                                        preds[i].attname);
                if (p == NULL)
                    continue;

                /* running mean of selectivity */
                p->avg_selectivity =
                    ((p->avg_selectivity * p->observation_count)
                     + preds[i].selectivity) / (p->observation_count + 1);
                p->observation_count++;

                if (n_shape < MAX_SHAPE_COLS)
                {
                    shape_attnos[n_shape] = preds[i].attno;
                    shape_strats[n_shape] = preds[i].strategy;
                    shape_sels[n_shape]   = preds[i].selectivity;
                    n_shape++;
                }
            }

            if (n_shape > 0)
                upsert_scan_shape(r, n_shape, shape_attnos,
                                  shape_strats, shape_sels);

            evaluate_and_propose(r);
        }
    }
    LWLockRelease(&auto_index_state->lock.lock);
}


/* ================================================================
 *  Phase 3 + 4 - candidate generation, scoring, selection
 * ================================================================ */

/*
 * Comparison for sort_candidate_columns.
 *
 * B-tree column ordering (matches the pganalyze rule):
 *   1. equality columns first
 *   2. then by selectivity ascending (most-selective first)
 */
static int
cmp_for_btree(OpStrategy sa, double sela, OpStrategy sb, double selb)
{
    bool a_eq = (sa == OP_EQ);
    bool b_eq = (sb == OP_EQ);

    if (a_eq && !b_eq) return -1;
    if (!a_eq && b_eq) return  1;

    if (sela < selb) return -1;
    if (sela > selb) return  1;
    return 0;
}

static void
sort_candidate_columns(IndexCandidate *c)
{
    int i, j;
    for (i = 1; i < c->num_cols; i++)
    {
        AttrNumber  ta = c->attnos[i];
        OpStrategy  ts = c->strategies[i];
        double      tx = c->selectivities[i];
        char        tn[NAMEDATALEN];
        memcpy(tn, c->attnames[i], NAMEDATALEN);

        j = i;
        while (j > 0 &&
               cmp_for_btree(c->strategies[j-1], c->selectivities[j-1],
                             ts, tx) > 0)
        {
            c->attnos[j]        = c->attnos[j-1];
            c->strategies[j]    = c->strategies[j-1];
            c->selectivities[j] = c->selectivities[j-1];
            memcpy(c->attnames[j], c->attnames[j-1], NAMEDATALEN);
            j--;
        }
        c->attnos[j]        = ta;
        c->strategies[j]    = ts;
        c->selectivities[j] = tx;
        memcpy(c->attnames[j], tn, NAMEDATALEN);
    }
}

/*
 * key_str is built from attnos in their FINAL (sorted-for-btree) order
 * so two candidates with identical column ordering compare equal.
 */
static void
serialize_candidate_key(IndexCandidate *c)
{
    int  i, off = 0;
    c->key_str[0] = '\0';
    for (i = 0; i < c->num_cols; i++)
    {
        int n = snprintf(c->key_str + off,
                         MAX_INDEX_KEY_STR - off,
                         (i == 0) ? "%d" : ",%d",
                         (int) c->attnos[i]);
        if (n < 0 || off + n >= MAX_INDEX_KEY_STR)
            break;
        off += n;
    }
}

static bool
candidate_already_created(RelStat *r, IndexCandidate *c)
{
    int i;
    for (i = 0; i < r->num_created; i++)
    {
        if (strcmp(r->created_keys[i], c->key_str) == 0)
            return true;
    }
    return false;
}

/*
 * Can this candidate index serve this scan shape?
 *
 * Rules:
 *   (1) every column referenced by the shape must appear in the
 *       candidate's column set, AND
 *   (2) the candidate's leading column must be one referenced by the
 *       shape (otherwise B-tree can't restrict on the leading col).
 *
 * This is a slight simplification of what the planner actually does
 * but it is correct and conservative for our cost-benefit pass.
 */
static bool
candidate_can_serve_shape(IndexCandidate *c, ScanShape *s)
{
    int i, j;
    bool leading_in_shape = false;

    for (i = 0; i < s->num_cols; i++)
    {
        bool found = false;
        for (j = 0; j < c->num_cols; j++)
        {
            if (c->attnos[j] == s->attnos[i])
            {
                found = true;
                break;
            }
        }
        if (!found)
            return false;
    }
    for (i = 0; i < s->num_cols; i++)
    {
        if (s->attnos[i] == c->attnos[0])
        {
            leading_in_shape = true;
            break;
        }
    }
    return leading_in_shape;
}

/*
 * Score one candidate against everything we have observed for the
 * relation: cost-improvement summed over all servable shapes, minus
 * maintenance cost for every recorded write, minus one-time creation.
 */
static void
score_candidate(IndexCandidate *c, RelStat *r)
{
    double  height;
    double  seq_cost;
    int     i, k;

    height   = estimate_index_height(r->cached_reltuples,
                                     r->cached_avg_width);
    seq_cost = estimate_seqscan_cost(r->cached_relpages, r->cached_reltuples);

    c->total_benefit  = 0.0;
    c->shapes_covered = 0;

    for (i = 0; i < r->num_shapes; i++)
    {
        ScanShape *s = &r->shapes[i];
        double     eff_sel;
        double     n_match;
        double     idx_cost;
        double     gain;

        if (!candidate_can_serve_shape(c, s))
            continue;

        /*
         * Effective selectivity = product of selectivities of the
         * candidate's leading-prefix columns that are actually used
         * by the shape.  This is the textbook A8 (composite index)
         * approximation under independence.
         */
        eff_sel = 1.0;
        for (k = 0; k < c->num_cols; k++)
        {
            int q;
            bool used_by_shape = false;
            double sel_for_col = 1.0;

            for (q = 0; q < s->num_cols; q++)
            {
                if (s->attnos[q] == c->attnos[k])
                {
                    used_by_shape = true;
                    sel_for_col   = s->selectivities[q];
                    break;
                }
            }
            if (!used_by_shape)
                break;          /* prefix terminates here */
            eff_sel *= sel_for_col;
        }
        if (eff_sel < 1e-9) eff_sel = 1e-9;

        n_match  = eff_sel * r->cached_reltuples;
        idx_cost = cost_index_scan(n_match, height);

        gain = (seq_cost - idx_cost) * (double) s->observation_count;
        if (gain < 0.0) gain = 0.0;

        c->total_benefit += gain;
        c->shapes_covered++;
    }

    c->maint_cost    = (double) r->write_count * cost_per_write_maint(height);
    c->creation_cost = cost_index_creation(r->cached_relpages);
    c->net_benefit   = c->total_benefit - c->maint_cost - c->creation_cost;
}

/*
 * Generate candidates for a relation.  For each shape we emit:
 *   (a) one composite candidate using ALL of the shape's columns
 *       (with btree-friendly ordering), and
 *   (b) one singleton per column in the shape.
 *
 * Duplicates (same key_str) are de-duped.
 */
static int
build_candidates(RelStat *r, IndexCandidate *out, int max_out)
{
    int      i, j, k, count = 0;

    for (i = 0; i < r->num_shapes; i++)
    {
        ScanShape *s = &r->shapes[i];

        /* (a) composite candidate using all columns in the shape */
        if (count < max_out)
        {
            IndexCandidate *c = &out[count];
            int eff_cols = (s->num_cols > MAX_SHAPE_COLS)
                            ? MAX_SHAPE_COLS : s->num_cols;

            MemSet(c, 0, sizeof(*c));
            c->num_cols = eff_cols;
            for (j = 0; j < eff_cols; j++)
            {
                PredicateInfo *p;
                c->attnos[j]        = s->attnos[j];
                c->strategies[j]    = s->strategies[j];
                c->selectivities[j] = s->selectivities[j];

                /* attempt to find a name from predicates (best effort) */
                c->attnames[j][0] = '\0';
                for (k = 0; k < r->num_predicates; k++)
                {
                    p = &r->predicates[k];
                    if (p->attno == s->attnos[j])
                    {
                        strlcpy(c->attnames[j], p->attname, NAMEDATALEN);
                        break;
                    }
                }
            }
            sort_candidate_columns(c);
            serialize_candidate_key(c);

            /* dedupe */
            {
                bool dup = false;
                int  q;
                for (q = 0; q < count; q++)
                {
                    if (strcmp(out[q].key_str, c->key_str) == 0)
                    {
                        dup = true;
                        break;
                    }
                }
                if (!dup) count++;
            }
        }

        /* (b) one singleton per column */
        for (j = 0; j < s->num_cols && count < max_out; j++)
        {
            IndexCandidate *c = &out[count];
            int             k2;

            MemSet(c, 0, sizeof(*c));
            c->num_cols           = 1;
            c->attnos[0]          = s->attnos[j];
            c->strategies[0]      = s->strategies[j];
            c->selectivities[0]   = s->selectivities[j];
            c->attnames[0][0]     = '\0';
            for (k2 = 0; k2 < r->num_predicates; k2++)
            {
                if (r->predicates[k2].attno == s->attnos[j])
                {
                    strlcpy(c->attnames[0],
                            r->predicates[k2].attname, NAMEDATALEN);
                    break;
                }
            }
            serialize_candidate_key(c);

            {
                bool dup = false;
                int  q;
                for (q = 0; q < count; q++)
                {
                    if (strcmp(out[q].key_str, c->key_str) == 0)
                    {
                        dup = true;
                        break;
                    }
                }
                if (!dup) count++;
            }
        }
    }

    return count;
}

/*
 * Build candidates, score them all, pick the best uncreated one with
 * positive net benefit, and stage it for the bgworker.
 */
static void
evaluate_and_propose(RelStat *r)
{
    IndexCandidate cands[MAX_LOCAL_CANDIDATES];
    int            n, i;
    int            best_idx = -1;
    double         best_net = 0.0;

    if (r->has_pending_request)
    {
        /* don't pile up work for the bgworker */
        return;
    }

    n = build_candidates(r, cands, MAX_LOCAL_CANDIDATES);
    if (n == 0)
        return;

    elog(LOG, "auto_index: evaluating %d candidate(s) for %s.%s "
              "(shapes=%d, writes=%ld, relpages=%u, reltuples=%.0f)",
         n, r->nspname, r->relname,
         r->num_shapes, (long) r->write_count,
         r->cached_relpages, r->cached_reltuples);

    for (i = 0; i < n; i++)
    {
        IndexCandidate *c = &cands[i];

        if (candidate_already_created(r, c))
        {
            elog(DEBUG1, "auto_index:   cand[%s] already created, skip",
                 c->key_str);
            continue;
        }

        score_candidate(c, r);

        elog(LOG, "auto_index:   cand[%s] cols=%d covers=%d "
                  "benefit=%.2f maint=%.2f creation=%.2f net=%.2f",
             c->key_str, c->num_cols, c->shapes_covered,
             c->total_benefit, c->maint_cost,
             c->creation_cost, c->net_benefit);

        if (c->net_benefit > best_net)
        {
            best_net = c->net_benefit;
            best_idx = i;
        }
    }

    if (best_idx < 0)
    {
        elog(LOG, "auto_index:   no candidate with positive net benefit");
        return;
    }

    /* stage the chosen candidate for the bgworker */
    {
        IndexCandidate *c = &cands[best_idx];
        char            cols_csv[512];
        int             off = 0;
        int             k;

        cols_csv[0] = '\0';
        for (k = 0; k < c->num_cols; k++)
        {
            const char *name = c->attnames[k][0]
                              ? c->attnames[k] : "?";
            int          w   = snprintf(cols_csv + off,
                                        sizeof(cols_csv) - off,
                                        (k == 0) ? "\"%s\"" : ",\"%s\"",
                                        name);
            if (w < 0 || off + w >= (int) sizeof(cols_csv))
                break;
            off += w;
        }

        snprintf(r->pending_sql, sizeof(r->pending_sql),
                 "CREATE INDEX IF NOT EXISTS auto_idx_%u_%s "
                 "ON %s.%s (%s)",
                 r->relid, c->key_str,
                 r->nspname, r->relname, cols_csv);

        /* sanitize key_str for the index name (commas -> underscores) */
        {
            char *p;
            for (p = r->pending_sql; *p; p++)
                if (*p == ',') *p = '_';
        }

        strlcpy(r->pending_key, c->key_str, MAX_INDEX_KEY_STR);
        r->has_pending_request = true;

        elog(LOG, "auto_index: PROPOSING %s (net_benefit=%.2f)",
             r->pending_sql, c->net_benefit);
    }
}


/* ================================================================
 *  Executor hook
 * ================================================================ */

static void
bump_writes_for_modify(QueryDesc *queryDesc)
{
    PlanState *ps = queryDesc->planstate;

    if (ps == NULL || !IsA(ps, ModifyTableState))
        return;

    {
        ModifyTableState *mts = (ModifyTableState *) ps;
        ResultRelInfo    *rri = mts->resultRelInfo;
        Oid               relid;
        int               i;

        if (rri == NULL || rri->ri_RelationDesc == NULL)
            return;

        relid = RelationGetRelid(rri->ri_RelationDesc);

        LWLockAcquire(&auto_index_state->lock.lock, LW_EXCLUSIVE);
        for (i = 0; i < auto_index_state->num_entries; i++)
        {
            RelStat *r = &auto_index_state->entries[i];
            if (r->relid == relid)
            {
                r->write_count++;
                elog(DEBUG1, "auto_index: write counted for %s.%s "
                             "(total=%ld)",
                     r->nspname, r->relname, (long) r->write_count);
                break;
            }
        }
        LWLockRelease(&auto_index_state->lock.lock);
    }
}

static void
walk_plan_tree(PlanState *planstate)
{
    if (planstate == NULL)
        return;

    if (IsA(planstate, SeqScanState))
        process_seqscan_node((SeqScanState *) planstate);

    walk_plan_tree(planstate->lefttree);
    walk_plan_tree(planstate->righttree);
}

static void
auto_index_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
                       uint64 count)
{
    if (queryDesc->operation == CMD_INSERT ||
        queryDesc->operation == CMD_UPDATE ||
        queryDesc->operation == CMD_DELETE)
    {
        bump_writes_for_modify(queryDesc);
    }

    if (prev_ExecutorRun)
        prev_ExecutorRun(queryDesc, direction, count);
    else
        standard_ExecutorRun(queryDesc, direction, count);

    if (queryDesc->planstate != NULL)
        walk_plan_tree(queryDesc->planstate);
}


/* ================================================================
 *  Background worker - executes pending CREATE INDEX requests
 * ================================================================ */

static void
auto_index_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

/*
 * Run the pending CREATE INDEX for one relation.  Called with NO lock
 * held; we re-acquire the lock briefly to swap state in/out.
 */
static void
maybe_create_pending_index(RelStat *entry)
{
    char  sql_copy[MAX_CREATE_SQL_LEN];
    char  key_copy[MAX_INDEX_KEY_STR];
    bool  has_request;

    LWLockAcquire(&auto_index_state->lock.lock, LW_SHARED);
    has_request = entry->has_pending_request;
    if (has_request)
    {
        strlcpy(sql_copy, entry->pending_sql, sizeof(sql_copy));
        strlcpy(key_copy, entry->pending_key, sizeof(key_copy));
    }
    LWLockRelease(&auto_index_state->lock.lock);

    if (!has_request)
        return;

    PG_TRY();
    {
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());

        elog(LOG, "auto_index: bgworker executing: %s", sql_copy);

        if (SPI_execute(sql_copy, false, 0) == SPI_OK_UTILITY)
        {
            elog(LOG, "auto_index: bgworker created index (key=%s)",
                 key_copy);

            LWLockAcquire(&auto_index_state->lock.lock, LW_EXCLUSIVE);
            if (entry->num_created < MAX_CANDIDATES_PER_REL)
            {
                strlcpy(entry->created_keys[entry->num_created],
                        key_copy, MAX_INDEX_KEY_STR);
                entry->num_created++;
            }
            entry->has_pending_request = false;
            entry->pending_sql[0]      = '\0';
            entry->pending_key[0]      = '\0';
            LWLockRelease(&auto_index_state->lock.lock);
        }

        PopActiveSnapshot();
        SPI_finish();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        EmitErrorReport();
        FlushErrorState();
        AbortCurrentTransaction();

        elog(WARNING, "auto_index: bgworker CREATE INDEX failed for "
                      "%s.%s (key=%s)",
             entry->nspname, entry->relname, key_copy);

        LWLockAcquire(&auto_index_state->lock.lock, LW_EXCLUSIVE);
        entry->has_pending_request = false;
        entry->pending_sql[0]      = '\0';
        entry->pending_key[0]      = '\0';
        LWLockRelease(&auto_index_state->lock.lock);
    }
    PG_END_TRY();
}

void
auto_index_worker_main(Datum main_arg)
{
    pqsignal(SIGTERM, auto_index_sigterm);
    BackgroundWorkerUnblockSignals();

    /*
     * Connect to the "postgres" database as the bootstrap user.
     * Adjust here if you need a different default DB / role.
     */
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    elog(LOG, "auto_index: background worker started");

    while (!got_sigterm)
    {
        int i;
        int snapshot_count = 0;
        int snapshot_indexes[MAX_TRACKED_ENTRIES];

        WaitLatch(MyLatch,
                  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                  5000L,
                  0);
        ResetLatch(MyLatch);
        if (got_sigterm)
            break;

        if (auto_index_state == NULL)
            continue;

        /* take a snapshot of which entries have pending work */
        LWLockAcquire(&auto_index_state->lock.lock, LW_SHARED);
        for (i = 0; i < auto_index_state->num_entries; i++)
        {
            if (auto_index_state->entries[i].has_pending_request)
                snapshot_indexes[snapshot_count++] = i;
        }
        LWLockRelease(&auto_index_state->lock.lock);

        for (i = 0; i < snapshot_count; i++)
        {
            int idx = snapshot_indexes[i];
            maybe_create_pending_index(&auto_index_state->entries[idx]);
        }
    }

    elog(LOG, "auto_index: background worker shutting down");
    proc_exit(0);
}
