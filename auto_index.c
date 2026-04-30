/* ================================================================
 * auto_index.c - v3
 *
 * Automatic index creation extension for PostgreSQL.
 *
 * Sections / phases:
 *
 *   Phase 0  Per-relation tracking (predicates, shapes, existing
 *            indexes, created indexes)
 *   Phase 1  Generalized operator detection (=, <, <=, >, >=, IN,
 *            range pairs) via pg_amop.  Var-on-right correctly
 *            flips the strategy.  Var op Var accepted when one
 *            Var is on the scan's relation (join case).
 *   Phase 2  Selectivity from pg_statistic histograms / MCV / min-
 *            max (chapter 16.39 / 16.41).  Cost model uses the
 *            planner GUCs (seq_page_cost / random_page_cost / ...).
 *   Phase 3  Multi-column candidates with B-tree column ordering.
 *            Candidate-vs-shape match uses prefix-length, not full
 *            shape inclusion -- so an index on (a,b) correctly
 *            serves a shape {a, b, c} (with c becoming a residual
 *            filter) and is correctly truncated by a non-equality
 *            predicate on a leading column.
 *   Phase 4  Cross-shape benefit accounting.  Each candidate is
 *            scored against every recorded shape on the relation
 *            AND against every existing real index on the relation
 *            (read from pg_index): benefit is counted only when
 *            the candidate beats the best existing option.  Write
 *            cost uses the pganalyze "Index Write Overhead" ratio
 *            applied as a surcharge to one-time creation cost.
 *
 * Logging:
 *   - elog(LOG, ...) for every meaningful decision.  Lines start
 *     with "auto_index:" so they are easy to grep for.
 *   - elog(DEBUG1, ...) for fine-grained tracing.
 * ================================================================ */

#include "postgres.h"

#include <math.h>

#include "auto_index.h"

#include "access/attnum.h"
#include "access/htup_details.h"
#include "access/relation.h"
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
#include "utils/relcache.h"
#include "utils/selfuncs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
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

static void  auto_index_shmem_request_hook(void);
static void  auto_index_shmem_startup_hook(void);
static void  auto_index_shmem_request(void);
static void  auto_index_shmem_startup(void);
static Size  auto_index_shmem_size(void);

static void  auto_index_ExecutorRun(QueryDesc *queryDesc,
                                    ScanDirection direction,
                                    uint64 count);
static void  walk_plan_tree(PlanState *planstate);
static void  process_seqscan_node(SeqScanState *seqstate);
static void  bump_writes_for_modify(QueryDesc *queryDesc);

static OpStrategy classify_btree_operator(Oid opno);
static OpStrategy flip_strategy(OpStrategy s);
static const char *opstrategy_name(OpStrategy s);

/* selectivity */
static int    get_column_avg_width(Oid relid, AttrNumber attno);
static int    compute_relation_row_size(Relation rel);
static double get_n_distinct(Form_pg_statistic stats, double ntuples);
static double selectivity_from_pg_stats(Oid relid, AttrNumber attno,
                                        OpStrategy s,
                                        Datum value, bool has_value,
                                        Oid value_type, Oid collation);

/* cost */
static double estimate_index_height(double reltuples, int avg_width);
static double estimate_seqscan_cost(BlockNumber relpages, double reltuples);
static double cost_index_scan(double n_matching, double height);
static double cost_index_creation(BlockNumber relpages);

/* shared-state helpers */
static RelStat       *find_or_create_relstat(Oid relid,
                                             const char *relname,
                                             const char *nspname,
                                             BlockNumber relpages,
                                             double reltuples,
                                             int avg_width,
                                             int row_size);
static PredicateInfo *find_or_create_predicate(RelStat *r,
                                               AttrNumber attno,
                                               OpStrategy s,
                                               int attwidth,
                                               const char *attname);
static void           upsert_scan_shape(RelStat *r,
                                        int n,
                                        AttrNumber *attnos,
                                        OpStrategy *strats,
                                        double *sels);

/* existing indexes */
static int    enumerate_existing_indexes(Relation rel,
                                         ExistingIndexInfo *out, int max_out);

/* candidate generation, scoring, selection */
typedef struct IndexCandidate
{
    int         num_cols;
    AttrNumber  attnos[MAX_SHAPE_COLS];
    OpStrategy  strategies[MAX_SHAPE_COLS];
    double      selectivities[MAX_SHAPE_COLS];
    int         attwidths[MAX_SHAPE_COLS];      /* per-col avg byte width */
    char        attnames[MAX_SHAPE_COLS][NAMEDATALEN];
    char        key_str[MAX_INDEX_KEY_STR];

    double      total_benefit;
    double      creation_cost;
    double      per_write_cost;        /* was write_overhead   */
    double      total_cost;            /* creation + per_write × write_count */
    double      net_benefit;
    int         shapes_covered;
} IndexCandidate;

#define MAX_LOCAL_CANDIDATES 64

static int  build_candidates(RelStat *r,
                             IndexCandidate *out, int max_out);
static void sort_candidate_columns(IndexCandidate *c);
static void serialize_candidate_key(IndexCandidate *c);
static bool candidate_already_exists(RelStat *r, IndexCandidate *c);
static int  prefix_length_for_shape(AttrNumber *idx_attnos,
                                    int idx_ncols,
                                    ScanShape *s,
                                    double *out_eff_sel);
static double cost_for_index_on_shape(AttrNumber *attnos, int ncols,
                                      RelStat *r, ScanShape *s,
                                      double height);
static double compute_per_write_cost(IndexCandidate *c, RelStat *r);
static void score_candidate(IndexCandidate *c, RelStat *r);
static void evaluate_and_propose(RelStat *r);

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
                  "MAX_SHAPES_PER_REL=%d, MAX_EXISTING_INDEXES_PER_REL=%d, "
                  "total_size=%zu bytes)",
             MAX_TRACKED_ENTRIES, MAX_PREDICATES_PER_REL,
             MAX_SHAPES_PER_REL, MAX_EXISTING_INDEXES_PER_REL,
             auto_index_shmem_size());
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

/*
 * If a predicate looks like "Const op Var" instead of "Var op Const",
 * the operator's semantics are reversed: 5 < x is x > 5.
 */
static OpStrategy
flip_strategy(OpStrategy s)
{
    switch (s)
    {
        case OP_LT: return OP_GT;
        case OP_LE: return OP_GE;
        case OP_GT: return OP_LT;
        case OP_GE: return OP_LE;
        default:    return s;       /* EQ, NE, RANGE, NONE: unchanged */
    }
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
 *  Phase 2 - selectivity & column-width helpers
 * ================================================================ */

/*
 * Average byte width for one column.  Falls back to typlen for fixed
 * types or 16 for variable-length when no pg_statistic row exists yet.
 */
static int
get_column_avg_width(Oid relid, AttrNumber attno)
{
    HeapTuple   stp;
    int         width = 0;

    stp = SearchSysCache3(STATRELATTINH,
                          ObjectIdGetDatum(relid),
                          Int16GetDatum(attno),
                          BoolGetDatum(false));
    if (HeapTupleIsValid(stp))
    {
        Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(stp);
        width = stats->stawidth;
        ReleaseSysCache(stp);
    }

    if (width <= 0)
    {
        Oid     atttypid;
        HeapTuple atp;

        atp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid),
                              Int16GetDatum(attno));
        if (HeapTupleIsValid(atp))
        {
            Form_pg_attribute att = (Form_pg_attribute) GETSTRUCT(atp);
            atttypid = att->atttypid;
            ReleaseSysCache(atp);

            {
                int16   typlen;
                bool    typbyval;
                get_typlenbyval(atttypid, &typlen, &typbyval);
                if (typlen > 0) width = typlen;
                else            width = 16;     /* generic guess */
            }
        }
        else
        {
            width = 16;
        }
    }
    return width;
}

/*
 * Per pganalyze's "Index Write Overhead" doc:
 *      row size = 23 (tuple header) + 4 (item ptr)
 *               + Sum(avg_width of all (non-dropped) columns)
 */
static int
compute_relation_row_size(Relation rel)
{
    TupleDesc   tupdesc = RelationGetDescr(rel);
    Oid         relid   = RelationGetRelid(rel);
    int         total   = 23 + 4;
    int         i;

    for (i = 0; i < tupdesc->natts; i++)
    {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);
        if (att->attisdropped)
            continue;
        total += get_column_avg_width(relid, att->attnum);
    }
    return total;
}

/*
 * Resolve pg_statistic.stadistinct to an absolute distinct-count.
 *      > 0 : absolute
 *      < 0 : fraction of ntuples (so n_distinct = -stadistinct * ntuples)
 *      = 0 : unknown -> caller must fall back
 */
static double
get_n_distinct(Form_pg_statistic stats, double ntuples)
{
    if (stats->stadistinct > 0.0)
        return stats->stadistinct;
    if (stats->stadistinct < 0.0 && ntuples > 0)
        return -stats->stadistinct * ntuples;
    return -1.0;
}

/*
 * Selectivity for one (column, op, value) triple, derived from
 * pg_statistic per chapter 16.39 / 16.41.
 *
 * Strategy:
 *   1. Equality + value present:
 *        a. Try MCV list (STATISTIC_KIND_MCV) - exact match wins.
 *        b. Otherwise (1 - sum_mcv) / (n_distinct - n_mcv).
 *   2. Equality without value:
 *        Per-column average  =  1 / n_distinct.
 *   3. Inequality (LT/LE/GT/GE) + value present:
 *        Histogram (STATISTIC_KIND_HISTOGRAM) bucket interpolation
 *        per slide 16.41.  No subtraction needed: bucket midpoint.
 *   4. Inequality without value:
 *        Planner default DEFAULT_INEQ_SEL.
 *   5. RANGE:  DEFAULT_RANGE_INEQ_SEL  (slide 16.42 approximation).
 *   6. Anything else, or no stats:  hard-coded planner defaults.
 *
 * Returns a value in (0, 1].
 */
static double
selectivity_from_pg_stats(Oid relid, AttrNumber attno, OpStrategy s,
                          Datum value, bool has_value,
                          Oid value_type, Oid collation)
{
    HeapTuple   stp;
    Form_pg_statistic stats;
    double      sel = -1.0;
    double      ntuples = 0.0;
    const char *source = "default";

    /* sane defaults if we never reach pg_statistic */
    double      default_sel;
    if (s == OP_EQ)
        default_sel = DEFAULT_EQ_SEL;
    else if (s == OP_RANGE)
        default_sel = DEFAULT_RANGE_INEQ_SEL;
    else if (s == OP_NE)
        default_sel = 1.0 - DEFAULT_EQ_SEL;
    else
        default_sel = DEFAULT_INEQ_SEL;

    stp = SearchSysCache3(STATRELATTINH,
                          ObjectIdGetDatum(relid),
                          Int16GetDatum(attno),
                          BoolGetDatum(false));
    if (!HeapTupleIsValid(stp))
    {
        elog(DEBUG1, "auto_index: no pg_statistic row for relid=%u attno=%d "
                     "(table not ANALYZE'd?), using default sel=%.4f",
             relid, attno, default_sel);
        return default_sel;
    }
    stats = (Form_pg_statistic) GETSTRUCT(stp);

    /* fetch ntuples too -- needed for n_distinct conversion */
    {
        HeapTuple ctp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if (HeapTupleIsValid(ctp))
        {
            Form_pg_class cf = (Form_pg_class) GETSTRUCT(ctp);
            ntuples = cf->reltuples;
            ReleaseSysCache(ctp);
        }
    }

    /* ---- Equality with value: try MCV ---- */
    if (s == OP_EQ && has_value && OidIsValid(value_type))
    {
        AttStatsSlot mcv;

        if (get_attstatsslot(&mcv, stp,
                             STATISTIC_KIND_MCV, InvalidOid,
                             ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS))
        {
            TypeCacheEntry *tce = lookup_type_cache(value_type,
                                          TYPECACHE_EQ_OPR_FINFO);
            if (OidIsValid(tce->eq_opr_finfo.fn_oid))
            {
                int i;
                for (i = 0; i < mcv.nvalues && i < mcv.nnumbers; i++)
                {
                    bool eq = DatumGetBool(
                        FunctionCall2Coll(&tce->eq_opr_finfo,
                                          collation,
                                          value, mcv.values[i]));
                    if (eq)
                    {
                        sel = mcv.numbers[i];
                        source = "MCV-hit";
                        break;
                    }
                }
            }
            free_attstatsslot(&mcv);
        }
    }

    /* ---- Equality fallback: (1-sumMCV) / (nd - nMCV) ---- */
    if (s == OP_EQ && sel < 0.0)
    {
        double  nd = get_n_distinct(stats, ntuples);
        double  sum_mcv = 0.0;
        int     n_mcv   = 0;
        AttStatsSlot mcv;

        if (get_attstatsslot(&mcv, stp,
                             STATISTIC_KIND_MCV, InvalidOid,
                             ATTSTATSSLOT_NUMBERS))
        {
            int i;
            for (i = 0; i < mcv.nnumbers; i++)
                sum_mcv += mcv.numbers[i];
            n_mcv = mcv.nnumbers;
            free_attstatsslot(&mcv);
        }

        if (nd > 0)
        {
            double  remaining_distinct = nd - (double) n_mcv;
            if (remaining_distinct < 1.0) remaining_distinct = 1.0;
            sel = (1.0 - sum_mcv) / remaining_distinct;
            source = (n_mcv > 0) ? "1/(nd-mcv)" : "1/n_distinct";
        }
    }

    /* ---- Inequality with value: histogram bucket interpolation ---- */
    if ((s == OP_LT || s == OP_LE || s == OP_GT || s == OP_GE)
        && has_value && OidIsValid(value_type) && sel < 0.0)
    {
        AttStatsSlot hist;

        if (get_attstatsslot(&hist, stp,
                             STATISTIC_KIND_HISTOGRAM, InvalidOid,
                             ATTSTATSSLOT_VALUES))
        {
            if (hist.nvalues >= 2)
            {
                TypeCacheEntry *tce = lookup_type_cache(value_type,
                                            TYPECACHE_CMP_PROC_FINFO);
                if (OidIsValid(tce->cmp_proc_finfo.fn_oid))
                {
                    int n = hist.nvalues;
                    int low = 0, high = n - 1;
                    double frac_le_v = 0.5;     /* default: middle */
                    int32 cmp;

                    /* binary search: smallest i with hist[i] >= v */
                    while (low < high)
                    {
                        int mid = (low + high) / 2;
                        cmp = DatumGetInt32(
                            FunctionCall2Coll(&tce->cmp_proc_finfo,
                                              collation,
                                              hist.values[mid], value));
                        if (cmp < 0) low = mid + 1;
                        else         high = mid;
                    }
                    cmp = DatumGetInt32(
                        FunctionCall2Coll(&tce->cmp_proc_finfo, collation,
                                          hist.values[low], value));

                    if (cmp < 0)
                        frac_le_v = 1.0;                /* v > all */
                    else if (cmp == 0)
                        frac_le_v = (double) low / (double)(n - 1);
                    else if (low == 0)
                        frac_le_v = 0.0;                /* v < all */
                    else
                        frac_le_v = ((double) low - 0.5) / (double)(n - 1);

                    if (s == OP_LT || s == OP_LE)
                        sel = frac_le_v;
                    else
                        sel = 1.0 - frac_le_v;
                    source = "histogram";
                }
            }
            free_attstatsslot(&hist);
        }
    }

    ReleaseSysCache(stp);

    if (sel < 0.0)
        sel = default_sel;
    if (sel <= 0.0) sel = 1e-9;
    if (sel >  1.0) sel = 1.0;

    elog(DEBUG1, "auto_index: sel(relid=%u attno=%d op=%s)=%.5f source=%s",
         relid, attno, opstrategy_name(s), sel, source);

    return sel;
}


/* ================================================================
 *  Cost helpers (PG-units, planner GUCs)
 * ================================================================ */

static double
estimate_index_height(double reltuples, int avg_width)
{
    double  per_tuple_bytes;
    double  leaf_pages;
    double  height;
    double  fanout = 100.0;

    if (reltuples <= 0) reltuples = 1.0;
    if (avg_width <= 0) avg_width = 8;

    per_tuple_bytes = (double) avg_width + 16.0;
    leaf_pages = ceil((reltuples * per_tuple_bytes) / (double) BLCKSZ);
    if (leaf_pages < 1.0) leaf_pages = 1.0;

    height = ceil(log(leaf_pages) / log(fanout));
    if (height < 1.0) height = 1.0;
    return height;
}

static double
estimate_seqscan_cost(BlockNumber relpages, double reltuples)
{
    double pages = (relpages > 0) ? (double) relpages : 1.0;
    double rows  = (reltuples > 0) ? reltuples         : 1.0;

    return pages * seq_page_cost
         + rows  * (cpu_tuple_cost + cpu_operator_cost);
}

static double
cost_index_scan(double n_matching, double height)
{
    double rows = (n_matching < 1.0) ? 1.0 : n_matching;

    return  height * random_page_cost
          + rows   * random_page_cost
          + rows   * (cpu_index_tuple_cost + cpu_tuple_cost
                      + cpu_operator_cost);
}

static double
cost_index_creation(BlockNumber relpages)
{
    double pages = (relpages > 0) ? (double) relpages : 1.0;
    double sort_log;

    sort_log = log(pages) / log(2.0);
    if (sort_log < 1.0) sort_log = 1.0;

    return  pages * seq_page_cost
          + pages * sort_log * cpu_operator_cost
          + pages * seq_page_cost;
}


/* ================================================================
 *  Phase 0 helpers - shared state mutation
 * ================================================================ */

static RelStat *
find_or_create_relstat(Oid relid, const char *relname, const char *nspname,
                       BlockNumber relpages, double reltuples,
                       int avg_width, int row_size)
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
            r->cached_row_size   = row_size;
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
        r->cached_row_size    = row_size;
        strlcpy(r->relname, relname, NAMEDATALEN);
        strlcpy(r->nspname, nspname, NAMEDATALEN);

        elog(LOG, "auto_index: now tracking %s.%s "
                  "(relpages=%u reltuples=%.0f avg_width=%d row_size=%d)",
             nspname, relname, relpages, reltuples, avg_width, row_size);
        return r;
    }
}

static PredicateInfo *
find_or_create_predicate(RelStat *r, AttrNumber attno, OpStrategy s,
                         int attwidth, const char *attname)
{
    int i;

    for (i = 0; i < r->num_predicates; i++)
    {
        if (r->predicates[i].attno == attno &&
            r->predicates[i].op_strategy == s)
        {
            r->predicates[i].attwidth = attwidth;
            return &r->predicates[i];
        }
    }

    if (r->num_predicates >= MAX_PREDICATES_PER_REL)
        return NULL;

    {
        PredicateInfo *p = &r->predicates[r->num_predicates++];

        p->attno              = attno;
        p->op_strategy        = s;
        p->observation_count  = 0;
        p->avg_selectivity    = 0.0;
        p->attwidth           = attwidth;
        strlcpy(p->attname, attname, NAMEDATALEN);
        return p;
    }
}

static void
upsert_scan_shape(RelStat *r, int n,
                  AttrNumber *attnos, OpStrategy *strats, double *sels)
{
    int i, j;

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
 *  Existing-index enumeration
 * ================================================================ */

static int
enumerate_existing_indexes(Relation rel, ExistingIndexInfo *out, int max_out)
{
    List       *indexoidlist;
    ListCell   *lc;
    int         count = 0;

    indexoidlist = RelationGetIndexList(rel);
    foreach(lc, indexoidlist)
    {
        Oid         indexoid = lfirst_oid(lc);
        Relation    idxrel;
        Form_pg_index idx;
        int         i, n, off;

        if (count >= max_out)
            break;

        idxrel = try_relation_open(indexoid, AccessShareLock);
        if (idxrel == NULL)
            continue;                  /* index dropped concurrently */
        if (idxrel->rd_rel->relam != BTREE_AM_OID)
        {
            relation_close(idxrel, AccessShareLock);
            continue;
        }

        idx = idxrel->rd_index;
        n = idx->indnatts;
        if (n > MAX_SHAPE_COLS)
            n = MAX_SHAPE_COLS;

        out[count].num_cols = n;
        for (i = 0; i < n; i++)
            out[count].attnos[i] = idx->indkey.values[i];

        off = 0;
        out[count].key_str[0] = '\0';
        for (i = 0; i < n; i++)
        {
            int w = snprintf(out[count].key_str + off,
                             MAX_INDEX_KEY_STR - off,
                             (i == 0) ? "%d" : ",%d",
                             (int) out[count].attnos[i]);
            if (w < 0 || off + w >= MAX_INDEX_KEY_STR) break;
            off += w;
        }

        count++;
        relation_close(idxrel, AccessShareLock);
    }
    list_free(indexoidlist);
    return count;
}


/* ================================================================
 *  Phase 1 - predicate harvesting from a SeqScan
 * ================================================================ */

typedef struct LocalPred
{
    AttrNumber  attno;
    OpStrategy  strategy;
    double      selectivity;            /* filled in caller-side */
    int         attwidth;

    /* value/type carried through to the selectivity estimator */
    bool        has_value;
    Datum       value;
    Oid         value_type;
    Oid         collation;

    char        attname[NAMEDATALEN];
} LocalPred;

#define MAX_LOCAL_PREDS 16

/*
 * If we have predicates ">=/>" and "<=/<" on the same column in the
 * same scan, fuse them into a single OP_RANGE.
 */
static void
collapse_range_pairs(LocalPred *preds, int *n_inout)
{
    int i, j, n = *n_inout;

    for (i = 0; i < n; i++)
    {
        bool i_lower = (preds[i].strategy == OP_GT || preds[i].strategy == OP_GE);
        bool i_upper = (preds[i].strategy == OP_LT || preds[i].strategy == OP_LE);

        if (!i_lower && !i_upper)
            continue;

        for (j = i + 1; j < n; j++)
        {
            bool j_lower = (preds[j].strategy == OP_GT || preds[j].strategy == OP_GE);
            bool j_upper = (preds[j].strategy == OP_LT || preds[j].strategy == OP_LE);

            if (preds[j].attno != preds[i].attno)
                continue;
            if (!((i_lower && j_upper) || (i_upper && j_lower)))
                continue;

            preds[i].strategy    = OP_RANGE;
            preds[i].has_value   = false;        /* range -> default sel */
            preds[i].selectivity = DEFAULT_RANGE_INEQ_SEL;

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
 * Try to interpret one qual node as "this-relation column op literal".
 *
 * Output (filled on success):
 *   *out_attno   - column number on our relation
 *   *out_opno    - operator OID
 *   *out_var_left - true iff Var was on the LHS (so the operator's
 *                  strategy applies as-is; if false, caller should
 *                  flip_strategy())
 *   *out_value   - Datum value of the literal (if present)
 *   *out_value_type - Oid of the literal's type
 *   *out_collation  - collation OID (from the OpExpr)
 *   *out_has_value  - true if we extracted a Const value
 *
 * Identifying "this relation":
 *   - If both sides are Vars, we keep the one whose varattno resolves
 *     to a valid attribute on `relid` via get_attname.  That handles
 *     join quals where the planner left a Var (rather than a Param)
 *     for the foreign side.  If both Vars resolve, the LHS is taken
 *     as ours.
 */
static bool
extract_simple_predicate(Node *clause, Oid relid,
                         AttrNumber *out_attno, Oid *out_opno,
                         bool *out_var_left,
                         Datum *out_value, Oid *out_value_type,
                         Oid *out_collation, bool *out_has_value)
{
    if (clause == NULL)
        return false;

    if (IsA(clause, OpExpr))
    {
        OpExpr *op = (OpExpr *) clause;
        Node   *l, *r;

        if (list_length(op->args) != 2)
            return false;

        l = (Node *) linitial(op->args);
        r = (Node *) lsecond(op->args);

        /* Var op Const  /  Var op Param */
        if (IsA(l, Var) && (IsA(r, Const) || IsA(r, Param)))
        {
            Var *v = (Var *) l;
            *out_attno     = v->varattno;
            *out_opno      = op->opno;
            *out_var_left  = true;
            *out_collation = op->inputcollid;
            *out_has_value = false;
            *out_value     = (Datum) 0;
            *out_value_type = InvalidOid;
            if (IsA(r, Const))
            {
                Const *c = (Const *) r;
                if (!c->constisnull)
                {
                    *out_value      = c->constvalue;
                    *out_value_type = c->consttype;
                    *out_has_value  = true;
                }
            }
            return true;
        }
        if (IsA(r, Var) && (IsA(l, Const) || IsA(l, Param)))
        {
            Var *v = (Var *) r;
            *out_attno     = v->varattno;
            *out_opno      = op->opno;
            *out_var_left  = false;          /* caller must flip strategy */
            *out_collation = op->inputcollid;
            *out_has_value = false;
            *out_value     = (Datum) 0;
            *out_value_type = InvalidOid;
            if (IsA(l, Const))
            {
                Const *c = (Const *) l;
                if (!c->constisnull)
                {
                    *out_value      = c->constvalue;
                    *out_value_type = c->consttype;
                    *out_has_value  = true;
                }
            }
            return true;
        }

        /* Var op Var (join case): pick the one that belongs to relid. */
        if (IsA(l, Var) && IsA(r, Var))
        {
            Var *vl = (Var *) l;
            Var *vr = (Var *) r;
            char *l_name = (vl->varattno > 0)
                ? get_attname(relid, vl->varattno, true) : NULL;
            char *r_name = (vr->varattno > 0)
                ? get_attname(relid, vr->varattno, true) : NULL;

            if (l_name != NULL)
            {
                *out_attno     = vl->varattno;
                *out_opno      = op->opno;
                *out_var_left  = true;
                *out_collation = op->inputcollid;
                *out_has_value = false;       /* foreign Var -> no value */
                *out_value     = (Datum) 0;
                *out_value_type = InvalidOid;
                return true;
            }
            if (r_name != NULL)
            {
                *out_attno     = vr->varattno;
                *out_opno      = op->opno;
                *out_var_left  = false;
                *out_collation = op->inputcollid;
                *out_has_value = false;
                *out_value     = (Datum) 0;
                *out_value_type = InvalidOid;
                return true;
            }
            return false;
        }
        return false;
    }

    if (IsA(clause, ScalarArrayOpExpr))
    {
        ScalarArrayOpExpr *sa = (ScalarArrayOpExpr *) clause;
        Node              *l;

        if (!sa->useOr)
            return false;
        if (list_length(sa->args) < 1)
            return false;

        l = (Node *) linitial(sa->args);
        if (!IsA(l, Var))
            return false;

        {
            Var *v = (Var *) l;
            *out_attno     = v->varattno;
            *out_opno      = sa->opno;
            *out_var_left  = true;
            *out_collation = sa->inputcollid;
            *out_has_value = false;        /* array of values: no single value */
            *out_value     = (Datum) 0;
            *out_value_type = InvalidOid;
        }
        return true;
    }

    return false;
}

static void
process_seqscan_node(SeqScanState *seqstate)
{
    Relation     rel;
    Oid          relid;
    char        *relname;
    char        *nspname;
    BlockNumber  relpages;
    double       reltuples;
    int          row_size;
    int          avg_width;
    SeqScan     *plan;
    List        *quals;
    ListCell    *lc;

    LocalPred    preds[MAX_LOCAL_PREDS];
    int          n_preds = 0;

    ExistingIndexInfo  local_existing[MAX_EXISTING_INDEXES_PER_REL];
    int                num_existing = 0;

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
    row_size  = compute_relation_row_size(rel);
    avg_width = (row_size > 27)
        ? (row_size - 27) / (RelationGetNumberOfAttributes(rel) > 0
                              ? RelationGetNumberOfAttributes(rel) : 1)
        : 8;

    if (reltuples <= 0) reltuples = (double) relpages * 100.0;
    if (relpages  == 0) relpages  = 1;

    plan  = (SeqScan *) seqstate->ss.ps.plan;
    quals = plan->scan.plan.qual;
    if (quals == NIL)
        return;

    /* enumerate existing indexes BEFORE acquiring our LWLock */
    num_existing = enumerate_existing_indexes(rel, local_existing,
                                              MAX_EXISTING_INDEXES_PER_REL);

    /* ---- Classify each operator we can use ---- */
    foreach(lc, quals)
    {
        Node       *clause = (Node *) lfirst(lc);
        AttrNumber  attno = InvalidAttrNumber;
        Oid         opno  = InvalidOid;
        bool        var_left = true;
        Datum       value = (Datum) 0;
        Oid         value_type = InvalidOid;
        Oid         collation  = InvalidOid;
        bool        has_value  = false;
        OpStrategy  strat;
        char       *att;

        if (n_preds >= MAX_LOCAL_PREDS)
            break;

        if (!extract_simple_predicate(clause, relid,
                                      &attno, &opno, &var_left,
                                      &value, &value_type,
                                      &collation, &has_value))
            continue;

        strat = classify_btree_operator(opno);
        if (strat == OP_NONE || strat == OP_NE)
            continue;
        if (!var_left)
            strat = flip_strategy(strat);    /* "5 < x" -> "x > 5" */

        att = get_attname(relid, attno, true);
        if (att == NULL)
            continue;

        preds[n_preds].attno       = attno;
        preds[n_preds].strategy    = strat;
        preds[n_preds].has_value   = has_value;
        preds[n_preds].value       = value;
        preds[n_preds].value_type  = value_type;
        preds[n_preds].collation   = collation;
        preds[n_preds].attwidth    = get_column_avg_width(relid, attno);
        strlcpy(preds[n_preds].attname, att, NAMEDATALEN);

        preds[n_preds].selectivity = selectivity_from_pg_stats(
            relid, attno, strat,
            value, has_value, value_type, collation);

        elog(LOG, "auto_index: predicate on %s.%s.%s op=%s sel=%.5f "
                  "(value=%s)",
             nspname, relname, att, opstrategy_name(strat),
             preds[n_preds].selectivity,
             has_value ? "literal" : "param/none");
        n_preds++;
    }

    if (n_preds == 0)
        return;

    collapse_range_pairs(preds, &n_preds);
    sort_preds_by_attno(preds, n_preds);

    /* ---- Update shared state under the lock ---- */
    LWLockAcquire(&auto_index_state->lock.lock, LW_EXCLUSIVE);
    {
        RelStat *r = find_or_create_relstat(relid, relname, nspname,
                                            relpages, reltuples,
                                            avg_width, row_size);
        if (r != NULL)
        {
            int         i;
            AttrNumber  shape_attnos[MAX_SHAPE_COLS];
            OpStrategy  shape_strats[MAX_SHAPE_COLS];
            double      shape_sels[MAX_SHAPE_COLS];
            int         n_shape = 0;

            /* refresh existing-index list every observation */
            r->num_existing_indexes = (num_existing > MAX_EXISTING_INDEXES_PER_REL)
                                    ? MAX_EXISTING_INDEXES_PER_REL
                                    : num_existing;
            for (i = 0; i < r->num_existing_indexes; i++)
                r->existing_indexes[i] = local_existing[i];

            for (i = 0; i < n_preds; i++)
            {
                PredicateInfo *p = find_or_create_predicate(r,
                                        preds[i].attno,
                                        preds[i].strategy,
                                        preds[i].attwidth,
                                        preds[i].attname);
                if (p == NULL)
                    continue;

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
 * Walk the index columns left-to-right.  For each:
 *   - if column not in shape:               stop, return prefix length
 *   - if column in shape with equality:     contribute selectivity, continue
 *   - if column in shape with non-equality: contribute selectivity, stop
 *
 * Returns the number of leading index columns usable for restriction.
 * out_eff_sel is the product of those columns' selectivities (1.0 if 0).
 */
static int
prefix_length_for_shape(AttrNumber *idx_attnos, int idx_ncols,
                        ScanShape *s, double *out_eff_sel)
{
    double  eff_sel = 1.0;
    int     prefix_len = 0;
    int     i, j;

    for (i = 0; i < idx_ncols; i++)
    {
        AttrNumber  col   = idx_attnos[i];
        int         s_idx = -1;

        for (j = 0; j < s->num_cols; j++)
        {
            if (s->attnos[j] == col)
            {
                s_idx = j;
                break;
            }
        }
        if (s_idx < 0)
            break;

        eff_sel *= s->selectivities[s_idx];
        prefix_len++;

        if (s->strategies[s_idx] != OP_EQ)
            break;
    }

    if (eff_sel < 1e-9) eff_sel = 1e-9;
    if (out_eff_sel) *out_eff_sel = eff_sel;
    return prefix_len;
}

/*
 * Cost of using a given index (described by its column list) to
 * answer a given scan shape on relation r.  Returns seq-scan cost
 * if the index can't be used.
 */
static double
cost_for_index_on_shape(AttrNumber *attnos, int ncols, RelStat *r,
                        ScanShape *s, double height)
{
    double  eff_sel;
    int     prefix_len = prefix_length_for_shape(attnos, ncols, s, &eff_sel);
    double  n_match;

    if (prefix_len == 0)
        return estimate_seqscan_cost(r->cached_relpages, r->cached_reltuples);

    n_match = eff_sel * r->cached_reltuples;
    return cost_index_scan(n_match, height);
}

/*
 * Per-DML maintenance cost in PG cost units, applied once per write.
 *
 * Reasoning:
 *   - cpu_index_tuple_cost: traversing/inserting the new index entry.
 *   - (entry_size / BLCKSZ) × random_page_cost: amortized leaf-page
 *     touch.  Wider entries fill leaves faster -> more splits -> more
 *     random I/O per write.
 *   - Scaled by the pganalyze ratio so wider indexed columns relative
 *     to the base row cost more (matches the "% extra rows touched"
 *     intuition).
 */
static double
compute_per_write_cost(IndexCandidate *c, RelStat *r)
{
    int     entry_size = 8;       /* btree item header */
    int     i;
    int     row_size = r->cached_row_size;
    double  ratio;
    double  base_per_write;

    for (i = 0; i < c->num_cols; i++)
        entry_size += (c->attwidths[i] > 0 ? c->attwidths[i] : 8);

    if (row_size <= 0) row_size = 100;
    ratio = (double) entry_size / (double) row_size;

    base_per_write =
          cpu_index_tuple_cost
        + ((double) entry_size / (double) BLCKSZ) * random_page_cost;

    return ratio * base_per_write;
}


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
        int         tw = c->attwidths[i];
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
            c->attwidths[j]     = c->attwidths[j-1];
            memcpy(c->attnames[j], c->attnames[j-1], NAMEDATALEN);
            j--;
        }
        c->attnos[j]        = ta;
        c->strategies[j]    = ts;
        c->selectivities[j] = tx;
        c->attwidths[j]     = tw;
        memcpy(c->attnames[j], tn, NAMEDATALEN);
    }
}

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

/*
 * True if this candidate is a duplicate of an existing real index,
 * an already-created auto_index index, or already pending creation.
 */
static bool
candidate_already_exists(RelStat *r, IndexCandidate *c)
{
    int i;
    for (i = 0; i < r->num_created; i++)
        if (strcmp(r->created_keys[i], c->key_str) == 0)
            return true;
    for (i = 0; i < r->num_existing_indexes; i++)
        if (strcmp(r->existing_indexes[i].key_str, c->key_str) == 0)
            return true;
    if (r->has_pending_request &&
        strcmp(r->pending_key, c->key_str) == 0)
        return true;
    return false;
}

/*
 * Score a candidate against every recorded shape.  For each shape:
 *   best_existing_cost = min(seq_cost, min over existing indexes)
 *   if candidate beats best_existing_cost, accumulate the gain.
 * Then compute creation surcharge from pganalyze write_overhead.
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
        ScanShape  *s = &r->shapes[i];
        double      cand_cost;
        double      best_existing_cost;
        double      gain;

        cand_cost = cost_for_index_on_shape(c->attnos, c->num_cols, r, s, height);
        if (cand_cost >= seq_cost)
            continue;                  /* candidate doesn't help this shape */

        /* what does the relation already offer for this shape? */
        best_existing_cost = seq_cost;
        for (k = 0; k < r->num_existing_indexes; k++)
        {
            ExistingIndexInfo *e = &r->existing_indexes[k];
            double ec = cost_for_index_on_shape(e->attnos, e->num_cols,
                                                r, s, height);
            if (ec < best_existing_cost)
                best_existing_cost = ec;
        }

        if (cand_cost >= best_existing_cost)
            continue;                  /* an existing index already beats us */

        gain  = (best_existing_cost - cand_cost) * (double) s->observation_count;
        if (gain < 0.0) gain = 0.0;
        c->total_benefit += gain;
        c->shapes_covered++;
    }

    c->creation_cost   = cost_index_creation(r->cached_relpages);
    c->per_write_cost  = compute_per_write_cost(c, r);
    c->total_cost      = c->creation_cost + c->per_write_cost * (double) r->write_count;
    c->net_benefit     = c->total_benefit - c->total_cost;
}

static int
build_candidates(RelStat *r, IndexCandidate *out, int max_out)
{
    int     i, j, k, count = 0;

    for (i = 0; i < r->num_shapes; i++)
    {
        ScanShape *s = &r->shapes[i];

        /* (a) one composite using all shape columns */
        if (count < max_out)
        {
            IndexCandidate *c = &out[count];
            int eff_cols = (s->num_cols > MAX_SHAPE_COLS)
                            ? MAX_SHAPE_COLS : s->num_cols;

            MemSet(c, 0, sizeof(*c));
            c->num_cols = eff_cols;
            for (j = 0; j < eff_cols; j++)
            {
                c->attnos[j]        = s->attnos[j];
                c->strategies[j]    = s->strategies[j];
                c->selectivities[j] = s->selectivities[j];
                c->attwidths[j]     = 0;        /* fill below */
                c->attnames[j][0]   = '\0';

                for (k = 0; k < r->num_predicates; k++)
                {
                    PredicateInfo *p = &r->predicates[k];
                    if (p->attno == s->attnos[j])
                    {
                        strlcpy(c->attnames[j], p->attname, NAMEDATALEN);
                        c->attwidths[j] = p->attwidth;
                        break;
                    }
                }
                if (c->attwidths[j] <= 0)
                    c->attwidths[j] = get_column_avg_width(r->relid,
                                                           c->attnos[j]);
            }
            sort_candidate_columns(c);
            serialize_candidate_key(c);

            {
                bool dup = false;
                int  q;
                for (q = 0; q < count; q++)
                    if (strcmp(out[q].key_str, c->key_str) == 0)
                    {
                        dup = true;
                        break;
                    }
                if (!dup) count++;
            }
        }

        /* (b) one singleton per column in the shape */
        for (j = 0; j < s->num_cols && count < max_out; j++)
        {
            IndexCandidate *c = &out[count];

            MemSet(c, 0, sizeof(*c));
            c->num_cols           = 1;
            c->attnos[0]          = s->attnos[j];
            c->strategies[0]      = s->strategies[j];
            c->selectivities[0]   = s->selectivities[j];
            c->attwidths[0]       = 0;
            c->attnames[0][0]     = '\0';
            for (k = 0; k < r->num_predicates; k++)
            {
                PredicateInfo *p = &r->predicates[k];
                if (p->attno == s->attnos[j])
                {
                    strlcpy(c->attnames[0], p->attname, NAMEDATALEN);
                    c->attwidths[0] = p->attwidth;
                    break;
                }
            }
            if (c->attwidths[0] <= 0)
                c->attwidths[0] = get_column_avg_width(r->relid, c->attnos[0]);
            serialize_candidate_key(c);

            {
                bool dup = false;
                int  q;
                for (q = 0; q < count; q++)
                    if (strcmp(out[q].key_str, c->key_str) == 0)
                    {
                        dup = true;
                        break;
                    }
                if (!dup) count++;
            }
        }
    }

    return count;
}

static void
evaluate_and_propose(RelStat *r)
{
    IndexCandidate cands[MAX_LOCAL_CANDIDATES];
    int            n, i;
    int            best_idx = -1;
    double         best_net = 0.0;

    if (r->has_pending_request)
        return;

    n = build_candidates(r, cands, MAX_LOCAL_CANDIDATES);
    if (n == 0)
        return;

    elog(LOG, "auto_index: evaluating %d candidate(s) for %s.%s "
              "(shapes=%d existing_idx=%d writes=%ld relpages=%u "
              "reltuples=%.0f row_size=%d)",
         n, r->nspname, r->relname,
         r->num_shapes, r->num_existing_indexes,
         (long) r->write_count,
         r->cached_relpages, r->cached_reltuples, r->cached_row_size);

    for (i = 0; i < r->num_existing_indexes; i++)
        elog(LOG, "auto_index:   existing_idx[%s]",
             r->existing_indexes[i].key_str);

    for (i = 0; i < n; i++)
    {
        IndexCandidate *c = &cands[i];

        if (candidate_already_exists(r, c))
        {
            elog(LOG, "auto_index:   cand[%s] skipped (already present)",
                 c->key_str);
            continue;
        }

        score_candidate(c, r);

       elog(LOG, "auto_index:   cand[%s] cols=%d covers=%d benefit=%.2f "
          "creation=%.2f per_write=%.4f writes=%ld total_cost=%.2f net=%.2f",
            c->key_str, c->num_cols, c->shapes_covered,
            c->total_benefit, c->creation_cost,
            c->per_write_cost, (long) r->write_count,
            c->total_cost, c->net_benefit);

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

    {
        IndexCandidate *c = &cands[best_idx];
        char            cols_csv[512];
        int             off = 0;
        int             k;
        char           *p;

        cols_csv[0] = '\0';
        for (k = 0; k < c->num_cols; k++)
        {
            const char *name = c->attnames[k][0] ? c->attnames[k] : "?";
            int         w   = snprintf(cols_csv + off,
                                       sizeof(cols_csv) - off,
                                       (k == 0) ? "\"%s\"" : ",\"%s\"",
                                       name);
            if (w < 0 || off + w >= (int) sizeof(cols_csv))
                break;
            off += w;
        }

        char safe_key[MAX_INDEX_KEY_STR];
        strlcpy(safe_key, c->key_str, sizeof(safe_key));
        for (p = safe_key; *p; p++)
            if (*p == ',') *p = '_';

        snprintf(r->pending_sql, sizeof(r->pending_sql),
                "CREATE INDEX IF NOT EXISTS auto_idx_%u_%s "
                "ON %s.%s (%s)",
         r->relid, safe_key, r->nspname, r->relname, cols_csv);

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
                             "(total=%ld) [logging only]",
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
 *  Background worker
 * ================================================================ */

static void
auto_index_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

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
