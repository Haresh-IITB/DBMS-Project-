#include "postgres.h"

#include "auto_index.h"

#include "access/attnum.h"
#include "access/htup_details.h"
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
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/selfuncs.h"

PG_MODULE_MAGIC;

/* ----------------------------------------------------------------
 Global state
 * ---------------------------------------------------------------- */

AutoIndexSharedState *auto_index_state = NULL;

static void auto_index_shmem_request_hook(void);
static void auto_index_shmem_startup_hook(void);
static void auto_index_ExecutorRun(QueryDesc *queryDesc,
                                   ScanDirection direction,
                                   uint64 count);
static void scan_for_equality_predicates(PlanState *planstate);
static void process_seqscan_quals(SeqScanState *seqstate);
static void record_scan(Oid relid, AttrNumber attno,
                        const char *relname, const char *attname,
                        const char *nspname,
                        Relation rel);
static bool is_equality_operator(Oid opno);

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;

static volatile sig_atomic_t got_sigterm = false;

static void
auto_index_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

void        _PG_init(void);
void        _PG_fini(void);


static void auto_index_ExecutorRun(QueryDesc *queryDesc,
                                   ScanDirection direction,
                                   uint64 count);

static void scan_for_equality_predicates(PlanState *planstate);
static void process_seqscan_quals(SeqScanState *seqstate);
static void record_scan(Oid relid, AttrNumber attno,
                        const char *relname, const char *attname,
                        const char *nspname,
                        Relation rel);
static bool is_equality_operator(Oid opno);

PGDLLEXPORT void auto_index_worker_main(Datum main_arg);


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
    }
}


/* ================================================================
 Extension Initialization
 * ================================================================ */
void
_PG_init(void)
{
    BackgroundWorker worker;

    if (!process_shared_preload_libraries_in_progress)
        ereport(ERROR,
                (errmsg("auto_index must be loaded via shared_preload_libraries")));

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
    snprintf(worker.bgw_name,           BGW_MAXLEN, "auto_index worker");
    snprintf(worker.bgw_library_name,   BGW_MAXLEN, "auto_index");
    snprintf(worker.bgw_function_name,  BGW_MAXLEN, "auto_index_worker_main");
    worker.bgw_main_arg  = (Datum) 0;
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}

void
_PG_fini(void)
{
    ExecutorRun_hook = prev_ExecutorRun;
}


/* ================================================================
 Executor Hook — Predicate Detection
 * ================================================================ */
static void
auto_index_ExecutorRun(QueryDesc *queryDesc,
                       ScanDirection direction,
                       uint64 count)            
{
    if (queryDesc->operation == CMD_INSERT ||
    queryDesc->operation == CMD_UPDATE ||
    queryDesc->operation == CMD_DELETE)
{
    PlanState *ps = queryDesc->planstate;

    if (ps && IsA(ps, ModifyTableState))
    {
        ModifyTableState *mts = (ModifyTableState *) ps;
        ResultRelInfo *rri = mts->resultRelInfo;

        if (rri && rri->ri_RelationDesc)
        {
            Oid relid = RelationGetRelid(rri->ri_RelationDesc);

            LWLockAcquire(&auto_index_state->lock.lock, LW_EXCLUSIVE);

            for (int i = 0; i < auto_index_state->num_entries; i++)
            {
                ScanStat *entry = &auto_index_state->entries[i];

                if (entry->relid == relid)
                    entry->write_count++;
            }

            LWLockRelease(&auto_index_state->lock.lock);
        }
    }
    }
    if (prev_ExecutorRun)
        prev_ExecutorRun(queryDesc, direction, count);
    else
        standard_ExecutorRun(queryDesc, direction, count);

    if (queryDesc->planstate != NULL)
        scan_for_equality_predicates(queryDesc->planstate);
}

static void
scan_for_equality_predicates(PlanState *planstate)
{
    if (planstate == NULL)
        return;

    elog(DEBUG1, "auto_index: visiting node type %d", nodeTag(planstate));

    if (IsA(planstate, SeqScanState))
    {
        elog(LOG, "auto_index: found SeqScanState");
        process_seqscan_quals((SeqScanState *) planstate);
    }

    scan_for_equality_predicates(planstate->lefttree);
    scan_for_equality_predicates(planstate->righttree);
}

static void
process_seqscan_quals(SeqScanState *seqstate)
{
    List       *quals;
    ListCell   *lc;
    Relation    rel;
    Oid         relid;
    char       *relname;
    char       *nspname;

    rel = seqstate->ss.ss_currentRelation;
    if (rel == NULL)
        return;

    relid   = RelationGetRelid(rel);
    relname = RelationGetRelationName(rel);
    nspname = get_namespace_name(RelationGetNamespace(rel));

    if (RelationGetNamespace(rel) == PG_CATALOG_NAMESPACE)
        return;

    {
        SeqScan *plan = (SeqScan *) seqstate->ss.ps.plan;
        quals = plan->scan.plan.qual;
    }

    if (quals == NIL)
        return;

    foreach(lc, quals)
    {
        Node *clause = (Node *) lfirst(lc);

        if (!IsA(clause, OpExpr))
            continue;

        {
            OpExpr *op    = (OpExpr *) clause;
            Node   *left  = linitial(op->args);
            Node   *right = lsecond(op->args);
            Var    *var   = NULL;

            if (!is_equality_operator(op->opno))
                continue;

            if (IsA(left, Var) &&
                (IsA(right, Const) || IsA(right, Param)))
                var = (Var *) left;
            else if (IsA(right, Var) &&
                     (IsA(left, Const) || IsA(left, Param)))
                var = (Var *) right;

            if (var == NULL)
                continue;

            {
                char *attname = get_attname(relid, var->varattno, false);
                if (attname)
                {
                    elog(LOG, "auto_index: detected scan on %s.%s col=%s opno=%u",
                         nspname, relname, attname, op->opno);
                    record_scan(relid, var->varattno,
                                relname, attname, nspname, rel);
                }
            }
        }
    }
}

static bool
is_equality_operator(Oid opno)
{
    HeapTuple        tp;
    Form_pg_operator optup;
    bool             result = false;

    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if (!HeapTupleIsValid(tp))
        return false;

    optup = (Form_pg_operator) GETSTRUCT(tp);
    if (strcmp(NameStr(optup->oprname), "=") == 0)
        result = true;

    ReleaseSysCache(tp);
    return result;
}


/* ================================================================
 Shared Memory Counter + Cost-Benefit Check
 * ================================================================ */

static void
record_scan(Oid relid, AttrNumber attno,
            const char *relname, const char *attname,
            const char *nspname, Relation rel)
{
    AutoIndexSharedState *state = auto_index_state;
    ScanStat *entry = NULL;
    int       i;
    double pages,seq_cost,index_cost,benefit_per_query,total_benefit,total_cost,total_rows;

    if (state == NULL)
        return;

    LWLockAcquire(&state->lock.lock, LW_EXCLUSIVE);

    for (i = 0; i < state->num_entries; i++)
    {
        if (state->entries[i].relid == relid &&
            state->entries[i].attno == attno)
        {
            entry = &state->entries[i];
            break;
        }
    }

    if (entry == NULL)
    {
        if (state->num_entries >= MAX_TRACKED_ENTRIES)
        {
            LWLockRelease(&state->lock.lock);
            return;
        }

        entry = &state->entries[state->num_entries++];
        entry->relid           = relid;
        entry->attno           = attno;
        entry->scan_count      = 0;
        entry->write_count     = 0;
        entry->index_requested = false;
        entry->index_created   = false;
        strlcpy(entry->relname, relname, NAMEDATALEN);
        strlcpy(entry->attname, attname, NAMEDATALEN);
        strlcpy(entry->nspname, nspname, NAMEDATALEN);
    }

    entry->scan_count++;
    pages = RelationGetNumberOfBlocks(rel);
    if(pages <= 0) pages = 1;
    seq_cost = SEQ_SCAN_COST_PER_PAGE * pages;
    index_cost = seq_cost * INDEX_SCAN_COST_FACTOR;
    benefit_per_query = seq_cost - index_cost;
    total_benefit = benefit_per_query * entry->scan_count;
    total_cost = INDEX_CREATION_COST + (entry->write_count * INDEX_MAINTENANCE_COST);
    total_rows = rel->rd_rel->reltuples;
    if(total_rows <= 0) total_rows = 1000;

    if (!entry->index_requested &&
        !entry->index_created   &&
        total_benefit > total_cost)
    {
        entry->index_requested = true;
        elog(LOG,
             "auto_index: threshold crossed for %s.%s.%s "
             "(benefit=%.2f cost=%.2f scans=" INT64_FORMAT " writes=" INT64_FORMAT ")",
             nspname, relname, attname,
             total_benefit, total_cost,
             entry->scan_count,
             entry->write_count);
    }

    LWLockRelease(&state->lock.lock);
}


/* ================================================================
 Background Worker
 * ================================================================ */
void
auto_index_worker_main(Datum main_arg)
{
    pqsignal(SIGTERM, auto_index_sigterm);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnection("postgres", "aman_nehra", 0);

    elog(LOG, "auto_index background worker started");

    while (!got_sigterm)
    {
        int i;

        // Sleep for 5 seconds, waiting for latch or timeout 
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
            ScanStat *entry = &auto_index_state->entries[i];

            if (entry->index_requested && !entry->index_created)
            {
                char sql[1024];

                snprintf(sql, sizeof(sql),
                         "CREATE INDEX IF NOT EXISTS "
                         "auto_idx_%u_%d ON %s.%s (%s)",
                         entry->relid,
                         (int) entry->attno,
                         entry->nspname,
                         entry->relname,
                         entry->attname);

                // Release shared lock before doing heavy SPI work 
                LWLockRelease(&auto_index_state->lock.lock);

                PG_TRY();
                {
                    SetCurrentStatementStartTimestamp();
                    StartTransactionCommand();
                    SPI_connect();
                    PushActiveSnapshot(GetTransactionSnapshot());

                    elog(LOG, "auto_index: executing: %s", sql);

                    if (SPI_execute(sql, false, 0) == SPI_OK_UTILITY)
                    {
                        elog(LOG, "auto_index: index created for %s.%s",
                             entry->relname, entry->attname);

                        LWLockAcquire(&auto_index_state->lock.lock, LW_EXCLUSIVE);
                        entry->index_created   = true;
                        entry->index_requested = false;
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

                    elog(WARNING, "auto_index: query failed, safely aborting transaction for %s.%s",
                         entry->relname, entry->attname);

                    LWLockAcquire(&auto_index_state->lock.lock, LW_EXCLUSIVE);
                    entry->index_requested = false;
                    LWLockRelease(&auto_index_state->lock.lock);
                }
                PG_END_TRY();

                // Re-acquire shared lock for the next loop iteration 
                LWLockAcquire(&auto_index_state->lock.lock, LW_SHARED);
            }
        }

        LWLockRelease(&auto_index_state->lock.lock);
    }

    elog(LOG, "auto_index background worker shutting down");
    proc_exit(0);
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