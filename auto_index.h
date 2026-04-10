#ifndef AUTO_INDEX_H
#define AUTO_INDEX_H

#include "postgres.h"
#include "access/attnum.h"     
#include "storage/lwlock.h"


#define SEQ_SCAN_COST_PER_PAGE     1.0
#define INDEX_SCAN_COST_FACTOR     0.2
#define INDEX_CREATION_COST        1000.0
#define INDEX_MAINTENANCE_COST     5.0
#define SELECTIVITY_THRESHOLD  0.5
#define MAX_TRACKED_ENTRIES   64

#define MAX_TABLE_PAGES 12800

typedef struct ScanStat
{
    Oid         relid;
    AttrNumber  attno;
    int64       scan_count;
    int64       write_count;
    bool        index_requested;
    bool        index_created;
    char        relname[NAMEDATALEN];
    char        attname[NAMEDATALEN];
    char        nspname[NAMEDATALEN];
} ScanStat;

typedef struct AutoIndexSharedState
{
    LWLockPadded    lock;
    int             num_entries;
    ScanStat        entries[MAX_TRACKED_ENTRIES];
} AutoIndexSharedState;

extern AutoIndexSharedState *auto_index_state;

#endif  