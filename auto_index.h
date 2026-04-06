#ifndef AUTO_INDEX_H
#define AUTO_INDEX_H

#include "postgres.h"
#include "access/attnum.h"       /* ← fixes 'unknown type AttrNumber' */
#include "storage/lwlock.h"

#define MAX_TRACKED_ENTRIES   64
#define SCAN_THRESHOLD        10
#define INDEX_BENEFIT         100.0
#define INDEX_CREATION_COST   10000.0

typedef struct ScanStat
{
    Oid         relid;
    AttrNumber  attno;
    int64       scan_count;
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

#endif  /* AUTO_INDEX_H */