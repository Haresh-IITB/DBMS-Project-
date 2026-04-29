# auto_index — automatic index recommender / creator

This is the v2 build with phases 0-4 from the plan:

| Phase | What it does |
|------:|---|
| **0** | Per-relation tracking with sub-arrays for predicates and scan shapes (replaces the flat per-(relid, attno) array). |
| **1** | Operator detection via `pg_amop` + B-tree strategy numbers. Handles `=`, `<`, `<=`, `>`, `>=`, `IN (...)`, and synthesizes `RANGE` from `>=`/`<=` pairs on the same column. |
| **2** | Cost model uses PostgreSQL's planner GUCs (`seq_page_cost`, `random_page_cost`, `cpu_tuple_cost`, `cpu_operator_cost`, `cpu_index_tuple_cost`). Selectivity comes from `pg_statistic.n_distinct` for equality and the planner's defaults for inequalities/ranges. Index height is estimated from `reltuples` and average key width. |
| **3** | Multi-column candidates with B-tree column ordering (equality columns first, then by selectivity ascending). Singletons and composites are both proposed; the scorer picks the winner. |
| **4** | Each candidate is scored against every recorded scan shape on the relation, not just the one that triggered it. Indexes already created by `auto_index` are tracked so they're not re-proposed. |

---
SHOW data_directory;
## Build & install

The extension must be loaded via `shared_preload_libraries` because it
registers a background worker.

```bash
# from inside the source directory:
make
sudo make install

# tell Postgres to load it on startup
echo "shared_preload_libraries = 'auto_index'" \
  | sudo tee -a $(pg_config --sharedir | sed 's|/share|/data|')/postgresql.conf
# (or edit your postgresql.conf manually)

# restart Postgres
sudo systemctl restart postgresql        # or pg_ctl restart
```

Then in any database:

```sql
CREATE EXTENSION auto_index;
```

The extension does its work whether or not you run `CREATE EXTENSION` —
the SQL object is mostly bookkeeping for `pg_extension`. The hooks are
active as soon as the library is preloaded.

> **Note on the bgworker connection.** It connects to a database called
> `postgres`. If that doesn't exist on your cluster, edit
> `auto_index_worker_main` (search for `BackgroundWorkerInitializeConnection`)
> and change the database name before building.

---

## How to verify it's working

### 1. Tail the postmaster log
Find your log file:

```bash
psql -At -c "SELECT current_setting('log_directory'), current_setting('log_filename');"
# typical: /var/log/postgresql/postgresql-16-main.log
sudo tail -F /var/log/postgresql/postgresql-16-main.log | grep auto_index
```

### 2. Run the test workload
```bash
psql -d postgres -f test_workload.sql
```

### 3. What you should see in the log

Right at server start:

```
auto_index: shared state initialized (MAX_TRACKED_ENTRIES=128, ...)
auto_index: extension loaded
auto_index: background worker started
```

When the first query in section **1** runs:

```
auto_index: predicate on public.orders.status op=EQ sel=0.0500
auto_index: now tracking public.orders (relpages=637 reltuples=100000 avg_width=40)
auto_index:   new shape on public.orders with 1 column(s)
auto_index: evaluating 1 candidate(s) for public.orders (shapes=1, writes=0, ...)
auto_index:   cand[7] cols=1 covers=1 benefit=Y maint=0.00 creation=Z net=...
```

After enough repetitions of the same shape, when the net benefit goes
positive:

```
auto_index: PROPOSING CREATE INDEX IF NOT EXISTS auto_idx_<oid>_7 ON public.orders ("status")
```

A few seconds later (the bgworker polls every 5s):

```
auto_index: bgworker executing: CREATE INDEX IF NOT EXISTS auto_idx_<oid>_7 ON public.orders ("status")
auto_index: bgworker created index (key=7)
```

For the **multi-column** workload (section 4) you should see lines like:

```
auto_index: predicate on public.orders.status op=EQ sel=0.0500
auto_index: predicate on public.orders.customer_id op=EQ sel=0.0010
auto_index:   new shape on public.orders with 2 column(s)
auto_index: evaluating 3 candidate(s) for public.orders ...
auto_index:   cand[2_3] cols=2 covers=1 benefit=... net=...      <-- composite (customer_id, status)
auto_index:   cand[3]   cols=1 covers=2 benefit=... net=...      <-- singleton on status
auto_index:   cand[2]   cols=1 covers=1 benefit=... net=...      <-- singleton on customer_id
auto_index: PROPOSING CREATE INDEX IF NOT EXISTS auto_idx_<oid>_2_3 ON public.orders ("customer_id","status")
```

Note that `cand[3]` (singleton on `status`) shows `covers=2` because it
also serves the section-1 shape — that's Phase 4's cross-shape benefit
accounting in action.

For the **range** workload (section 2), look for the collapse:

```
auto_index: predicate on public.orders.amount op=GE sel=0.3333
auto_index: predicate on public.orders.amount op=LE sel=0.3333
auto_index:   new shape on public.orders with 1 column(s)         <-- collapsed to RANGE
```

For the **write-heavy** part (section 9), the `maint=` number in the
`cand[...]` LOG line should be much larger than it was earlier in the
session — that's the per-write maintenance cost (Phase 2) being
multiplied by `write_count` (Phase 0).

### 4. Verify the indexes exist
The test script ends with:

```sql
SELECT indexname, indexdef FROM pg_indexes
 WHERE tablename = 'orders' AND indexname LIKE 'auto_idx%';
```

You should see one row per auto-created index, with names like
`auto_idx_16384_7` (single column) and `auto_idx_16384_2_7` (multi-column).

### 5. Confirm the planner uses them
The script also runs `EXPLAIN` after the indexes exist; you should see
`Index Scan` / `Bitmap Index Scan` instead of `Seq Scan`.

---

## Troubleshooting

* **No `auto_index:` lines at all in the log** — the library isn't
  preloaded. Check `SHOW shared_preload_libraries;`.
* **"auto_index must be loaded via shared_preload_libraries"** on
  start — same root cause; check `postgresql.conf`.
* **Bgworker can't connect** — change the database name in
  `BackgroundWorkerInitializeConnection`.
* **Net benefit never goes positive** — the table is too small relative
  to `seq_page_cost`. Either bump the row count in the test workload or
  run the same query more times. The `cand[...]` LOG line shows you
  exactly what numbers are being compared.
* **Lots of `predicate on ...` lines for queries that already use an
  existing index** — shouldn't happen, since we only walk `SeqScanState`
  nodes. If it does, double-check that PostgreSQL is actually choosing
  a seq scan via `EXPLAIN`.

---

## Files

| File | Purpose |
|---|---|
| `auto_index.c` | The whole implementation, organized into the phase sections noted at the top of the file. |
| `auto_index.h` | Public-ish data structures held in shared memory. |
| `auto_index.control` | PGXS extension metadata. |
| `auto_index--1.0.sql` | Empty SQL upgrade file (PGXS requires one). |
| `Makefile` | Standard PGXS Makefile. |
| `test_workload.sql` | End-to-end driver that exercises every phase. |
