# auto_index — v3

A PostgreSQL extension that observes SeqScans, scores candidate
B-tree indexes against a textbook + pganalyze cost model, and asks a
background worker to create the winners.

This is **v3**.  It builds on v2's per-relation tracking, generalized
operator detection, multi-column candidates, and cross-shape benefit
accounting.  v3 fixes four specific weaknesses that surfaced during
review of v2.

---

## What changed in v3

### 1. Write overhead is now the pganalyze ratio
v2 multiplied a fixed `INDEX_MAINTENANCE_COST` by `write_count` and
charged that into the candidate's cost.  That conflated "how much
have we written historically" with "how much extra work will this
new index cost on every future write".

v3 uses the pganalyze "Index Write Overhead" formula instead:

```
write_overhead = (8 + Σ avg_width(indexed columns)) / row_size
row_size       = 23 (tuple header) + 4 (item ptr) + Σ avg_width(all columns)
total_cost     = creation_cost × (1 + write_overhead)
```

It is a unitless ratio, applied as a surcharge to the one-time
creation cost.  `write_count` is still tracked on `RelStat` and
printed in log lines, but **it does not enter the math**.

Code: `compute_write_overhead()`, `compute_relation_row_size()`.

### 2. Selectivity from real pg_statistic
v2 used `1 / n_distinct` for equality (good, but with a bug for
negative `stadistinct`) and a hardcoded `1/3` for inequality.

v3 reads pg_statistic directly:

* **Equality with a Const value**: probes the MCV list
  (`STATISTIC_KIND_MCV`).  If the value matches an MCV, that
  bucket's frequency is used.  Otherwise:
  `(1 − Σ MCV freq) / (n_distinct − n_mcv)`.
* **Inequality with a Const value**: histogram bucket interpolation
  (`STATISTIC_KIND_HISTOGRAM`) via binary search using the type's
  comparison proc from typcache.  Implements the chapter 16.41
  fraction-of-buckets formula.
* **Param / foreign Var / no value**: falls back to planner defaults
  (`DEFAULT_EQ_SEL`, `DEFAULT_INEQ_SEL`, `DEFAULT_RANGE_INEQ_SEL`).

`get_n_distinct()` correctly resolves negative `stadistinct`
(meaning "fraction of ntuples") into an absolute number — the v2
bug where this came out negative is fixed.

Each call logs the source it used (`source=MCV-hit`,
`source=histogram`, `source=1/(nd-mcv)`, `source=1/n_distinct`,
`source=default`) so the cost-model decisions are auditable.

Code: `selectivity_from_pg_stats()`, `get_n_distinct()`.

### 3. Prefix matching + awareness of existing indexes
**Prefix matching.** v2's `candidate_can_serve_shape` had a bug: it
checked the first index column against `c->attnos[0]`, which assumed
the candidate's column ordering matched the shape's column ordering.
It also required all shape columns to be in the index.

v3's `prefix_length_for_shape()` walks index columns left-to-right
and returns the number of leading columns that match the shape with
**equality**, stopping at the first absent or non-equality column.
This is the actual B-tree access rule:

* Index `(a, b, c, d)` on shape `{a=, b=, c=}` → prefix=3.
* Index `(a, b, c)` on shape `{a=, b=}` → prefix=2 (good).
* Index `(a, b)` on shape `{a=, b=, c=}` → prefix=2, with `c`
  becoming a residual filter (not a seek key).
* Index `(a, b, c)` on shape `{a=, b>, c=}` → prefix=2 because the
  range on `b` truncates the prefix; `c` is a residual.

`cost_for_index_on_shape()` uses prefix length to decide between
seq-scan cost and index-scan cost.

**Existing-index awareness.** v2 happily proposed indexes that
duplicated or barely improved on indexes that already existed on the
relation.  v3 enumerates real indexes from `pg_index` via
`RelationGetIndexList()`, filters to `BTREE_AM_OID`, and uses
`try_relation_open()` for safety against concurrent drops.

`score_candidate()` now computes, per shape:
```
best_existing_cost = min(seq_cost, min over existing indexes)
gain               = (best_existing_cost − cand_cost) × observation_count
```
Benefit is counted only when the candidate strictly beats what's
already there.  Duplicates and small improvements score `net=0` and
are not proposed.

`candidate_already_exists()` blocks proposals that match an existing
index, an already-created auto_index index, or a pending request.

Code: `prefix_length_for_shape()`, `cost_for_index_on_shape()`,
`enumerate_existing_indexes()`, `score_candidate()`,
`candidate_already_exists()`.

### 4. Joins (`Var op Var`)
v2 only matched `Var op Const` and `Var op Param`.  Join quals like
`o.customer_id = c.customer_id` produce `Var op Var` after planning
on whichever side the planner chose to scan.

v3's `extract_simple_predicate()` takes `relid` and handles:

* `Var op Const/Param` (LHS or RHS).  When the Var is on the right
  (`5 < x`), the operator strategy is run through `flip_strategy()`
  to recover `x > 5`.
* `Var op Var`: identifies "ours" via `get_attname(relid, attno, true)`.
  If exactly one side resolves to a column on `relid`, that's ours
  and the other side becomes "no value" (selectivity falls back to
  per-column average / planner defaults).  If both resolve (intra-rel
  comparison), the LHS is taken.

Code: `extract_simple_predicate()`, `flip_strategy()`.

---

## Build & install

```bash
cd /path/to/auto_index_v3
make USE_PGXS=1 PG_CONFIG=$(which pg_config)
sudo make USE_PGXS=1 PG_CONFIG=$(which pg_config) install
```

In `postgresql.conf`:
```
shared_preload_libraries = 'auto_index'
```

Restart Postgres, then:
```sql
CREATE EXTENSION auto_index;
```

The bgworker connects to database `postgres` by default (see
`BackgroundWorkerInitializeConnection` near the bottom of
auto_index.c).  Edit if your cluster uses a different db.

---

## Testing

`test_workload.sql` exercises all four fixes:

| Section | Exercises | What to look for in the log |
|---|---|---|
| 1 | MCV-based equality selectivity | `source=MCV-hit`, sels for `'COMPLETED'` (~0.80) vs `'FAILED'` (~0.05) |
| 2 | Histogram-based range selectivity | `source=histogram`, sel for `amount > 800` ≈ 0.20, NOT 0.33 |
| 3 | Multi-column composite, B-tree column ordering | Composite candidate puts higher-cardinality column first |
| 4 | EQ + RANGE composite, prefix truncation | Range column placed last; prefix walk stops at it |
| 5 | Bgworker creates the winners | `auto_idx_*` rows in `pg_indexes` |
| 6 | Existing-index dedup | "skipped (already present)" or "no candidate with positive net benefit" after manually creating `manual_idx_orders_created_at` |
| 7 | `(a,b)` covers shape `{a,b,c}` via prefix | Composite candidate scores positive `covers` on the 3-col shape |
| 8 | `Var op Var` join smoke test | No crash; predicate logged with `value=param/none` if foreign-Var case is hit |
| 9 | Final state + EXPLAIN | Planner uses the auto-created indexes |

Run:
```bash
psql -d postgres -f test_workload.sql
tail -F /var/log/postgresql/postgresql-*.log | grep auto_index
```

---

## Files

| File | Purpose |
|---|---|
| `auto_index.c`         | All logic. ~1950 lines. |
| `auto_index.h`         | Shared structs (`RelStat`, `PredicateInfo`, `ScanShape`, `ExistingIndexInfo`). |
| `auto_index.control`   | Extension control file. |
| `auto_index--1.0.sql`  | Empty extension SQL (no user-facing functions yet). |
| `Makefile`             | Standard PGXS Makefile. |
| `test_workload.sql`    | The workload above. |

---

## Logging

Every meaningful decision is on `LOG` and prefixed `auto_index:`.
Fine-grained traces (per-predicate selectivity sources, per-write
counters) are on `DEBUG1` — set `log_min_messages = debug1` to see
them.

Useful greps:

```bash
grep "auto_index: predicate"     # one line per qual
grep "auto_index:   cand\["      # one line per scored candidate
grep "auto_index: PROPOSING"     # winners
grep "auto_index: bgworker"      # what the bgworker did
grep "source="                   # which stat source drove a sel
```
