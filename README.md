# ChronoDim

**An embedded, single-process, bitemporal SCD Type 2 storage engine for the JVM.**

ChronoDim does one job — applying versioned (SCD2) changes to dimensional and
reference tables and answering point-in-time queries — dramatically faster and
cheaper than general-purpose lakehouse platforms, while continuously publishing
its full table state as open, engine-readable files so DuckDB, Spark, Databricks
and Polars can query the same data with zero migration.

*Sub-millisecond point-in-time reads and 100k+/sec versioned writes on one
machine, with complete history always queryable from any external engine.*

Deployment model is SQLite/DuckDB-style: a library plus a CLI. **No server, no
network listener, no external database.**

---

## Who this is for

Data engineering teams in regulated industries (banking, insurance) running
dimension loads, reference data management, and regulatory **adjustment
tables** — workloads currently overpaying for Spark clusters to run MERGE
statements on small-to-medium data.

## Goals

- **Correct bitemporal SCD2 semantics** — valid time (business effective dates)
  and transaction time (when the engine learned it) tracked independently for
  every version. Corrections never destroy information: "what did the engine
  believe at time T" is a first-class query.
- **Performance** — bulk loads through a sorted-ingest path; group-committed
  WAL; sub-millisecond warm point reads.
- **Zero-setup deployment** — one fat jar (`chronodim.jar`), or a Maven library.
- **Strong durability without a server** — fsync'd checksummed WAL, continuous
  WAL shipping + snapshots to an object store, restore-anywhere with RPO in
  seconds.
- **Open interoperability** — full state (current + history) published as open
  files under a frozen column contract with an SCD2 view template.
- **Audit-grade operation** — every load produces an immutable audit manifest;
  the WAL is a replayable, checksummed record of every change ever applied.
- **Swappable storage engine** — the hot KV store hides behind an internal
  interface. Two backends ship today: **RocksDB** (default) and a
  **pure-Java LSM** (zero native dependencies).

## Non-goals

- **Not distributed.** Single machine, single process.
- **Not multi-writer.** One writer process per data directory, enforced by a
  lockfile. Concurrent readers within the same process are fully supported.
- **Not a general SQL database.** No SQL parser, no joins. Analytical SQL runs
  on the published tables via external engines.
- **Not for multi-TB hot fact tables.** Design target: up to ~2 billion rows /
  ~500 GB hot data per instance.
- **No writes from external engines into the published output.** ChronoDim is
  the single writer of its cold tier; external changes enter as CDC input only.

---

## Build (Maven)

```bash
mvn clean install            # builds all modules + runs the full test suite
mvn -DskipTests package      # fast build; engine-cli/target/chronodim.jar is the CLI
```

Requires JDK 21+ (production target JDK 25; pass `-Djava.release=25` on a 25 toolchain).

## Quick start

> Prefer a guided drill? [`docs/runbook.md`](docs/runbook.md) walks build →
> load → adjust → time-travel → UI verification end to end, with the expected
> output of every command.

```bash
alias chronodim='java -jar engine-cli/target/chronodim.jar'

# 1. a database is just a directory
chronodim init -d ./dims

# 2. declare a table (YAML)
cat > customer.yaml <<'EOF'
table: customer
business_key: [customer_id]
schema:
  - {name: customer_id, type: string}
  - {name: name,        type: string}
  - {name: segment,     type: string}
  - {name: risk_score,  type: double}
  - {name: updated_at,  type: timestamp}
tracked_columns: [name, segment, risk_score]
ignored_columns: [updated_at]
valid_time:
  mode: source_column          # or: load_time
  column: updated_at
late_arrival:
  policy: split                # split | reject | quarantine
deletes:
  mode: soft                   # soft | ignore
quality_gates:
  - {column: customer_id, rule: not_null}
  - {column: risk_score,  rule: "between 0 and 1000"}
publish:
  enabled: true
  location: ./export/customer
EOF
chronodim table create -d ./dims -f customer.yaml
```

**`business_key` is the table's primary key.** One column or a combination —
uniqueness always holds on the *full* combination:

```yaml
business_key: [account_id, symbol]     # composite: (A1, AAPL) ≠ (A1, MSFT)
```

Like a primary key it is NOT NULL by definition (a row missing any key part
is rejected with a reason), immutable (`table alter` refuses key changes —
rekeying would silently re-identify every entity), and scalar-only. Unlike a
plain database, "unique" here means one *live entity* per combination:
re-sending the same key doesn't violate uniqueness, it *versions* that entity
(update / no-op), which is exactly the upsert semantics adjustments need.

```bash

# 3. load data — a single JSON file, CSV, or JSON Lines all work
chronodim apply changes.json  -d ./dims -t customer --load-id load-2026-07-11   # JSON array
chronodim apply changes.csv   -d ./dims -t customer --load-id load-2026-07-12   # CSV w/ header
cat stream.jsonl | chronodim apply --stdin -d ./dims -t customer --load-id load-2026-07-13

# rows are upserts; mark deletes with "_op": "delete" (JSON) or an _op column (CSV)
# re-running the same --load-id with the SAME batch is a safe no-op (idempotent);
# reusing a load_id for DIFFERENT content is refused loudly — nothing applies.

# run ids: mint globally unique numeric (BIGINT) ids, time-ordered, ~270 years of space
chronodim run-id                  # → e.g. 81985529216486895
chronodim apply changes.json -d ./dims -t customer --load-id "$(chronodim run-id)"
# enforce numeric-only ids per process: --numeric-load-ids (env CHRONODIM_NUMERIC_LOAD_IDS=true)

# 4. query
chronodim get     -d ./dims -t customer customer_id=C42
chronodim asof    -d ./dims -t customer --valid-time 2025-12-31T23:59:59Z customer_id=C42
chronodim asof    -d ./dims -t customer --valid-time 2025-12-31T23:59:59Z \
                  --tx-time 2026-01-15T00:00:00Z customer_id=C42     # bitemporal: "as we believed on Jan 15"
chronodim history -d ./dims -t customer customer_id=C42
chronodim scan    -d ./dims -t customer --format csv -o current.csv

# 5. operate
chronodim verify   -d ./dims                       # state fingerprint + counts
chronodim snapshot -d ./dims --object-store /mnt/backup --prune-wal
chronodim restore  --from /mnt/backup --target-dir ./dims-restored [--as-of-txn N]
chronodim manifest show load-2026-07-11 -d ./dims  # immutable audit record of that load
chronodim bench    -d ./bench-dir                  # standard benchmark on this box
```

Every command accepts `--json` for machine-readable output.

**Long-running deployments:** schedule `chronodim snapshot --object-store ... --prune-wal`
(e.g. weekly). It checkpoints the hot store, uploads the snapshot, and deletes
local WAL segments that are already durable, published, and fully shipped — the
object store keeps the complete replayable audit log, so local disk stays
bounded over years while restores remain snapshot + short-tail replay.

### The adjustment envelope — metadata + records in one JSON file

Teams that send "a few metadata columns and an array of records" use the
envelope form. No operation names, no CLI flags — the file is self-describing:

```json
{
  "table": "customer",
  "load_id": "adj-2026-Q2-0417",
  "effective_at": "2026-06-30T00:00:00Z",
  "source": "finance-ops",
  "approved_by": "jsmith",
  "reason": "quarterly regulatory true-up",
  "defaults": { "segment": "RETAIL" },
  "records": [
    { "customer_id": "C1", "risk_score": 120.5 },
    { "customer_id": "C2", "risk_score": 250.0 }
  ]
}
```

```bash
chronodim apply adjustments.json -d ./dims       # table + load_id come from the file
```

- `load_id` → idempotency key (re-sending the file is a no-op)
- `effective_at` → valid time (`__START_AT`) for records without their own
- `defaults` → merged into every record (record wins)
- `metadata` **and any unrecognised top-level keys** (approver, reason, source —
  whatever the team's template says) → stored in the audit manifest forever
- `records` → plain upserts; the engine derives insert/new-version/no-op itself

Related feed styles with no operation column:

```bash
# full snapshot: keys absent from the file are soft-deleted (delete-by-absence)
chronodim apply snapshot.json -d ./dims -t customer --load-id snap-07-12 --full-snapshot

# rows that ALREADY carry SCD2 intervals (__START_AT/__END_AT, END null = active):
# auto-detected and imported — history reproduced, closed tails become deletes
chronodim load scd2_export.jsonl -d ./dims -t customer --load-id migrate-1
chronodim apply delta.jsonl -d ./dims -t customer --load-id d1 \
          --scd2-start-column EFF_START_DT --scd2-end-column EFF_END_DT   # any team template
```

### Publishing in your team's SCD2 template

The published column names are per-table configuration — Databricks convention
or any custom template:

```yaml
publish:
  enabled: true
  location: ./export/customer
  column_style: databricks        # → __START_AT / __END_AT, END null = active,
                                  #   no _op column, deletes = closed final version
  # or any custom convention:
  # scd2_columns: {start: EFF_START_DT, end: EFF_END_DT, include_ops: false}
```

### Cross-table transactions

A JSON file with an object of table → rows applies atomically as one engine
transaction:

```json
{
  "customer":   [ {"customer_id": "C1", "name": "...", "updated_at": "2026-07-01T00:00:00Z"} ],
  "adjustment": [ {"adj_id": "A9", "amount": "125.50", "_op": "delete"} ]
}
```

```bash
chronodim apply multi.json -d ./dims --load-id adj-batch-7
```

## Built-in console (UI)

Everything you'd do against a normal database, in a browser — with zero
frameworks. The server is the JDK's own `HttpServer`, the page is one
hand-written HTML file baked into the jar; there is no React, no npm, no CDN,
nothing to install.

```bash
chronodim ui -d ./dims            # → http://127.0.0.1:8420
```

Or let one script do everything — build if needed, seed demo data, start the
console, and smoke-test 12 behaviors through the HTTP API (time travel,
idempotent duplicate loads, gate rejections, audit trail, fingerprint):

```bash
scripts/ui-demo.sh                # ...then leaves the UI running for you
scripts/ui-demo.sh --ci           # same, but tears down after — exit 0/1 for CI
scripts/ui-demo.sh --storage lsm  # pure-Java backend — use this if rocksdb ever
                                   # fails with a native crash (see below)
```

On Windows (PowerShell 5.1 or 7 — same behavior, same 12 checks). Scripts are
not signed, so either bypass the policy for the call or for the session:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\ui-demo.ps1
powershell -ExecutionPolicy Bypass -File scripts\ui-demo.ps1 -CI -Port 9000
# or, once per shell session: Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

**RocksDB is the default storage backend and loads a native library; on some
Windows machines (usually missing the Microsoft Visual C++ Redistributable
x64) that native load crashes java with an unhelpful exit code before
ChronoDim's own error handling runs.** Both scripts print a hint and the
fix when that happens; the durable fix is `-Storage lsm` /
`--storage lsm` — ChronoDim's pure-Java backend, zero native dependencies,
same correctness guarantees and test coverage as RocksDB.

| Tab | What it does |
|---|---|
| **Data** | Browse current rows (or *as-of* any past date), substring filter, paging, optional system columns |
| **Entity** | Full bitemporal history of one key — every version ever recorded, changed cells highlighted, superseded beliefs included |
| **Adjust** | Submit adjustment batches: a spreadsheet-style form, or paste the JSON envelope / JSONL / CSV — identical formats to the CLI |
| **Audit** | Every load's manifest: who/when/what counts, free-form metadata |
| **Quarantine** | Inspect gate-failed rows, re-apply after fixing |
| **Health** | Engine stats, on-demand full-scan integrity fingerprint |

Creating a table is the same YAML the CLI takes, pasted into the *New table*
dialog. Applies are atomic and idempotent — the form generates a `load_id`,
and resubmitting a duplicate shows the original receipt instead of
double-applying.

The `ui` process holds the single-writer lock, which makes it the recommended
deployment for shared use: any number of people work through one console
process concurrently (the engine's thread-safe apply path — see the
Concurrency model section). It binds to `127.0.0.1` and has no authentication;
pass `--host` only if you understand what that exposes.

## Library use

```java
try (Engine engine = ChronoDim.open(Path.of("/data/dims"), EngineOptions.defaults())) {
    engine.createTable(TableConfigIO.fromYaml(Files.readString(Path.of("customer.yaml"))));
    AuditManifest m = engine.apply(ApplyBatch.single("load-1", "customer", rows));
    Optional<Version> v = engine.getAsOf("customer", Map.of("customer_id", "C42"),
                                         validTimeMicros, txTimeMicros);   // bitemporal
    try (VersionCursor c = engine.scanCurrent("customer")) { ... }         // streaming
}
```

Maven coordinates: `io.chronodim:engine-api` (+ `engine-core`, `engine-storage`
at runtime; `engine-export`/`engine-durability` optional — they self-attach via
`ServiceLoader` when present).

## The published cold tier

Every committed transaction's version records are appended (asynchronously,
exactly-once) to each published table's location:

```
export/customer/
  _contract.json          # column contract descriptor (incl. partition_by)
  scd2_view.sql           # LEAD()-based SCD2 view template (DuckDB/Spark/Databricks)
  _chronodim_log/         # atomic commit log (the source of truth for readers)
  data/part-*.jsonl       # version records: payload + _valid_from, _valid_to,
                          #   _tx_time, _txn_id, _op, _schema_version
  finalized/              # after `chronodim finalize`: consolidated log +
                          #   materialized current snapshot (_is_current)
```

### Partitioning

`publish.partition_by: [region, year]` produces Hive-style partitioned output —
`data/region=EU/year=2026/part-*.jsonl` — which Spark, Databricks and DuckDB
prune natively. Partition values also stay in the row payload, so plain glob
readers keep working; nulls land in `__HIVE_DEFAULT_PARTITION__`. `finalize`
preserves the same layout (`finalized/region=EU/year=2026/log-*.jsonl` +
`current-*.jsonl`).

### Data types

Every type Spark SQL can store in a table:

| category | types |
|---|---|
| numbers | `tinyint`, `smallint`, `int`, `bigint`/`long`, `float`, `double`, `decimal(p,s)` (p ≤ 38, exact) |
| text/binary | `string` (`char(n)`/`varchar(n)` accepted as aliases), `binary`/`bytes` |
| temporal | `date`, `timestamp` (µs UTC), `timestamp_ntz` |
| boolean | `boolean` |
| nested | `array<T>`, `map<K,V>` (scalar keys), `struct<name:type,...>` — arbitrarily nested |

Not storable (rejected with a clear error, same as Delta tables in practice):
interval types, `variant`, `void`. Complex values arrive as native JSON, or as
JSON strings inside CSV cells.

Query it from DuckDB today:

```sql
SELECT * FROM read_json_auto('export/customer/data/*.jsonl') WHERE _op <> 'DELETE';
-- or create the shipped scd2_view.sql for point-in-time SQL
```

v1 publishes JSON Lines (dependency-free, readable everywhere). The publisher is
format-pluggable; the Delta Lake (Parquet + Delta log via Delta Kernel Java)
writer drops in behind the same interface and contract — see `docs/formats.md`.

## Storage backends

| Backend | Flag | Notes |
|---|---|---|
| RocksDB (default) | `--storage rocksdb` | RocksJava, bloom filters, block cache; RocksDB WAL disabled — ChronoDim's own WAL is authoritative |
| Pure-Java LSM | `--storage lsm` | Zero native dependencies; memtable + sorted segments + bloom filters + size-tiered compaction |

Both pass the same contract test suite and the same differential SCD2 suite;
data directories are backend-specific (choose at `init`, keep forever — or
restore through a snapshot to switch).

## Correctness story

- **Differential testing** — an independent, deliberately-simple reference SCD2
  implementation (TreeMap-based, value-equality change detection). Randomized
  change streams (late arrivals, same-batch duplicates, deletes, re-inserts)
  run against both implementations; full state is compared after every batch on
  both temporal axes, for both storage backends.
- **Crash recovery** — acked transactions survive: hot store destroyed and
  rebuilt from WAL; crash-image copies recover to exactly the reference state;
  torn WAL tails truncate cleanly, interior corruption fails loudly.
- **Disaster drill** — snapshot + WAL shipping to an object store, full machine
  loss, restore (with optional `--as-of-txn` point-in-time), fingerprint match.
- **Concurrent load** — 50 threads submitting adjustment batches simultaneously
  against one engine (heavily contended shared keys, a scanning reader running
  the whole time): no lost updates, no torn reads, every SCD2 chain contiguous,
  duplicate `load_id` races resolve to exactly one winner, and a reopen replays
  to the identical state fingerprint.

```bash
mvn test          # runs the whole suite
```

### Concurrency model

`Engine.apply()` is safe to call from any number of threads in one process:
a fair internal mutex serializes the read–decide–write section (so decisions
are always made against fully committed state), while the fsync wait happens
*outside* the mutex — concurrent callers share group commits instead of
queueing behind each other's disk flushes. Reads are snapshot-isolated and
never block writers. The `load_id` idempotency check runs inside the mutex,
so two services retrying the same load can never double-apply.

What is **not** supported: two *processes* opening the same data directory.
The second one fails fast with a lock error (see Non-goals). If many users
or jobs need to submit adjustments, run one long-lived embedder (service or
single applier draining an inbox of envelope files) — envelope `load_id`s
make client retries safe by construction.

## On-disk formats

All frozen contracts (WAL record framing, KV key/value encodings, snapshot and
audit manifest schemas, published column contract) are documented in
[`docs/formats.md`](docs/formats.md). Every structure carries a magic number and
format version byte.

## Module map

```
engine-api/         Public Java API, config model, exceptions. Zero dependencies.
engine-storage/     StorageEngine SPI + RocksDB and pure-Java LSM backends.
                    (Only this module may import org.rocksdb — enforced by test.)
engine-core/        SCD2 logic, WAL, txn manager, catalog, codecs.
engine-export/      Publisher + Finalizer (open column contract, exactly-once).
engine-durability/  WAL shipping, snapshots, restore, verify.
engine-cli/         picocli CLI; `mvn package` produces the fat chronodim.jar.
engine-testkit/     Reference SCD2 impl, generators, the full test suite.
engine-bench/       Standard benchmark workload (§10) with JSON report.
```

## Performance status

Measured on a small shared CI container (4 vCPU, containerized disk) — the §10
acceptance targets are defined for 8 physical cores + NVMe, so these are floors:

| metric | measured | §10 target (reference hw) |
|---|---|---|
| warm point read p50 / p99 | **21–30 µs / ~100 µs** | ≤ 50 µs / ≤ 1 ms ✅ |
| cold read p99 (after restart) | **113–137 µs** | ≤ 5 ms ✅ |
| **1M adjustments** (CDC mix vs 1M-row table) | **34.6 s = 28.9k applies/s sustained** | ≥ 100k/s on reference hw |
| **200k adjustments vs a 20M-row table** | **21.2 s = 9.4k applies/s** (4 GB store; deeper index, cold cache) | — |
| **200k × 100-column adjustments** (all real updates, 1M-row table) | **engine 11.4 s = 17.5k applies/s** (8 GB heap; 41 s end-to-end incl. parsing 400 MB of JSON) | — |
| bulk backfill | 64–67k rows/s (20M rows in 5.4 min); **1M × 100-col (2 GB JSONL) in 69 s, streamed in constant memory** | 50M ≤ 5 min (≈167k/s) |

The write path is sharded and batched (R-PERF-2/3): one `multiGet` resolves all
business keys per batch, entity chains are read lazily (latest-version-only for
plain appends), placement fans out across cores by key hash, and the hot
encoders reuse per-thread buffers. On this 4-vCPU box that took CDC apply from
10.3k/s to ~30k/s (3×); throughput scales with physical cores and fsync speed,
so reference hardware lands materially higher. Remaining headroom: Arrow-batch
scan output and the JMH zero-allocation audit (R-PERF-1/P5). Reproduce with
`chronodim bench -d <empty-dir> --rows 1000000 --changes 1000000 --json`.

**Memory sizing for wide rows.** `load` streams and runs in constant memory at
any file size. `apply` holds one atomic batch in memory by design — for wide
rows give the JVM roughly **4 GB of heap per 100k rows × 100 columns**
(`java -Xmx8g -jar chronodim.jar apply …` handled 200k × 100 columns; the
default heap on a 16 GB box did not) or split the file into several load_ids.
Too small a heap fails cleanly with the cause named, nothing partial is
committed.
