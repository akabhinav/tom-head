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

# 3. load data — a single JSON file, CSV, or JSON Lines all work
chronodim apply changes.json  -d ./dims -t customer --load-id load-2026-07-11   # JSON array
chronodim apply changes.csv   -d ./dims -t customer --load-id load-2026-07-12   # CSV w/ header
cat stream.jsonl | chronodim apply --stdin -d ./dims -t customer --load-id load-2026-07-13

# rows are upserts; mark deletes with "_op": "delete" (JSON) or an _op column (CSV)
# re-running the same --load-id is a safe no-op (idempotent)

# 4. query
chronodim get     -d ./dims -t customer customer_id=C42
chronodim asof    -d ./dims -t customer --valid-time 2025-12-31T23:59:59Z customer_id=C42
chronodim asof    -d ./dims -t customer --valid-time 2025-12-31T23:59:59Z \
                  --tx-time 2026-01-15T00:00:00Z customer_id=C42     # bitemporal: "as we believed on Jan 15"
chronodim history -d ./dims -t customer customer_id=C42
chronodim scan    -d ./dims -t customer --format csv -o current.csv

# 5. operate
chronodim verify   -d ./dims                       # state fingerprint + counts
chronodim snapshot -d ./dims --object-store /mnt/backup
chronodim restore  --from /mnt/backup --target-dir ./dims-restored [--as-of-txn N]
chronodim manifest show load-2026-07-11 -d ./dims  # immutable audit record of that load
chronodim bench    -d ./bench-dir                  # standard benchmark on this box
```

Every command accepts `--json` for machine-readable output.

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

```bash
mvn test          # runs the whole suite
```

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

Correctness and architecture are complete; the §10 acceptance run belongs on the
reference hardware (8 physical cores, 64 GB, NVMe). Measured on a small shared
CI container (`chronodim bench --rows 500000 --changes 100000`, RocksDB backend):

| metric | result | §10 target (reference hw) |
|---|---|---|
| warm point read p50 / p99 | **21 µs / 82 µs** | ≤ 50 µs / ≤ 1 ms ✅ |
| bulk backfill | 60k rows/s | 50M ≤ 5 min (≈167k/s) — pending hot-path pass |
| CDC apply | 10k applies/s | ≥ 100k/s — pending hot-path pass |

The remaining performance work is the planned Phase-5 hardening: sharded apply
(N shards by key hash), batched keymap lookups via `multiGet`, buffer reuse on
the coercion path, and the JMH `-prof gc` zero-allocation audit. None of it
changes any public API or on-disk contract.
