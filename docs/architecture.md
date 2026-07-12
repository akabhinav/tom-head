# ChronoDim — internal architecture (the whiteboard walkthrough)

> The visual version of this document lives in `docs/architecture.html`
> (open it in a browser). This file is the same walkthrough in plain text,
> so it renders on GitHub and survives any toolchain.

**The one idea everything follows from:** ChronoDim is a notebook that
**never uses an eraser**. When a value changes, the old line gets an end date
and a new line is written below it. Deletes write "ended here" instead of
tearing out the page. Every component below exists to do that *fast*,
*crash-safely*, and in a way *Databricks can read*.

---

## 1. The big picture — one process, three jobs

No server, no external database. One Java process around one data folder.

```
                          ┌──────────────────────────────────────────┐
                          │        THE ENGINE (one Java process)     │
 your files               │                                          │      query answers
 adj.json ───────────────►│  ┌────────────────┐                      │───►  get / asof / history
 .csv / .jsonl            │  │ Reader+Checker │ parse, types, gates  │      (~21µs warm)
 envelope                 │  └───────┬────────┘                      │
                          │          ▼                               │
                          │  ┌────────────────┐                      │
                          │  │   SCD2 Brain   │ insert? new version? │
                          │  └───────┬────────┘ no-op? splice?       │
                          │          ▼                               │
                          │  ┌────────────────┐    ┌╌╌╌╌╌╌╌╌╌╌╌╌╌┐   │      published files
                          │  │ WAL (journal)  │╌╌╌►┆  Publisher  ┆───│───►  __START_AT/__END_AT
                          │  └───────┬────────┘    └╌╌╌╌╌╌╌╌╌╌╌╌╌┘   │      → Databricks/Spark/DuckDB
                          │          ▼                               │
                          │  ┌────────────────┐    ┌╌╌╌╌╌╌╌╌╌╌╌╌╌┐   │      backups
                          │  │ Hot Store      │    ┆   Shipper   ┆───│───►  snapshots + journal copy
                          │  │ (RocksDB)      │    └╌╌╌╌╌╌╌╌╌╌╌╌╌┘   │      → object store
                          │  └────────────────┘                      │
                          └──────────────────────────────────────────┘

  solid arrows  = synchronous (a caller is waiting; happens before "done")
  dashed boxes  = background workers (async; catch up within seconds)
```

## 2. The write path — follow one file through

Finance drops `adj.json` (an envelope: metadata + records). In order:

```
adj.json ──► check rows ──► decide ──► JOURNAL + fsync ──► hot store ──► receipt
             types+gates    (SCD2)          ★                 │
                                            │                 └─ queries see it now
                                            └─ ★ moment of truth: after this fsync,
                                                 a power cut CANNOT lose this batch
```

1. **Read & understand.** Parse JSON/CSV/JSONL; pull `load_id`, `effective_at`,
   metadata from the envelope. Seen this `load_id` before? Stop, return the
   original receipt — re-sending a file can never double-apply.
2. **Check every row.** Coerce to declared column types; run quality gates.
   Bad rows → reject or quarantine (per-table policy).
3. **Look up each key's story so far** (its version chain in the hot store).
4. **Decide per row** — see the decision flow below.
5. **Journal it.** The whole batch + its audit receipt = ONE record in the WAL,
   fsync'd. This is the crash-proof moment.
6. **Update the hot store** atomically (readers never see half a batch).
7. **Return the receipt** (audit manifest) — kept forever, queryable by load id.

Why journal FIRST? If we updated the hot store first and crashed before
journaling, the change would exist nowhere durable. Journal-first means the
worst crash can only lose work we never acknowledged.

## 3. The SCD2 brain — one decision per row

Nobody sends operation names. Every row is just "here is the truth about C1":

```
  row arrives ──► key seen before? ──no──► INSERT version 1
                       │yes
                       ▼
              tracked values changed? ──no──► NO-OP (write nothing)
                       │yes
                       ▼
              effective date OLDER than latest? ──no──► close old version at new date
                       │yes                             + add new open version
                       ▼
              SPLICE into history (late arrival:
              neighbours' dates adjusted, nothing lost)

  row marked delete / absent from a --full-snapshot
              ──► TOMBSTONE version ("ended here"), history kept
```

"Changed?" is a hash fingerprint over `tracked_columns` only; columns in
`ignored_columns` (feed timestamps etc.) never create versions.

## 4. How a customer's story is stored

Two lookups: the **keymap** turns a business key into a compact internal
address; that address prefixes the entity's **whole chain**, newest first:

```
 customer_id="R1" ──keymap──► [table 7 | hash 0x9F3A… | #0]
                                        │  one prefix scan = the whole story
                                        ▼
   ┌──────────────────────────────────────────────────────┐
   │ addr | May 1 | HIGH   | ends: open   ◄─ top = CURRENT │
   │ addr | Feb 1 | MEDIUM | ends: May 1                   │
   │ addr | Jan 1 | LOW    | ends: Feb 1                   │
   │ addr | Jan 1 | LOW    | ends: open   ◄─ superseded    │
   └──────────────────────────────────────────────────────┘  belief, kept for audit

 timeline:  Jan ──LOW──┤ Feb ──MEDIUM──┤ May ──HIGH──────► today
```

Keyspaces (mental models):

| keyspace | maps | think of it as |
|---|---|---|
| `DATA` | address+dates → version record | the notebook pages |
| `KEYMAP` | business key → address | the index at the back |
| `HASHREG` | address → full key | hash-collision safety net |
| `MANIFEST`/`LOADID` | receipts by txn / load id | the filing cabinet |
| `QUAR` | rows that failed checks | the "needs review" tray |
| `META` | table configs | the table of contents |

## 5. Reading time back — three questions, one scan

- **get** (now?) — top record; open + not a tombstone → answer.
- **asof** (true on Feb 15?) — first record with start ≤ Feb 15; check the end.
- **asof --tx-time** (what did we *believe* in March?) — same walk, skipping
  records the engine learned after March. Works because superseded records
  are never erased. Two clocks: *valid time* (true in the business) vs
  *transaction time* (when the engine found out) = **bitemporal**.

## 6. When the machine dies

```
 journal:  [txn 1 │ … │ txn 850 │ txn 851 │ … │ txn 900]
                            ▲
                 watermark: hot store safely flushed ≤ 850

 💥 crash: memory gone, txns 851–900 not flushed by the hot store
 restart:  read watermark(850) ──► replay 851–900 from the journal ──► all back
```

Replaying twice is harmless (same keys, same values), so recovery can never
over- or under-apply. The Shipper also copies journal + snapshots to an object
store — losing the whole disk is recoverable (`chronodim restore`). Local WAL
stays bounded via `snapshot --prune-wal` (only segments that are durable,
published, AND shipped are deleted; the archive keeps everything).

## 7. The cold tier — what Databricks sees

```
 export/customer/
   _contract.json                         ← column template, machine-readable
   scd2_view.sql                          ← paste into Databricks: point-in-time SQL
   _chronodim_log/00…01.json              ← commit log = exactly-once, no duplicates
   data/region=EU/year=2026/part-….jsonl  ← Hive partitions, Spark prunes them
   finalized/…/current-….jsonl            ← materialized "active rows only"
```

Row shape = payload + `__START_AT` + `__END_AT` (END null = ACTIVE). Names are
per-table configurable (`column_style: databricks` or custom `scd2_columns`).
Crash between part file and log entry? The orphan file is swept at restart;
readers trust the log → exactly once. The engine is the only writer here.

## 8. The code map

| module | owns | box in chapter 1 |
|---|---|---|
| `engine-api` | public types (`Engine`, `TableConfig`, `Version`, `DataType`) | the doorway |
| `engine-cli` | `chronodim` commands, file readers, envelope | your files → Reader |
| `engine-core` | SCD2 brain, WAL, catalog, codecs, `EngineImpl` | Reader · Brain · Journal |
| `engine-storage` | `StorageEngine` SPI + RocksDB + pure-Java LSM | Hot Store (swappable) |
| `engine-export` | Publisher, Finalizer, column templates | Publisher (dashed) |
| `engine-durability` | Shipper, snapshots, restore, WAL pruning | Shipper (dashed) |
| `engine-testkit` | independent reference SCD2 + 75-test suite | the proof |
| `engine-bench` | standard performance workload | the stopwatch |

### Glossary

| word | plain meaning |
|---|---|
| upsert | "here's the truth" — engine decides new / changed / nothing |
| WAL / journal | append-only diary written before anything else |
| fsync | force bytes physically onto disk, not just promised |
| tombstone | a version saying "ended here" — delete without erasing |
| watermark | "hot store safely holds ≤ txn N" — replay starts after it |
| idempotent | same load twice = once (thanks to `load_id`) |
