# ChronoDim Runbook — build, run, test end to end, verify in the UI

A copy-paste script for a brand-new machine or a brand-new teammate. Every
command below was executed exactly as written and the outputs shown are real.
Total time: about 10 minutes.

Conventions used throughout:

```bash
JAR=engine-cli/target/chronodim.jar     # the fat jar, built in step 1
DATA=./demo                             # the database directory for this drill
```

---

## 0. Prerequisites

| Need | Check | Expect |
|---|---|---|
| Java 21+ | `java -version` | `21` or newer |
| Maven 3.9+ | `mvn -version` | any recent 3.x |
| A browser | — | for section 8 |

No database server, no Docker, nothing else. ChronoDim is a library + CLI;
the "server" in section 8 is inside the jar.

## 1. Build and run the test suite

```bash
mvn test                        # full suite — differential, crash, concurrency, UI
mvn -DskipTests package         # builds engine-cli/target/chronodim.jar
```

Expect `BUILD SUCCESS` on both. The suite (84 tests) includes the correctness
proofs: every batch is compared against an independent reference SCD2
implementation, on both storage backends, plus crash-recovery, disaster-drill,
50-writer concurrency, and HTTP console tests. If `mvn test` is green, the
engine on this machine is trustworthy.

## 2. Create the database and a table

```bash
java -jar $JAR table create -f examples/customer.yaml -d $DATA
```

```json
{ "created": "customer" }
```

The directory `$DATA` now exists with `wal/`, `storage/`, `snapshots/`, `LOCK`. The table
config declared: business key `customer_id`, effective time from the
`updated_at` column, quality gates (`risk_score between 0 and 1000`,
`segment in (RETAIL, SME, CORP, PRIVATE)`), and publishing to
`./export/customer`.

## 3. Load the first batch (JSON) and read the receipt

```bash
java -jar $JAR apply examples/changes.json -d $DATA -t customer --load-id day1
```

Key fields of the returned manifest (your receipt — it is also stored forever):

```json
{
  "load_id": "day1", "txn_id": 1,
  "tables": [{ "table": "customer",
    "rows_in": 4, "inserts": 3, "updates": 0, "deletes": 1, "rejects": 0 }],
  "errors": []
}
```

Why 3 inserts + 1 delete from 4 rows: the file inserts C1, C2, C3 and then
deletes C2 — an insert-then-delete in one atomic batch.

**Checkpoint** — current state must be C1 and C3 only:

```bash
java -jar $JAR scan -d $DATA -t customer
```

```json
{"row":{"customer_id":"C1","name":"Alice", ... },"_valid_to":null,"_op":"INSERT","_is_current":true}
{"row":{"customer_id":"C3","name":"Carol", ... },"_valid_to":null,"_op":"INSERT","_is_current":true}
```

Point read:

```bash
java -jar $JAR get -d $DATA -t customer customer_id=C1
```

## 4. Day-2 changes from CSV (update + insert + delete)

```bash
java -jar $JAR apply examples/changes.csv -d $DATA -t customer --load-id day2 --json
```

```json
"rows_in": 3, "inserts": 1, "updates": 1, "deletes": 1
```

(C1 renamed to "Alice Smith" = update, C4 "Dan" = insert, C3 = delete via the
`_op` column. An empty CSV cell means NULL.)

## 5. The adjustment envelope + idempotency

This is the shape adjustment teams send — one JSON file carrying audit
metadata and the records:

```bash
cat > adj.json <<'EOF'
{
  "table": "customer",
  "load_id": "adj-2026-07-A",
  "approved_by": "jsmith",
  "reason": "quarterly risk adjustment",
  "records": [
    {"customer_id": "C1", "name": "Alice Smith", "segment": "RETAIL",
     "risk_score": 140.0, "exposure": "11000.00", "updated_at": "2026-07-10T00:00:00Z"}
  ]
}
EOF
java -jar $JAR apply adj.json -d $DATA --json
```

```json
"load_id": "adj-2026-07-A", "txn_id": 3, "updates": 1,
"metadata": {"approved_by": "jsmith", "reason": "quarterly risk adjustment"}
```

No `-t`, no `--load-id` on the command line — the envelope carries both, and
the free-form metadata is stored in the audit manifest.

**Now run the exact same command again:**

```bash
java -jar $JAR apply adj.json -d $DATA --json
```

Same manifest comes back — **same `txn_id: 3`**, nothing was re-applied. This
is the idempotency guarantee: a retried job, a double-clicked submit, a
re-delivered message can never double-apply a load.

## 6. Time travel — the whole point of SCD2

What did C1 look like on July 2nd (before any of the day-2/adjustment changes)?

```bash
java -jar $JAR asof -d $DATA -t customer --valid-time 2026-07-02T00:00:00Z customer_id=C1 --json
```

```json
{"row":{"name":"Alice","risk_score":120.5, ...},
 "_valid_from":"2026-07-01T00:00:00Z","_valid_to":"2026-07-04T00:00:00Z","_is_current":false}
```

The original "Alice" record, with its interval now closed at July 4 — even
though today's current row says "Alice Smith" with risk 140. Full history,
newest first, every version ever written:

```bash
java -jar $JAR history -d $DATA -t customer customer_id=C1 --json
```

Expect 5 version records for C1: 3 "latest beliefs" (one per valid_from) plus
the superseded originals that were closed. Records are never rewritten;
closing an interval writes a *new* superseding record.

## 7. Bad rows, integrity, restart durability

**Quality gate rejection** — a segment outside the allowed set:

```bash
echo '[{"customer_id":"C9","name":"Zed","segment":"UNKNOWN","risk_score":50.0,
        "exposure":"1.00","updated_at":"2026-07-11T00:00:00Z"}]' > bad.json
java -jar $JAR apply bad.json -d $DATA -t customer --load-id bad-1 --json
```

```json
"rejects": 1,
"errors": [{"table":"customer","row_index":0,
  "reason":"column 'segment' value 'UNKNOWN' not in [SME, PRIVATE, CORP, RETAIL]"}]
```

The row was skipped with a precise reason (this table uses
`on_row_error: skip_rows`; `quarantine` would park it for later re-apply,
`fail_batch` would reject the whole file atomically).

**Integrity fingerprint** — an order-independent hash of the entire state:

```bash
java -jar $JAR verify -d $DATA --json
```

```json
{"state_fingerprint":"9249b92cb29f4cc4","last_committed_txn":4,"durable_txn":4,
 "tables":{"customer":{"version_records":11,"entities":4,"current_rows":2}}, ...}
```

Write the fingerprint down. Note that **every command in this runbook opened
and closed the engine as a separate process** — you have already proven
restart durability a dozen times. Run `verify` again after any restart: the
fingerprint must be identical.

**Checkpoint + audit trail:**

```bash
java -jar $JAR snapshot -d $DATA --json        # {"txn_id":4,"path":"demo/snapshots/checkpoint-4"}
java -jar $JAR manifest list -d $DATA          # every load ever applied, with metadata
ls export/customer                              # the published cold tier:
# _chronodim_log  _contract.json  data  scd2_view.sql
```

## 8. Verify in the UI

```bash
java -jar $JAR ui -d $DATA
# ChronoDim console →  http://127.0.0.1:8420   (Ctrl-C to stop)
```

Open http://127.0.0.1:8420 and walk this checklist. Every item states what
you must see — if you see it, that subsystem is working.

| # | Do | Must see |
|---|---|---|
| 1 | Page loads | `customer` in the left sidebar; footer shows `txn 4 · 1 table(s)`; header chips show `key: customer_id`, `valid time: SOURCE_COLUMN (updated_at)` |
| 2 | **Data** tab | Exactly 2 rows: C1 "Alice Smith" (risk 140) and C4 "Dan". C2/C3 absent (deleted), C9 absent (rejected) |
| 3 | Tick *system columns* | `_valid_from`, `_valid_to` (null = open), `_txn_id`, `_op` appear |
| 4 | Type `2026-07-02T00:00:00Z` in *as of…* → Query | The past state: C1 (old "Alice", risk 120.5) **and C2 Bob** — who is deleted today but was alive then |
| 5 | Clear *as of*, click row C1 | **Entity** tab opens: 5 versions, newest interval `open`, changed cells highlighted yellow, deleted/superseded rows visible — the full append-only history |
| 6 | **Adjust** tab → Form: fill `C4 / Dan / PRIVATE / 61 / 92000.00 / 2026-07-12T00:00:00Z` → Apply batch | Green receipt: `updates: 1`, everything else 0. Data tab now shows the new values |
| 7 | Click **Apply batch** again *without changing the load_id* | Amber warning: *duplicate load_id — already applied earlier, original receipt returned (nothing re-applied)* |
| 8 | Adjust → JSON mode: paste the `adj.json` envelope from step 5 (with a **new** load_id) but set `"segment": "BOGUS"` | Receipt shows `rejects: 1` with the gate reason in the errors panel |
| 9 | **Audit** tab | One line per load (`day1`, `day2`, `adj-2026-07-A`, `bad-1`, your UI loads), with counts and the `approved_by`/`reason` metadata |
| 10 | **Quarantine** tab | "quarantine is empty 🎉" (this table skips bad rows instead) |
| 11 | **Health** tab → *Run verify* | `state_fingerprint` — must match section 7 if you haven't changed data since, and must change after step 6's update |
| 12 | **+ New table** → keep the template → Create | New table appears in the sidebar with an empty Data tab |

Scripted spot-checks of the same API the page uses (run while `ui` is up):

```bash
curl -s localhost:8420/api/tables                                        | head -c 200
curl -s "localhost:8420/api/tables/customer/rows?limit=5"                # current rows
curl -s "localhost:8420/api/tables/customer/rows?as_of=2026-07-02T00:00:00Z"   # time travel
curl -s "localhost:8420/api/tables/customer/history?customer_id=C1"     # 5 versions
curl -s localhost:8420/api/verify                                        # fingerprint
```

## 9. Reset / cleanup

```bash
rm -rf $DATA export adj.json bad.json     # the drill leaves nothing else behind
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `LockException: … already locked` | A second process opened the same `-d` directory (usually a running `ui`) | By design — one writer process per directory. Stop the other process, or do your work *through* the UI/API, which is the recommended shared pattern |
| `Address already in use` on `ui` | Port 8420 taken | `--port 9000` (or `--port 0` for an ephemeral port, printed at startup) |
| `unknown table 'x'` | Wrong `-d` — each data directory is its own database | Point `-d` (or `CHRONODIM_DATA`) at the right directory |
| `a load id is required` | Neither `--load-id` nor envelope `load_id` given | Add one — it is the idempotency key, never optional |
| Rows silently missing after apply | They were rejected or quarantined | Read the manifest's `rejects`/`quarantined` and `errors`; check the Quarantine tab |
| UI reachable from another machine? | It binds `127.0.0.1` and has no auth | Deliberate. Tunnel over SSH, or front it with an authenticating proxy before using `--host` |
| Exit code 2 from any command | Usage/config/validation error (bad YAML, bad flag) | Message on stderr says exactly what; exit 1 = engine error, 3 = not found |
