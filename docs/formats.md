# ChronoDim on-disk formats (frozen contracts)

Every structure below is **version-stamped from day one**; changing any of them
requires migration tooling. All integers are **big-endian** unless noted (the
WAL frame is little-endian-free: it also uses big-endian fields; "frozen" refers
to layout, not endianness notes in the original sketch).

Contents:

1. [Database directory layout](#1-database-directory-layout)
2. [WAL record format](#2-wal-record-format) (R-WAL-2)
3. [KV key encoding](#3-kv-key-encoding) (R-STORE-2)
4. [KV value layout](#4-kv-value-layout) (R-STORE-3)
5. [Storage backends](#5-storage-backends)
6. [Audit manifest schema](#6-audit-manifest-schema) (R-APPLY-7)
7. [Snapshot manifest schema](#7-snapshot-manifest-schema) (R-DUR-2)
8. [Published column contract](#8-published-column-contract) (R-PUB-3)

---

## 1. Database directory layout

```
<data-dir>/
  LOCK              single-writer advisory lock (pid/host recorded)
  wal/              wal-<%016x start txn>.log segments
  storage/          hot store (RocksDB files, or LSM seg-*.cds + MANIFEST)
  snapshots/        local checkpoints created by `snapshot`
  tmp/              backfill spill files (transient)
```

## 2. WAL record format

Segmented log, default 64 MB per segment, named `wal-<%016x>.log` by the first
txn id that may appear in the segment.

Record frame:

```
[u32 crc32c][u32 len][u8 record_version=1][u8 type][u64 txn_id][payload...]
```

- `len` counts `record_version + type + txn_id + payload` (i.e. 10 + payload).
- `crc32c` covers `len` through the end of payload.
- Types: `1 TXN_COMMIT`, `2 BACKFILL_BEGIN`, `3 BACKFILL_END`, `4 SNAPSHOT_MARK`,
  `5 CONFIG_CHANGE`, `6 PUBLISH_MARK`.

`TXN_COMMIT` / `CONFIG_CHANGE` payload:

```
[u32 mutation_count]
  repeat: [u8 op 0=put 1=delete][u32 klen][u32 vlen][key][value if put]
[u32 manifest_len][audit manifest JSON, utf-8]
```

Recovery contract (R-WAL-3):

- Replay applies mutations of records with `txn_id >` the storage engine's
  durable watermark; raw-KV replay is idempotent.
- A torn record at the tail of the **last** segment (bad length running past
  EOF, or CRC failure on the final frame) is truncated cleanly.
- A CRC failure followed by more data, or any corruption in a non-final
  segment, is **interior corruption**: recovery fails loudly; restore from
  snapshot + shipped WAL.
- `txn_id` is monotonic, durable, never reused (R-WAL-4).

Group commit (R-WAL-1): appends are sequenced; a dedicated syncer thread
batches fsyncs (default window 2 ms / 512 txns); commits acknowledge only after
their sequence is covered by an fsync.

## 3. KV key encoding

First byte selects the keyspace:

| ks | name | layout after the ks byte |
|----|------|--------------------------|
| 0x01 | DATA | `[u32 table_id][u64 key_hash][u16 disambig][u64 desc(valid_from)][u64 desc(tx_time)]` |
| 0x02 | KEYMAP | `[u32 table_id][business-key bytes]` → `[u64 key_hash][u16 disambig]` |
| 0x03 | META | `[u8 sub][name]` → catalog JSON (sub=1: table config) / counters (sub=2) |
| 0x04 | MANIFEST | `[u64 txn_id]` → audit manifest JSON |
| 0x05 | LOADID | `[utf8 load_id]` → `[u64 txn_id]` |
| 0x06 | QUAR | `[u32 table_id][u64 seq]` → quarantine entry JSON |
| 0x07 | HASHREG | `[u32 table_id][u64 key_hash][u16 disambig]` → business-key bytes |

- `desc(x) = ~(x ^ Long.MIN_VALUE)` — an order-preserving transform for signed
  epoch-micros, inverted so **newest sorts first**. Entity chains therefore
  stream `valid_from DESC, tx_time DESC`, and "latest knowledge" is simply the
  first record of each `valid_from` group.
- `disambig` resolves xxHash64 collisions of different business keys; the full
  business key is stored in the value and in HASHREG. *(Deliberate deviation
  from the original sketch, which placed `disambig` last: placing it before the
  temporal fields makes one entity's whole chain a contiguous prefix scan.)*
- Business-key bytes: for each key column in config order:
  `[u8 type_tag][value]` where values encode as in §4's payload encoding
  (strings/bytes length-prefixed).

## 4. KV value layout

DATA values — fixed header at fixed offsets, then business key, then payload:

```
[u8 value_version=1][u8 op 1=INSERT 2=UPDATE 3=DELETE]
[i64 valid_to]              Long.MAX_VALUE = open
[i64 tx_time][u64 txn_id]
[u32 schema_version][u64 attr_hash]
[u16 bk_len][business-key bytes]
payload: per column of schema(schema_version), in declared order:
  [u8 present 0|1] then, when present:
    string/bytes:      [u32 len][bytes]        (strings are UTF-8)
    long/timestamp(_ntz): [i64]
    double:            [u64 raw IEEE bits]
    float:             [u32 raw IEEE bits]
    boolean:           [u8]
    date/int:          [i32]
    smallint:          [i16]
    tinyint:           [i8]
    decimal(p,s):      [u32 len][unscaled two's-complement bytes]  (scale from schema)
    array<T>:          [u32 count] count × ([u8 present][T])
    map<K,V>:          [u32 count] count × ([K key][u8 present][V])   (keys non-null scalars)
    struct<...>:       per declared field: [u8 present][field type]
```

Type tags (business-key encoding and attr-hash canonical form) are the
ColumnType ordinal + 1 and are **append-only**: STRING=1, LONG=2, DOUBLE=3,
BOOLEAN=4, DATE=5, TIMESTAMP=6, DECIMAL=7, BYTES=8, INT=9, SMALLINT=10,
TINYINT=11, FLOAT=12, TIMESTAMP_NTZ=13, ARRAY=14, MAP=15, STRUCT=16.

**Superseding, not overwriting**: closing or correcting a version writes a
*new* record with the same `valid_from` and the current `tx_time` (patched
header, identical payload bytes). Prior beliefs remain on disk — this is what
makes the bitemporal read (R-READ-5) a pure scan with no WAL reconstruction.
*(Deliberate deviation from the original in-place `valid_to` rewrite sketch,
which would have destroyed prior belief.)*

`attr_hash` = xxHash64 over `[u8 type_tag][u8 null_flag][value bytes]` of each
tracked column in config order.

## 5. Storage backends

Both implement the same `StorageEngine` SPI; everything above is
backend-agnostic (G7). The engine WAL is the durability authority for both; the
backend persists a **durable txn watermark** that recovery replays after.

**RocksDB** (default): RocksDB's own WAL disabled; watermark key
(`0xFF 'durable'`) written immediately before every flush; bloom filters +
block cache; zstd at the bottom level.

**Pure-Java LSM**: `storage/MANIFEST` (text, CRC32C-tailed) lists live
segments newest-first plus `durableTxn` and `nextSegId`. Segment file
(`seg-*.cds`):

```
"CDS1"
entries:  [u32 klen][i32 vlen (-1 = tombstone)][key][value]   sorted, unique
index:    [u64 count][u32 sparse_interval=64][u32 sparse_count]
          sparse_count × ([u32 klen][key][u64 entry_offset])
bloom:    [u32 words][u32 hashes=7][words × u64]
footer:   [u64 index_off][u64 bloom_off][u64 count][u8 version=1][u32 crc32c]["CDS1"]
```

Orphan segments not listed in MANIFEST are removed at open (crash between
segment write and manifest swap).

## 6. Audit manifest schema

JSON, stored at `MANIFEST[txn_id]`, indexed by `LOADID[load_id]`, and embedded
in the TXN_COMMIT WAL record itself:

```json
{
  "format_version": 1,
  "load_id": "load-2026-07-11",
  "txn_id": 4711,
  "started_at_micros": 1786543210000000,
  "wall_clock_millis": 1834,
  "wal_segment": "wal-0000000000001234.log",
  "wal_start_offset": 8388608,
  "wal_end_offset": 8390120,
  "backfill": false,
  "tables": [{
    "table": "customer", "config_hash": "9f2c...", "schema_version": 2,
    "rows_in": 100000, "inserts": 1200, "updates": 71000, "no_ops": 25000,
    "deletes": 800, "late_splits": 1500, "rejects": 3, "quarantined": 0
  }],
  "errors": [{"table": "customer", "row_index": 4521, "reason": "risk_score ... outside [0, 1000]"}]
}
```

`errors` is capped (default 100 entries); counters are always exact.

## 7. Snapshot manifest schema

`snapshots/snap-<%020d txn>.json` next to `snap-<txn>.zip` in the object store:

```json
{
  "format_version": 1,
  "txn_id": 4711,
  "files": {"MANIFEST": "xxhash64-hex", "seg-000000000001.cds": "..."},
  "zip_bytes": 123456789,
  "created_ms": 1786543210000
}
```

Restore verifies every file's xxHash64 before use. Shipped WAL lives under
`wal/` in the store: closed segments whole, the active segment as
`<segment>.chunk-<offset>` pieces concatenated in offset order at restore.

## 8. Published column contract

Every published row = the table's payload columns (rendered: timestamps/dates
ISO-8601 UTC, decimals as plain strings, bytes as base64) plus:

| column | type | meaning |
|---|---|---|
| `_valid_from` | timestamp | validity start (inclusive) |
| `_valid_to` | timestamp / null | validity end (exclusive); null = open *as of this belief* |
| `_tx_time` | timestamp | when the engine committed this belief |
| `_txn_id` | long | engine transaction id |
| `_op` | string | INSERT / UPDATE / DELETE |
| `_schema_version` | int | schema the record was written under |

Layout at each `publish.location`:

```
_contract.json        machine-readable descriptor of the above (incl. partition_by)
scd2_view.sql         LEAD()-based view template (latest belief per (bk, _valid_from))
_chronodim_log/       %020d.json commit entries — the reader's source of truth
data/[col=value/...]part-*.jsonl   append-only version records, exactly-once via
                      the log; Hive-style partition dirs when publish.partition_by
                      is set (values percent-encoded, null → __HIVE_DEFAULT_PARTITION__;
                      partition values are ALSO kept in the row payload)
finalized/[col=value/...]log-*.jsonl      consolidated full log (after `finalize`)
finalized/[col=value/...]current-*.jsonl  materialized current snapshot with _is_current
```

Exactly-once (R-PUB-2): the publisher's watermark is the max `txn_to` in the
commit log; parts are written tmp→atomic-rename before their log entry; parts
not referenced by any log entry are swept at startup. A crash between hot
commit and publish causes re-publish, never duplicate rows.

The v1 part format is JSON Lines. The part writer is pluggable: the Delta Lake
writer (Parquet files + `_delta_log` via Delta Kernel Java, registerable in
Unity Catalog with `CREATE TABLE ... USING DELTA LOCATION`) implements this
same contract — column names and semantics above are frozen precisely so parts
can switch format without breaking readers.
