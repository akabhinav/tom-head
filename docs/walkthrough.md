# ChronoDim in five minutes — one customer, four events

Everything below is real engine output. You send plain data files; the engine
turns them into a **versioned timeline** instead of overwriting rows.

## 1. January — Ravi joins, risk LOW

```bash
echo '[{"customer_id":"R1","name":"Ravi","risk":"LOW","as_of":"2026-01-01T00:00:00Z"}]' > jan.json
chronodim apply jan.json -d db -t customer --load-id jan-load
```

Stored: `LOW, valid [Jan 1 → open)` — "open" means *still true today*.

## 2. March — the monthly feed says risk is now HIGH

A plain database UPDATE would overwrite LOW and lose it. ChronoDim **closes**
the old version and **appends** a new one:

```
risk=HIGH  valid [Mar 1 .. open)      <-- current
risk=LOW   valid [Jan 1 .. Mar 1)         (closed, kept forever)
```

"What was Ravi's risk on **Feb 15**?" → `chronodim asof --valid-time 2026-02-15…` → **LOW**.
That's SCD Type 2: updates never erase, they slice time.

## 3. April — a late correction: "actually MEDIUM since Feb 1"

Real feeds are late all the time. The engine **splits** the timeline and slots
the correction into its historical place (`late_arrival: split`):

```
risk=HIGH    valid [Mar 1 .. open)    <-- current
risk=MEDIUM  valid [Feb 1 .. Mar 1)       (late correction, slotted in)
risk=LOW     valid [Jan 1 .. Feb 1)       (LOW shortened, not deleted)
```

Same question now answers **MEDIUM**. And the engine still remembers what it
*used to* believe — the bitemporal read:

```bash
# as known just after the March load (before the correction): LOW
chronodim asof -t customer --valid-time 2026-02-15T00:00:00Z --tx-time <march-commit-time> customer_id=R1
# as known today: MEDIUM
chronodim asof -t customer --valid-time 2026-02-15T00:00:00Z customer_id=R1
```

Two clocks: *valid time* = when something was true in the business;
*transaction time* = when the engine learned it. A regulator asking "why did
your February report say LOW?" gets an exact, reproducible answer.

## 4. May — account closed (delete)

```bash
echo '[{"customer_id":"R1","as_of":"2026-05-01T00:00:00Z","_op":"delete"}]' > close.json
chronodim apply close.json -d db -t customer --load-id close-load
```

`get` now reports the customer doesn't exist — but nothing was physically
deleted. The delete is one more version (a tombstone):

```
DELETE   valid [May 1 .. open)
HIGH     valid [Mar 1 .. May 1)
MEDIUM   valid [Feb 1 .. Mar 1)
LOW      valid [Jan 1 .. Feb 1)
```

## The same story as a picture

```
what you send:            what the engine keeps:
Jan: LOW          ─────►  ──LOW──────────────────────────────►
Mar: HIGH         ─────►  ──LOW──────────┤──HIGH─────────────►
Apr: MEDIUM@Feb1  ─────►  ──LOW──┤MEDIUM─┤──HIGH─────────────►   (split, LOW kept)
May: delete       ─────►  ──LOW──┤MEDIUM─┤──HIGH──┤ ✝ closed
```

## Three things to remember

1. **Every apply is an upsert.** The engine compares tracked columns and decides
   insert / new version / no-op itself. Re-sending the same file with the same
   `--load-id` does nothing (safe retries).
2. **Nothing is overwritten or deleted.** Updates close-and-append, deletes are
   tombstones, corrections split. `history` even shows the superseded beliefs —
   the audit trail of what was known before each change.
3. **Three kinds of answers, always:** *now* (`get`), *any business date*
   (`asof --valid-time`), *any business date as known on any past day*
   (`asof --valid-time --tx-time`).
