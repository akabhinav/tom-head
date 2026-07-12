#!/usr/bin/env bash
# ChronoDim — start the UI with demo data and smoke-test it end to end.
#
#   scripts/ui-demo.sh                # build if needed, seed, start UI, test, keep running
#   scripts/ui-demo.sh --ci           # same, but stop the UI afterwards (exit 0/1) — for CI
#   scripts/ui-demo.sh --port 9000    # custom port          (default 8420)
#   scripts/ui-demo.sh --data ./mydb  # custom data dir      (default ./ui-demo-data, reused if present)
#   scripts/ui-demo.sh --storage lsm  # pure-Java backend, no native library (default: rocksdb)
#
# Requires: java 21+, curl. maven only if the jar isn't built yet.

set -euo pipefail
cd "$(dirname "$0")/.."

PORT=8420
DATA=./ui-demo-data
CI=0
STORAGE=rocksdb
while [ $# -gt 0 ]; do
  case "$1" in
    --ci) CI=1 ;;
    --port) PORT=$2; shift ;;
    --data) DATA=$2; shift ;;
    --storage) STORAGE=$2; shift ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
  shift
done

JAR=engine-cli/target/chronodim.jar
BASE="http://127.0.0.1:$PORT"
PASS=0; FAIL=0

say()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
ok()   { PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad()  { FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

# assert "<description>" <actual> <expected-substring-or-value>
assert_eq() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (got '$2', want '$3')"; fi; }
assert_has() { if printf '%s' "$2" | grep -q "$3"; then ok "$1"; else bad "$1 (no '$3' in response)"; fi; }

# jget <json> <python-expr over d>  — tiny JSON probe
jget() { printf '%s' "$1" | python3 -c "import json,sys; d=json.load(sys.stdin); print($2)"; }

say "1/5 build"
if [ ! -f "$JAR" ]; then
  echo "  jar missing — building (first time only)…"
  mvn -q -DskipTests package
fi
echo "  using $JAR"

say "2/5 seed demo data in $DATA (storage: $STORAGE)"
run() {
  local err
  if err=$(java -jar "$JAR" "$@" --storage "$STORAGE" 2>&1 >/dev/null); then
    return 0
  fi
  local code=$?
  [ -n "$err" ] && printf '%s\n' "$err" >&2
  if [ "$STORAGE" = rocksdb ] && [ "$code" -gt 128 ]; then
    echo "  native crash (exit $code) — usually RocksDB's native library failing to load." >&2
    echo "  Try:  --storage lsm   (pure-Java backend, no native dependency)" >&2
  fi
  return "$code"
}
if [ ! -d "$DATA" ]; then
  run table create -f examples/customer.yaml -d "$DATA" >/dev/null
  run apply examples/changes.json -d "$DATA" -t customer --load-id demo-day1 --json >/dev/null
  run apply examples/changes.csv  -d "$DATA" -t customer --load-id demo-day2 --json >/dev/null
  echo "  created table 'customer' + 2 demo loads"
else
  echo "  reusing existing $DATA"
fi

say "3/5 start UI on port $PORT"
java -jar "$JAR" ui -d "$DATA" --port "$PORT" --storage "$STORAGE" >"$DATA-ui.log" 2>&1 &
UI_PID=$!
cleanup() { kill "$UI_PID" 2>/dev/null || true; wait "$UI_PID" 2>/dev/null || true; }
if [ "$CI" = 1 ]; then trap cleanup EXIT; fi
for i in $(seq 1 50); do
  curl -sf "$BASE/api/stats" >/dev/null 2>&1 && break
  kill -0 "$UI_PID" 2>/dev/null || { echo "UI process died — $DATA-ui.log:"; tail -5 "$DATA-ui.log"; exit 1; }
  sleep 0.2
done
echo "  up: $BASE (pid $UI_PID, log $DATA-ui.log)"

say "4/5 smoke tests"

PAGE=$(curl -s "$BASE/")
assert_has "console page serves"            "$PAGE" "ChronoDim Console"

TABLES=$(curl -s "$BASE/api/tables")
assert_has "catalog lists 'customer'"       "$TABLES" '"table":"customer"'

ROWS=$(curl -s "$BASE/api/tables/customer/rows?limit=50")
assert_eq  "current rows = 2 (C1, C4)"      "$(jget "$ROWS" "len(d['rows'])")" 2

ASOF=$(curl -s "$BASE/api/tables/customer/rows?as_of=2026-07-02T00:00:00Z")
assert_has "time travel sees deleted C2"    "$(jget "$ASOF" "[r['row']['customer_id'] for r in d['rows']]")" "C2"

HIST=$(curl -s "$BASE/api/tables/customer/history?customer_id=C1")
H=$(jget "$HIST" "len(d['versions'])")
if [ "$H" -ge 2 ]; then ok "C1 has history ($H versions)"; else bad "C1 history (got $H versions)"; fi

# Values must differ from whatever a previous run stored, or the engine
# (correctly) reports a no-op instead of an update — salt them with the run id.
LOAD="script-$(date +%s)"
SALT=$(( $(date +%s) % 500 ))
APPLY=$(curl -s -X POST "$BASE/api/tables/customer/apply?load_id=$LOAD" \
  -d '[{"customer_id":"C1","name":"Alice '"$LOAD"'","segment":"RETAIL","risk_score":'"$SALT"'.5,"exposure":"11500.00","updated_at":"2026-07-12T09:00:00Z"}]')
assert_eq  "apply via API counts 1 update"  "$(jget "$APPLY" "d['tables'][0]['updates']")" 1

DUP=$(curl -s -X POST "$BASE/api/tables/customer/apply?load_id=$LOAD" \
  -d '[{"customer_id":"C1","name":"EVIL OVERWRITE","segment":"RETAIL","risk_score":1.0,"exposure":"1.00","updated_at":"2026-07-12T10:00:00Z"}]')
assert_eq  "duplicate load_id not re-applied" "$(jget "$DUP" "d['already_applied']")" True
CUR=$(curl -s "$BASE/api/tables/customer/history?customer_id=C1")
assert_has "stored row is the winner's"     "$(jget "$CUR" "d['versions'][0]['row']['name']")" "Alice $LOAD"

BADROW=$(curl -s -X POST "$BASE/api/tables/customer/apply?load_id=$LOAD-bad" \
  -d '[{"customer_id":"C9","name":"Zed","segment":"NOPE","risk_score":1.0,"exposure":"1.00","updated_at":"2026-07-12T11:00:00Z"}]')
assert_eq  "quality gate rejects bad segment" "$(jget "$BADROW" "d['tables'][0]['rejects']")" 1

MAN=$(curl -s "$BASE/api/manifests?table=customer&limit=100")
assert_has "audit trail has our load"       "$MAN" "\"load_id\":\"$LOAD\""

VERIFY=$(curl -s "$BASE/api/verify")
FP=$(jget "$VERIFY" "d['state_fingerprint']")
if [ -n "$FP" ]; then ok "verify fingerprint: $FP"; else bad "verify returned no fingerprint"; fi

BADCFG=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/tables" -d 'table: 1 bad')
assert_eq  "bad config rejected with 400"   "$BADCFG" 400

say "5/5 result: $PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
  [ "$CI" = 1 ] || cleanup
  exit 1
fi
if [ "$CI" = 1 ]; then
  echo "CI mode — stopping UI."
else
  echo "UI is RUNNING → $BASE   (stop with: kill $UI_PID)"
  echo "Try: Data tab as-of 2026-07-02T00:00:00Z → deleted customer C2 is visible in the past."
fi
