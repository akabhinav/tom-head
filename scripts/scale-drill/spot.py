"""Deep SCD2 checks through the API on sampled keys per table: belief chains
(dedup per valid_from) must match the model exactly — values, interval
contiguity, open head, tombstones — and as-of time travel must return the
right historical values."""
import json, pickle, urllib.request, urllib.parse, sys

BASE = "http://127.0.0.1:8543"
SEG = ["A", "B", "C", "D"]
with open("model.pickle", "rb") as f:
    MODEL = pickle.load(f)

def hist(tbl, k, asof=None):
    q = "id=" + k + (("&as_of=" + urllib.parse.quote(asof)) if asof else "")
    with urllib.request.urlopen("%s/api/tables/%s/history?%s" % (BASE, tbl, q), timeout=120) as r:
        return json.loads(r.read())["versions"]

checks = fails = 0
def ck(cond, msg):
    global checks, fails
    checks += 1
    if not cond:
        fails += 1
        if fails <= 8: print("FAIL:", msg)

for t in range(1, 16):
    tbl = "t%02d" % t
    N = 30_000_000 if t == 1 else 2_000_000
    m = MODEL[t]["adjusted"]
    multi   = [i for i, a in m.items() if a["ver"] > 1][:40]
    late    = [i for i, a in m.items() if a["late"]][:40]
    deleted = [i for i, a in m.items() if a["deleted"]][:40]
    single  = [i for i, a in m.items() if a["ver"] == 1 and not a["late"] and not a["deleted"]][:40]
    untouched = list(range(N - 20, N))

    for i in multi + late + deleted + single:
        a = m[i]
        vs = hist(tbl, "K%08d" % i)
        beliefs, seen_vf = [], set()
        for v in vs:  # newest-first; first record per valid_from = latest belief
            if v["_valid_from"] in seen_vf: continue
            seen_vf.add(v["_valid_from"]); beliefs.append(v)
        exp = sorted(a["beliefs"], reverse=True)
        if a["deleted"]:
            ck(beliefs[0]["_op"] == "DELETE" and beliefs[0]["_valid_from"] == a["deleted"],
               "%s K%d tombstone head" % (tbl, i))
            data_beliefs = beliefs[1:]
        else:
            ck(beliefs[0]["_valid_to"] is None, "%s K%d newest open" % (tbl, i))
            data_beliefs = beliefs
        ck(len(data_beliefs) == len(exp), "%s K%d belief count %d vs %d" % (tbl, i, len(data_beliefs), len(exp)))
        for b, (e_eff, e_name, e_seg, e_amt) in zip(data_beliefs, exp):
            r = b["row"]
            ck(b["_valid_from"] == e_eff and r["name"] == e_name and r["seg"] == e_seg
               and float(r["amt"]) == e_amt, "%s K%d belief %s vs %s" % (tbl, i, b["_valid_from"], e_eff))
        for j in range(1, len(beliefs)):  # contiguity: older closes where newer opens
            ck(beliefs[j]["_valid_to"] == beliefs[j - 1]["_valid_from"],
               "%s K%d gap at %s" % (tbl, i, beliefs[j]["_valid_from"]))

    for i in multi[:15]:  # time travel: before any adjustment -> seed values
        v = hist(tbl, "K%08d" % i, asof="2025-06-15T00:00:00Z")
        ck(len(v) == 1 and v[0]["row"]["name"] == "N-%d-0" % i, "%s K%d asof pre-adjust" % (tbl, i))
    for i in late[:15]:   # inside the late interval -> late values; key without late -> nothing
        v = hist(tbl, "K%08d" % i, asof="2025-03-01T00:00:00Z")
        ck(len(v) == 1 and v[0]["row"]["name"] == "L-%d" % i, "%s K%d asof late window" % (tbl, i))
    for i in untouched[:5]:
        v = hist(tbl, "K%08d" % i, asof="2025-03-01T00:00:00Z")
        ck(len(v) == 0, "%s K%d untouched has no pre-seed state" % (tbl, i))
    print(tbl, "spot ok", flush=True)

print("SPOT: %d checks, %d failures" % (checks, fails))
sys.exit(1 if fails else 0)
