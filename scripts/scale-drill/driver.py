"""50 runs x 100k adjustments over 15 tables, with exact expected-counter
assertions per run and a full expectation model persisted for phase C."""
import json, pickle, sys, time, urllib.request

BASE = "http://127.0.0.1:8543"
SEG = ["A", "B", "C", "D"]
N = {i: (30_000_000 if i == 1 else 2_000_000) for i in range(1, 16)}
V0_EFF = "2025-06-01T00:00:00Z"
LATE_EFF = "2025-01-01T00:00:00Z"

def key(i): return "K%08d" % i
def v0(i):  return ("N-%d-0" % i, SEG[i % 4], float(i * 7 % 100000))
def eff_of_run(r): return "2025-07-%02dT00:00:00Z" % (r + 1) if r < 31 else "2025-08-%02dT00:00:00Z" % (r - 30)

def post(path, body):
    req = urllib.request.Request(BASE + path, data=body.encode(), method="POST")
    with urllib.request.urlopen(req, timeout=1800) as r:
        return json.loads(r.read())

# per-table model: adjusted[key_idx] = {"beliefs":[(eff,name,seg,amt)],"deleted":eff|None,"ver":n,"late":bool}
model = {t: {"adjusted": {}, "cursor": 0, "next_new": N[t], "updated_list": [], "spot": {}} for t in range(1, 16)}

def take(t, n):
    m = model[t]; a = m["cursor"]; m["cursor"] = a + n
    assert m["cursor"] <= N[t], "cursor exhausted"
    return range(a, a + n)

t_start = time.time()
for r in range(50):
    t = (r % 15) + 1
    tbl = "t%02d" % t
    m = model[t]
    eff = eff_of_run(r)
    first = r < 15
    n_upd_fresh = 80_000 if first else 70_000
    n_repeat = 0 if first else 5_000
    n_noop, n_ins, n_del = 8_000, 7_000, 5_000
    n_late = 0 if first else 5_000

    rows = []
    exp = {"rows_in": 0, "inserts": 0, "updates": 0, "no_ops": 0, "deletes": 0, "late_splits": 0}

    def upd_row(i):
        cur = m["adjusted"].get(i)
        ver = (cur["ver"] + 1) if cur else 1
        name, seg, amt = "N-%d-%d" % (i, ver), SEG[(i + ver) % 4], float(i * 7 % 100000 + ver)
        rows.append({"id": key(i), "name": name, "seg": seg, "amt": amt, "eff": eff})
        if cur is None:
            m["adjusted"][i] = {"beliefs": [(V0_EFF,) + v0(i)], "deleted": None, "ver": 0, "late": False}
            cur = m["adjusted"][i]
        cur["beliefs"].append((eff, name, seg, amt)); cur["ver"] = ver
        exp["updates"] += 1

    for i in take(t, n_upd_fresh):
        upd_row(i); m["updated_list"].append(i)
    for i in m["updated_list"][:n_repeat]:
        upd_row(i)
    for i in take(t, n_noop):  # resend v0 verbatim (values identical -> no-op)
        name, seg, amt = v0(i)
        rows.append({"id": key(i), "name": name, "seg": seg, "amt": amt, "eff": eff})
        exp["no_ops"] += 1
    for _ in range(n_ins):
        i = m["next_new"]; m["next_new"] += 1
        name, seg, amt = "NEW-%d" % i, SEG[i % 4], float(i % 999)
        rows.append({"id": key(i), "name": name, "seg": seg, "amt": amt, "eff": eff})
        m["adjusted"][i] = {"beliefs": [(eff, name, seg, amt)], "deleted": None, "ver": 0, "late": False}
        exp["inserts"] += 1
    for i in take(t, n_del):
        rows.append({"id": key(i), "eff": eff, "_op": "delete"})
        m["adjusted"][i] = {"beliefs": [(V0_EFF,) + v0(i)], "deleted": eff, "ver": 0, "late": False}
        exp["deletes"] += 1
    for i in take(t, n_late):  # late prepend: eff before the key's first interval
        name, seg, amt = "L-%d" % i, SEG[(i + 3) % 4], float(i % 777)
        rows.append({"id": key(i), "name": name, "seg": seg, "amt": amt, "eff": LATE_EFF})
        m["adjusted"][i] = {"beliefs": [(LATE_EFF, name, seg, amt), (V0_EFF,) + v0(i)],
                            "deleted": None, "ver": 0, "late": True}
        exp["inserts"] += 1; exp["late_splits"] += 1
    exp["rows_in"] = len(rows)
    assert exp["rows_in"] == 100_000

    body = json.dumps({"table": tbl, "load_id": "adj-run-%02d" % r,
                       "metadata": {"run": r, "driver": "scale-drill"}, "records": rows})
    t0 = time.time()
    man = post("/api/tables/%s/apply" % tbl, body)
    st = man["tables"][0]
    for k_, v_ in exp.items():
        assert st[k_] == v_, "RUN %d %s: %s expected %s got %s || %s" % (r, tbl, k_, v_, st[k_], st)
    print("run %02d %s ok: upd=%d ins=%d noop=%d del=%d late=%d in %.1fs (engine %sms)"
          % (r, tbl, st["updates"], st["inserts"], st["no_ops"], st["deletes"],
             st["late_splits"], time.time() - t0, man["wall_clock_millis"]), flush=True)

print("ALL 50 RUNS: exact SCD2 counter match, %.1f min total" % ((time.time() - t_start) / 60))
with open("model.pickle", "wb") as f:
    pickle.dump({t: {"adjusted": model[t]["adjusted"], "next_new": model[t]["next_new"]}
                 for t in model}, f)
print("PHASE_B_DONE")
