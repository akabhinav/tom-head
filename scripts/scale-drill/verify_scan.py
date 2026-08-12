"""Streams `chronodim scan --format csv` and verifies EVERY current row of a
table against the independent model: adjusted keys must show their newest
belief, untouched keys must still be the exact seed formula, deleted keys must
not appear, and the total must match. CSV columns: id,name,seg,amt,eff,_valid_from."""
import pickle, sys

t = int(sys.argv[1])
N = 30_000_000 if t == 1 else 2_000_000
SEG = ["A", "B", "C", "D"]
with open("model.pickle", "rb") as f:
    m = pickle.load(f)[t]
adjusted = m["adjusted"]

expected_absent = sum(1 for a in adjusted.values() if a["deleted"])
expected_rows = N + (m["next_new"] - N) - expected_absent
seen = 0
seen_adjusted = 0
bad = 0

hdr = sys.stdin.readline()
assert hdr.strip().split(",")[:5] == ["id", "name", "seg", "amt", "eff"], hdr
for line in sys.stdin:
    parts = line.rstrip("\n").split(",")
    idx = int(parts[0][1:])
    name, seg, amt, eff, vf = parts[1], parts[2], float(parts[3]), parts[4], parts[5]
    a = adjusted.get(idx)
    if a is None:  # untouched: must equal the seed formula exactly
        ok = (name == "N-%d-0" % idx and seg == SEG[idx % 4]
              and amt == float(idx * 7 % 100000) and vf == "2025-06-01T00:00:00Z")
    else:
        seen_adjusted += 1
        if a["deleted"]:
            print("FAIL: deleted key present:", parts[:3]); bad += 1; continue
        e_eff, e_name, e_seg, e_amt = max(a["beliefs"])  # newest belief wins
        ok = (name == e_name and seg == e_seg and amt == e_amt and vf == e_eff)
    if not ok:
        bad += 1
        if bad <= 5: print("FAIL row:", parts, "model:", a)
    seen += 1

assert bad == 0, "%d bad rows" % bad
assert seen == expected_rows, "row count: got %d expected %d" % (seen, expected_rows)
print("t%02d OK: %d current rows all correct (%d adjusted, %d deleted absent)"
      % (t, seen, seen_adjusted, expected_absent))
