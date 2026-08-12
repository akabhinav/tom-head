# Scale drill: 15 tables, 58M rows, 5M adjustments, exact verification

Reproduces the large-scale SCD2 correctness drill (adjust row counts to your
hardware/disk; as run: t01 = 30M rows, t02..t15 = 2M each, ~5 GB store).

1. `./phaseA.sh` — creates the 15 tables and streams the seed loads.
2. `java -jar chronodim.jar ui -d db --port 8543 --no-publish &` then
   `python3 driver.py` — 50 runs x 100k adjustments through the HTTP applier.
   Every run's inserts/updates/no-ops/deletes/late-splits are computed BEFORE
   the apply and asserted against the manifest; the model is saved to
   model.pickle.
3. `python3 spot.py` (applier still up) — belief chains, interval contiguity,
   tombstones and as-of time travel on sampled keys, checked against the model.
4. Stop the applier, then per table:
   `chronodim scan -d db -t tNN --format csv | python3 verify_scan.py NN`
   — every current row diffed against the model (adjusted keys) or the seed
   formula (untouched keys); deleted keys must be absent; counts must match.
5. `chronodim verify -d db --json` twice — restart-replay fingerprints must be
   identical.

Result on a 4-vCPU container: seed 16 min, 50 runs in 4.1 min (~20k
adjustments/s end-to-end through HTTP+JSON), 14,725 spot checks and a 58M-row
full diff with zero mismatches, identical fingerprints across reopens.
