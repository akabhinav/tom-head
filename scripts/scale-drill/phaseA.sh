set -e
JAR=/home/user/tom-head/engine-cli/target/chronodim.jar
cd "$(dirname "$0")"
for i in $(seq 1 15); do
  T=$(printf "t%02d" $i)
  N=2000000; [ $i -eq 1 ] && N=30000000
  AVAIL=$(df --output=avail -B G / | tail -1 | tr -dc 0-9)
  [ "$AVAIL" -lt 5 ] && { echo "ABORT: only ${AVAIL}G left"; exit 9; }
  sed "s/TBL/$T/" table.yaml.tpl > cfg.yaml
  java -jar $JAR table create -f cfg.yaml -d db --no-publish >/dev/null 2>&1
  T0=$(date +%s)
  python3 gen_bulk.py $N > bulk.jsonl
  T1=$(date +%s)
  java -jar $JAR load bulk.jsonl -d db -t $T --load-id "seed-$T" --no-publish --json 2>/dev/null \
    | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['tables'][0]['rows_in']==$N, d; print('$T loaded', d['tables'][0]['inserts'])"
  T2=$(date +%s)
  rm -f bulk.jsonl
  echo "$T rows=$N gen=$((T1-T0))s load=$((T2-T1))s"
done
echo "PHASE_A_DONE"
