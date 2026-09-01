#!/bin/bash
# Launch all four processes on one machine, then run the collector.
# Used for development. For the lab demo, edit config.json and start each
# process by hand on its own node.

set -u
CFG=${1:-config.json}
OUT=$(python3 -c "import json,sys; print(json.load(open('$CFG')).get('output_dir','./out'))")

rm -rf "$OUT"
mkdir -p "$OUT"

echo "starting P1 P2 P3 first, then P0 (P0 drives the scenario)"
python3 main.py 1 "$CFG" > "$OUT/log_P1.txt" 2>&1 &
python3 main.py 2 "$CFG" > "$OUT/log_P2.txt" 2>&1 &
python3 main.py 3 "$CFG" > "$OUT/log_P3.txt" 2>&1 &
python3 main.py 0 "$CFG" > "$OUT/log_P0.txt" 2>&1 &

wait

echo
echo "=== per-process logs ==="
for i in 0 1 2 3; do
  echo
  echo "--- P$i ---"
  cat "$OUT/log_P$i.txt"
done

echo
python3 collector.py "$OUT"
