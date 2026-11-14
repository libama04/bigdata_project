#!/usr/bin/env bash
# Runs the whole pipeline. Every step overwrites its own outputs, so after a
# failure simply re-run this script, or restart from one step: ./src/run_all.sh 3
set -euo pipefail
cd "$(dirname "$0")"
FROM="${1:-1}"

step() {
  local n="$1"; shift
  if [ "$n" -ge "$FROM" ]; then
    echo; echo ">>> [$(date -u '+%Y-%m-%d %H:%M:%S UTC')] step $n: $*"
    "$@"
  fi
}

step 1 python 01_ingest_raw.py
step 2 python 02_clean_spark.py
step 3 python 03_analyze_spark.py
step 4 python 04_verify_duckdb.py
step 5 python 05_benchmark.py
echo; echo ">>> [$(date -u '+%Y-%m-%d %H:%M:%S UTC')] pipeline finished"
