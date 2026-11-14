# Evidence

Each screenshot must show the Onyxia ID / namespace **and** the date and time. Every script
prints a banner line `=== step | namespace/user=... | host=... | <UTC time> | root=s3://... ===`:
keep it visible, together with the browser/OS clock when possible.

| File | Content |
|---|---|
| `01_s3_raw.png` | Onyxia file explorer (or `mc ls`) on `bigdata_project/raw/` |
| `02_ingest_trace.png` | terminal output of `01_ingest_raw.py` (size, sha256, timestamp) |
| `03_spark_clean_trace.png` | output of `02_clean_spark.py` with the data-quality table |
| `04_spark_analyze_trace.png` | output of `03_analyze_spark.py` (top routes, yearly table) |
| `05_spark_ui.png` | Spark UI jobs page for the run (optional) |
| `06_duckdb_verify_trace.png` | output of `04_verify_duckdb.py` ending with `VERIFICATION PASSED` |
| `07_benchmark_trace.png` | output of `05_benchmark.py` |
| `08_s3_processed.png` | Onyxia file explorer on `bigdata_project/processed/` (silver, gold, reports) |
| `09_k8s_job.png` | `kubectl get jobs,pods` and `kubectl logs job/tgv-verify` (if the job is used) |
