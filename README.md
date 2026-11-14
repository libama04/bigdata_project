# Big Data project - TGV punctuality by route (2018-2026)

**Authors:** LIBAMA David, `LASTNAME Firstname` <!-- TODO: second team member -->

**Prerequisites:** an Onyxia account with a personal S3 bucket; a *Jupyter-PySpark* or
*VSCode-PySpark* service (Python >= 3.10, Spark >= 3.5, Java, S3 variables `AWS_*` pre-set);
`pip install -r src/requirements.txt`; `kubectl` (already in the service) for the optional job.

**Dataset:** *Régularité mensuelle TGV par liaisons* (SNCF Voyageurs / AQST) -
<https://ressources.data.sncf.com/explore/dataset/regularite-mensuelle-tgv-aqst/> -
Open Database License (ODbL) - downloaded on `YYYY-MM-DD` <!-- TODO: date in raw/_ingest_metadata.json -->
- 2.9 MB, one CSV, 12,907 rows x 26 columns (one row per month, service and route).

**Problem statement:** Which TGV routes had the highest share of late arrivals between 2018
and 2026, and how did punctuality and the causes of delay evolve year by year? Rates are
recomputed from train counts (not averaged percentages) and checked by a second engine.

**S3 paths** (bucket = `$S3_BUCKET`, your Onyxia bucket):

| Layer | Path |
|---|---|
| Raw | `s3://$S3_BUCKET/bigdata_project/raw/` (CSV snapshot + `_ingest_metadata.json`) |
| Clean (Parquet, by year) | `s3://$S3_BUCKET/bigdata_project/processed/silver/tgv_clean/` |
| Final datasets | `s3://$S3_BUCKET/bigdata_project/processed/gold/{route_punctuality,yearly_punctuality,delay_causes_by_year}/` |
| Reports | `s3://$S3_BUCKET/bigdata_project/processed/{quality_report,verification,benchmark}/` |

**Reproduction** (terminal of the Onyxia PySpark service):

```bash
git clone <REPO_URL> && cd bigdata_project        # TODO: repository URL
pip install -r src/requirements.txt
export S3_BUCKET=<your-onyxia-bucket>             # AWS_* variables are already set by Onyxia
./src/run_all.sh                                  # runs steps 1 to 5 in order
```

| Step | Script | Tool | Output |
|---|---|---|---|
| 1 | `src/01_ingest_raw.py` | Python + s3fs | raw CSV on S3 (checks the 1-5 MB limit, sha256) |
| 2 | `src/02_clean_spark.py` | Spark | clean Parquet + data-quality report |
| 3 | `src/03_analyze_spark.py` | Spark | 3 final datasets |
| 4 | `src/04_verify_duckdb.py` | DuckDB (`src/sql/`) | recomputes rates from the raw CSV, fails on mismatch |
| 5 | `src/05_benchmark.py` | pandas / DuckDB / Spark | timings of the alternatives |

- **Restart:** every step overwrites its own output, so re-run the script, or resume with
  `./src/run_all.sh 3` (from step 3). Step 1 keeps the existing snapshot; the source is updated
  monthly, use `python src/01_ingest_raw.py --force` to refresh it (figures will then change).
- **Expired S3 token:** restart the Onyxia service (or renew the `AWS_*` variables) and re-run.
- **Optional Kubernetes job** (step 4 in a separate pod): commands in `k8s/verify-job.yaml`.
- **Local run without S3:** `export DATA_ROOT=/tmp/tgv` instead of `S3_BUCKET`.
- **Evidence:** numbered screenshots in `evidence/` (list in `evidence/README.md`).
