"""Step 4 - independent verification of the Spark results with DuckDB.

DuckDB re-reads the RAW csv (not the Spark silver layer) and recomputes the
yearly and per-route late-arrival rates with the SQL in src/sql/. The figures
are then compared with the Spark gold datasets. Exit code 1 on any mismatch.

  python src/04_verify_duckdb.py
"""
import json
import pathlib
import sys
from datetime import datetime, timezone

import duckdb

import common as c

SQL_DIR = pathlib.Path(__file__).parent / "sql"
TOLERANCE = 1e-9


def query(con, sql):
    """Run a statement and fully consume its result."""
    return con.execute(sql).fetchall()


def compare(con, name, sql_file, gold, keys):
    query(con, f"CREATE OR REPLACE TEMP TABLE check_{name} AS {(SQL_DIR / sql_file).read_text().rstrip().rstrip(';')}")
    query(con, f"CREATE OR REPLACE TEMP TABLE gold_{name} AS SELECT * FROM read_parquet('{c.path(*gold)}/*.parquet')")
    join = " AND ".join(f"g.{k} = d.{k}" for k in keys)
    row = query(con, f"""
        SELECT
            (SELECT COUNT(*) FROM gold_{name})                           AS rows_spark,
            (SELECT COUNT(*) FROM check_{name})                          AS rows_duckdb,
            COUNT(*)                                                     AS rows_matched,
            COALESCE(SUM(CAST(g.trains_circulated <> d.trains_circulated AS INT)), 0) AS count_mismatches,
            COALESCE(SUM(CAST(g.late_arrivals <> d.late_arrivals AS INT)), 0)         AS late_mismatches,
            COALESCE(MAX(ABS(g.late_arrival_rate - d.late_arrival_rate)), 0)          AS max_abs_rate_diff
        FROM gold_{name} g JOIN check_{name} d ON {join}
    """)[0]
    result = dict(zip(
        ["rows_spark", "rows_duckdb", "rows_matched", "count_mismatches", "late_mismatches", "max_abs_rate_diff"],
        row))
    result["ok"] = (
        result["rows_spark"] == result["rows_duckdb"] == result["rows_matched"] > 0
        and result["count_mismatches"] == 0
        and result["late_mismatches"] == 0
        and result["max_abs_rate_diff"] <= TOLERANCE
    )
    return result


def main() -> None:
    c.banner("04 verify (DuckDB)")
    fs = c.filesystem()
    con = duckdb.connect()
    if c.is_s3():
        con.register_filesystem(fs)  # S3 access through s3fs, no DuckDB extension needed
    print(f"DuckDB {duckdb.__version__}")

    query(con, f"""
        CREATE TEMP TABLE raw AS
        SELECT * FROM read_csv('{c.path(*c.RAW)}', delim=';', header=true, quote='"', escape='"')
    """)
    raw_rows = query(con, "SELECT COUNT(*) FROM raw")[0][0]

    report = {
        "verified_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "engine": f"duckdb {duckdb.__version__}",
        "raw_rows": raw_rows,
        "tolerance": TOLERANCE,
        "yearly_punctuality": compare(con, "yearly", "yearly_late_rate.sql", c.GOLD_YEAR, ["year", "service"]),
        "route_punctuality": compare(con, "route", "route_late_rate.sql", c.GOLD_ROUTE,
                                     ["gare_depart", "gare_arrivee"]),
    }
    report["all_ok"] = report["yearly_punctuality"]["ok"] and report["route_punctuality"]["ok"]

    print("Spot check - late-arrival rate per year, all services (DuckDB, from raw):")
    print(f"{'year':>6}{'trains_circulated':>19}{'late_arrivals':>15}{'late_pct':>10}")
    for year, circulated, late, pct in query(con, """
        SELECT year, SUM(trains_circulated) AS trains_circulated, SUM(late_arrivals) AS late_arrivals,
               ROUND(100.0 * SUM(late_arrivals) / SUM(trains_circulated), 2) AS late_pct
        FROM check_yearly GROUP BY year ORDER BY year
    """):
        print(f"{year:>6}{circulated:>19}{late:>15}{pct:>10}")

    fs.makedirs(c.path(*c.VERIFICATION[:-1]), exist_ok=True)
    with fs.open(c.path(*c.VERIFICATION), "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))
    print("VERIFICATION", "PASSED" if report["all_ok"] else "FAILED")
    sys.exit(0 if report["all_ok"] else 1)


if __name__ == "__main__":
    main()
