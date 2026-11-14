"""Step 5 - measure the alternatives: Spark vs DuckDB vs pandas on the same query.

The raw CSV is copied once to local disk so that only the engines are compared
(not the network). Each engine parses the CSV and computes the yearly
late-arrival rate; wall-clock time is taken over several runs.

  python src/05_benchmark.py [--runs 5]
"""
import argparse
import os
import statistics
import tempfile
import time

import common as c


def run_pandas(path):
    import pandas as pd

    df = pd.read_csv(path, sep=";", encoding="utf-8-sig")
    df = df[(df.nb_train_prevu > 0) & (df.nb_annulation <= df.nb_train_prevu)]
    g = df.assign(year=df["date"].str[:4].astype(int), circ=df.nb_train_prevu - df.nb_annulation) \
          .groupby("year")[["circ", "nb_train_retard_arrivee"]].sum()
    return (g.nb_train_retard_arrivee / g.circ).to_dict()


def run_duckdb(path):
    import duckdb

    rows = duckdb.connect().execute(f"""
        SELECT CAST(substr("date", 1, 4) AS INTEGER),
               SUM(nb_train_retard_arrivee) / SUM(nb_train_prevu - nb_annulation)
        FROM read_csv('{path}', delim=';', header=true, quote='"', escape='"')
        WHERE nb_train_prevu > 0 AND nb_annulation <= nb_train_prevu GROUP BY 1
    """).fetchall()
    return dict(rows)


def run_spark(path, spark):
    from pyspark.sql import functions as F

    df = (spark.read.option("header", True).option("sep", ";").option("multiLine", True)
          .option("quote", '"').option("escape", '"').csv(path))
    df = df.toDF(*[n.replace("﻿", "") for n in df.columns])
    planned, cancelled = F.col("nb_train_prevu").cast("int"), F.col("nb_annulation").cast("int")
    rows = (df.where((planned > 0) & (cancelled <= planned))
            .groupBy(F.substring("date", 1, 4).cast("int").alias("year"))
            .agg((F.sum(F.col("nb_train_retard_arrivee").cast("int")) / F.sum(planned - cancelled)).alias("r"))
            .collect())
    return {r["year"]: r["r"] for r in rows}


def timed(fn, runs):
    durations, result = [], None
    for _ in range(runs):
        start = time.perf_counter()
        result = fn()
        durations.append(time.perf_counter() - start)
    return durations, result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()

    c.banner("05 benchmark")
    fs = c.filesystem()
    local = os.path.join(tempfile.mkdtemp(prefix="tgv_bench_"), c.RAW_FILE)
    with fs.open(c.path(*c.RAW), "rb") as src, open(local, "wb") as dst:
        dst.write(src.read())

    start = time.perf_counter()
    spark = c.spark_session("tgv-05-benchmark")
    spark.range(1).count()
    spark_startup = time.perf_counter() - start

    results = {}
    lines = ["engine,runs,first_run_s,median_s,min_s,note"]
    for name, fn, note in [
        ("pandas", lambda: run_pandas(local), ""),
        ("duckdb", lambda: run_duckdb(local), ""),
        ("spark", lambda: run_spark(local, spark), f"plus {spark_startup:.2f}s session startup"),
    ]:
        durations, results[name] = timed(fn, args.runs)
        lines.append(f"{name},{args.runs},{durations[0]:.4f},{statistics.median(durations):.4f},"
                     f"{min(durations):.4f},{note}")
    spark.stop()

    reference = results["duckdb"]
    for name, result in results.items():
        assert result.keys() == reference.keys(), f"{name}: different years"
        assert all(abs(result[y] - reference[y]) < 1e-9 for y in reference), f"{name}: different rates"
    print("All three engines return identical yearly late-arrival rates.")

    output = "\n".join(lines) + "\n"
    fs.makedirs(c.path(*c.BENCHMARK[:-1]), exist_ok=True)
    with fs.open(c.path(*c.BENCHMARK), "w") as f:
        f.write(output)
    print(output)
    print(f"Benchmark written: {c.path(*c.BENCHMARK)}")


if __name__ == "__main__":
    main()
