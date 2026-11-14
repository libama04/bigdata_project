"""Step 2 - Spark: raw CSV -> typed, cleaned Parquet (silver) + data-quality report.

Every quality rule is counted on the raw data before being applied, so the
report gives the number of affected rows per issue.

  python src/02_clean_spark.py
"""
import json
from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

import common as c

INT_COLS = [
    "duree_moyenne", "nb_train_prevu", "nb_annulation", "nb_train_depart_retard",
    "nb_train_retard_arrivee", "nb_train_retard_sup_15", "nb_train_retard_sup_30",
    "nb_train_retard_sup_60",
]
DOUBLE_COLS = [
    "retard_moyen_depart", "retard_moyen_tous_trains_depart", "retard_moyen_arrivee",
    "retard_moyen_tous_trains_arrivee", "retard_moyen_trains_retard_sup15",
]
CAUSE_COLS = [
    "prct_cause_externe", "prct_cause_infra", "prct_cause_gestion_trafic",
    "prct_cause_materiel_roulant", "prct_cause_gestion_gare",
    "prct_cause_prise_en_charge_voyageurs",
]
COMMENT_COLS = ["commentaire_annulation", "commentaire_retards_depart", "commentaires_retard_arrivee"]
KEY_COLS = ["date", "service", "gare_depart", "gare_arrivee"]
EXPECTED = KEY_COLS + INT_COLS + DOUBLE_COLS + CAUSE_COLS + COMMENT_COLS


def count_if(condition):
    return F.sum(F.when(condition, 1).otherwise(0))


def main() -> None:
    c.banner("02 clean (Spark)")
    spark = c.spark_session("tgv-02-clean")
    print(f"Spark {spark.version} master={spark.sparkContext.master} app={spark.sparkContext.applicationId}")

    # Comments are quoted free text containing line breaks and ';' -> multiLine parsing.
    raw = (
        spark.read.option("header", True).option("sep", ";").option("multiLine", True)
        .option("quote", '"').option("escape", '"').option("encoding", "UTF-8")
        .csv(c.spark_path(*c.RAW))
    )
    raw = raw.toDF(*[name.replace("﻿", "").strip() for name in raw.columns])  # UTF-8 BOM
    missing = sorted(set(EXPECTED) - set(raw.columns))
    if missing:
        raise SystemExit(f"Unexpected schema, missing columns: {missing}")

    numeric = INT_COLS + DOUBLE_COLS + CAUSE_COLS
    typed = raw.select(
        *KEY_COLS,
        *[F.col(x).cast("int").alias(x) for x in INT_COLS],
        *[F.col(x).cast("double").alias(x) for x in DOUBLE_COLS + CAUSE_COLS],
        *COMMENT_COLS,
        reduce(lambda a, b: a + b, [
            F.when(F.col(x).isNotNull() & F.col(x).cast("double").isNull(), 1).otherwise(0)
            for x in numeric
        ]).alias("_cast_failures"),
    ).withColumn("_cause_sum", reduce(lambda a, b: a + b, [F.col(x) for x in CAUSE_COLS]))
    typed = typed.withColumn(
        "_services_for_route_month",
        F.count("*").over(Window.partitionBy("date", "gare_depart", "gare_arrivee")),
    ).cache()

    zero_planned = F.col("nb_train_prevu") <= 0
    over_cancelled = F.col("nb_annulation") > F.col("nb_train_prevu")
    cause_zero = F.col("_cause_sum") == 0
    cause_off = (~cause_zero) & (F.abs(F.col("_cause_sum") - 100) > 1)
    sup15_incoherent = F.col("nb_train_retard_sup_15") > F.col("nb_train_retard_arrivee")

    checks = [
        ("rows_raw", F.count("*"), "-"),
        ("numeric_cast_failures", F.sum("_cast_failures"), "value set to NULL"),
        ("bad_date_format", count_if(~F.col("date").rlike(r"^\d{4}-\d{2}$")), "row dropped"),
        ("missing_key", count_if(reduce(lambda a, b: a | b, [F.col(k).isNull() for k in KEY_COLS])), "row dropped"),
        ("exact_duplicate_key_rows", F.count("*") - F.countDistinct(*KEY_COLS), "deduplicated"),
        ("route_month_listed_under_two_services", count_if(F.col("_services_for_route_month") > 1),
         "kept; summed across services in route-level results"),
        ("zero_planned_trains", count_if(zero_planned), "row dropped (no service, mostly COVID lockdown)"),
        ("cancellations_gt_planned", count_if(over_cancelled), "row dropped"),
        ("zero_journey_time", count_if(F.col("duree_moyenne") <= 0), "duree_moyenne set to NULL"),
        ("negative_nb_train_retard_sup_30", count_if(F.col("nb_train_retard_sup_30") < 0), "value set to NULL"),
        ("negative_retard_moyen_arrivee", count_if(F.col("retard_moyen_arrivee") < 0), "value set to NULL"),
        ("negative_retard_moyen_trains_retard_sup15", count_if(F.col("retard_moyen_trains_retard_sup15") < 0),
         "value set to NULL"),
        ("late_over_15_gt_late_arrivals", count_if(sup15_incoherent), "kept, flagged"),
        ("cause_percentages_all_zero", count_if(cause_zero), "causes set to NULL, excluded from cause analysis"),
        ("cause_percentages_sum_not_100", count_if(cause_off), "causes set to NULL, excluded from cause analysis"),
        *[(f"empty_{x}", count_if(F.col(x).isNull()), "free-text column dropped") for x in COMMENT_COLS],
    ]
    values = typed.agg(*[expr.alias(name) for name, expr, _ in checks]).first().asDict()

    def nullify_negative(name):
        return F.when(F.col(name) < 0, None).otherwise(F.col(name)).alias(name)

    causes_valid = (~cause_zero) & (~cause_off) & F.col("_cause_sum").isNotNull()
    negatives = {"nb_train_retard_sup_30", "retard_moyen_arrivee", "retard_moyen_trains_retard_sup15"}

    clean = (
        typed
        .where(F.col("date").rlike(r"^\d{4}-\d{2}$"))
        .dropna(subset=KEY_COLS)
        .where(~zero_planned & ~over_cancelled)
        .dropDuplicates(KEY_COLS)
        .select(
            F.to_date(F.concat(F.col("date"), F.lit("-01"))).alias("period"),
            F.substring("date", 1, 4).cast("int").alias("year"),
            F.substring("date", 6, 2).cast("int").alias("month"),
            "service",
            F.upper(F.trim("gare_depart")).alias("gare_depart"),
            F.upper(F.trim("gare_arrivee")).alias("gare_arrivee"),
            F.when(F.col("duree_moyenne") > 0, F.col("duree_moyenne")).alias("duree_moyenne"),
            "nb_train_prevu",
            "nb_annulation",
            (F.col("nb_train_prevu") - F.col("nb_annulation")).alias("nb_train_circule"),
            *[nullify_negative(x) if x in negatives else F.col(x)
              for x in INT_COLS[3:] + DOUBLE_COLS],
            *[F.when(causes_valid, F.col(x)).alias(x) for x in CAUSE_COLS],
            causes_valid.alias("causes_valid"),
            sup15_incoherent.alias("flag_sup15_gt_late"),
        )
    )

    (clean.repartition("year").write.mode("overwrite").partitionBy("year")
     .parquet(c.spark_path(*c.SILVER)))
    rows_clean = spark.read.parquet(c.spark_path(*c.SILVER)).count()

    report = [{"check": name, "affected_rows": int(values[name] or 0), "action": action}
              for name, _, action in checks]
    report.append({"check": "rows_clean", "affected_rows": rows_clean, "action": "-"})
    spark.stop()

    fs = c.filesystem()
    fs.makedirs(c.path(*c.QUALITY[:-1]), exist_ok=True)
    with fs.open(c.path(*c.QUALITY), "w") as f:
        json.dump(report, f, indent=2)

    print(f"Silver written: {c.spark_path(*c.SILVER)}")
    print(f"Quality report written: {c.path(*c.QUALITY)}")
    print(f"{'check':<45}{'affected_rows':>14}  action")
    for line in report:
        print(f"{line['check']:<45}{line['affected_rows']:>14}  {line['action']}")


if __name__ == "__main__":
    main()
