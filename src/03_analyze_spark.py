"""Step 3 - Spark: silver -> final (gold) datasets answering the research question.

  route_punctuality     one row per route: late-arrival rate, cancellation rate, rank
  yearly_punctuality    one row per year and service
  delay_causes_by_year  share of each delay cause per year (weighted by late trains)

Rates are always recomputed from summed counts (never averages of monthly
percentages), so months with many trains weigh more than months with few.

  python src/03_analyze_spark.py
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

import common as c

MIN_MONTHS_FOR_RANKING = 24  # routes observed for less than 2 years are not ranked

CAUSE_COLS = [
    "prct_cause_externe", "prct_cause_infra", "prct_cause_gestion_trafic",
    "prct_cause_materiel_roulant", "prct_cause_gestion_gare",
    "prct_cause_prise_en_charge_voyageurs",
]


def punctuality_aggregates():
    circulated = F.sum("nb_train_circule")
    return [
        F.sum("nb_train_prevu").alias("trains_planned"),
        F.sum("nb_annulation").alias("trains_cancelled"),
        circulated.alias("trains_circulated"),
        F.sum("nb_train_retard_arrivee").alias("late_arrivals"),
        F.sum("nb_train_retard_sup_15").alias("late_over_15min"),
        F.sum("nb_train_retard_sup_60").alias("late_over_60min"),
        (F.sum("nb_train_retard_arrivee") / circulated).alias("late_arrival_rate"),
        (F.sum("nb_annulation") / F.sum("nb_train_prevu")).alias("cancellation_rate"),
        (F.sum(F.col("retard_moyen_tous_trains_arrivee") * F.col("nb_train_circule")) / circulated)
        .alias("avg_arrival_delay_min"),
    ]


def main() -> None:
    c.banner("03 analyze (Spark)")
    spark = c.spark_session("tgv-03-analyze")
    print(f"Spark {spark.version} master={spark.sparkContext.master} app={spark.sparkContext.applicationId}")
    silver = spark.read.parquet(c.spark_path(*c.SILVER)).cache()

    by_route = (
        silver.groupBy("gare_depart", "gare_arrivee")
        .agg(
            F.countDistinct("period").alias("months_observed"),
            F.min("period").alias("first_month"),
            F.max("period").alias("last_month"),
            *punctuality_aggregates(),
        )
    )
    ranked = (
        by_route.where(F.col("months_observed") >= MIN_MONTHS_FOR_RANKING)
        .withColumn("rank_worst", F.row_number().over(
            Window.orderBy(F.desc("late_arrival_rate"), "gare_depart", "gare_arrivee")))
        .select("gare_depart", "gare_arrivee", "rank_worst")
    )
    route = by_route.join(ranked, ["gare_depart", "gare_arrivee"], "left").orderBy(
        F.col("rank_worst").asc_nulls_last(), "gare_depart", "gare_arrivee")

    yearly = silver.groupBy("year", "service").agg(*punctuality_aggregates()).orderBy("year", "service")

    weight = F.col("nb_train_retard_arrivee")
    causes = (
        silver.where("causes_valid")
        .groupBy("year")
        .agg(
            F.sum(weight).alias("late_arrivals_with_cause"),
            *[(F.sum(F.col(x) * weight) / F.sum(weight)).alias(x) for x in CAUSE_COLS],
        )
        .orderBy("year")
    )

    for name, df, target in [
        ("route_punctuality", route, c.GOLD_ROUTE),
        ("yearly_punctuality", yearly, c.GOLD_YEAR),
        ("delay_causes_by_year", causes, c.GOLD_CAUSES),
    ]:
        df.coalesce(1).write.mode("overwrite").parquet(c.spark_path(*target))
        print(f"Gold written: {c.spark_path(*target)} ({df.count()} rows)")

    print("Worst 10 routes by late-arrival rate (>= 24 months observed):")
    route.where("rank_worst <= 10").select(
        "rank_worst", "gare_depart", "gare_arrivee", "trains_circulated",
        F.round(F.col("late_arrival_rate") * 100, 2).alias("late_pct"),
        F.round("avg_arrival_delay_min", 2).alias("avg_delay_min"),
    ).show(truncate=False)
    print("Punctuality by year and service:")
    yearly.select(
        "year", "service", "trains_circulated",
        F.round(F.col("late_arrival_rate") * 100, 2).alias("late_pct"),
        F.round(F.col("cancellation_rate") * 100, 2).alias("cancel_pct"),
    ).show(40, truncate=False)
    print("Delay causes by year (% of late arrivals):")
    causes.select("year", *[F.round(x, 1).alias(x.replace("prct_cause_", "")) for x in CAUSE_COLS]).show()
    spark.stop()


if __name__ == "__main__":
    main()
