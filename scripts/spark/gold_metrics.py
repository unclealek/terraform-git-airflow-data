from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum as _sum, count, to_date, round

from scripts.pipeline_config import GOLD_PATH, GOLD_REPORT_PATH, SILVER_PATH


def build_gold_metrics(spark: SparkSession, silver_events_path: str) -> DataFrame:
    df_silver = spark.read.parquet(silver_events_path)
    df_purchases = df_silver.filter(col("event_type") == "purchase")
    df_daily_revenue = (
        df_purchases
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("revenue", round(col("quantity") * col("price"), 2))
        .filter(col("event_date").isNotNull())
        .groupBy("event_date")
        .agg(
            _sum("revenue").alias("total_revenue"),
            count("*").alias("purchase_count")
        )
        .orderBy("event_date")
    )

    return df_daily_revenue


def main():
    spark = (
        SparkSession.builder.appName("GoldMetrics")
        .config("spark.sql.parquet.mergeSchema", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")


    df_gold = build_gold_metrics(spark, str(SILVER_PATH))
    df_gold.show(5, truncate=False)

    df_gold.write.mode("overwrite").parquet(str(GOLD_PATH))
    (
        df_gold.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(GOLD_REPORT_PATH))
    )

    spark.stop()


if __name__ == "__main__":
    main()
