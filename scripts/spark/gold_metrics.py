from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum as _sum, count, to_date, round


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


    silver_events_path = "data/silver/silver_events"
    df_gold = build_gold_metrics(spark, silver_events_path)
    df_gold.show(5, truncate=False)

    df_gold.write.mode("overwrite").parquet("data/gold/gold_daily_revenue")
    (
        df_gold.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv("data/gold/reports/gold_daily_revenue")
    )

    spark.stop()


if __name__ == "__main__":
    main()
