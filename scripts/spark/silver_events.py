from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, current_timestamp, lit, to_json, try_to_timestamp, when, expr
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window


def transform_silver_events(df_bronze: DataFrame) -> DataFrame:
    df_bronze_filtered = df_bronze.filter(col("event_id").isNotNull())

    return df_bronze_filtered.select(
        col("event_id"),
        col("event_type"),
        coalesce(
            try_to_timestamp(col("event_timestamp"), lit("yyyy-MM-dd'T'HH:mm:ssX")),
            try_to_timestamp(col("event_timestamp"), lit("yyyy-MM-dd HH:mm:ss")),
            try_to_timestamp(col("event_timestamp"), lit("MM/dd/yyyy HH:mm:ss")),
        ).alias("event_timestamp"),
        col("user_id"),
        col("session_id"),
        when(col("product_id") == "", None).otherwise(col("product_id")).alias("product_id"),
        when(col("page_url") == "", None).otherwise(col("page_url")).alias("page_url"),
        expr("try_cast(quantity as int)").alias("quantity"),
        expr("try_cast(price as double)").alias("price"),
        when(col("currency") == "", None).otherwise(col("currency")).alias("currency"),
        col("source_file"),
        coalesce(
            try_to_timestamp(col("ingested_at"), lit("yyyy-MM-dd'T'HH:mm:ssX")),
            try_to_timestamp(col("ingested_at"), lit("yyyy-MM-dd HH:mm:ss")),
        ).alias("ingested_at"),
        col("bronze_loaded_at"),
        to_json(col("event_properties")).alias("event_properties"),
        current_timestamp().alias("silver_loaded_at"),
    )

def transform_silver_dedup(df_silver_clean: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("event_id").orderBy(col("ingested_at").desc_nulls_last())

    return (
        df_silver_clean
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("SilverEvents")
        .config("spark.sql.parquet.mergeSchema", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df_bronze = spark.read.parquet("data/bronze/bronze_events")
    bronze_count = df_bronze.count()
    print(f"[INFO] Bronze row count: {bronze_count}")

    null_event_id_count = df_bronze.filter(col("event_id").isNull()).count()
    if null_event_id_count > 0:
        print(f"[WARN] Dropping {null_event_id_count} rows with null event_id")

    eligible_count = bronze_count - null_event_id_count
    df_silver_clean = transform_silver_events(df_bronze)
    df_silver_dedup = transform_silver_dedup(df_silver_clean)

    null_ts_count = df_silver_clean.filter(col("event_timestamp").isNull()).count()
    null_ingested_count = df_silver_dedup.filter(col("ingested_at").isNull()).count()
    null_quantity_count = df_silver_dedup.filter(col("quantity").isNull()).count()
    null_price_count = df_silver_dedup.filter(col("price").isNull()).count()

    print(f"[QC] Null event_timestamp : {null_ts_count}")
    print(f"[QC] Null ingested_at     : {null_ingested_count}")
    print(f"[QC] Null quantity (diag) : {null_quantity_count}  — expected for non-commerce events")
    print(f"[QC] Null price    (diag) : {null_price_count}  — expected for non-commerce events")

    ts_null_pct = null_ts_count / eligible_count if eligible_count > 0 else 0
    if ts_null_pct > 0.2:
        raise ValueError(
            f"[ERROR] {ts_null_pct:.1%} of eligible rows have null event_timestamp — "
            "exceeds 5% threshold. Check bronze timestamp formats."
        )

    silver_count = df_silver_dedup.count()
    print(f"[INFO] Silver row count after dedup : {silver_count}")
    print(f"[INFO] Duplicates removed           : {eligible_count - silver_count}")

    (
        df_silver_dedup.write.mode("overwrite")
        .partitionBy("event_type")
        .parquet("data/silver/silver_events")
    )

    print("[INFO] Silver layer write complete.")
    spark.stop()


if __name__ == "__main__":
    main()
