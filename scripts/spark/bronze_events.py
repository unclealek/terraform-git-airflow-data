from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp


def build_bronze_events(
    spark: SparkSession,
    add_to_cart_path: str,
    page_views_path: str,
    product_clicks_path: str,
    purchases_path: str,
    user_sessions_path: str,
) -> DataFrame:
    df_add_to_cart = spark.read.option("multiLine", "true").json(add_to_cart_path)
    df_page_views = spark.read.option("multiLine", "true").json(page_views_path)
    df_product_clicks = spark.read.option("multiLine", "true").json(product_clicks_path)
    df_purchases = spark.read.option("multiLine", "true").json(purchases_path)
    df_user_sessions = spark.read.option("multiLine", "true").json(user_sessions_path)

    df_add_to_cart_bronze = df_add_to_cart.withColumn("bronze_loaded_at", current_timestamp())
    df_page_views_bronze = df_page_views.withColumn("bronze_loaded_at", current_timestamp())
    df_product_clicks_bronze = df_product_clicks.withColumn("bronze_loaded_at", current_timestamp())
    df_purchases_bronze = df_purchases.withColumn("bronze_loaded_at", current_timestamp())
    df_user_sessions_bronze = df_user_sessions.withColumn("bronze_loaded_at", current_timestamp())

    return (
        df_add_to_cart_bronze.unionByName(df_page_views_bronze, allowMissingColumns=True)
        .unionByName(df_product_clicks_bronze, allowMissingColumns=True)
        .unionByName(df_purchases_bronze, allowMissingColumns=True)
        .unionByName(df_user_sessions_bronze, allowMissingColumns=True)
    )


def main() -> None:
    spark = SparkSession.builder.appName("BronzeEvents").getOrCreate()

    df_bronze_events = build_bronze_events(
        spark=spark,
        add_to_cart_path="data/raw/add_to_cart/add_to_cart.json",
        page_views_path="data/raw/page_views/page_views.json",
        product_clicks_path="data/raw/product_clicks/product_clicks.json",
        purchases_path="data/raw/purchases/purchases.json",
        user_sessions_path="data/raw/user_sessions/user_sessions.json",
    )

    df_bronze_events.printSchema()
    df_bronze_events.show(truncate=False)
    df_bronze_events.write.mode("overwrite").parquet("data/bronze/bronze_events")

    spark.stop()


if __name__ == "__main__":
    main()
