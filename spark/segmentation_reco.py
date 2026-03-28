"""
High-level pipeline overview:
1) Build Spark-based RFM segments from dbt-corrected sales activity.
2) Generate top-N personalized recommendations with ALS.
3) Automatically fall back to heuristic co-purchase recommendations when
    ALS cannot run reliably (sparse data or runtime failure).
4) Persist both outputs to BigQuery analytics tables for dashboarding.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    current_timestamp,
    datediff,
    expr,
    lit,
    max as spark_max,
    row_number,
    sum as spark_sum,
    when,
)
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS
from google.cloud import bigquery
import os

PROJECT = os.getenv("GCP_PROJECT", "your-project-id")
RAW_DATASET = "grocery_raw"
STAGING_DATASET = "grocery_staging"
ANALYTICS_DATASET = "grocery_analytics"
RAW_TABLE = "sales"
CURATED_SALES_TABLE = "st_sales"
MAX_ROWS = int(os.getenv("SPARK_MAX_ROWS", "200000"))
TOP_N_RECOMMENDATIONS = int(os.getenv("SPARK_TOP_N_RECOMMENDATIONS", "10"))
ALS_RANK = int(os.getenv("SPARK_ALS_RANK", "20"))
ALS_MAX_ITER = int(os.getenv("SPARK_ALS_MAX_ITER", "10"))
ALS_REG_PARAM = float(os.getenv("SPARK_ALS_REG_PARAM", "0.1"))
MIN_ALS_USERS = int(os.getenv("SPARK_MIN_ALS_USERS", "50"))
MIN_ALS_ITEMS = int(os.getenv("SPARK_MIN_ALS_ITEMS", "50"))
MIN_ALS_INTERACTIONS = int(os.getenv("SPARK_MIN_ALS_INTERACTIONS", "500"))


def drop_bq_table_if_exists(dataset_name, table_name):
    client = bigquery.Client(project=PROJECT)
    table_id = f"{PROJECT}.{dataset_name}.{table_name}"
    client.delete_table(table_id, not_found_ok=True)
    print(f"Prepared target table for overwrite: {table_id}")


def materialize_dataframe(dataframe, label):
    """Attempt to materialize and cache dataframe; gracefully handle OOM/executor failures."""
    try:
        cached_dataframe = dataframe.cache()
        row_count = cached_dataframe.count()
        print(f"Materialized {label} in Spark cache with {row_count} rows")
        return cached_dataframe
    except Exception as exc:
        print(f"WARNING: Failed to materialize {label}; using lazy evaluation instead. Error: {exc}")
        print(f"  This may impact performance but allows job to proceed with limited cluster memory.")
        return dataframe


spark = SparkSession.builder.appName("grocery-segmentation-reco").getOrCreate()

try:
    # Prefer dbt-corrected sales values from st_sales.
    sales_df = (
        spark.read.format("bigquery")
        .option("table", f"{PROJECT}:{STAGING_DATASET}.{CURATED_SALES_TABLE}")
        .load()
        .select(
            col("sales_date").cast("timestamp").alias("SalesDate"),
            col("customer_id").cast("int").alias("CustomerID"),
            col("product_id").cast("int").alias("ProductID"),
            col("total_price").cast("double").alias("TotalPrice"),
            col("quantity").cast("double").alias("Quantity"),
            col("discount").cast("double").alias("Discount"),
            col("transaction_number").alias("TransactionNumber"),
        )
        .limit(MAX_ROWS)
    )
    print(f"Using curated sales source: {PROJECT}:{STAGING_DATASET}.{CURATED_SALES_TABLE}")
except Exception as exc:
    print(
        "Curated sales source unavailable; falling back to raw sales. "
        f"Error: {exc}"
    )
    sales_df = (
        spark.read.format("bigquery")
        .option("table", f"{PROJECT}:{RAW_DATASET}.{RAW_TABLE}")
        .load()
        .limit(MAX_ROWS)
    )

# Load products with error handling
try:
    products_base = spark.read.format("bigquery").option("table", f"{PROJECT}:{RAW_DATASET}.products").load().select(
        col("ProductID").cast("int").alias("product_id"),
        col("ProductName").alias("product_name"),
        col("CategoryID").cast("int").alias("category_id"),
        col("Price").cast("double").alias("product_price"),
    )
    
    categories = spark.read.format("bigquery").option("table", f"{PROJECT}:{RAW_DATASET}.categories").load().select(
        col("CategoryID").cast("int").alias("category_id"),
        col("CategoryName").alias("category_name"),
    )
    
    products_df = materialize_dataframe(
        products_base.join(categories, on="category_id", how="left"),
        "products source",
    )
    print("Successfully loaded products and categories")
except Exception as exc:
    print(f"ERROR: Failed to load products/categories: {exc}")
    print("  Products dataframe will be empty; recommendations cannot be generated.")
    products_df = spark.createDataFrame([], "product_id INT, product_name STRING, category_id INT, product_price DOUBLE, category_name STRING")

# Load customers with error handling
try:
    customers_df = materialize_dataframe(
        spark.read.format("bigquery")
        .option("table", f"{PROJECT}:{RAW_DATASET}.customers")
        .load()
        .select(
            col("CustomerID").cast("int").alias("customer_id"),
            expr("trim(concat(coalesce(FirstName, ''), ' ', coalesce(LastName, '')))").alias("customer_name"),
        ),
        "customers source",
    )
    print("Successfully loaded customers")
except Exception as exc:
    print(f"ERROR: Failed to load customers: {exc}")
    print("  Customers dataframe will be empty; customer names will be missing from recommendations.")
    customers_df = spark.createDataFrame([], "customer_id INT, customer_name STRING")

# Proceed only if we have data to work with
if products_df.count() == 0:
    print("FATAL: Products dataframe is empty; cannot proceed with recommendations. Exiting.")
    spark.stop()
    exit(1)

sales_df = (
    sales_df.withColumn("SalesDate", col("SalesDate").cast("timestamp"))
    .withColumn("CustomerID", col("CustomerID").cast("int"))
    .withColumn("ProductID", col("ProductID").cast("int"))
    .withColumn("TotalPrice", col("TotalPrice").cast("double"))
    .withColumn("Quantity", col("Quantity").cast("double"))
    .withColumn("Discount", col("Discount").cast("double"))
)

sales_df = materialize_dataframe(
    sales_df.join(products_df.select("product_id", "product_price"), sales_df.ProductID == col("product_id"), "left")
    .withColumn(
        "TotalPrice",
        when(
            col("TotalPrice").isNull() | (col("TotalPrice") <= 0),
            (coalesce(col("Quantity"), lit(0.0)) * coalesce(col("product_price"), lit(0.0))) - coalesce(col("Discount"), lit(0.0)),
        ).otherwise(col("TotalPrice")),
    )
    .withColumn("TotalPrice", when(col("TotalPrice") < 0, lit(0.0)).otherwise(col("TotalPrice")))
    .drop("product_id", "product_price")
    .na.drop(subset=["SalesDate", "CustomerID", "ProductID", "Quantity", "TotalPrice", "TransactionNumber"]),
    "sales source",
)


# ------------------------------------------------------------------------------
# Legacy heuristic logic (kept commented for learning/reference)
# ------------------------------------------------------------------------------
# customer_features = sales_df.groupBy('CustomerID').agg(
#     expr('count(distinct TransactionNumber) as num_orders'),
#     expr('sum(TotalPrice) as total_spend'),
#     expr('avg(Quantity) as avg_quantity'),
#     expr('avg(TotalPrice) as avg_order_value')
# )
#
# quantiles = customer_features.approxQuantile('total_spend', [0.2, 0.4, 0.6, 0.8], 0.05)
# q1, q2, q3, q4 = quantiles
#
# segments = customer_features.select(
#     col('CustomerID').cast('int').alias('customer_id'),
#     when(col('total_spend') <= q1, 0)
#     .when(col('total_spend') <= q2, 1)
#     .when(col('total_spend') <= q3, 2)
#     .when(col('total_spend') <= q4, 3)
#     .otherwise(4)
#     .alias('segment_id')
# )
#
# pair_counts = interaction_df.alias('a').join(
#     interaction_df.alias('b'),
#     (col('a.transaction_number') == col('b.transaction_number')) &
#     (col('a.product_id') != col('b.product_id')),
#     'inner'
# ).groupBy(
#     col('a.product_id').alias('anchor_product_id'),
#     col('b.product_id').alias('recommended_product_id')
# ).agg(expr('count(*) as co_purchase_count'))
# ------------------------------------------------------------------------------


def build_rfm_segments(input_sales_df):
    rfm_base = input_sales_df.groupBy("CustomerID").agg(
        expr("count(distinct TransactionNumber) as frequency"),
        expr("sum(TotalPrice) as monetary"),
        expr("avg(TotalPrice) as avg_order_value"),
        expr("avg(Quantity) as avg_quantity"),
        spark_max("SalesDate").alias("last_purchase_date"),
    )

    snapshot_date = input_sales_df.select(spark_max("SalesDate").alias("snapshot_date")).first()["snapshot_date"]

    rfm = (
        rfm_base.withColumn("customer_id", col("CustomerID").cast("int"))
        .withColumn("recency_days", datediff(lit(snapshot_date), col("last_purchase_date")))
        .drop("CustomerID")
    )

    rfm_scored = (
        rfm.withColumn("recency_score", (lit(6) - expr("ntile(5) over (order by recency_days asc)")))
        .withColumn("frequency_score", expr("ntile(5) over (order by frequency asc)"))
        .withColumn("monetary_score", expr("ntile(5) over (order by monetary asc)"))
        .withColumn(
            "rfm_score",
            concat(col("recency_score").cast("string"), col("frequency_score").cast("string"), col("monetary_score").cast("string")),
        )
        .withColumn(
            "segment_name",
            when((col("recency_score") >= 4) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4), "Champions")
            .when((col("recency_score") >= 4) & (col("frequency_score") >= 3), "Loyal Customers")
            .when((col("recency_score") == 5) & (col("frequency_score") <= 2), "New Customers")
            .when((col("recency_score") <= 2) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4), "At Risk High Value")
            .when((col("recency_score") <= 2) & (col("frequency_score") <= 2), "Hibernating")
            .otherwise("Needs Attention"),
        )
        .withColumn(
            "segment_id",
            when(col("segment_name") == "Champions", lit(5))
            .when(col("segment_name") == "Loyal Customers", lit(4))
            .when(col("segment_name") == "New Customers", lit(3))
            .when(col("segment_name") == "Needs Attention", lit(2))
            .when(col("segment_name") == "At Risk High Value", lit(1))
            .otherwise(lit(0)),
        )
        .withColumn("snapshot_date", lit(snapshot_date))
        .withColumn("model_generated_at", current_timestamp())
    )

    return rfm_scored


def build_als_recommendations(input_sales_df, input_products_df, input_customers_df):
    interactions = input_sales_df.select(
        col("CustomerID").cast("int").alias("customer_id"),
        col("ProductID").cast("int").alias("product_id"),
        col("Quantity").cast("double").alias("quantity"),
        col("TotalPrice").cast("double").alias("total_price"),
    )

    ratings = interactions.groupBy("customer_id", "product_id").agg(
        spark_sum(
            when(col("total_price") > 0, col("total_price")).otherwise(coalesce(col("quantity"), lit(0.0)))
        ).alias("raw_rating")
    )

    ratings = ratings.withColumn("rating", expr("log(1 + raw_rating)"))

    als = ALS(
        userCol="customer_id",
        itemCol="product_id",
        ratingCol="rating",
        rank=ALS_RANK,
        maxIter=ALS_MAX_ITER,
        regParam=ALS_REG_PARAM,
        coldStartStrategy="drop",
        nonnegative=True,
    )

    model = als.fit(ratings)
    raw_recommendations = model.recommendForAllUsers(TOP_N_RECOMMENDATIONS)

    flat_recommendations = raw_recommendations.selectExpr(
        "customer_id",
        "explode(recommendations) as rec",
    ).select(
        col("customer_id"),
        col("rec.product_id").cast("int").alias("product_id"),
        col("rec.rating").cast("double").alias("score"),
    )

    already_purchased = interactions.select("customer_id", "product_id").distinct()

    candidate_recommendations = flat_recommendations.join(
        already_purchased,
        on=["customer_id", "product_id"],
        how="left_anti",
    )

    reco_rank_window = Window.partitionBy("customer_id").orderBy(col("score").desc())
    ranked_recommendations = (
        candidate_recommendations.withColumn("recommendation_rank", row_number().over(reco_rank_window))
        .filter(col("recommendation_rank") <= TOP_N_RECOMMENDATIONS)
        .join(input_products_df, on="product_id", how="left")
        .join(input_customers_df, on="customer_id", how="left")
        .withColumn("model_type", lit("ALS"))
        .withColumn("model_generated_at", current_timestamp())
    )

    return ranked_recommendations.select(
        "customer_id",
        "customer_name",
        "product_id",
        "product_name",
        "category_id",
        "category_name",
        "score",
        "recommendation_rank",
        "model_type",
        "model_generated_at",
    )


def build_heuristic_recommendations(input_sales_df, input_products_df, input_customers_df):
    interactions = input_sales_df.select(
        col("CustomerID").cast("int").alias("customer_id"),
        col("ProductID").cast("int").alias("product_id"),
        col("TransactionNumber").alias("transaction_number"),
    )

    customer_products = interactions.select("customer_id", "product_id").distinct()

    pair_counts = interactions.alias("a").join(
        interactions.alias("b"),
        (col("a.transaction_number") == col("b.transaction_number"))
        & (col("a.product_id") != col("b.product_id")),
        "inner",
    ).groupBy(
        col("a.product_id").alias("anchor_product_id"),
        col("b.product_id").alias("recommended_product_id"),
    ).agg(expr("count(*) as co_purchase_count"))

    candidate_reco = customer_products.alias("cp").join(
        pair_counts.alias("pc"),
        col("cp.product_id") == col("pc.anchor_product_id"),
        "inner",
    ).select(
        col("cp.customer_id").alias("customer_id"),
        col("pc.recommended_product_id").alias("product_id"),
        col("pc.co_purchase_count").cast("double").alias("score"),
    )

    already_owned = customer_products.select("customer_id", col("product_id").alias("owned_product_id"))

    filtered = candidate_reco.join(
        already_owned,
        (candidate_reco.customer_id == already_owned.customer_id)
        & (candidate_reco.product_id == already_owned.owned_product_id),
        "left_anti",
    )

    ranked_window = Window.partitionBy("customer_id").orderBy(col("score").desc())
    ranked = (
        filtered.withColumn("recommendation_rank", row_number().over(ranked_window))
        .filter(col("recommendation_rank") <= TOP_N_RECOMMENDATIONS)
        .join(input_products_df, on="product_id", how="left")
        .join(input_customers_df, on="customer_id", how="left")
        .withColumn("model_type", lit("HEURISTIC_FALLBACK"))
        .withColumn("model_generated_at", current_timestamp())
    )

    return ranked.select(
        "customer_id",
        "customer_name",
        "product_id",
        "product_name",
        "category_id",
        "category_name",
        "score",
        "recommendation_rank",
        "model_type",
        "model_generated_at",
    )


def build_recommendations_with_fallback(input_sales_df, input_products_df, input_customers_df):
    interactions = input_sales_df.select(
        col("CustomerID").cast("int").alias("customer_id"),
        col("ProductID").cast("int").alias("product_id"),
        col("TotalPrice").cast("double").alias("total_price"),
    )

    user_count = interactions.select("customer_id").distinct().count()
    item_count = interactions.select("product_id").distinct().count()
    interaction_count = interactions.count()

    has_enough_data = (
        user_count >= MIN_ALS_USERS
        and item_count >= MIN_ALS_ITEMS
        and interaction_count >= MIN_ALS_INTERACTIONS
    )

    if not has_enough_data:
        print(
            "ALS fallback triggered due to sparse data: "
            f"users={user_count}, items={item_count}, interactions={interaction_count}"
        )
        return build_heuristic_recommendations(input_sales_df, input_products_df, input_customers_df)

    try:
        print(
            "Running ALS recommender with data profile: "
            f"users={user_count}, items={item_count}, interactions={interaction_count}"
        )
        als_recommendations = build_als_recommendations(input_sales_df, input_products_df, input_customers_df)
        if als_recommendations.limit(1).count() == 0:
            print("ALS produced zero candidate recommendations after filtering; switching to heuristic fallback")
            return build_heuristic_recommendations(input_sales_df, input_products_df, input_customers_df)

        return als_recommendations
    except Exception as exc:
        print(f"ALS training/inference failed, switching to heuristic fallback. Error: {exc}")
        return build_heuristic_recommendations(input_sales_df, input_products_df, input_customers_df)


segments_df = build_rfm_segments(sales_df)
target_segments = f"{PROJECT}:{ANALYTICS_DATASET}.customer_segments"
drop_bq_table_if_exists(ANALYTICS_DATASET, "customer_segments")
segments_df.write.format("bigquery").option("table", target_segments).option("writeMethod", "direct").mode("overwrite").save()

recommendations_df = build_recommendations_with_fallback(sales_df, products_df, customers_df)
target_reco = f"{PROJECT}:{ANALYTICS_DATASET}.customer_recommendations"
drop_bq_table_if_exists(ANALYTICS_DATASET, "customer_recommendations")
recommendations_df.write.format("bigquery").option("table", target_reco).option("writeMethod", "direct").mode("overwrite").save()

spark.stop()
