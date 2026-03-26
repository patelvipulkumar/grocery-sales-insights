from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, row_number
from pyspark.sql.window import Window
import os

PROJECT = os.getenv('GCP_PROJECT', 'your-project-id')
RAW_DATASET = 'grocery_raw'
ANALYTICS_DATASET = 'grocery_analytics'
RAW_TABLE = 'sales'
MAX_ROWS = int(os.getenv('SPARK_MAX_ROWS', '200000'))

spark = SparkSession.builder \
    .appName('grocery-segmentation-reco') \
    .getOrCreate()

sales_df = spark.read.format('bigquery').option('table', f'{PROJECT}:{RAW_DATASET}.{RAW_TABLE}').load().limit(MAX_ROWS)

sales_df = sales_df.withColumn('SalesDate', col('SalesDate').cast('timestamp')).na.drop()

customer_features = sales_df.groupBy('CustomerID').agg(
    expr('count(distinct TransactionNumber) as num_orders'),
    expr('sum(TotalPrice) as total_spend'),
    expr('avg(Quantity) as avg_quantity')
).fillna({'CustomerID': 0, 'num_orders': 0, 'total_spend': 0, 'avg_quantity': 0})

customer_features = customer_features.withColumn('customer_id', col('CustomerID').cast('int'))

quantiles = customer_features.approxQuantile('total_spend', [0.2, 0.4, 0.6, 0.8], 0.05)
q1, q2, q3, q4 = quantiles

segments = customer_features.select(
    'customer_id',
    when(col('total_spend') <= q1, 0)
    .when(col('total_spend') <= q2, 1)
    .when(col('total_spend') <= q3, 2)
    .when(col('total_spend') <= q4, 3)
    .otherwise(4)
    .alias('segment_id')
)

target_segments = f'{PROJECT}:{ANALYTICS_DATASET}.customer_segments'
segments.write.format('bigquery').option('table', target_segments).option('writeMethod', 'direct').mode('overwrite').save()

rating_df = sales_df.filter(col('CustomerID').isNotNull() & col('ProductID').isNotNull())
rating_df = rating_df.withColumn('customer_id', col('CustomerID').cast('integer'))
rating_df = rating_df.withColumn('product_id', col('ProductID').cast('integer'))

ratings = rating_df.groupBy('customer_id', 'product_id').agg(expr('sum(TotalPrice) as rating')).na.drop()

window_spec = Window.partitionBy('customer_id').orderBy(col('rating').desc())
recommendations_flat = ratings.withColumn('rank', row_number().over(window_spec)) \
    .filter(col('rank') <= 10) \
    .select('customer_id', 'product_id', col('rating').alias('score'))

target_reco = f'{PROJECT}:{ANALYTICS_DATASET}.customer_recommendations'
recommendations_flat.write.format('bigquery').option('table', target_reco).option('writeMethod', 'direct').mode('overwrite').save()

spark.stop()
