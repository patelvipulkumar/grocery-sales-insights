from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, expr, explode
import os

PROJECT = os.getenv('GCP_PROJECT', 'your-project-id')
RAW_DATASET = 'grocery_raw'
ANALYTICS_DATASET = 'grocery_analytics'
RAW_TABLE = 'sales'

spark = SparkSession.builder \
    .appName('grocery-segmentation-reco') \
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0') \
    .getOrCreate()

sales_df = spark.read.format('bigquery').option('table', f'{PROJECT}:{RAW_DATASET}.{RAW_TABLE}').load()

sales_df = sales_df.withColumn('invoice_date', col('invoice_date').cast('timestamp')).na.drop()

customer_features = sales_df.groupBy('customer_id').agg(
    expr('count(distinct invoice_id) as num_orders'),
    expr('sum(quantity * unit_price) as total_spend'),
    expr('avg(quantity) as avg_quantity')
).fillna({'customer_id': 0, 'num_orders': 0, 'total_spend': 0, 'avg_quantity': 0})

assembler = VectorAssembler(inputCols=['num_orders', 'total_spend', 'avg_quantity'], outputCol='features_raw')
feature_vec = assembler.transform(customer_features)

scaler = StandardScaler(inputCol='features_raw', outputCol='features', withMean=True, withStd=True)
feature_data = scaler.fit(feature_vec).transform(feature_vec)

kmeans = KMeans(k=5, seed=42, featuresCol='features')
segment_model = kmeans.fit(feature_data)
segments = segment_model.transform(feature_data).select('customer_id', col('prediction').alias('segment_id'))

target_segments = f'{PROJECT}:{ANALYTICS_DATASET}.customer_segments'
segments.write.format('bigquery').option('table', target_segments).mode('overwrite').save()

rating_df = sales_df.filter(col('customer_id').isNotNull() & col('stock_code').isNotNull())
rating_df = rating_df.withColumn('customer_id', col('customer_id').cast('integer'))
rating_df = rating_df.withColumn('stock_code', col('stock_code').cast('integer'))

ratings = rating_df.groupBy('customer_id', 'stock_code').agg(expr('sum(quantity * unit_price) as rating')).na.drop()

als = ALS(userCol='customer_id', itemCol='stock_code', ratingCol='rating', rank=20, maxIter=8, regParam=0.05, coldStartStrategy='drop')
als_model = als.fit(ratings)

recommendations = als_model.recommendForAllUsers(10)
recommendations_flat = recommendations.select(col('customer_id'), explode(col('recommendations')).alias('recommendation')) \
    .select(col('customer_id'), col('recommendation.stock_code').alias('stock_code'), col('recommendation.rating').alias('score'))

target_reco = f'{PROJECT}:{ANALYTICS_DATASET}.customer_recommendations'
recommendations_flat.write.format('bigquery').option('table', target_reco).mode('overwrite').save()

spark.stop()
