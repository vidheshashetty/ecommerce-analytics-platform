import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType, DoubleType


spark = SparkSession.builder.appName("ProcessLargeOrders").getOrCreate()


INPUT_PATH = "s3://ecommerce/raw/large_data/"
OUTPUT_PATH = "s3://ecommerce/processed/orders_2/"


df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)


df = df.withColumn("order_id", col("order_id").cast(IntegerType())) \
       .withColumn("customer_id", col("customer_id").cast(IntegerType())) \
       .withColumn("product_id", col("product_id").cast(IntegerType())) \
       .withColumn("amount", col("amount").cast(DoubleType())) \
       .withColumn("order_date", to_date(col("order_date")))
df = df.filter(
    (col("order_id").isNotNull()) &
    (col("customer_id").isNotNull()) &
    (col("product_id").isNotNull()) &
    (col("amount") > 0)
)
df = df.dropDuplicates(["order_id"])
df = df.repartition("order_date")

df.write.mode("overwrite") \
    .partitionBy("order_date") \
    .parquet(OUTPUT_PATH)

print("✅ Large orders processed and saved to S3 (Parquet)")
