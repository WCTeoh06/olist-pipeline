from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("monthly_revenue").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

orders = spark.read.csv("data/raw/olist_orders_dataset.csv",
                        header=True, inferSchema=True)
items = spark.read.csv("data/raw/olist_order_items_dataset.csv",
                       header=True, inferSchema=True)

# 1. Keep only delivered orders and fix the date column
delivered = (
    orders.filter(F.col("order_status") == "delivered")
          .withColumn("purchase_ts", F.to_timestamp("order_purchase_timestamp"))
          .withColumn("year_month", F.date_format("purchase_ts", "yyyy-MM"))
)

# 2. Join orders with their items
joined = delivered.join(items, on="order_id", how="inner")

# 3. Revenue and order count per month
monthly = (
    joined.groupBy("year_month")
          .agg(F.round(F.sum("price"), 2).alias("revenue"),
               F.countDistinct("order_id").alias("orders"))
          .orderBy("year_month")
)

monthly.show(30, truncate=False)

# 4. Save as Parquet
monthly.write.mode("overwrite").parquet("data/processed/monthly_revenue")
print("Saved!")
