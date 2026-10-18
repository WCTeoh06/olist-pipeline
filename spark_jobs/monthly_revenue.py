from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("monthly_revenue").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

orders = spark.read.csv("data/raw/olist_orders_dataset.csv",
                        header=True, inferSchema=True)
items = spark.read.csv("data/raw/olist_order_items_dataset.csv",
                       header=True, inferSchema=True)

delivered = (
    orders.filter(F.col("order_status") == "delivered")
          .withColumn("purchase_ts", F.to_timestamp("order_purchase_timestamp"))
          .withColumn("year_month", F.date_format("purchase_ts", "yyyy-MM"))
)

joined = delivered.join(items, on="order_id", how="inner")

monthly = (
    joined.groupBy("year_month")
          .agg(F.round(F.sum("price"), 2).alias("revenue"),
               F.countDistinct("order_id").alias("orders"))
          .orderBy("year_month")
)

monthly.show(30, truncate=False)
monthly.write.mode("overwrite").parquet("data/processed/monthly_revenue")
print("Saved!")
