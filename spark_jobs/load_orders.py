import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

password = os.environ["WAREHOUSE_PASSWORD"]   # never hardcode passwords

spark = (SparkSession.builder.appName("load_orders")
         .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

orders = spark.read.csv("data/raw/olist_orders_dataset.csv",
                        header=True, inferSchema=True)

# Clean: drop rows missing keys, remove duplicates, fix date columns
date_cols = ["order_purchase_timestamp", "order_approved_at",
             "order_delivered_carrier_date", "order_delivered_customer_date",
             "order_estimated_delivery_date"]
clean = orders.dropna(subset=["order_id", "customer_id"]).dropDuplicates(["order_id"])
for c in date_cols:
    clean = clean.withColumn(c, F.to_timestamp(c))

print("Rows to load:", clean.count())

# truncate=true keeps the table and replaces its rows, so re-running is safe
(clean.write.format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5433/olist")
      .option("dbtable", "orders")
      .option("user", "warehouse")
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .option("truncate", "true")
      .mode("overwrite")
      .save())
print("Loaded into PostgreSQL")
