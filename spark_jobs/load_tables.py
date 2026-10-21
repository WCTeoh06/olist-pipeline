import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

password = os.environ["WAREHOUSE_PASSWORD"]
URL = "jdbc:postgresql://localhost:5433/olist"
PROPS = {"user": "warehouse", "password": password,
         "driver": "org.postgresql.Driver"}

spark = (SparkSession.builder.appName("load_tables")
         .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

TABLES = {
    "order_items": {
        "file": "olist_order_items_dataset.csv",
        "keys": ["order_id", "order_item_id"],
        "dates": ["shipping_limit_date"],
    },
    "order_payments": {
        "file": "olist_order_payments_dataset.csv",
        "keys": ["order_id", "payment_sequential"],
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "keys": ["product_id"],
        # the source file has these two column names misspelled
        "rename": {"product_name_lenght": "product_name_length",
                   "product_description_lenght": "product_description_length"},
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "keys": ["seller_id"],
    },
    "category_translation": {
        "file": "product_category_name_translation.csv",
        "keys": ["product_category_name"],
    },
}


def load(name, cfg):
    df = spark.read.csv(f"data/raw/{cfg['file']}", header=True, inferSchema=True)
    for old, new in cfg.get("rename", {}).items():
        df = df.withColumnRenamed(old, new)
    for c in cfg.get("dates", []):
        df = df.withColumn(c, F.to_timestamp(c))
    df = df.dropna(subset=cfg["keys"]).dropDuplicates(cfg["keys"])
    expected = df.count()

    # overwrite + truncate: re-running gives the same result
    df.write.jdbc(URL, name, mode="overwrite",
                  properties={**PROPS, "truncate": "true"})

    # quality gate: count what actually landed in the database
    actual = spark.read.jdbc(URL, name, properties=PROPS).count()
    if actual != expected:
        raise ValueError(f"{name}: expected {expected} rows, found {actual}")
    print(f"OK  {name}: {actual} rows")


for table, config in TABLES.items():
    load(table, config)
