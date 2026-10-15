from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("explore_customers").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("data/raw/olist_customers_dataset.csv",
                    header=True, inferSchema=True)

df.printSchema()
df.show(5)
print("Rows:", df.count())

# customers per state
df.groupBy("customer_state").count().orderBy(F.desc("count")).show(10)
