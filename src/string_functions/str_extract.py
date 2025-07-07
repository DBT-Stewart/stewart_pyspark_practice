from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName("SimpleRegexExample").getOrCreate()

data = [("OrderID: 12345",), ("OrderID: 98765",)]
df = spark.createDataFrame(data, ["info"])

# Extract only the number after "OrderID: "
df.withColumn("order_id", regexp_extract("info", r"OrderID: (\d+)", 1)).show()
