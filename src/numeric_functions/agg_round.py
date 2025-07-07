# round() is a numeric function used to round off decimal values in a column to the nearest whole number or to a specific number of decimal places.
from pyspark.sql import SparkSession
from pyspark.sql.functions import round, col

spark = SparkSession.builder.appName("RoundExample").getOrCreate()

data = [("Jeho", 89.456), ("Cylvia", 72.897), ("Brinit", 95.123)]
df = spark.createDataFrame(data, ["name", "score"])
df.show()

# 1. Round to nearest integer (0 decimal places)
df.withColumn("rounded_score", round("score")).show()

# 2. Round to 1 decimal place
df.withColumn("rounded_1dp", round("score", 1)).show()

# 3. Round to 2 decimal places using col()
df.withColumn("rounded_2dp", round(col("score"), 2)).show()
