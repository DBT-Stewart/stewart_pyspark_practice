from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("SubstrFunctionExample").getOrCreate()

data = [("Stewart",), ("Edwin",), ("Pooja",), ("Hema",)]
df = spark.createDataFrame(data, ["name"])

df.withColumn("first_3_chars", col("name").substr(1, 3)).show()

df.withColumn("mid_chars", col("name").substr(2, 4)).show()
