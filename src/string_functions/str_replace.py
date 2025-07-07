from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

spark = SparkSession.builder.appName("ReplaceFunctionExample").getOrCreate()

data = [("hello-world",), ("spark_tutorial",), ("123-456-7890",)]
df = spark.createDataFrame(data, ["text"])
df.show()

df.withColumn("replace_dash", regexp_replace("text", "-", " ")).show()

df.withColumn("mask_digits", regexp_replace("text", "[0-9]", "X")).show()