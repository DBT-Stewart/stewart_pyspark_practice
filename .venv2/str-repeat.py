from pyspark.sql import SparkSession
from pyspark.sql.functions import repeat

spark = SparkSession.builder.appName("RepeatFunctionExample").getOrCreate()

data = [("Zz",), ("Ooh",), ("Hi",)]
df = spark.createDataFrame(data, ["text"])

# Repeat the string 3 times
df.withColumn("repeated_text", repeat("text", 3)).show()
