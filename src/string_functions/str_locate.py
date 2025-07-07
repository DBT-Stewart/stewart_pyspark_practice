from pyspark.sql import SparkSession
from pyspark.sql.functions import locate

spark = SparkSession.builder.appName("LocateFunctionExample").getOrCreate()

data = [("hello spark world",), ("spark tutorial",), ("pyspark usage",)]
df = spark.createDataFrame(data, ["text"])
df.show()

# to find the substring wherever it is
df.withColumn("first_spark", locate("spark", "text")).show()

# to find the substring with the starting string index as 7
df.withColumn("spark_after_7", locate("spark", "text", 7)).show()