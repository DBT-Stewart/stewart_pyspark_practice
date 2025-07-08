# Returns the smallest integer greater than or equal to the number (rounds up).
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CeilExample").getOrCreate()

data = [(10.1,), (3.7,), (6.0,), (7.01,)]
df = spark.createDataFrame(data, ["value"])

df.withColumn("ceil_value", ceil("value")).show()