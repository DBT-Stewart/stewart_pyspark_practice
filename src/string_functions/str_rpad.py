from pyspark.sql import SparkSession
from pyspark.sql.functions import rpad

# Initialize Spark session
spark = SparkSession.builder.appName("RpadFunctionExample").getOrCreate()

# Sample data
data = [(" ",), ("Hello",), ("PySpark",)]
df = spark.createDataFrame(data, ["text"])

# Pad strings to length 10 with '*'
df.withColumn("padded_text", rpad("text", 10, "*/")).show()
