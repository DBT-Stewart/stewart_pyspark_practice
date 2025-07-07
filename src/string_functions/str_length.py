from pyspark.sql import SparkSession
from pyspark.sql.functions import length

spark = SparkSession.builder.appName("LengthFunctionExample").getOrCreate()

data = [("Stewart",), ("Pooja",), ("Hemalekha",), ("",), (None,)]
df = spark.createDataFrame(data, ["name"])

df.withColumn("name_length", length("name")).show()
