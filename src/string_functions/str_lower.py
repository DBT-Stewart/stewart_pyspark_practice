from pyspark.sql import SparkSession
from pyspark.sql.functions import lower

spark = SparkSession.builder.appName("LowerFunctionExample").getOrCreate()

data = [("STEWART",), ("Pooja",), ("charan",)]
df = spark.createDataFrame(data, ["name"])

df.withColumn("name_lowercase", lower("name")).show()
