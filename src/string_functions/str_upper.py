from pyspark.sql import SparkSession
from pyspark.sql.functions import upper

spark = SparkSession.builder.appName("UpperFunctionExample").getOrCreate()

data = [("stewart",), ("brinit",), ("CHARLIE",)]
df = spark.createDataFrame(data, ["name"])

df.withColumn("name_uppercase", upper("name")).show()
