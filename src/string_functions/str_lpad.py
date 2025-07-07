from pyspark.sql import SparkSession
from pyspark.sql.functions import lpad

spark = SparkSession.builder.appName("LpadFunctionExample").getOrCreate()

data = [("1",), ("88",), ("12345678",)]
df = spark.createDataFrame(data, ["code"])
# if we give more string characters than the allocated number the table will consider only the allocated size characters and remaining won't get printed

# Left pad to 6 characters with '0'
df.withColumn("padded_code", lpad("code", 6, "0")).show()
