# concat with separator
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col

spark = SparkSession.builder.appName("ConcatWSExample").getOrCreate()

data = [("Stewart", "Prince", "Data Engineer"), ("Brinit", "Sharon", "Developer")]
df = spark.createDataFrame(data, ["first_name", "last_name", "title"])
df.show()

df.withColumn("full_description", concat_ws(" ", "first_name", "last_name", "title")).show(truncate=False)

df.withColumn("csv_format", concat_ws(",", "first_name", "last_name", "title")).show(truncate=False)

df.withColumn("csv_format", concat_ws("|", "first_name", "last_name", "title")).show(truncate=False)
