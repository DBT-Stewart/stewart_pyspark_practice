from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit

spark = SparkSession.builder.appName("ConcatFunctionExample").getOrCreate()

data = [("Stewart", "Prince"), ("Brinit", "Sharon"), ("Pooja", "Sivannan")]
df = spark.createDataFrame(data, ["first_name", "last_name"])
df.show()

df.withColumn("full_name", concat(col("first_name"), col("last_name"))).show()

df.withColumn("full_name_with_space", concat(col("first_name"), lit(" "), col("last_name"))).show()
