from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("WithColumnExample").getOrCreate()

data = [("Stewart", 30), ("Pooja", 25)]
df = spark.createDataFrame(data, ["name", "age"])

# Add multiple columns
df = df.withColumns({'name': upper('name'),'age': col('age')+10 })
df.show()

# df.select(
#     col("name"),
#     col("age"),
#     (col("age") * 2).alias("double_age"),
#     upper(col("name")).alias("name_upper")
# ).show()
