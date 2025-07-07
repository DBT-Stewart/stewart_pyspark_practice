from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ArrayExample").getOrCreate()

data = [("Ankush", 90, 85), ("Black", 80, 88)]
df = spark.createDataFrame(data, ["name", "math", "science"])
df.show()

df_array = df.withColumn("scores", array("math", "science"))
df_array.show()
