from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("SplitExample").getOrCreate()

data = [("Anandhi,Python,SQL",), ("Bargavi,Java,C++",)]
df = spark.createDataFrame(data, ["info"])

df_split = df.withColumn("split_info", split(df["info"], ","))
df_split.show(truncate=False)

df_exploded = df_split.withColumn("skill", explode(df_split.split_info))
df_exploded.show(truncate=False)
