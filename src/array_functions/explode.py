from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ExplodeExample").getOrCreate()

data = [("Aparna", ["Python", "SQL"]), ("Bandana", ["Java", "C++"])]
df = spark.createDataFrame(data, ["name", "skills"])
df.show()

df_exploded = df.withColumn("skill", explode(df.skills))
df_exploded.show()
df_exploded.printSchema()
df.printSchema()
df_exploded.select(col('name'), col('skill'))
