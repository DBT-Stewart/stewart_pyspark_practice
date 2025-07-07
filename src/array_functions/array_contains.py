from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Array_ContainsExample").getOrCreate()

data = [("Godzilla", ["Python", "SQL"]), ("Kong", ["Java", "C++"])]
df = spark.createDataFrame(data, ["name", "skills"])
df.show()

df_filter = df.filter(array_contains(df.skills, "Python"))
df_filter.show()

df_wc = df.withColumn("skill_check", array_contains(col('skills'),'Java'))
df_wc.show()
