from pyspark.sql import SparkSession
from pyspark.sql.functions import rtrim

spark = SparkSession.builder.appName("LtrimFunctionExample").getOrCreate()

data = [("   Stewart    ",), ("Pooja  ",), ("Chutki",)]
df = spark.createDataFrame(data, ["name"])
df.show()

df.withColumn("cleaned_name", rtrim("name")).show()
