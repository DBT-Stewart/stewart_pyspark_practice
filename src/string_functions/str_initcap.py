from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap

spark = SparkSession.builder.appName("InitCapExample").getOrCreate()

data = [("pooja siva",), ("DATA ENGINEER",), ("Diggibyte tech",)]
df = spark.createDataFrame(data, ["name"])

# Capitalize each word
df.withColumn("formatted_name", initcap("name")).show()
