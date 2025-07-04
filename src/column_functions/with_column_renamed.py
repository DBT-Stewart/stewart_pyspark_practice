from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RenameExample").getOrCreate()

data = [("Jesus", 30), ("Christ", 20)]
df = spark.createDataFrame(data, ["full_name", "years_old"])
df.show()

# 1. Rename a single column
df.withColumnRenamed("full_name", "name").show()

# 2. Rename multiple columns (chaining)
df = df.withColumnRenamed("full_name", "name") \
       .withColumnRenamed("years_old", "age")
df.show()
