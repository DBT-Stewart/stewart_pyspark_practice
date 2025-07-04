from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, lit

spark = SparkSession.builder.appName("WithColumnExample").getOrCreate()

data = [("Stewart", 30), ("Pooja", 25)]
df = spark.createDataFrame(data, ["name", "age"])

# 1. Add a new constant column
df.withColumn("country", lit("India")).show()

# 2. Modify existing column - uppercase name
df.withColumn("name", upper(col("name"))).show()

# 3. Add a calculated column - age * 2
df.withColumn("double_age", col("age") * 2).show()
