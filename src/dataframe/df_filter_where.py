from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark
spark = SparkSession.builder.appName("FilterExample").getOrCreate()

# Sample data
data = [("Stewart", 30), ("Bhuvan", 25), ("Pooja", 35), ("Hema", 22)]
columns = ["name", "age"]

df = spark.createDataFrame(data, columns)

# 1. Filter rows where age > 25
df.filter(col("age") > 25).show()

# 2. Filter using SQL-style where()
df.where(col("age") < 30).show()

# 3. Filter with multiple conditions
df.filter((col("age") > 25) & (col("age") < 35)).show()

# 4. Filter names starting with 'A'
df.filter(col("name").startswith("P")).show()

# 5. Filter using '==' instead of '='
df.filter(col("name") == "Pooja").show()
