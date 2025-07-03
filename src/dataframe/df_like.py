from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder.appName("LikeExample").getOrCreate()

# Sample data
data = [("Jesus",), ("Pooja",), ("Hema",), ("Bhuvan",), ("Stewart",)]
columns = ["name"]
df = spark.createDataFrame(data, columns)

# 1. Name starts with 'A'
df.filter(col("name").like("P%")).show()

# 2. Name ends with 'e'
df.filter(col("name").like("%a")).show()

# 3. Name contains 'li'
df.filter(col("name").like("%oo%")).show()

# 4. Name is 5 characters long and ends with 'e'
df.filter(col("name").like("____s")).show()
