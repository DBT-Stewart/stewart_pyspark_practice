from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# Create SparkSession
spark = SparkSession.builder.appName("SortExample").getOrCreate()

# Sample data
data = [("Jesus", 30), ("Pooja", 25), ("Bhuvan", 35), ("Hema", 25)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)
df.show()

# 1. Sort by age ascending (default)
df.sort("age").show()

# 2. Sort by age descending
df.sort(col("age").desc()).show()

# 3. Sort by multiple columns: age ascending, name descending
df.sort(col("age").asc(), col("name").desc()).show()

# 4. Sort using orderBy() (alternate to sort)
df.orderBy("name").show()
