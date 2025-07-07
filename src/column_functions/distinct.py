from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("DistinctExample").getOrCreate()

# Sample data with duplicates
data = [
    ("Stewart", "HR"),
    ("Jesus", "IT"),
    ("Stewart", "HR"),
    ("Pooja", "IT")
]
columns = ["name", "department"]
df = spark.createDataFrame(data, columns)

# 1. Show original DataFrame
df.show()

# 2. Apply distinct()
df.distinct().show()

# 3. Count before and after distinct
print("Original count:", df.count())
print("Distinct count:", df.distinct().count())
