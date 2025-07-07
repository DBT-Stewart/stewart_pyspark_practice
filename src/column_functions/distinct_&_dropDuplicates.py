from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DistinctExample").getOrCreate()

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

# 4. dropDuplicates()
df.dropDuplicates(['department']).show()
df.dropDuplicates(['name']).show()