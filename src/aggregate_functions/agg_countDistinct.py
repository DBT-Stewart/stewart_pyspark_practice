from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct,col

spark = SparkSession.builder.appName("CountDistinctExample").getOrCreate()

data = [
    ("Edwin", "HR"),
    ("Diva", "IT"),
    ("Chandu", "IT"),
    ("Draken", "Finance"),
    ("Eva", "HR"),
    ("Wall-E", "Finance")
]
df = spark.createDataFrame(data, ["name", "department"])
df.show()

# 1. Count distinct departments
df.select(countDistinct("department").alias("unique_departments")).show()

# 2. Count distinct combinations of name and department (just as example)
df.select(countDistinct("name", "department").alias("unique_departments")).show()