from pyspark.sql import SparkSession
from pyspark.sql.functions import last

spark = SparkSession.builder.appName("LastFunctionExample").getOrCreate()

data = [
    ("Aldrin", "HR"),
    ("Bannerji", "IT"),
    ("Chinmai", "IT"),
    ("Stewart", "Finance"),
    ("Edwin", "HR")
]
df = spark.createDataFrame(data, ["name", "department"])

# 1. Last department overall (physical order-based)
df.select(last("department").alias("last_department")).show()

# 2. Last employee in each department (without sorting — non-deterministic)
data2 = [
    ("HR", "Ambrin"),
    ("HR", "Eashwar"),
    ("IT", "Brinit"),
    ("IT", "Chandana"),
    ("Finance", "Edwin")
]
df2 = spark.createDataFrame(data2, ["department", "employee"])
df2.groupBy("department").agg(last("employee").alias("last_employee")).show()
