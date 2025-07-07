from pyspark.sql import SparkSession
from pyspark.sql.functions import first

spark = SparkSession.builder.appName("FirstFunctionExample").getOrCreate()

data = [
    ("Aldrin", "HR"),
    ("Bannerji", "IT"),
    ("Chinmai", "IT"),
    ("Stewart", "Finance"),
    ("Edwin", "HR")
]
df = spark.createDataFrame(data, ["name", "department"])

# 1. First department overall
df.select(first("department").alias("first_department")).show()

# 2. Grouped data: First person in each department (NOTE: may vary without sorting)
data2 = [
    ("HR", "Ambrin"),
    ("HR", "Eashwar"),
    ("IT", "Brinit"),
    ("IT", "Chandana"),
    ("Finance", "Edwin")
]
df2 = spark.createDataFrame(data2, ["department", "employee"])
df2.groupBy("department").agg(first("employee").alias("first_employee")).show()


