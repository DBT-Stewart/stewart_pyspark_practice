from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName("AvgFunction").getOrCreate()

data = [("Elisa", 85), ("Jacob", 78), ("Jenni", 92), ("David", 70)]
df = spark.createDataFrame(data, ["name", "score"])

# 1. Calculate average of all scores
df.select(avg("score").alias("average_score")).show()

# 2. Grouped average (e.g., department-wise average salary)
data2 = [("Alice", "HR", 3000), ("Bob", "IT", 2500), ("Charlie", "IT", 3200), ("David", "HR", 2800)]
df2 = spark.createDataFrame(data2, ["name", "department", "salary"])
df2.groupBy("department").agg(avg("salary").alias("average_salary")).show()
