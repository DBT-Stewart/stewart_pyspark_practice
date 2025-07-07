from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("SumFunction").getOrCreate()

data = [("Andria", 3000), ("Bobby", 2500), ("Chanakya", 3200)]
df = spark.createDataFrame(data, ["name", "salary"])
df.show()

# 1. Total salary across all employees
df.select(sum("salary").alias("total_salary")).show()

# 2. Grouped sum (e.g., department-wise)
data2 = [("Andria", "HR", 3000), ("Bobby", "IT", 2500), ("Chanakya", "IT", 3200)]
df2 = spark.createDataFrame(data2, ["name", "department", "salary"])
df2.groupBy("department").agg(sum("salary").alias("department_salary")).show()
