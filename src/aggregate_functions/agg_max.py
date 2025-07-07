from pyspark.sql import SparkSession
from pyspark.sql.functions import max

spark = SparkSession.builder.appName("MinFunction").getOrCreate()

data = [("Grace", 85), ("Benny", 78), ("Jimmy", 92), ("Feros", 70)]
df = spark.createDataFrame(data, ["name", "score"])
df.show()

# 1. Maximum score
df.select(max("score").alias("max_score")).show()

# 2. Group-wise maximum salary
data2 = [("Grace", "HR", 3000), ("Benny", "IT", 2500), ("Jimmy", "IT", 3200), ("Feros", "HR", 2800)]
df2 = spark.createDataFrame(data2, ["name", "department", "salary"])
df2.groupBy("department").agg(max("salary").alias("max_salary")).show()

# 3. Alphabetically last name
df.select(max("name").alias("last_name_alphabetically")).show()