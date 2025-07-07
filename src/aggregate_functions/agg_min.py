from pyspark.sql import SparkSession
from pyspark.sql.functions import min

spark = SparkSession.builder.appName("MinFunction").getOrCreate()

data = [("Grace", 85), ("Benny", 78), ("Jimmy", 92), ("Feros", 70)]
df = spark.createDataFrame(data, ["name", "score"])
df.show()

# 1. Minimum score overall
df.select(min("score").alias("minimum_score")).show()

# 2. Grouped minimum salary
data2 = [("Grace", "HR", 3000), ("Benny", "IT", 2500), ("Jimmy", "IT", 3200), ("Feros", "HR", 2800)]
df2 = spark.createDataFrame(data2, ["name", "department", "salary"])
df2.groupBy("department").agg(min("salary").alias("min_salary")).show()

# 3. Minimum name alphabetically
df.select(min("name").alias("first_alphabetical_name")).show()
