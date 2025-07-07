from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("DropExample").getOrCreate()

# Sample data
data = [("Chanakya", 30, "HR", 50000), ("Brindha", 25, "IT", 60000)]
columns = ["name", "age", "department", "salary"]
df = spark.createDataFrame(data, columns)
df.show()

# 1. Drop a single column
df.drop("salary").show()

# 2. Drop multiple columns
df.drop("department", "salary").show()

# 3. Drop a column that doesn't exist (no error; silently ignored)
df.drop("bonus").show()

# 4. Chain drop with other transformations
df.drop("department").withColumnRenamed("name", "employee_name").show()
