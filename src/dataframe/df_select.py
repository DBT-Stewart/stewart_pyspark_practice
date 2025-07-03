from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

# Initialize Spark
spark = SparkSession.builder.appName("SelectExample").getOrCreate()

# Sample data
data = [("Stewart", 30, "HR"), ("Pooja", 25, "IT"), ("Hema", 28, "Finance")]
columns = ["name", "age", "department"]

df = spark.createDataFrame(data, columns)

# 1. Select single column
df.select("name").show()

# 2. Select multiple columns
df.select("name", "age").show()

# 3. Select using col() expression
df.select(col("department")).show()

# 4. Select and rename using alias
df.select(col("name").alias("employee_name")).show()

# 5. Apply function to column (uppercase department names)
df.select(upper(col("department")).alias("DEPT")).show()
