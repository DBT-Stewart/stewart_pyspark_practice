from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("DescribeExample").getOrCreate()

# Sample data
data = [("Jesus", 30, 2000.5), ("Pooja", 25, 1800.0), ("Hema", 35, 2400.0)]
columns = ["name", "age", "salary"]
df = spark.createDataFrame(data, columns)

# 1. Describe all numeric columns
df.describe().show()

# 2. Describe specific column: age
df.describe("age").show()

# 3. Describe multiple columns
df.describe("age", "salary").show()

# 4. Describe non-numeric column (only count, min, max shown)
df.describe("name").show()
# In non-numerics column example (name):
# min = name with alphabetically first string 
# max = name with alphabetically second string
