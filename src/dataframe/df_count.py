from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("CountExample").getOrCreate()

# Sample DataFrame
data = [("Stewart", "DE"), ("Pooja", "DS"), ("Bhuvan", "DS"), ("Hema", "DE")]
columns = ["name", "department"]
df = spark.createDataFrame(data, columns)

df.show()

# 1. Total row count
print("Total Rows:", df.count())

# 2. Count after filtering
filtered_df = df.filter(df.department == "DE")
print("Filtered Rows (DE):", filtered_df.count())
filtered_df.show()

# 3. Count on empty DataFrame
empty_df = df.filter(df.name == "Stewart")
print("Empty Filter Result:", empty_df.count())
empty_df.show()