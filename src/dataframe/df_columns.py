# columns is a property of a DataFrame (not a function).
# Used in looping, renaming, selecting, and validation tasks.
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("ColumnsExample").getOrCreate()

# Sample DataFrame
data = [("Stewart", 30, "HR"), ("Jesus", 25, "IT")]
columns = ["name", "age", "department"]
df = spark.createDataFrame(data, columns)

# 1. Get all column names
print("Columns:", df.columns)

# 2. Loop over column names
for col_name in df.columns:
    print(f"Column: {col_name}")

# 3. Select only numeric columns (example logic)
numeric_columns = [col for col in df.columns if col in ["age"]]  # Custom logic
df.select(numeric_columns).show()
