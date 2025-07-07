# abs() is a numeric function in PySpark that returns the absolute (non-negative) value of a number.
# It removes the negative sign, if any.
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs

spark = SparkSession.builder.appName("AbsFunctionExample").getOrCreate()

data = [("Merry", -10), ("Genilia", 20), ("Demon", -15), ("Fayaz", 0)]
df = spark.createDataFrame(data, ["name", "change"])
df.show()

# Apply abs function to remove negative signs
df.withColumn("absolute_change", abs(df["change"])).show()
