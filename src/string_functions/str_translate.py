from pyspark.sql import SparkSession
from pyspark.sql.functions import translate

spark = SparkSession.builder.appName("TranslateFunctionExample").getOrCreate()

data = [("spark123",), ("data2025",), ("abcDEF",)]
df = spark.createDataFrame(data, ["text"])

# Replace 1->X, 2->Y, 3->Z
df.withColumn("replaced_numbers", translate("text", "123", "XYZ")).show()

df.withColumn("masked_vowels", translate("text", "aeiou", "*****")).show()
