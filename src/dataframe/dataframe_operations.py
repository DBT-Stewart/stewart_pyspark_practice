from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("DFOperations").getOrCreate()

data = [("1","Stewart", 30, "DE", "Hosur"),
        ("2","Pooja", 25, "IT", "Bangalore"),
        ("3","Bhuvan", 30, "HR", "Bangalore"),
        ("4","Hema", 35, "IT", "Hosur"),
        ("5","Edwin", 25, "DE", "Bangalore"),
        ("6","Divakar", 30, "DS", "Bangalore"),
        ("7","Vegas", 35, "DS", "Hosur")]

columns = ["id", "Name", "Age", "Department", "Branch"]

df = spark.createDataFrame(data, columns)
df.show()

df.select("Name", "Department").show()

df.filter(df["Age"] > 25).show()

df.groupBy("Department").count().show()

df.orderBy("Age").show()

df.withColumn("AgePlusFive", col("Age") + 5).show()

df.drop("Department").show()

df.select("Age", "Department").distinct().show()

