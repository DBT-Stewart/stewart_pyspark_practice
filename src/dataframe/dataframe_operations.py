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


from pyspark.sql.functions import *

wc = spark.read.csv("../../sample_files/Employee-Q1.csv",header=True,inferSchema=True)
wc.show()
wc.printSchema()

wc1 = wc.withColumn('salary', col('salary').cast('long'))
# wc1 = wc.withColumn('salary', col('salary').cast('string'))
wc1.show()
wc1.printSchema()
wc2 = wc.withColumn('salary', col('salary')*0.5)
wc3 = wc.withColumn('avg_salary', col('salary')*0.5)
wc2.show()
wc3.show()
wc4 = wc.withColumn('salary', col('salary')*0.5).withColumn('half_salary', col('salary')*0.5)
wc4.show()
wc4.printSchema()
wc5 = wc.withColumn("gender",lit("unknown"))
wc5.show()
wc5.printSchema()
