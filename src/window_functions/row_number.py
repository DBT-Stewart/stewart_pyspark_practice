from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName("RowNumberExample").getOrCreate()

data = [("HR", "Stewart", 3000),
        ("IT", "Divakar", 3500),
        ("IT", "Shreya", 4000),
        ("HR", "Edwin", 4500),
        ("HR", "Dhina", 3200)]

df = spark.createDataFrame(data, ["dept", "name", "salary"])

windowSpec = Window.partitionBy("dept").orderBy("salary")

df.withColumn("row_num", row_number().over(windowSpec)).show()