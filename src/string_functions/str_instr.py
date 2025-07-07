# to check the given string is a string in either of the row and gives the position of the sub-string

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("InstrFunctionExample").getOrCreate()

data = [("hello spark world",), ("spark tutorial",), ("data engineer",), ("Pyspark",)]
df = spark.createDataFrame(data, ["text"])

df.withColumn("position_spark", instr("text", "spark")).show(truncate=False)
# this gives a string as an input and iterates all the rows and finds whether any substring exists

# Hari's Question : To change the name of the column name using withColumn()
# df2 = df.withColumns({'text_2' : col('text')})
# df2.show()