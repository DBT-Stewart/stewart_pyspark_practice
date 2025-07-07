from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("AliasExample").getOrCreate()

data = [(1,'Stewart',20),(2,'Pooja',19),(3,'Edwin','52')]
schema = ['id', 'name', 'age']

df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()

df.select(df.age.cast('int')).printSchema()
df.show()