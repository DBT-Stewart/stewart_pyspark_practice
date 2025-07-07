from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("AliasExample").getOrCreate()

data = [(1,'Stewart',20),(2,'Pooja',19),(3,'Prabhu','52')]
schema = ['id', 'name', 'age']

df = spark.createDataFrame(data, schema)
df.show()

df.select(df.age.like('p%'))
df.show()