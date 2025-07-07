from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("PivotExample").getOrCreate()

data = [(1,'Stewart','male','HR'),(2,'Pooja','female','HR'),(3,'Hema','female','IT'),(4,'Sharukh','male','HR'),(5,'Prabha','male','IT'),(6,'Sushmi','female','HR'),(7,'Hema','female','IT'),(8,'Fayaz','male','IT')]
schema = ['id', 'name', 'gender', 'department']

df = spark.createDataFrame(data, schema)
df.show()

df.groupby('department', 'gender').count().show()

df.groupby('department').pivot('gender').count().show()

df.groupby('department').pivot('gender',['male']).count().show()

df.groupby('department').pivot('gender', ['female','male']).count().show()