from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Dataframe").getOrCreate()

data = [(1,"Stewart", 25), (2,"Pooja", 30)]
columns = ["Id", "Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()

