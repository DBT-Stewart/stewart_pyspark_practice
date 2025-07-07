from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

spark = SparkSession.builder.appName("CollectListExample").getOrCreate()

# data = [("Stewart", "Math"), ("Stewart", "Math"), ("Pooja", "English"), ("Pooja", "Math")]
data = [("Stewart", "Math"), ("Stewart", "Science"), ("Pooja", "English"), ("Pooja", "Math")]
df = spark.createDataFrame(data, ["name", "subject"])
df.show()

df.groupBy("name").agg(collect_list("subject").alias("subject_list")).show(truncate=False)
