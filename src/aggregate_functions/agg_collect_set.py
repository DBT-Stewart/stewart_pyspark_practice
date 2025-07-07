from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set

spark = SparkSession.builder.appName("CollectListExample").getOrCreate()

data = [("Stewart", "Math"), ("Stewart", "Math"), ("Pooja", "English"), ("Pooja", "Math")]
# data = [("Stewart", "Math"), ("Stewart", "Science"), ("Pooja", "English"), ("Pooja", "Math")]
df = spark.createDataFrame(data, ["name", "subject"])
df.show()

df.groupBy("name").agg(collect_set("subject").alias("subject_list")).show(truncate=False)
