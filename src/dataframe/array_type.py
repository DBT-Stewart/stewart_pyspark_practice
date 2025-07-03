from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("array type").getOrCreate()

data = [
    ("Alice", [1,2]),
    ("Bob", [3,4]),
    ("Charlie", [5,6])
]

schema = StructType([
    StructField("name", StringType(), True),
    StructField("numbers", ArrayType(IntegerType()), True)
])

df = spark.createDataFrame(data,schema)

df.show()
df.printSchema()

df2 = df.withColumn('first_number', col('numbers')[0])

df1 = df.withColumn('swaped_number', array(col('numbers')[1], col('numbers')[0]))
df1.show()