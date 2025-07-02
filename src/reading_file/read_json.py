from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("read_json").master("local[*]").getOrCreate()

r_json_df_1 = spark.read.json('../../sample_files/employer_new_1.json',multiLine=True)
r_json_df_1.show()

schema = StructType([
    StructField("employee_id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("gender", StringType()),
    StructField("salary", IntegerType()),
    StructField("joining_date", StringType()),
    StructField("active", BooleanType())
])

r_json_df_2 = spark.read.option("multiline", True).schema(schema).json('../../sample_files/employer_new_1.json')
r_json_df_2.show()
r_json_df_2.printSchema()
