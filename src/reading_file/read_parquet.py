from pyspark.sql.session import SparkSession

spark = SparkSession.builder.appName("read_parquet").master("local[*]").getOrCreate()

parquet_df = spark.read.format('parquet').load("sample_files/sampleuserdata.parquet")
parquet_df.show(n=500)
parquet_df.printSchema()
print("Total number of rows are :",parquet_df.count())