from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("basic").getOrCreate()
csv_df = spark.read.csv("../../sample_files/Employee-Q1.csv", header=True, inferSchema=True)
csv_df.show()

csv_df.createOrReplaceTempView("employees")

csv_df_2 = spark.sql("SELECT Department, COUNT(*) as count FROM employees GROUP BY Department")
csv_df_2.show()