from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("read_csv").master("local[*]").getOrCreate()

r_csv_df_1 = spark.read.csv('../../sample_files/Employee-Q1.csv')
r_csv_df_1.show()

r_csv_df_2 = spark.read.csv('../../sample_files/Employee-Q1.csv', header=True)
r_csv_df_2.show()

r_csv_df_3 = spark.read.format('csv').option('header','True').load('../../sample_files/Employee-Q1.csv')
r_csv_df_3.show()

r_csv_df_4 = spark.read.format('csv').option('header','True').load(['../../sample_files/Employee-Q1.csv','../../sample_files/Employee-Q2.csv'])
r_csv_df_4.show()

# r_csv_df_5 = spark.read.format('csv').option('header','True').load('../../sample_files/*.csv')
# r_csv_df_5.show()
# this is not possible in pycharm