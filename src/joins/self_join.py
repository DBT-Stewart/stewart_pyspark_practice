from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("LeftSemiAntiExample").getOrCreate()

data_1 = [(1,'Stewart',None),(2,'Pooja',1),(3,'Hema',1),(4,'Sushmitha',2),(5,'Edwin',3),(6,'Aishu',4)]
schema_1 = ['id', 'name', 'm_id']

# data_2 = [(1,'Hosur'),(2,'Bangalur'),(5,'Swedan')]
# schema_2 = ['br_id','br_name']

empDf = spark.createDataFrame(data_1,schema_1)
# brDf = spark.createDataFrame(data_2,schema_2)

empDf.show()
# brDf.show()

empDf.alias('emp').join(empDf.alias('manager'),col('emp.m_id')==col('manager.id'),"left").select(col('emp.name').alias("employerName"),col('manager.name').alias('managerName')).show()