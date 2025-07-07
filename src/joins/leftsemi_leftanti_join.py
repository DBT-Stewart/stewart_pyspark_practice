from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("LeftSemiAntiExample").getOrCreate()

data_1 = [(1,'Stewart',21,1),(2,'Pooja',19,2),(3,'Hema',23,1),(4,'Sushmitha',22,2),(5,'Edwin',25,3),(6,'Aishu',20,4)]
schema_1 = ['id', 'name', 'age', 'branch_id']

data_2 = [(1,'Hosur'),(2,'Bangalur'),(5,'Swedan')]
schema_2 = ['br_id','br_name']

empDf = spark.createDataFrame(data_1,schema_1)
brDf = spark.createDataFrame(data_2,schema_2)

empDf.show()
brDf.show()

# leftsemi
empDf.join(empDf, empDf.id==empDf.id, 'leftsemi').show()

# leftanti
empDf.join(brDf, empDf.branch_id==brDf.br_id, 'leftanti').show()