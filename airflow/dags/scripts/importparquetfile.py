from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions
from pyspark.sql.functions import *

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Assignment1") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.parquet("hdfs://namenode:9000/user/root/userdata1.parquet")
#df.show()

# Export the dataframe into the Hive table test_airflowdb
df.write.mode("overwrite").saveAsTable("airflowdb")

# Read thhe Hive table test_airflowdb
df_read_from_hive = spark.sql("select * from airflowdb")

# Dump the data in the form of csv file
df_read_from_hive.write.csv("hdfs://namenode:9000/user/root/airflowdb.csv")

# filter on the basis of gender
df_female = df.filter(df.gender == "Female")
df_female.write.format("json").save("hdfs://namenode:9000/user/root/json/female.json")

df_male = df.filter(df.gender == "Male")
df_male.write.format("json").save("hdfs://namenode:9000/user/root/json/male.json")

#Male and female avg salary
df_avgMaleSal = df.filter(df.gender == "Male").agg(avg(col("salary")))
df_avgMaleSal.write.format("json").save("hdfs://namenode:9000/user/root/json/avgMaleSalary.json")

df_avgFemaleSal = df.filter(df.gender == "Female").agg(avg(col("salary")))
df_avgFemaleSal.write.format("json").save("hdfs://namenode:9000/user/root/json/avgFemaleSalary.json")

df_avgSal = df.agg(avg(col("salary")))
df_avgSal.write.format("json").save("hdfs://namenode:9000/user/root/json/avgSalary.json")

# max sal of all
df_maxSalFem = df.filter(df.gender == "Female").agg(max(col("salary")))
df_maxSalFem.write.format("json").save("hdfs://namenode:9000/user/root/json/maxFemaleSalary.json")

df_maxSalMale = df.filter(df.gender == "Male").agg(max(col("salary")))
df_maxSalMale.write.format("json").save("hdfs://namenode:9000/user/root/json/maxMaleSalary.json")

df_maxSal = df.agg(max(col("salary")))
df_maxSal.write.format("json").save("hdfs://namenode:9000/user/root/json/maxSalary.json")

#convert json to parquet
jsonDfRead = spark.read.json("hdfs://namenode:9000/user/root/json/female.json")
jsonDfRead.write.format("parquet").save("hdfs://namenode:9000/user/root/parquet/jsonToParquet.parquet")
