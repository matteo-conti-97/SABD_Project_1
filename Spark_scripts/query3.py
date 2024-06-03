from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from time import time
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, DateType
from datetime import datetime
import math

spark = SparkSession.builder \
  .appName("Query3") \
  .config("spark.mongodb.input.uri", "mongodb://spark_user:spark_password@mongo:27017/results.query3") \
  .config("spark.mongodb.output.uri", "mongodb://spark_user:spark_password@mongo:27017/results.query3") \
  .getOrCreate()

#PARQUET
start = time()
df = spark.read.parquet("hdfs://namenode:8020/disk_data_filtered.parquet") \
  .drop("model", "vault_id") \
  .withColumn('date', date_format(col('date'), 'dd-MM-yyyy'))
df.cache()

# divido il dataframe in due parti, una con failure = 0 e una con failure = 1 e calcolo i quantili per entrambe
failure_1 = df.filter(df["failure"] == 1) \
  .groupBy("serial_number").agg(first("date").alias("dates"), first("s9_power_on_hours").alias("power_on")) \
  .drop("serial_number", "dates") 
  

failure_0 = df.filter(df["failure"] == 0) \
  .groupBy("serial_number").agg(first("date").alias("dates"), first("s9_power_on_hours").alias("power_on")) \
  .drop("serial_number", "dates")


failure_1_list = failure_1.approxQuantile("power_on", [0.0, 0.25, 0.5, 0.75, 1.0], 0)

failure_0_list = failure_0.approxQuantile("power_on", [0.0, 0.25, 0.5, 0.75, 1.0], 0)


df_final = spark.createDataFrame(
    [
      (1, failure_1_list[0], failure_1_list[1], failure_1_list[2], failure_1_list[3], failure_1_list[4], failure_1.count()), 
      (0, failure_0_list[0], failure_0_list[1], failure_0_list[2], failure_0_list[3], failure_0_list[4], failure_0.count())
    ], 
    ["failure", "min", "25th_percentile", "50th_percentile", "75th_percentile", "max", "count"])


'''
#Without approxQuantile
failure_1 = failure_1.orderBy("power_on")
failure_1.cache()
cnt=failure_1.count()
min_f=failure_1.first().power_on
max_f=failure_1.take(cnt)[-1].power_on
q25_idx=math.ceil((cnt)*0.25)
q_75_idx=math.ceil((cnt)*0.75)
q50_idx=math.ceil((cnt)*0.5)
q25=failure_1.take(q25_idx)[-1].power_on
q50=failure_1.take(q50_idx)[-1].power_on
q75=failure_1.take(q_75_idx)[-1].power_on
failure_1_list=[min_f,q25,q50,q75,max_f,cnt]

failure_0 = failure_0.orderBy("power_on")
failure_0.cache()
cnt=failure_0.count()
min_f=failure_0.first().power_on
max_f=failure_0.take(cnt)[-1].power_on
q25_idx=math.ceil((cnt)*0.25)
q_75_idx=math.ceil((cnt)*0.75)
q50_idx=math.ceil((cnt)*0.5)
q25=failure_0.take(q25_idx)[-1].power_on
q50=failure_0.take(q50_idx)[-1].power_on
q75=failure_0.take(q_75_idx)[-1].power_on
failure_0_list=[min_f,q25,q50,q75,max_f,cnt]


df_final = spark.createDataFrame(
    [
      (1, failure_1_list[0], failure_1_list[1], failure_1_list[2], failure_1_list[3], failure_1_list[4], failure_1_list[5]), 
      (0, failure_0_list[0], failure_0_list[1], failure_0_list[2], failure_0_list[3], failure_0_list[4], failure_0_list[5])
    ], 
    ["failure", "min", "25th_percentile", "50th_percentile", "75th_percentile", "max", "count"])
failure_1.unpersist()
failure_0.unpersist()
'''


df_final.show()

end = time()

# Save the results of the query
df_final.write.format("com.mongodb.spark.sql.DefaultSource") \
     .mode("overwrite") \
     .save()

elapsed = end - start
print("Execution time Parquet: ", elapsed)

# Save the performance of the query
perf = spark.createDataFrame([(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), "Query3", "Parquet", elapsed)], ["Timestamp", "Query", "File format", "Execution time (s)"])

perf.write.format("com.mongodb.spark.sql.DefaultSource") \
  .mode("append") \
  .option("collection", "performance") \
  .save()
  
df.unpersist()
  
  
#CSV
schema = StructType([
    StructField("date", DateType(), True),
    StructField("serial_number", StringType(), True),
    StructField("model", StringType(), True),
    StructField("failure", IntegerType(), True),
    StructField("vault_id", IntegerType(), True),
    StructField("s9_power_on_hours", FloatType(), True),
    ])

start = time()
df = spark.read.csv("hdfs://namenode:8020/disk_data_filtered.csv", header=True, schema=schema) \
  .drop("model", "vault_id") \
  .withColumn('date', date_format(col('date'), 'dd-MM-yyyy'))
df.cache()

# divido il dataframe in due parti, una con failure = 0 e una con failure = 1 e calcolo i quantili per entrambe
failure_1 = df.filter(df["failure"] == 1) \
  .groupBy("serial_number").agg(first("date").alias("dates"), first("s9_power_on_hours").alias("power_on")) \
  .drop("serial_number", "dates")

failure_0 = df.filter(df["failure"] == 0) \
  .groupBy("serial_number").agg(first("date").alias("dates"), first("s9_power_on_hours").alias("power_on")) \
  .drop("serial_number", "dates")


failure_1_list = failure_1.approxQuantile("power_on", [0.0, 0.25, 0.5, 0.75, 1.0], 0)

failure_0_list = failure_0.approxQuantile("power_on", [0.0, 0.25, 0.5, 0.75, 1.0], 0)


df_final = spark.createDataFrame(
    [
      (1, failure_1_list[0], failure_1_list[1], failure_1_list[2], failure_1_list[3], failure_1_list[4], failure_1.count()), 
      (0, failure_0_list[0], failure_0_list[1], failure_0_list[2], failure_0_list[3], failure_0_list[4], failure_0.count())
    ], 
    ["failure", "min", "25th_percentile", "50th_percentile", "75th_percentile", "max", "count"])


'''
#Without approxQuantile
failure_1 = failure_1.orderBy("power_on")
failure_1.cache()
cnt=failure_1.count()
min=failure_1.first().power_on
max=failure_1.take(cnt)[-1].power_on
q25_idx=int((cnt)*0.25)
q_75_idx=int((cnt)*0.75)
q50_idx=int((cnt)*0.5)
q25=failure_1.take(q25_idx)[-1].power_on
q50=failure_1.take(q50_idx)[-1].power_on
q75=failure_1.take(q_75_idx)[-1].power_on
failure1_list=[min,q25,q50,q75,max,cnt]

failure_0 = failure_0.orderBy("power_on")
failure_0.cache()
cnt=failure_0.count()
min=failure_0.first().power_on
max=failure_0.take(cnt)[-1].power_on
q25_idx=int((cnt)*0.25)
q_75_idx=int((cnt)*0.75)
q50_idx=int((cnt)*0.5)
q25=failure_0.take(q25_idx)[-1].power_on
q50=failure_0.take(q50_idx)[-1].power_on
q75=failure_0.take(q_75_idx)[-1].power_on
failure0_list=[min,q25,q50,q75,max,cnt]
df_final = spark.createDataFrame(
    [
      (1, failure_1_list[0], failure_1_list[1], failure_1_list[2], failure_1_list[3], failure_1_list[4], failure_1_list[5]), 
      (0, failure_0_list[0], failure_0_list[1], failure_0_list[2], failure_0_list[3], failure_0_list[4], failure_0_list[5])
    ], 
    ["failure", "min", "25th_percentile", "50th_percentile", "75th_percentile", "max", "count"])
failure_1.unpersist()
failure_0.unpersist()
'''

df_final.show()

end = time()

elapsed = end - start
print("Execution time CSV: ", elapsed)

# Save the performance of the query
perf = spark.createDataFrame([(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), "Query3", "CSV", elapsed)], ["Timestamp", "Query", "File format", "Execution time (s)"])

perf.write.format("com.mongodb.spark.sql.DefaultSource") \
  .mode("append") \
  .option("collection", "performance") \
  .save()

spark.stop()