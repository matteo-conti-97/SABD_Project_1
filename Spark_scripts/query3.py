from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from time import time
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, DateType

spark = SparkSession.builder \
  .appName("Query3") \
  .config("spark.mongodb.input.uri", "mongodb://spark_user:spark_password@mongo:27017/results.query3") \
  .config("spark.mongodb.output.uri", "mongodb://spark_user:spark_password@mongo:27017/results.query3") \
  .getOrCreate()

#PARQUET
start = time()
df = spark.read.parquet("hdfs://namenode:8020/disk_data_filtered.parquet")
df = df.drop("model", "vault_id")
df = df.withColumn('date', date_format(col('date'), 'dd-MM-yyyy'))
df.cache()

# divido il dataframe in due parti, una con failure = 0 e una con failure = 1 e calcolo i quantili per entrambe
failure_1 = df.filter(df["failure"] == 1)

failure_1 = failure_1.groupBy("serial_number").agg(first("date").alias("dates"), first("s9_power_on_hours").alias("power_on"))
failure_1.drop("serial_number", "dates")

failure_1_list = failure_1.approxQuantile("power_on", [0.0, 0.25, 0.5, 0.75, 1.0], 0)

failure_0 = df.filter(df["failure"] == 0)

failure_0 = failure_0.groupBy("serial_number").agg(first("date").alias("dates"), first("s9_power_on_hours").alias("power_on"))
failure_0.drop("serial_number", "dates")

failure_0_list = failure_0.approxQuantile("power_on", [0.0, 0.25, 0.5, 0.75, 1.0], 0)


df_final = spark.createDataFrame(
    [
      (1, failure_1_list[0], failure_1_list[1], failure_1_list[2], failure_1_list[3], failure_1_list[4], failure_1.count()), 
      (0, failure_0_list[0], failure_0_list[1], failure_0_list[2], failure_0_list[3], failure_0_list[4], failure_0.count())
    ], 
    ["failure", "min", "25th_percentile", "50th_percentile", "75th_percentile", "max", "count"])
df_final.show()

end = time()



# Save the results of the query
df_final.write.format("com.mongodb.spark.sql.DefaultSource") \
     .mode("overwrite") \
     .save()

elapsed = end - start
print("Execution time Parquet: ", elapsed)

# Save the performance of the query
perf = spark.createDataFrame([(end, "Query3", "Parquet", elapsed)], ["Timestamp", "Query", "File format", "Execution time (s)"])

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
df = spark.read.csv("hdfs://namenode:8020/disk_data_filtered.csv", header=True, schema=schema)
df = df.drop("model", "vault_id")
df = df.withColumn('date', date_format(col('date'), 'dd-MM-yyyy'))
df.cache()

# divido il dataframe in due parti, una con failure = 0 e una con failure = 1 e calcolo i quantili per entrambe
failure_1 = df.filter(df["failure"] == 1)

failure_1 = failure_1.groupBy("serial_number").agg(first("date").alias("dates"), first("s9_power_on_hours").alias("power_on"))
failure_1.drop("serial_number", "dates")

failure_1_list = failure_1.approxQuantile("power_on", [0.0, 0.25, 0.5, 0.75, 1.0], 0)

failure_0 = df.filter(df["failure"] == 0)

failure_0 = failure_0.groupBy("serial_number").agg(first("date").alias("dates"), first("s9_power_on_hours").alias("power_on"))
failure_0.drop("serial_number", "dates")

failure_0_list = failure_0.approxQuantile("power_on", [0.0, 0.25, 0.5, 0.75, 1.0], 0)


df_final = spark.createDataFrame(
    [
      (1, failure_1_list[0], failure_1_list[1], failure_1_list[2], failure_1_list[3], failure_1_list[4], failure_1.count()), 
      (0, failure_0_list[0], failure_0_list[1], failure_0_list[2], failure_0_list[3], failure_0_list[4], failure_0.count())
    ], 
    ["failure", "min", "25th_percentile", "50th_percentile", "75th_percentile", "max", "count"])

df_final.show()

end = time()

elapsed = end - start
print("Execution time CSV: ", elapsed)

# Save the performance of the query
perf = spark.createDataFrame([(end, "Query3", "CSV", elapsed)], ["Timestamp", "Query", "File format", "Execution time (s)"])

perf.write.format("com.mongodb.spark.sql.DefaultSource") \
  .mode("append") \
  .option("collection", "performance") \
  .save()

spark.stop()