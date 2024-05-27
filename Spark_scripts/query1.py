from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from time import time

spark = SparkSession.builder \
  .appName("Query1") \
  .config("spark.mongodb.input.uri", "mongodb://spark_user:spark_password@mongo:27017/results") \
  .config("spark.mongodb.output.uri", "mongodb://spark_user:spark_password@mongo:27017/results") \
  .getOrCreate()

start = time()
df = spark.read.parquet("hdfs://namenode:8020/disk_data_filtered.parquet")
df = df.drop("serial_number", "model", "s9_power_on_hours")
df = df.withColumn('date', date_format(col('date'), 'dd-MM-yyyy'))
df = df.groupBy("date", "vault_id").agg(sum("failure").alias("failures"))
df = df.filter((df["failures"] <= 4) & (df["failures"] >= 2))
df.show()

end = time()

df.write.format("com.mongodb.spark.sql.DefaultSource") \
  .mode("overwrite") \
  .option("collection", "query1") \
  .save()

time = end - start

perf = spark.createDataFrame([(end, "Query1", "Parquet",time)], ["Timestamp", "Query", "File format", "Execution time (s)"])

perf.write.format("com.mongodb.spark.sql.DefaultSource") \
  .mode("append") \
  .option("collection", "performance") \
  .save()



spark.stop()