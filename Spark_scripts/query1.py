from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from time import time
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, DateType

spark = SparkSession.builder \
  .appName("Query1") \
  .config("spark.mongodb.input.uri", "mongodb://spark_user:spark_password@mongo:27017/results") \
  .config("spark.mongodb.output.uri", "mongodb://spark_user:spark_password@mongo:27017/results") \
  .getOrCreate()

#PARQUET
start = time()
df = spark.read.parquet("hdfs://namenode:8020/disk_data_filtered.parquet")
df = df.drop("serial_number", "model", "s9_power_on_hours")
df = df.withColumn('date', date_format(col('date'), 'dd-MM-yyyy'))
df = df.groupBy("date", "vault_id").agg(sum("failure").alias("failures"))
df = df.filter((df["failures"] <= 4) & (df["failures"] >= 2))
df.show()

end = time()

# Save the results of the query
df.write.format("com.mongodb.spark.sql.DefaultSource") \
  .mode("overwrite") \
  .option("collection", "query1") \
  .save()

elapsed = end - start
print("Execution time Parquet: ", elapsed)

# Save the performance of the query
perf = spark.createDataFrame([(end, "Query1", "Parquet", elapsed)], ["Timestamp", "Query", "File format", "Execution time (s)"])

perf.write.format("com.mongodb.spark.sql.DefaultSource") \
  .mode("append") \
  .option("collection", "performance") \
  .save()
  
  
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
df = df.drop("serial_number", "model", "s9_power_on_hours")
df = df.withColumn('date', date_format(col('date'), 'dd-MM-yyyy'))
df = df.groupBy("date", "vault_id").agg(sum("failure").alias("failures"))
df = df.filter((df["failures"] <= 4) & (df["failures"] >= 2))
df.show()

end = time()

elapsed = end - start
print("Execution time CSV: ", elapsed)

# Save the performance of the query
perf = spark.createDataFrame([(end, "Query1", "CSV", elapsed)], ["Timestamp", "Query", "File format", "Execution time (s)"])

perf.write.format("com.mongodb.spark.sql.DefaultSource") \
  .mode("append") \
  .option("collection", "performance") \
  .save()



spark.stop()