from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
  .appName("Query1") \
  .config("spark.mongodb.input.uri", "mongodb://spark_user:spark_password@mongo:27017/results.query1") \
  .config("spark.mongodb.output.uri", "mongodb://spark_user:spark_password@mongo:27017/results.query1") \
  .getOrCreate()

df = spark.read.parquet("hdfs://namenode:8020/disk_data_filtered.parquet")
df = df.drop("serial_number", "model", "s9_power_on_hours")
df = df.withColumn('date', date_format(col('date'), 'dd-MM-yyyy'))
df = df.groupBy("date", "vault_id").agg(sum("failure").alias("failures"))
df = df.filter((df["failures"] <= 4) & (df["failures"] >= 2))
#df.show()

df.write.format("com.mongodb.spark.sql.DefaultSource") \
  .mode("overwrite") \
  .save()

spark.stop()