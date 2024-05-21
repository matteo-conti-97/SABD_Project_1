from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
  .appName("HelloWorld") \
  .config("spark.mongodb.input.uri", "mongodb://spark_user:spark_password@mongo:27017/results.hello_world") \
  .config("spark.mongodb.output.uri", "mongodb://spark_user:spark_password@mongo:27017/results.hello_world") \
  .getOrCreate()

dataparquet = spark.read.parquet("hdfs://namenode:8020/disk_data_filtered.parquet")
dataparquet.printSchema()
res = dataparquet.groupBy("failure").count()
res.show()

res.write.format("com.mongodb.spark.sql.DefaultSource") \
  .mode("overwrite") \
  .save()

spark.stop()