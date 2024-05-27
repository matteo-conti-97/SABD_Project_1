from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
  .appName("Query2") \
  .config("spark.mongodb.input.uri", "mongodb://spark_user:spark_password@mongo:27017/results.query2.1") \
  .config("spark.mongodb.output.uri", "mongodb://spark_user:spark_password@mongo:27017/results.query2.1") \
  .getOrCreate()

#prima parte
start = time.time()

df = spark.read.parquet("hdfs://namenode:8020/disk_data_filtered.parquet")
df = df.drop("serial_number", "s9_power_on_hours","date")
df.cache()
df1 = df.groupBy("model").agg(sum("failure").alias("failures"))
df1 = df1.orderBy("failures", ascending=False).limit(10)
df.show()

end = time.time()
print("Execution time: first part " + str(end - start))

#seconda parte

df2 = df.groupBy("vault_id").agg(sum("failure").alias("failures")) #AGG a quanto pare non era un bug e non metterlo non permette l'aliasing
df3 = df.filter(df["failure"] > 0).groupBy("vault_id").agg(collect_set("model").alias("list_of_models"))
df4 = df3.join(df2, "vault_id").orderBy("failures", ascending=False).limit(10)
df4.show()

end = time.time()
print("Execution time: " + str(end - start))

df1.write.format("com.mongodb.spark.sql.DefaultSource") \
  .mode("overwrite") \
  .save()

df4.write.format("com.mongodb.spark.sql.DefaultSource") \
  .mode("overwrite") \
  .option("collection", "query2.2") \
  .save()
# df5.write.format("com.mongodb.spark.sql.DefaultSource") \
#   .mode("overwrite") \
#   .save()

spark.stop()