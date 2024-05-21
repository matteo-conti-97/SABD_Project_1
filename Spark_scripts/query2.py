from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
  .appName("Query2") \
  .config("spark.mongodb.input.uri", "mongodb://spark_user:spark_password@mongo:27017/results.query2") \
  .config("spark.mongodb.output.uri", "mongodb://spark_user:spark_password@mongo:27017/results.query2") \
  .getOrCreate()

#prima parte
start = time.time()
df = spark.read.parquet("hdfs://namenode:8020/disk_data_filtered.parquet")
df = df.drop("serial_number", "s9_power_on_hours")
df.cache()
df1 = df.groupBy("model").agg(sum("failure").alias("failures"))
df1 = df1.orderBy("failures", ascending=False).limit(10)
end = time.time()
df1.show()
print("Execution time: " + str(end - start))

#seconda parte
df2 = df.groupBy("vault_id").sum("failure")
df2.show()
df2.orderBy("sum(failure)", ascending=False).show()
df4 = df
df4.show()
df3 = df4.filter(df["failure"] > 0).groupBy("vault_id").agg(collect_set("model").alias("list_of_models"))
df3.show()
df5 = df3.join(df2, "vault_id").orderBy("failure", ascending=False).limit(10)
df5.show()
#df = df.orderBy("failures", ascending=False).limit(10)
end = time.time()
#df.show()
print("Execution time: " + str(end - start))


#df = df.filter((df["failures"] <= 4) & (df["failures"] >= 2))

df5.write.format("com.mongodb.spark.sql.DefaultSource") \
  .mode("overwrite") \
  .save()

spark.stop()