from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
  .appName("Query3") \
  .config("spark.mongodb.input.uri", "mongodb://spark_user:spark_password@mongo:27017/results/query3") \
  .config("spark.mongodb.output.uri", "mongodb://spark_user:spark_password@mongo:27017/results/query3") \
  .getOrCreate()

df = spark.read.parquet("hdfs://namenode:8020/disk_data_filtered.parquet")
df = df.drop("model", "vault_id")
df = df.withColumn('date', date_format(col('date'), 'dd-MM-yyyy'))
df.cache()
# divido il dataframe in due parti, una con failure = 0 e una con failure > 0
failure_1 = df.filter(df["failure"] == 1)
failure_0 = df.filter(df["failure"] == 0)

start = time.time()
df_first = failure_1.groupBy("serial_number").agg(first("date").alias("dates"), first("s9_power_on_hours").alias("power_on"))
df_first.drop("serial_number", "dates")
df_first.cache()
start = time.time()
#ordino il dataframe 
df_first = df_first.orderBy("power_on", ascending=True)
#calcolo il 25 percentile
df_perc1 = df_first.approxQuantile("power_on", [0.25, 0.5, 0.75], 0)
#calcolo il 50 percentile
#df_perc2 = df_first.approxQuantile("power_on", [0.5], 0)
#calcolo il 75 percentile
#df_perc3 = df_first.approxQuantile("power_on", [0.75], 0)




#calcolo il min e il max delle date e delle ore di accensione sul totale del dataframe 25 percentile
# df_final = df_first.agg(min("power_on").alias("min_power_on"), max("power_on").alias("max_power_on"), count("power_on").alias("count_power_on"), )
# #aggiungo i percentile al dataframe
# df_final = df_final.withColumns({"25_percentile": lit(df_perc1[0]), "50_percentile": lit(df_perc1[1]), "75_percentile": lit(df_perc1[2])})
# df_final.show()
# end = time.time()
# print("Execution time appr: ", end - start)

start = time.time()
df_perc2 = df_first.approxQuantile("power_on", [0.0, 0.25, 0.5, 0.75, 1.0], 0)
df_final2 = spark.createDataFrame([(df_perc2[0], df_perc2[1], df_perc2[2], df_perc2[3], df_perc2[4], df_first.count())], ["min", "25_percentile", "50_percentile", "75_percentile", "max", "count"])
#df_final2 = df_final2.withColumns({"min": lit(df_perc2[0]), "25_percentile": lit(df_perc2[1]), "50_percentile": lit(df_perc2[2]), "75_percentile": lit(df_perc2[3]), "max": lit(df_perc2[4]), "count": lit(df_first.count())})
df_final2.show()
end = time.time()
print("Execution time appr 2: ", end - start)


#df.show()


# df.write.format("com.mongodb.spark.sql.DefaultSource") \
#     .mode("overwrite") \
#     .save()

spark.stop()