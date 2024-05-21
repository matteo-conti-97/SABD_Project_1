# Import the necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

# Create a SparkSession
spark = SparkSession.builder \
   .appName("HelloWorld") \
   .getOrCreate()

rdd = spark.sparkContext.parallelize(range(1, 100))
time.sleep(100)
print("THE SUM IS HERE: ", rdd.sum())
# Stop the SparkSession
spark.stop()