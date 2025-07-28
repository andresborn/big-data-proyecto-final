from config import DB_OPTIONS
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read Database").getOrCreate()

dataframe = spark.read.format("jdbc").options(**DB_OPTIONS).load()

dataframe.show()
