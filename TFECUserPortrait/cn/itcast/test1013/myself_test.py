from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.format('es')
