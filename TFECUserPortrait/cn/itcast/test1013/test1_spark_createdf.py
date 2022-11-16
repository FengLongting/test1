#!/usr/bin/env python
# @desc : 编写spark测试程序，测试远程环境
__coding__ = "utf-8"
__author__ = "itcast team"

from pyspark.sql import SparkSession

# SPARK_HOME = '/export/server/spark'
# os.environ['SPARK_HOME'] = SPARK_HOME

if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .getOrCreate()

    # print(spark)
    df = spark.createDataFrame([('张三', 18), ('李四', 19)], ['name', 'age'])
    df.printSchema()
    df.show(truncate=False)
