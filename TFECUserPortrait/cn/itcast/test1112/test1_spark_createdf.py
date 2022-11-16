#!/usr/bin/env python
# @desc : 编写Spark测试程序, 测试远程环境.
__coding__ = "utf-8"
__author__ = "itcast team"

import os
from pyspark.sql import SparkSession

# 1. 本地路径
# SPARK_HOME = 'C:\Software\DevelopSofware\spark-2.4.8-bin-hadoop2.7'
# PYSPARK_PYTHON = 'C:\Software\DevelopSofware\Python37\python.exe'

# 2. 服务器路径
# SPARK_HOME = '/export/server/spark'
# PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'

# 3. 导入路径
# os.environ['SPARK_HOME'] = SPARK_HOME
# os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON



if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').appName('test_spark_dataframe').getOrCreate()

    # df = spark.createDataFrame([('刘亦菲', ), ('高圆圆', )], ['name',])     -- 如果是1列, 记得后边写 逗号(,)
    df = spark.createDataFrame([('刘亦菲', 33), ('高圆圆', 35)], ['name', 'age'])
    df.printSchema()
    df.show(truncate=False)

























