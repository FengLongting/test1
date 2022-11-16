# @desc : Spark读写ES数据
# 更多的es的参数解释, 详见Es官网: https://www.elastic.co/guide/en/elasticsearch/hadoop/7.10/configuration.html
__coding__ = "utf-8"
__author__ = "itcast team"

import os, logging
from pyspark.sql import SparkSession

# 指定服务器的路径
SPARK_HOME = '/export/server/spark'
os.environ['SPARK_HOME'] = SPARK_HOME

if __name__ == '__main__':
    # 1. 创建Spark执行环境, 获得SparkSession对象, 默认: 200个分区.
    spark = SparkSession.builder\
            .master('local[*]')\
            .appName('test06_spark_operator_es')\
            .config('spark.sql.shuffle.partitions', '10')\
            .getOrCreate()

    # 2. Spark读取es数据, 方式: format('es')方式
    # 参数:es.nodes,  意思是: es服务器地址 和 端口号
    # 参数:es.resource,  意思是: 具体的索引表名,  文档类型 _doc 可以省略
    # 参数:es.index.read.missing.as.empty,  意思是:  读不到就设置为空, 即使你写了yes, 对Spark程序不生效,  Spark作业(内部实现MR引擎方式)可以支持.
    # 参数:es.read.field.include,  意思是: 只筛选出我们指定的字段.
    # 参数:es.read.field.exclude,  意思是: 除了我们写的字段, 其它都要.

    # esDF1 = spark.read \
    #     .format('es') \
    #     .option('es.nodes', 'up01:9200') \
    #     .option('es.resource', 'tfec_tbl_users') \
    #     .option('es.index.read.missing.as.empty', 'yes') \
    #     .load()

    # include  只筛选出我们要的字段.
    esDF1 = spark.read \
        .format('es') \
        .option('es.nodes', 'up01:9200') \
        .option('es.resource', 'tfec_tbl_users') \
        .option('es.read.field.include', 'id, birthday') \
        .load()

    # exclude: 除了我们写的字段, 其它都要.
    # esDF1 = spark.read \
    #     .format('es') \
    #     .option('es.nodes', 'up01:9200') \
    #     .option('es.resource', 'tfec_tbl_users') \
    #     .option('es.read.field.exclude', 'id, birthday') \
    #     .load()

    esDF1.printSchema()
    esDF1.show(truncate=False)

    print('-' * 39)
    # 3. Spark写数据到es中, 方式: format('es')方式
    # 3.1 准备源数据, 一会儿写到es中.
    preDF = esDF1.orderBy(esDF1.id.desc()).limit(3)

    # 3.2 写入数据到es中
    # 参数解释: es.write.operation
    #   值1: index(默认值), 根据文档id判断, 如果新的数据文档id存在, 就更新数据(替换原来的数据)
    #   值2: create, 根据文档id判断, 如果新的数据文档id存在, 就报错.
    #   值3: update, 根据文档id判断, 如果新的数据文档id存在, 就报错, 不存在就更新数据.
    #   值4: upsert, 根据文档id判断, 如果新的数据文档id存在, 就更新数据(替换原来的数据), 不存在就插入数据.

    # mode('模式') 这个参数必须和 es.mapping.id 一起使用.
    preDF.write.format('es') \
        .option('es.nodes', 'up01:9200') \
        .option('es.resource', 'tfec_tbl_users_test02') \
        .option('es.mapping.id', 'id') \
        .option('es.write.operation', 'upsert') \
        .mode('append') \
        .save()
    logging.warning('Spark写入数据到es中成功')
