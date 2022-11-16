# @desc : 演示PySpark操作MySQL, 读取数据, 写数据,  分别有两种方式: jdbc, format('jdbc')
# 更多的Spark函数解释, 详见Spark官网: https://spark.apache.org/docs/2.4.8/api/python/pyspark.sql.html

# 问题: 编程错误如何解决?
#   1. 定位错误, 定位具体的哪一行.
#   2. 根据报错信息和参数以及代码的每一行返回, 或日志信息来判断错误(经验)
#   3. 找解决方案, 测试方案, 选中方案.
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
            .appName('test05_spark_operator_mysql')\
            .config('spark.sql.shuffle.partitions', '10')\
            .getOrCreate()

    # 需求1: Spark读取MySQL表数据, 方式1: jdbc方式
    # url: mysql的连接字符串,  table: 具体的要操作的MySQL的表名.
    mysqlDF1 = spark.read.jdbc(
        url='jdbc:mysql://up01:3306/spark_mysql_test1113?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=false&user=root&password=123456',
        table='t_pyspark_mysql01')
    mysqlDF1.printSchema()
    mysqlDF1.show(truncate=False)


    print('-' * 31)
    # 需求2: Spark读取MySQL表数据, 方式2: format('jdbc')方式
    # dbtable: 后边跟的是表名, 即: 查询该表所有的数据.
    # mysqlDF2 = spark.read\
    #         .format('jdbc')\
    #         .option('url', 'jdbc:mysql://up01:3306/spark_mysql_test1113?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=false&user=root&password=123456') \
    #         .option('dbtable', 't_pyspark_mysql01')\
    #         .load()

    # query: 后边可以写SQL语句, 实现用户自定义查询.
    mysqlDF2 = spark.read \
        .format('jdbc') \
        .option('url',
                'jdbc:mysql://up01:3306/spark_mysql_test1113?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=false&user=root&password=123456') \
        .option('query', 'select name, age from t_pyspark_mysql01') \
        .load()

    mysqlDF2.printSchema()
    mysqlDF2.show(truncate=False)

    # Spark写入数据到MySQL, 准备数据, 为了简单, 我们直接从上边的查询数据中, 获取即可.
    preDF = mysqlDF1.limit(2)

    print('-' * 31)
    # 需求3: Spark写数据到MySQL表, 方式1: jdbc方式
    # mode参数解释: error(默认的): 如果已存在, 就抛出异常.    ignore: 忽略已存在数据,    overwrite: 覆盖写入.   append: 追加写入.
    preDF.write.jdbc(
        url='jdbc:mysql://up01:3306/spark_mysql_test1113?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=false&user=root&password=123456',
        table='t_pyspark_mysql02',
        mode='append')
    # print('Spark写数据成功')   仅仅会把结果打印到 控制台上.
    logging.warning('Spark写数据成功')  # 不仅会将结果打印到控制台上, 还会记录错误信息.

    print('-' * 31)
    # 需求4: Spark写数据到MySQL表, 方式2: format('jdbc')方式
    preDF.write \
        .format('jdbc') \
        .option('url',
                'jdbc:mysql://up01:3306/spark_mysql_test1113?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=false&user=root&password=123456') \
        .option('dbtable', 't_pyspark_mysql03') \
        .mode('overwrite') \
        .save()
    # print('Spark写数据成功')
    logging.warning('Spark写数据成功')  # 不仅会将结果打印到控制台上, 还会记录错误信息.
