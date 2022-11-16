__coding__ = "utf-8"
__author__ = "longting Feng"

import os
from pyspark.sql import SparkSession,functions as F

from cn.itcast.tag.bean.EsMeta import ruleStrToEsMeta

SPARK_HOME = '/export/server/spark'
os.environ['SPARK_HOME'] = SPARK_HOME

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master('local[*]') \
        .appName('spark_longting_self') \
        .config('spark.config.shuffle.partitions', 10) \
        .getOrCreate()

    fourTagsId = 14

    # 1.2 具体的Spark从MySQL读取数据的代码.
    mysqlDF = spark.read \
        .format('jdbc') \
        .option('url',
                'jdbc:mysql://up01:3306/tfec_tags?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=false&user=root&password=123456') \
        .option('query',
                f'select id, rule, pid from tfec_tags.tbl_basic_tag where id = {fourTagsId} or pid = {fourTagsId}') \
        .load()
    mysqlDF.printSchema()
    mysqlDF.show(truncate=False)

    fourDF = mysqlDF.where('id = 14').select(mysqlDF.rule, mysqlDF.id)
    fourDF.show()

    # 得到规则
    fourRule = fourDF.rdd.map(lambda x: x.rule).collect()[0]
    print(fourRule)

    # 化为类对象
    esMeta = ruleStrToEsMeta(fourRule)
    print(esMeta)

    # 3.3 根据上述的EsMeta对象, 获取到具体的要的字段.
    esDF = spark.read \
        .format('es') \
        .option('es.nodes', esMeta.esNodes) \
        .option('es.resource', esMeta.esIndex) \
        .option('es.read.field.include', esMeta.selectFields) \
        .load()
    esDF.printSchema()
    esDF.show(truncate=False)

    # 得到FiveDF
    fiveDF = mysqlDF.where(f'pid={fourTagsId}').select(mysqlDF.id, mysqlDF.rule)

    # 对规则时间判断字段的解析
    esDF2 = esDF.select(
        esDF.id.ali('userId'),
        F.regexp_replace(F.substring(esDF.birthday, 0, 10), '-', '').alias('birth')
    )

    # 5.2 切割fiveDF的rule字段的值, 根据中间的"-", 获取到 start 和 end
    fiveDF2 = fiveDF.select(
        fiveDF.id.alias('tagsId'),
        F.split(fiveDF.rule, '-')[0].alias('start'),
        F.split(fiveDF.rule, '-')[1].alias('end'),
    )

    resultDF = (esDF2.join(fiveDF2).where(esDF2.birth.between(fiveDF.start, fiveDF2.end))).\
        select(esDF2.userId,fiveDF2.tagsId)

    resultDF.write.format('es') \
        .option('es.nodes', esMeta.esNodes) \
        .option('es.resource', 'tfec_userprofile_result') \
        .option('es.mapping.id', 'userId') \
        .option('es.write.operation', 'upsert') \
        .mode('append') \
        .save()