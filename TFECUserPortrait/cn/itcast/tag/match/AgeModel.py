# @desc : 年龄标签, 匹配类.
__coding__ = "utf-8"
__author__ = "itcast team"

import os, logging
from pyspark.sql import SparkSession, DataFrame, functions as F

# 指定服务器的路径
from cn.itcast.tag.bean.EsMeta import ruleStrToEsMeta

SPARK_HOME = '/export/server/spark'
os.environ['SPARK_HOME'] = SPARK_HOME

if __name__ == '__main__':
    # 定义变量, 记录当前标签(任务)的名字.
    taskName = 'AgeModel'

    # 0. 创建Spark执行环境, 获得SparkSession对象, 默认: 200个分区.
    spark = SparkSession.builder\
            .master('local[*]')\
            .appName(taskName)\
            .config('spark.sql.shuffle.partitions', '10')\
            .getOrCreate()

    # 具体的实现步骤
    '''
        1. spark读取标签的业务规则数据, 返回mysqlDF
        2. spark过滤出4级标签的规则数据，返回fourDF
        3. 根据fourDF的rule字段值，得到rule的值字符串，解析4级标签规则，从df得到dict或esMeta，根据es元数据信息读取es中原始用户或用户关联数据
        4. 过滤从5级标签的规则数据，返回fiveDF
        5. 实现标签的匹配、统计、挖掘计算逻辑，返回resultDF
        6. 把结果数据写入到es中
    '''
    logging.warning('------------------------------ 1. spark读取标签的业务规则数据, 返回mysqlDF ------------------------------')
    # 1.1 定义变量, 表示要操作的 业务规则的id
    fourTagsId = 14

    # 1.2 具体的Spark从MySQL读取数据的代码.
    mysqlDF = spark.read \
        .format('jdbc') \
        .option('url',
                'jdbc:mysql://up01:3306/tfec_tags?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=false&user=root&password=123456') \
        .option('query', f'select id, rule, pid from tfec_tags.tbl_basic_tag where id = {fourTagsId} or pid = {fourTagsId}') \
        .load()
    mysqlDF.printSchema()
    mysqlDF.show(truncate=False)

    logging.warning('------------------------------ 2. spark过滤出4级标签的规则数据，返回fourDF ------------------------------')
    # 2. spark过滤出4级标签的规则数据，返回fourDF
    # fourDF = mysqlDF.where('id = 14').select(mysqlDF.rule)       # 写法1
    # fourDF = mysqlDF.where('id = 14').select(mysqlDF['rule'])      # 写法2
    fourDF = mysqlDF.where('id = 14').select(mysqlDF['rule'],mysqlDF['id'])      # 写法2
    # fourDF = mysqlDF.where('id = 14').select('rule'])      # 写法3

    fourDF.printSchema()
    fourDF.show(truncate=False)

    logging.warning('------------------------------ 3. 根据fourDF的rule字段值，得到rule的值字符串，解析4级标签规则，从df得到dict或esMeta，根据es元数据信息读取es中原始用户或用户关联数据 ------------------------------')
    # 3. 根据fourDF的rule字段值，得到rule的值字符串，解析4级标签规则，从df得到dict或esMeta，根据es元数据信息读取es中原始用户或用户关联数据
    # 目前的数据格式: inType=Elasticsearch##esNodes=up01:9200##esIndex=tfec_tbl_users##esType=_doc##selectFields=id,birthday
    # 问题: 如何实现获取df中的rule值, 值是 python str类型?         map()函数: 转换函数.    flatMap():  flatten + map,  扁平化 + 映射
    # 3.1 从FourDF对象中, 获取具体的rule(四级标签规则) 的 字符串形式, 即: python的 str 类型.
    fourRule = fourDF.rdd.map(lambda row: row.rule).collect()[0]
    # fourRule = fourDF.rdd.map(lambda row: row.rule).collect()[0]
    print(fourRule)
    logging.warning('四级标签规则是 %s', fourRule)

    # 3.2 根据上述的 四级标签规则 字符串, 获取其对应的EsMeta对象.
    # 目前的数据格式: EsMeta对象数据为: EsMeta(inType='Elasticsearch', esNodes='up01:9200', esIndex='tfec_tbl_users', esType='_doc', selectFields='id,birthday')
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

    logging.warning('------------------------------ 4. 过滤从5级标签的规则数据，返回fiveDF ------------------------------')
    # 4. 过滤从5级标签的规则数据，返回fiveDF
    print('6666666666666666666666666666666666666666666666666666666666666666666666666666666666666')
    fiveDF = mysqlDF.where(f'pid={fourTagsId}').select(mysqlDF.id, mysqlDF.rule)
    fiveDF.printSchema()
    fiveDF.show(truncate=False)

    logging.warning('------------------------------ 5. 实现标签的匹配、统计、挖掘计算逻辑，返回resultDF ------------------------------')
    # 5.1 切分esDF的birthday字段字段的值,  切割数据, 把中间的-号给替换为空
    esDF2 = esDF.select(
        esDF.id.alias('userId'),
        F.regexp_replace(F.substring(esDF.birthday, 0, 10), '-', '').alias('birth')
    )
    esDF2.printSchema()
    esDF2.show(truncate=False)

    # 5.2 切割fiveDF的rule字段的值, 根据中间的"-", 获取到 start 和 end
    fiveDF2 = fiveDF.select(
        fiveDF.id.alias('tagsId'),
        F.split(fiveDF.rule, '-')[0].alias('start'),
        F.split(fiveDF.rule, '-')[1].alias('end')
    )
    fiveDF2.printSchema()
    fiveDF2.show(truncate=False)

    # 5.3 把esDF 和 fiveDF使用join方法关联, 不需要指定关联条件, 默认是 inner join, 关联条件不用写, 会根据where()内容自动关联.
    resultDF = esDF2\
        .join(fiveDF2) \
        .where(esDF2.birth.between(fiveDF2.start, fiveDF2.end)) \
        .select(esDF2.userId, fiveDF2.tagsId.cast('string'))
    resultDF.printSchema()
    resultDF.show(truncate=False)

    logging.warning('------------------------------ 6. 把结果数据写入到es中 ------------------------------')
    # 其实就是Spark写数据到ES的动作.
    resultDF.write.format('es') \
        .option('es.nodes', esMeta.esNodes) \
        .option('es.resource', 'tfec_userprofile_result') \
        .option('es.mapping.id', 'userId') \
        .option('es.write.operation', 'upsert') \
        .mode('append') \
        .save()

    # logging.warning(f'{taskName} 任务执行完成')
    logging.warning('%s 任务执行完成', taskName)       # 两种方式二选一即可, 效果一样.