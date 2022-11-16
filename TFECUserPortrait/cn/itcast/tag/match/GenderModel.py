# @desc : 性别标签, 匹配类.
__coding__ = "utf-8"
__author__ = "itcast team"


import os, logging
from pyspark.sql import SparkSession, DataFrame, functions as F
from cn.itcast.tag.bean.EsMeta import ruleStrToEsMeta

# 指定服务器的路径
SPARK_HOME = '/export/server/spark'
os.environ['SPARK_HOME'] = SPARK_HOME

# 查询esDF的gender, 作为参数, 编写udf函数, 实现传入gender, 作为key, 从字典获得value
@F.udf
def genderToTagsId(gender):
    return fiveRuleDict[str(gender)]

# 负责新标签 和 旧标签数据的合并.
@F.udf
def mergeDF(newTagsId, oldTagsId):
    # 情况1: 新标签不存在, 但是旧标签存在, 就只返回 旧标签即可.
    # 情况2: 旧标签不存在, 但是新标签存在, 就只返回 新标签即可.
    # 情况3: 新旧标签都存在.  如果代码能执行到这里, 说明 新旧标签都是有的.
    if newTagsId is None:
        return oldTagsId
    if oldTagsId is None:
        return newTagsId
    oldTagsIdList = str(oldTagsId).split(',')       # 假设 '19, 5' => ['19', '5']
    # 具体的合并过程
    oldTagsIdList.append(str(newTagsId))            # 得到, 假设: ['19', '5', '100']
    # 用 , 号连接各种标签, 然后返回最终结果
    return ','.join(set(oldTagsIdList))             # 细节: 去重后, 连接. 防止出现重复指标.

if __name__ == '__main__':
    # 定义变量, 记录当前标签(任务)的名字.
    taskName = 'GenderModel'

    # 0. 创建Spark执行环境, 获得SparkSession对象, 默认: 200个分区.
    spark = SparkSession.builder\
            .master('local[*]')\
            .appName(taskName)\
            .config('spark.sql.shuffle.partitions', '10')\
            .getOrCreate()

    # 具体的实现步骤
    '''
        1. Spark读取MySQL标签规则数据.
        2. 过滤出MySQL的4级标签规则数据, 得到FourDF
        3. 根据4级标签规则, 得到rule值, 作为ES的元数据信息, 查询标签需要的ES字段数据, 返回esDF
        4. 过滤出MySQL的5级标签规则数据, 得到FiveDF.
        5. 根据5级标签规则和es标签源数据, 实现标签的匹配, 统计, 挖掘, 返回标签userId, tagsId(newDF)
        6. 查询旧的标签结果数据, 返回oldDF
        7. 把当前标签计算结果(newDF) 和 之前的标签计算结果(oldDF)进行合并, 根据用户id相同, 实现 tagsId的 upsert(更新)
        8. 把标签结果数据, 写入到ES中.
    '''
    # 1. Spark读取MySQL标签规则数据.
    # 1.1 定义变量, 表示要操作的 业务规则的id
    fourTagsId = 4

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

    # 2. 过滤出MySQL的4级标签规则数据, 得到FourDF
    fourDF = mysqlDF.where(f'id = {fourTagsId}').select(mysqlDF.rule)
    fourDF.printSchema()
    fourDF.show(truncate=False)

    # 3. 根据4级标签规则, 得到rule值, 作为ES的元数据信息, 查询标签需要的ES字段数据, 返回esDF
    # 3.1 从FourDF对象中, 获取具体的rule(四级标签规则) 的 字符串形式, 即: python的 str 类型.
    fourRule = fourDF.rdd.map(lambda row: row.rule).collect()[0]

    # 3.2 根据上述的 四级标签规则 字符串, 获取其对应的EsMeta对象.
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

    # 4. 过滤出MySQL的5级标签规则数据, 得到FiveDF.
    fiveDF = mysqlDF.where(f'pid={fourTagsId}').select(mysqlDF.id, mysqlDF.rule)
    fiveDF.printSchema()
    fiveDF.show(truncate=False)

    # 5. 根据5级标签规则和es标签源数据, 实现标签的匹配, 统计, 挖掘, 返回标签userId, tagsId(newDF)
    # 5.1 把fiveDF转换成dict, rule的值作为key(键), id值作为value(值)
    fiveRuleDict = fiveDF.rdd.map(lambda row : (row.rule, row.id)).collectAsMap()   # rule做键, id做值
    print('五级标签的id转换为字典后, 内容为: %s' % fiveRuleDict)

    # 5.2 查询esDF的gender, 作为参数, 编写udf函数, 实现传入gender, 作为key, 从字典获得value
    newDF = esDF.select(esDF.id.alias('userId'), genderToTagsId(esDF.gender).alias('tagsId'))
    newDF.printSchema()
    newDF.show(truncate=False)

    # 6. 查询旧的标签结果数据, 返回oldDF
    oldDF = spark.read \
        .format('es') \
        .option('es.nodes', esMeta.esNodes) \
        .option('es.resource', 'tfec_userprofile_result') \
        .option('es.read.field.include', 'userId, tagsId') \
        .load()
    oldDF.printSchema()
    oldDF.show(truncate=False)

    # 7. 把当前标签计算结果(newDF) 和 之前的标签计算结果(oldDF)进行合并, 根据用户id相同, 实现 tagsId的 upsert(更新)
    resultDF = newDF.join(oldDF, newDF.userId == oldDF.userId, 'left')\
        .select(newDF.userId, mergeDF(newDF.tagsId, oldDF.tagsId).alias('tagsId'))
    resultDF.printSchema()
    resultDF.show(truncate=False)

    # 8. 把标签结果数据, 写入到ES中.
    resultDF.write.format('es') \
        .option('es.nodes', esMeta.esNodes) \
        .option('es.resource', 'tfec_userprofile_result') \
        .option('es.mapping.id', 'userId') \
        .option('es.write.operation', 'upsert') \
        .mode('append') \
        .save()