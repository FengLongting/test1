# @desc : 定义标签开发基类(父类)
__coding__ = "utf-8"
__author__ = "itcast team"

import os, logging, abc
from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame, functions as F
from cn.itcast.tag.bean.EsMeta import ruleStrToEsMeta

# 指定服务器的路径
SPARK_HOME = '/export/server/spark'
os.environ['SPARK_HOME'] = SPARK_HOME


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


@dataclass
class AbstractBaseModel(object):
    '''
       思路总览:
           0. 准备Spark开发环境
           1. 读取MySQL中的数据
           2. 读取模型/标签相关的4级标签rule并解析--==标签id不一样==
           3. 根据解析出来的rule读取ES数据
           4. 读取模型/标签相关的5级标签(根据4级标签的id作为pid查询)---==标签id不一样==
           5. 根据ES数据和5级标签数据进行匹配,得出userId,tagsId---==实现代码不一样==
           6. 查询elasticsearch中的oldDF
           7. 合并newDF和oldDF
           8. 将最终结果写到ES
       '''

    # 定义变量, 记录当前标签(任务)的名字.
    taskName: str

    # 0. 创建Spark执行环境, 获得SparkSession对象, 默认: 200个分区.
    def __createSparkSession(self):
        self.spark = SparkSession.builder \
            .master('local[*]') \
            .appName(self.taskName) \
            .config('spark.sql.shuffle.partitions', '10') \
            .getOrCreate()
        return self.spark

    # 1. 读取MySQL中的数据
    # 1.1 定义抽象方法(因为不同标签, id不一样), 用于获取4级标签id
    @abc.abstractmethod
    def getFourTagsId(self):
        pass

    # 1.2 定义方法, getMySQLDF, 读取MySQL数据, 私有方法, 子类能用, 但是不能修改.
    def __getMysqlDF(self, fourTagsId):
        mysqlDF = self.spark.read \
            .format('jdbc') \
            .option('url',
                    'jdbc:mysql://up01:3306/tfec_tags?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=false&user=root&password=123456') \
            .option('query',
                    f'select id, rule, pid from tfec_tags.tbl_basic_tag where id = {fourTagsId} or pid = {fourTagsId}') \
            .load()
        return mysqlDF

    # 2. 读取模型/标签相关的4级标签rule并解析--==标签id不一样==
    def __getFourRuleDF(self, mysqlDF: DataFrame, fourTagsId: int):
        fourDF = mysqlDF.where(f'id = {fourTagsId}').select(mysqlDF.rule)
        return fourDF

    # 3. 根据解析出来的rule读取ES数据
    # 3.1 从FourDF对象中, 获取具体的rule(四级标签规则) 的 字符串形式, 四级标签规则 字符串, 获取其对应的EsMeta对象.
    def __getEsMetaFromFourDF(self, fourDF):
        esMeta = ruleStrToEsMeta(fourDF.rdd.map(lambda row: row.rule).collect()[0])
        return esMeta

    # 3.2 根据上述的EsMeta对象, 获取到具体的要的字段.
    def __getEsDF(self, esMeta):
        esDF = self.spark.read \
            .format('es') \
            .option('es.nodes', esMeta.esNodes) \
            .option('es.resource', esMeta.esIndex) \
            .option('es.read.field.include', esMeta.selectFields) \
            .load()
        return esDF

    # 4. 读取模型/标签相关的5级标签(根据4级标签的id作为pid查询)---==标签id不一样==
    def __getFiveDF(self, mysqlDF, fourTagsId):
        fiveDF = mysqlDF.where(f'pid={fourTagsId}').select(mysqlDF.id, mysqlDF.rule)
        return fiveDF

    # 5. 根据ES数据和5级标签数据进行匹配,得出userId,tagsId---==实现代码不一样==
    # 因为实现动作不同, 我们定义为抽象方法.
    @abc.abstractmethod
    def compute(self, esDF: DataFrame, fiveDF: DataFrame):
        pass

    # 6. 查询elasticsearch中的oldDF
    # 为了方便测试数据, 我们先修改索引表的名字为: tfec_userprofile_result
    def __getOldDF(self, spark, esMeta):
        oldDF = self.spark.read \
            .format('es') \
            .option('es.nodes', esMeta.esNodes) \
            .option('es.resource', 'tfec_userprofile_result') \
            .option('es.read.field.include', 'userId, tagsId') \
            .load()
        return oldDF

    # 7. 合并newDF和oldDF
    def __mergeNewAndOldDF(self, newDF, oldDF):
        resultDF = newDF.join(oldDF, newDF.userId == oldDF.userId, 'left') \
            .select(newDF.userId, mergeDF(newDF.tagsId, oldDF.tagsId).alias('tagsId'))
        return resultDF

    # 8. 将最终结果写到ES
    def __writeResulttoES(self, resultDF, esMeta):
        resultDF.write.format('es') \
            .option('es.nodes', esMeta.esNodes) \
            .option('es.resource', 'tfec_userprofile_result') \
            .option('es.mapping.id', 'userId') \
            .option('es.write.operation', 'upsert') \
            .mode('append') \
            .save()

    # 9. 把上述所有的函数组合起来.
    def execute(self):
        # 0. 数据全拿到.
        sparkSession = self.__createSparkSession()
        #抽象方法的调用.
        fourTagsId = self.getFourTagsId()

        # 细节
        # 1. 读取MySQL中的数据
        mysqlDF = self.__getMysqlDF(fourTagsId)

        # 2. 读取模型/标签相关的4级标签rule并解析--==标签id不一样==
        fourDF = self.__getFourRuleDF(mysqlDF, fourTagsId)

        # 3. 根据解析出来的rule读取ES数据
        esMeta = self.__getEsMetaFromFourDF(fourDF)
        esDF = self.__getEsDF(esMeta)

        # 4. 读取模型/标签相关的5级标签(根据4级标签的id作为pid查询)---==标签id不一样==
        fiveDF = self.__getFiveDF(mysqlDF, fourTagsId)

        # 5. 根据ES数据和5级标签数据进行匹配,得出userId,tagsId---==实现代码不一样==
        newDF = self.compute(esDF, fiveDF)

        # 6. 查询elasticsearch中的oldDF
        oldDF = self.__getOldDF(sparkSession, esMeta)

        # 7. 合并newDF和oldDF
        resultDF = self.__mergeNewAndOldDF(newDF, oldDF)

        # 8. 将最终结果写到ES
        self.__writeResulttoES(resultDF, esMeta)
        print('基类任务执行完成.')