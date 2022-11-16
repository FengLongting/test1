# @desc : 性别标签, 因为性别标签不是我们计算的第1个标签, 所以它添加的时候, 是有 "旧标签oldDF"数据的, 不需要处理, 直接继承即可.
__coding__ = "utf-8"
__author__ = "itcast team"

from pyspark.sql import DataFrame, functions as F

from cn.itcast.tag.base.AbstractBaseModel import AbstractBaseModel

import logging

class GenderModelChild(AbstractBaseModel):
    # 1.1 定义抽象方法(因为不同标签, id不一样), 用于获取4级标签id
    def getFourTagsId(self):
        return 4

    # 5. 根据ES数据和5级标签数据进行匹配,得出userId,tagsId---==实现代码不一样==
    def compute(self, esDF: DataFrame, fiveDF: DataFrame):
        # 5.1 把fiveDF转换成dict, rule的值作为key(键), id值作为value(值)
        fiveRuleDict = fiveDF.rdd.map(lambda row: (row.rule, row.id)).collectAsMap()  # rule做键, id做值

        # 查询esDF的gender, 作为参数, 编写udf函数, 实现传入gender, 作为key, 从字典获得value
        @F.udf
        def genderToTagsId(gender):
            return fiveRuleDict[str(gender)]

        # 5.2 查询esDF的gender, 作为参数, 编写udf函数, 实现传入gender, 作为key, 从字典获得value
        newDF = esDF.select(esDF.id.alias('userId'), genderToTagsId(esDF.gender).alias('tagsId'))

        return newDF

if __name__ == '__main__':
    # 创建对象
    gm = GenderModelChild('GenderModelChildTask')

    # 执行父类整合方法, 完整 性别标签计算即可.
    gm.execute()

    # 4. 打印结果
    logging.warning('GenderModelChildTask 任务执行成功')