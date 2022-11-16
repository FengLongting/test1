__coding__ = "utf-8"
__author__ = "longting Feng"

import logging

from pyspark.sql import DataFrame,functions as F

from cn.itcast.tag.base.AbstractBaseModelOpt import AbstractBaseModelOpt


class XiaofeizhouqiMixin(AbstractBaseModelOpt):
    def getFourTagsId(self):
        return 23

    def compute(self, esDF: DataFrame, fiveDF: DataFrame):
        # 5.1 把fiveDF转换成dict, rule的值作为key(键), id值作为value(值)
        fiveRuleDict = fiveDF.rdd.map(lambda row: (row.rule, row.id)).collectAsMap()  # rule做键, id做值
        # print(fiveRuleDict)
        esDF.select(esDF.memberid,esDF.finishtime).show(truncate=False)
        # 查询esDF的gender, 作为参数, 编写udf函数, 实现传入gender, 作为key, 从字典获得value
        @F.udf
        def genderToTagsId(Xiaofeizhouqi):
            return fiveRuleDict[str(Xiaofeizhouqi)]

        # 5.2 查询esDF的gender, 作为参数, 编写udf函数, 实现传入gender, 作为key, 从字典获得value
        newDF = esDF.select(esDF.id.alias('userId'), genderToTagsId(esDF.Xiaofeizhouqi).alias('tagsId'))
        newDF.show(truncate=False)
        return newDF


if __name__ == '__main__':
    # 创建对象
    gm =XiaofeizhouqiMixin('XiaofeizhouqiModelChildTask')

    # 执行父类整合方法, 完整 性别标签计算即可.
    gm.execute()

    # 4. 打印结果
    logging.warning('XiaofeizhouqiModelChildTask 任务执行成功')