# @desc : 解决第1个标签索引不存在的问题, 且使用标准标签流程方案.   方案2: try except
__coding__ = "utf-8"
__author__ = "itcast team"

from pyspark.sql import DataFrame, functions as F
import logging
from cn.itcast.tag.base.AbstractBaseModelOpt import AbstractBaseModelOpt


class AgeModelChild(AbstractBaseModelOpt):
    # 1.1 定义抽象方法(因为不同标签, id不一样), 用于获取4级标签id
    def getFourTagsId(self):
        return 14       # 年龄标签, id是14

    # 5. 根据ES数据和5级标签数据进行匹配,得出userId,tagsId
    def compute(self, esDF: DataFrame, fiveDF: DataFrame):
        # 5.1 切分esDF的birthday字段字段的值,  切割数据, 把中间的-号给替换为空
        esDF2 = esDF.select(
            esDF.id.alias('userId'),
            F.regexp_replace(F.substring(esDF.birthday, 0, 10), '-', '').alias('birth')
        )

        # 5.2 切割fiveDF的rule字段的值, 根据中间的"-", 获取到 start 和 end
        fiveDF2 = fiveDF.select(
            fiveDF.id.alias('tagsId'),
            F.split(fiveDF.rule, '-')[0].alias('start'),
            F.split(fiveDF.rule, '-')[1].alias('end')
        )

        # 5.3 把esDF 和 fiveDF使用join方法关联, 不需要指定关联条件, 默认是 inner join, 关联条件不用写, 会根据where()内容自动关联.
        newDF = esDF2 \
            .join(fiveDF2) \
            .where(esDF2.birth.between(fiveDF2.start, fiveDF2.end)) \
            .select(esDF2.userId, fiveDF2.tagsId.cast('string'))

        return newDF

# 为了不和之前的索引表冲突, 我们可以去 AbstractBaseModelOpt 类中,
# 修改最终ES索引表的名字为: tfec_userprofile_result_test2
if __name__ == '__main__':
    # 演示 年龄标签 计算代码.
    # 1. 记录当前标签(任务)的名字.
    taskName = 'AgeModelChild'

    # 2. 创建子类对象
    ageChild = AgeModelChild(taskName)

    # 3. 通过父类的 execute() 该方法整合了标签计算的所有过程, 执行即可.
    ageChild.execute()

    # 4. 打印结果
    logging.warning('%s 任务执行成功', taskName)