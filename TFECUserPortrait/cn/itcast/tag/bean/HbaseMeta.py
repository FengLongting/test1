# @desc : HbaseMeta对象, 基于EsMeta类复制过来的, 用来给大家演示 代码的扩展性的.
__coding__ = "utf-8"
__author__ = "itcast team"

from dataclasses import dataclass

# 定义函数, 根据4级标签规则字符串, 返回其对应的 字典形式.
def ruleStrToDict(fourRuleStr):
    # 1. 按照##进行切割, 获取切割后的结果 list
    fourRuleList = fourRuleStr.split('##')
    # 2. 定义字典, 用于存储 解析后的数据
    ruleDict = {}
    # 3. 遍历list列表, 获取到每个元素(例如: 'inType=Elasticsearch')
    for kv in fourRuleList:
        # 4. 对上述获取到的内容, 进行切割, 第1列(索引为0)充当键, 第2列(索引为1)充当值.
        key = kv.split('=')[0]
        value = kv.split('=')[1]
        # 5. 把上述获取到的键和值添加到子弹中.
        ruleDict[key] = value
    # 6. 返回结果
    return ruleDict

# 定义函数, 根据传入的4级标签规则字符串, 对其解析, 然后封装HbaseMeta对象, 并返回.
def ruleStrToEsMeta(fourRuleStr):
    return HbaseMeta(**ruleStrToDict(fourRuleStr))


@dataclass
class HbaseMeta:
    inType: str
    esNodes: str
    esIndex: str
    esType: str
    selectFields: str


