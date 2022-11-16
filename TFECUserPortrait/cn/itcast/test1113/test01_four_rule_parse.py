# @desc : 解析4级标签规则, 并存储到字典中
__coding__ = "utf-8"
__author__ = "itcast team"

# from cn.itcast.test1113.test02_four_rule_obj import EsMeta   测试的是自己写EsMeta元数据对象.
from cn.itcast.test1113.test03_four_rule_esmeta import EsMeta  # 加入dataclass装饰器后的EsMeta类.


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


if __name__ == '__main__':
    # 1. 定义变量, 记录4级标签规则.
    fourRuleStr = 'inType=Elasticsearch##esNodes=up01:9200##esIndex=tfec_tbl_users##esType=_doc##selectFields=id,gender'
    # 2. 调用方法, 获取字典.
    # 格式为: {'inType': 'Elasticsearch', 'esNodes': 'up01:9200', 'esIndex': 'tfec_tbl_users', 'esType': '_doc', 'selectFields': 'id,gender'}
    ruleDict = ruleStrToDict(fourRuleStr)

    # 3. 根据上述的字典结果, 将其封装成EsMeta对象.
    # 方式1: 手动逐个填写
    # esMeta = EsMeta(ruleDict['inType'], ruleDict['esNodes'],ruleDict['esIndex'],ruleDict['esType'],ruleDict['selectFields'])
    # 方式2: 加入 **, 下述写法作用同上.
    esMeta = EsMeta(**ruleDict)  # 根据字典中的键, 找EsMeta对象的各个属性名, 只要名字一致, 就将字典中该键对应的值, 赋值给属性值.

    # 4. 打印最终结果(字典)
    print(esMeta)
