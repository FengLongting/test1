# @desc : 测试自定义后的 EsMeta Bean类
__coding__ = "utf-8"
__author__ = "itcast team"

from cn.itcast.tag.bean.EsMeta import ruleStrToEsMeta   # 自定义的EsMeta类, 表示Es的元数据对象 类型

if __name__ == '__main__':
    # 1. 定义变量, 记录4级标签规则.
    fourRuleStr = 'inType=Elasticsearch##esNodes=up01:9200##esIndex=tfec_tbl_users##esType=_doc##selectFields=id,gender'

    # 2. 调用方法, 获取EsMeta对象.
    esMeta = ruleStrToEsMeta(fourRuleStr)   # 根据字典中的键, 找EsMeta对象的各个属性名, 只要名字一致, 就将字典中该键对应的值, 赋值给属性值.

    # 3. 打印最终结果(字典)
    print(esMeta)