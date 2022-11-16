# @desc : 创建Es的元数据对象
__coding__ = "utf-8"
__author__ = "itcast team"


# 目前4级标签规则的字典形式:  {'inType': 'Elasticsearch', 'esNodes': 'up01:9200', 'esIndex': 'tfec_tbl_users', 'esType': '_doc', 'selectFields': 'id,gender'}


class EsMeta:

    # 初始化的
    def __init__(self, inType, esNodes, esIndex, esType, selectFields):
        self.inType = inType
        self.esNodes = esNodes
        self.esIndex = esIndex
        self.esType = esType
        self.selectFields = selectFields

    # 用来打印结果, 即: 打印该对象的各个属性值.
    def __str__(self):
        return f'EsMeta(inType={self.inType}, esNodes={self.esNodes},esIndex={self.esIndex},esType={self.esType},selectFields={self.selectFields})'
