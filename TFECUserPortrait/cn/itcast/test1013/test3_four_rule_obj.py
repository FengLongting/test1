#!/usr/bin/env python
# @desc : 创建es的元数据对象
__coding__ = "utf-8"
__author__ = "itcast team"


class EsMeta:

    def __init__(self, inType, esNodes, esIndex, esType, selectFields):
        self.inType = inType
        self.esNodes = esNodes
        self.esIndex = esIndex
        self.esType = esType
        self.selectFields = selectFields

    def __str__(self):
        return f'EsMeta(inType={self.inType}, esNodes={self.esNodes}, ' \
               f' esIndex={self.esIndex}, esType={self.esType},' \
               f' selectFields={self.selectFields})'


