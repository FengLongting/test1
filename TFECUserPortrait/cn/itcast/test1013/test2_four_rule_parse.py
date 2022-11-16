#!/usr/bin/env python
# @desc : 把4级标签的rule字符串解析为字典
__coding__ = "utf-8"
__author__ = "itcast team"

from cn.itcast.test1013.test4_four_rule_esmeta import EsMeta


def ruleStrToDict(fourFuleStr):
    fourFuleList = fourFuleStr.split('##')
    ruleDict = {}
    for kv in fourFuleList:
        print(kv)
        key = kv.split('=')[0]
        value = kv.split('=')[1]
        ruleDict[key] = value
    return ruleDict


if __name__ == '__main__':
    fourFuleStr = 'inType=Elasticsearch##esNodes=up01:9200##esIndex=tfec_tbl_users##esType=_doc##selectFields=id,gender'
    ruleDict = ruleStrToDict(fourFuleStr)
    # esMeta = EsMeta(ruleDict['inType'], ruleDict['esNodes'], ruleDict['esIndex'], ruleDict['esType'], ruleDict['selectFields'])
    # esMeta = EsMeta(**ruleDict)
    esMeta = EsMeta(**ruleDict)
    print(esMeta)

