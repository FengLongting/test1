#!/usr/bin/env python
# @desc : 把4级标签的rule字符串解析为字典
__coding__ = "utf-8"
__author__ = "itcast team"

from cn.itcast.tag.bean.EsMeta import ruleStrToEsMeta

if __name__ == '__main__':
    fourFuleStr = 'inType=Elasticsearch##esNodes=up01:9200##' \
                  'esIndex=tfec_tbl_users##esType=_doc##selectFields=id,gender'
    esMeta = ruleStrToEsMeta(fourFuleStr)
    print(esMeta)
