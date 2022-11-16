#!/usr/bin/env python
# @desc : 创建es的元数据对象
__coding__ = "utf-8"
__author__ = "itcast team"

from dataclasses import dataclass


@dataclass
class EsMeta:
    inType: str
    esNodes: str
    esIndex: str
    esType: str
    selectFields: str
