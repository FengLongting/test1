# @desc : 创建Es的元数据对象, 加入 dataclass 装饰器
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
