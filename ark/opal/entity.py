# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
实体类定义，实体类定义操作对象属性及操作方法
"""
from ark.are.common import OpObject
from ark.are.exception import ENotImplement


class Entity(OpObject):
    """
    实体类基类
    """

    def __init__(self, type, name, platform):
        """
        初始化方法

        :param str type: 实体类型，如 host,instance,service等
        :param str name: 实体名，如机器名，实例名，服务名等
        :param str platform: 实体所属平台
        """
        super(Entity, self).__init__(type, name, platform)

    def get_entity(self):
        """
        获取实体

        :return: 实体对象
        :rtype: Entity
        """
        pass

    def get_relate_entity(self, relate_type):
        """
        根据目标类型获取关联实体

        :param str relate_type: 目标类型
        :return: 关联实体
        :rtype: Entity
        :raises ENotImplement: 未实现
        """
        raise ENotImplement("function is not implement")
