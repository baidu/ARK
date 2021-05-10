# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**exception** 异常定义模块
"""


class ENotImplement(Exception):
    """
    方法未实现
    """
    pass


class ETypeMismatch(Exception):
    """
    类型不匹配
    """
    pass


class EMissingParam(Exception):
    """
    缺参数
    """
    pass


class EFailedRequest(Exception):
    """
    请求失败
    """
    pass


class EStatusMismatch(Exception):
    """
    状态不匹配
    """
    pass


class EInvalidOperation(Exception):
    """
    操作不合法
    """
    pass


class EUnknownNode(Exception):
    """
    未知节点
    """
    pass


class EUnInited(Exception):
    """
    未初始化
    """
    pass


class ECheckFailed(Exception):
    """
    检查失败
    """
    pass


class EUnknownEvent(Exception):
    """
    未知事件
    """
    pass


class ENodeExist(Exception):
    """
    节点已存在
    """
    pass


class ControlFailed(Exception):
    """
    控制失败
    """
    pass


class EPNoNodeError(Exception):
    """
    持久化节点不存在
    """
    pass


class EPIOError(Exception):
    """
    持久化IO异常
    """
    pass


class EPConnectTimeout(Exception):
    """
    持久化连接超时
    """
    pass


class EPServerError(Exception):
    """
    持久化Server端异常, 如redis、zookeeper
    """
    pass