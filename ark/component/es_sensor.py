# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
使用从elasticsearch拉取外部事件的方式实现的感知器
"""
from are.sensor import PullCallbackSensor
from are.client import ESClient


class EsCallbackSensor(PullCallbackSensor):
    """
    使用从elasticsearch拉取外部事件的方式实现的感知器。该感知器开启独立的线程从es中轮询消息，
    并在获取消息后放入暂存队列中，进行后续处理
    """
    def __init__(self, index, type, condition, query_interval=3):
        """
        初始化方法

        :param str index: 事件在es中的index名
        :param str type: 事件在es中的type名
        :param dict condition: 查询条件
        :param int query_interval: 查询事件的事件间隔
        """
        super(EsCallbackSensor, self).__init__(query_interval)
        self._pull_client = ESClient(index, type)
        self._condition = condition

    def get_event(self):
        """
        获取事件

        :return: 事件
        :rtype: dict
        """
        event = self._pull_client.get_data_with_condition(self._condition)
        return event
