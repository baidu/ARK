# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
使用activeMq外部推送类型的感知器具体实现
"""
from are.sensor import CallbackSensor
from assemble.client.amq_client import ActiveMQClient


class MqPushCallbackSensor(CallbackSensor):
    """
    使用activeMQ实现的感知器，该感知器启动独立的线程订阅topic，并在订阅到事件后回调进行事件处理操作
    """
    def __init__(self, subscribe_condition):
        """
        初始化方法

        :param str subscribe_condition: 订阅条件，等同于activeMQ里的topic
        """
        self._subscribe_condition = subscribe_condition
        self._push_client = ActiveMQClient(self.callback_event)

    def active(self):
        """
        感知器生效，会在Guardian获取领导权时调用

        :return: 无返回
        :rtype: None
        """
        self._push_client.subscribe(self._subscribe_condition)

    def inactive(self):
        """
        感知器时效， 会在Guardian失去领导权时调用

        :return: 无返回
        :rtype: None
        """
        self._push_client.unsubscribe()
