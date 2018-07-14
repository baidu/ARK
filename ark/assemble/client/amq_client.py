# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
activemq 操作客户端
"""
from are import config
from are import log
from are.client import BaseClient

import stomp


class ActiveMQClient(BaseClient):
    """
    activeMQ 操作客户端，提供对activeMq的订阅、取消订阅功能。
    """
    ARK_MQ_CONFIG = "ARK_MQ_CONFIG"

    def __init__(self, callback_message):
        """
        初始化方法

        :param func callback_message: 订阅到消息后的回调函数
        """
        mq_config = config.GuardianConfig.get(self.ARK_MQ_CONFIG)
        log.info("activeMQ config:{}".format(mq_config))
        self.conn = self.__get_connection(mq_config)
        self.conn.set_listener('', ActiveMQListener(callback_message))
        self.conn.start()
        self.conn.connect()

    def subscribe(self, subscribe_condition):
        """
        根据订阅条件进行订阅

        :param str subscribe_condition: 订阅条件
        :return: 无返回
        :rtype: None
        """
        self.conn.subscribe(destination=subscribe_condition)
        log.info("start subscribe success, topic:{}".format(
            subscribe_condition))

    def unsubscribe(self):
        """
        取消订阅，关闭连接

        :return: 无返回
        :rtype: None
        """
        self.conn.disconnected()
        log.info("unsubscribe success, connect closed")

    def __get_connection(self, conf):
        """
        获取连接

        :param dict conf: activeMq配置
        :return:
        """
        return stomp.Connention10(conf)


class ActiveMQListener(object):
    """
    activeMQ监听器，完成对订阅到的消息的处理。
    """
    def __init__(self, callback_message):
        """
        初始化方法

        :param func callback_message: 回调方法
        """
        self._callback_message = callback_message

    def on_error(self, header, message):
        """
        响应错误，本处只打印日志，便于追查问题

        :param dict header: 消息头
        :param dict message: 消息体
        :return: 无返回
        :rtype: None
        """
        log.error("receive an error:{}".format(message))

    def on_message(self, header, message):
        """
        响应订阅到的消息，调用回调方法进行操作

        :param dict header: 消息头
        :param dict message: 消息体
        :return: 无返回
        :rtype: None
        """
        log.info("receive a message:{}".format(message))
        self._callback_message(message)
