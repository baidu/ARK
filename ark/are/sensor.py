# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
``BaseSensor`` 感知器模型实现，具体参见 :mod:`framework`
"""
import Queue
import threading
import time
import uuid

from ark.are import log
from ark.are import exception
from ark.are.framework import BaseSensor
from ark.are.framework import OperationMessage


class CallbackSensor(BaseSensor):
    """
    回调型感知器，``CallbackSensor`` 提供的功能如下：

    * 注册到感知事件消息队列
    * 等待报警事件通知
    * 将事件推送到下游决策

    .. Note:: 由于消息泵中消息处理是单线程的，因此感知器处理函数逻辑应尽量简单，
             不能包含耗时的操作（如IO操作等）。对于耗时的IO操作，应在独立的线程中进行。
             因此 ``CallbackGuardian`` 维护一个事件队列，主处理逻辑中仅进行取消息并推
             送到下游，不涉及其他复杂的操作。

    """
    _event_queue = Queue.Queue()

    _concerned_message_list = ["IDLE_MESSAGE"]

    def on_sensor_message(self, message):
        """
        感知事件处理。从事件队列中取消息，并发送给下游

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        """
        event = self.wait_event()
        if event is None:
            return
        log.i("get new event:{}".format(event))
        operation_id = self.get_operation_id(event)
        sensed_message = OperationMessage("SENSED_MESSAGE", operation_id, event)
        self.send(sensed_message)

    def wait_event(self, block=False):
        """
        从事件队列中取出事件

        :param bool block: 是否阻塞取出事件
        :return: 取出的事件
        :rtype: dict
        """
        try:
            return self._event_queue.get(block=block)
        except Queue.Empty:
            return None

    def callback_event(self, event):
        """
        事件回调，将事件放入事件队列

        :param dict event: 外部事件
        :return: 无返回
        :rtype: None
        """
        self._event_queue.put(event)

    def get_operation_id(self, event):
        """
        获取操作id。操作id作为操作的唯一标识，需自行保证操作id的全局唯一性。

        .. Note:: 默认操作id为event中operation_id字段的值，如果不存在该字段则生成uuid
                作为操作id。如需修改操作id的获取方式，需重写改方法。

        :param dict event: 外部事件
        :return: 操作id
        :rtype: str
        """
        ret = event["operation_id"] if "operation_id" in event else uuid.uuid1()
        log.i("event operation_id:{}".format(ret))
        return str(ret)


class PullCallbackSensor(CallbackSensor):
    """
    主动拉取外部事件的感知器。感知器生效时会创新子线程定期进行外部事件拉取操作，并将事件推入事件队列中
    """
    def __init__(self, query_interval=3):
        """
        初始化方法

        :param int query_interval: 查询间隔
        """
        self._query_interval = query_interval
        self._pull_thread = None
        self._stop_tag = False

    def active(self):
        """
        感知器生效函数。该函数创建新线程，定期进行外部事件拉取操作

        :return: 无返回
        :rtype: None
        :raises ThreadError: 创建线程失败
        """
        try:
            self._pull_thread = threading.Thread(
                target=self.event_dealer)
            self._pull_thread.daemon = True
            self._pull_thread.start()
        except threading.ThreadError as e:
            log.r(e, "create new thread err")

    def inactive(self):
        """
        感知器结束工作。该函数会关闭拉取外部事件的线程，并清空事件队列，避免残留事件在感知器再次启动后继续执行。

        :return: 无返回
        :rtype: None
        """
        self._stop_tag = True
        if self._pull_thread:
            self._pull_thread = None
        self._event_queue = Queue.Queue()

    def event_dealer(self):
        """
        事件处理函数，定期拉取外部事件

        :return: 无返回
        :rtype: None
        """
        while not self._stop_tag:
            try:
                event = self.get_event()
                if event is None:
                    time.sleep(self._query_interval)
                    continue
            except:
                log.f("get event failed")
                time.sleep(self._query_interval)
                continue
            log.i("get a new event from external system")
            self.callback_event(event)

    def get_event(self):
        """
        获取外部事件的虚接口

        :return: 外部事件
        :rtype: dict
        :raises ENotImplement: 虚接口，不能直接调用
        """
        raise exception.ENotImplement("function is not implement")


class PushCallbackSensor(CallbackSensor):
    """
    外部事件自动推送的感知器

    .. Note:: 一般消息队列推送为回调的方式，因此回调 ``callback_event`` 将外部事件写入事件队列即可

    """
    pass

