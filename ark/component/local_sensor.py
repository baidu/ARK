# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
从本地文件中获取事件的感知器
"""
import json
import os

from ark.are.sensor import PullCallbackSensor


class LocalCallbackSensor(PullCallbackSensor):
    """
    从本地文件中获取事件的感知器。该感知器从指定的本地文件中读取事件，模拟感知外部事件，通常
    该感知器仅适用于本地调试和试运行

    .. Note:: 在读取文件内容后，进行修改文件名的操作，以避免对同一事件的重复调用（重复读取文件）
    """
    def __init__(self, event_file, query_interval=3, max_queue=10):
        """
        初始化方法

        :param str event_file: 存放事件的文件名
        :param float query_interval: 查询事件的时间间隔
        :param int max_queue: 最大队列长度
        """
        super(LocalCallbackSensor, self).__init__(query_interval, max_queue)
        self._event_file = event_file
        self._file_handle = None

    def get_event(self):
        """
        从文件中获取事件，文件中的每一行都会做为一个事件读入，读取完成后，文件会被改名

        :return: 事件
        :rtype: dict
        """
        if self._file_handle is None:
            if not os.path.exists(self._event_file):
                return None
            try:
                self._file_handle = open(self._event_file, 'r')
            except IOError:
                return None
            if self._file_handle is None:
                return None

        json_str = self._file_handle.readline()
        if json_str == "":
            self._file_handle.close()
            self._file_handle = None
            os.system("mv {} {}".format(
                self._event_file, self._event_file + ".1"))
            return None
        event = json.loads(json_str)
        return event
