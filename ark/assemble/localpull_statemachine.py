# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
使用轮询本地文件进行感知的状态机Guardian
"""
from are import decision
from are import executor
from are import framework
from component.local_sensor import LocalCallbackSensor


class LocalPullStateMachineGuardian(framework.GuardianFramework):
    """
    使用轮询本地文件进行感知的状态机Guardian
    """
    def __init__(self, event_file, nodes, process_count=1, query_interval=3):
        """
        初始化方法

        :param str event_file: 存放事件的本地文件路径
        :param list[Node] nodes: 状态机节点列表
        :param int process_count: 进程数量
        :param int query_interval: 查询事件的时间间隔
        """
        sen = LocalCallbackSensor(event_file, query_interval)
        dec = decision.StateMachineDecisionMaker()
        exe = executor.StateMachineExecutor(nodes, process_count)
        listener_list = [sen, dec, exe]
        for listener in listener_list:
            self.add_listener(listener)
