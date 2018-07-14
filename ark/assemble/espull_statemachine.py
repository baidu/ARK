# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
使用elasticsearch进行感知的状态机Guardian
"""
from are import decision
from are import executor
from are import framework
from assemble.sensor.es_sensor import EsCallbackSensor


class EsPullStateMachineGuardian(framework.GuardianFramework):
    """
    使用elasticsearch进行感知的状态机Guardian
    """
    def __init__(self, index, type, condition, nodes,
                 process_count=1, query_interval=3):
        """
        初始化方法

        :param str index: 事件在es中的index名
        :param str type: 事件在es中的type名
        :param dict condition: 查询条件
        :param list[Node] nodes: 状态机节点列表
        :param int process_count: 进程数量
        :param int query_interval: 事件查询时间间隔
        """
        sen = EsCallbackSensor(index, type, condition, query_interval)
        dec = decision.StateMachineDecisionMaker()
        exe = executor.StateMachineExecutor(nodes, process_count)
        listener_list = [sen, dec, exe]
        for listener in listener_list:
            self.add_listener(listener)
