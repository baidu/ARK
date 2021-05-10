# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
使用activeMq进行感知的状态机Guardian
"""
import ark.are.decision as decision
import ark.are.executor as executor
import ark.are.framework as framework
from ark.component.amq_sensor import MqPushCallbackSensor


class AmqPushStateMachineGuardian(framework.GuardianFramework):
    """
    使用activeMq进行感知的状态机Guardian
    """
    def __init__(self, subscribe_condition, nodes, process_count=1):
        """
        初始化方法

        :param str subscribe_condition: 订阅条件
        :param list[Node] nodes: 状态机节点列表
        :param int process_count: 进程数量
        """
        sen = MqPushCallbackSensor(subscribe_condition)
        dec = decision.StateMachineDecisionMaker()
        exe = executor.StateMachineExecutor(nodes, process_count)
        listener_list = [sen, dec, exe]
        for listener in listener_list:
            self.add_listener(listener)
