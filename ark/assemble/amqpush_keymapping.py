# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
使用activeMq进行事件感知的key-mapping映射类型的Guardian
"""
from are import decision
from are import executor
from are import framework
from component.amq_sensor import MqPushCallbackSensor


class AmqPushKeyMappingGuardian(framework.GuardianFramework):
    """
    使用activeMq进行事件感知的key-mapping映射类型的Guardian
    """
    def __init__(self, subscribe_condition,
                 mapping, from_key, func_set, process_count=1):
        """
        初始化方法

        :param str subscribe_condition: 订阅条件
        :param dict mapping: 策略名与执行方法的映射
        :param str from_key: 关注的事件中的字段名
        :param BaseExecFuncSet func_set: 执行方法类
        :param int process_count: 进程数量
        """
        sen = MqPushCallbackSensor(subscribe_condition)
        dec = decision.KeyMappingDecisionMaker(mapping, from_key)
        exe = executor.CallbackExecutor(func_set, process_count)
        listener_list = [sen, dec, exe]
        for listener in listener_list:
            self.add_listener(listener)
