# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
使用elasticsearch进行事件感知的key-mapping映射类型的Guardian
"""
from ark.are import decision
from ark.are import executor
from ark.are import framework
from ark.component.es_sensor import EsCallbackSensor


class EsPullKeyMappingGuardian(framework.GuardianFramework):
    """
    使用elasticsearch进行事件感知的key-mapping映射类型的Guardian
    """
    def __init__(self, index, type, condition,
                 mapping, from_key, func_set,
                 process_count=1, query_interval=3):
        """
         初始化方法

         :param str index: 事件在es中的index名
         :param str type: 事件在es中的type名
         :param dict condition: 查询条件
         :param dict mapping: 策略名和执行方法的映射
         :param str from_key: 关注的事件中的字段名
         :param BaseExecFuncSet func_set: 执行方法类
         :param int process_count: 进程数量
         :param int query_interval: 查询事件的事件间隔
        """
        sen = EsCallbackSensor(index, type, condition, query_interval)
        dec = decision.KeyMappingDecisionMaker(mapping, from_key)
        exe = executor.CallbackExecutor(func_set, process_count)
        listener_list = [sen, dec, exe]
        for listener in listener_list:
            self.add_listener(listener)
