# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
使用本地文件进行事件感知的key-mapping映射类型的Guardian
"""
import ark.are.decision as decision
import ark.are.executor as executor
import ark.are.framework as framework
from ark.component.local_sensor import LocalCallbackSensor


class LocalPullKeyMappingGuardian(framework.GuardianFramework):
    """
    使用本地文件进行事件感知的key-mapping映射类型的Guardian
    """
    def __init__(self, event_file,
                 mapping, from_key, func_set,
                 process_count=1, query_interval=3, max_queue=10):
        """
        初始化方法

        :param str event_file: 存放事件的文件路径
        :param dict mapping: 策略名和执行方法的映射
        :param str from_key: 关注的事件中的字段名
        :param BaseExecFuncSet func_set: 执行方法类
        :param int process_count: 进程数
        :param float query_interval: 查询事件的时间间隔
        :param int max_queue: 最大队列长度
        """
        sen = LocalCallbackSensor(event_file, query_interval, max_queue)
        dec = decision.KeyMappingDecisionMaker(mapping, from_key)
        exe = executor.CallbackExecutor(func_set, process_count)
        listener_list = [exe, sen, dec, ]
        for listener in listener_list:
            self.add_listener(listener)
